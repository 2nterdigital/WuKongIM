package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/cmd/wkcli/internal/command"
	"github.com/WuKongIM/WuKongIM/internal/bench/reference"
)

func stubReferenceRun(rate uint64, status string) *reference.RunResult {
	return &reference.RunResult{
		Schema:        reference.RunSchema,
		RunID:         "run-1",
		RatePerSecond: rate,
		Workload:      reference.WorkloadJSON{Family: "person_send", Connections: 273, Senders: 256, Recipients: 17, PayloadBytes: 256, Endpoints: 3, OperationsInFlightPerConnection: 1, MeasuredPhase: "measure"},
		Lifecycle:     reference.LifecycleJSON{DeclaredStatus: status},
		Native:        map[string]any{},
		Process:       map[string]reference.ProcessResource{},
	}
}

func exitCode(t *testing.T, err error) int {
	t.Helper()
	var exit command.Exit
	if !errors.As(err, &exit) {
		t.Fatalf("error %v is not a command.Exit", err)
	}
	return exit.Code
}

func TestBenchReferenceRunParsesFlagsWritesTheRunResultAndKeepsPathsOffStdout(t *testing.T) {
	orig := executeReference
	t.Cleanup(func() { executeReference = orig })
	var captured referenceConfig
	executeReference = func(_ context.Context, cfg referenceConfig) (*reference.RunResult, error) {
		captured = cfg
		return stubReferenceRun(cfg.RatePerSecond, "passed"), nil
	}
	output := filepath.Join(t.TempDir(), "reference-run.json")
	var stdout, stderr bytes.Buffer
	cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{
		"reference", "run",
		"--server", "http://127.0.0.1:5001,http://127.0.0.1:5002",
		"--server", "http://127.0.0.1:5003",
		"--gateway", "127.0.0.1:5100", "--gateway", "127.0.0.1:5101", "--gateway", "127.0.0.1:5102",
		"--rate", "150", "--seed", "seed-1", "--run-id", "run-1",
		"--instrumentation", "on", "--ack-timeout", "20s", "--output", output,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v stderr %q", err, stderr.String())
	}
	if captured.RatePerSecond != 150 || captured.IdentitySeed != "seed-1" || captured.RunID != "run-1" || captured.Instrumentation != "on" || captured.AckTimeout != 20*time.Second {
		t.Fatalf("captured = %+v", captured)
	}
	if len(splitValues(captured.ServerAddrs)) != 3 || len(splitValues(captured.GatewayAddrs)) != 3 || captured.UIDPrefix != "ref" || captured.DrainBound != 60*time.Second || captured.HistoryPageLimit != 1000 {
		t.Fatalf("defaults and repeated flags = %+v", captured)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("run result not written: %v", err)
	}
	var written reference.RunResult
	if err := json.Unmarshal(data, &written); err != nil || written.Schema != reference.RunSchema || written.RatePerSecond != 150 {
		t.Fatalf("written run result = %v %+v", err, written)
	}
	info, _ := os.Stat(output)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("run result mode = %o, want 0600", info.Mode().Perm())
	}
	if !strings.HasPrefix(stdout.String(), "reference run: rate=150/s") || !strings.Contains(stdout.String(), "status=passed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if strings.Contains(stdout.String(), output) || strings.Contains(stdout.String(), "127.0.0.1") {
		t.Fatalf("stdout carries a path or address: %q", stdout.String())
	}
}

func TestBenchReferenceRunFailsClosedOnMissingOrInvalidInputs(t *testing.T) {
	orig := executeReference
	t.Cleanup(func() { executeReference = orig })
	called := false
	executeReference = func(context.Context, referenceConfig) (*reference.RunResult, error) {
		called = true
		return nil, nil
	}
	base := []string{"--server", "http://127.0.0.1:5001", "--gateway", "127.0.0.1:5100", "--rate", "150", "--seed", "s", "--run-id", "r", "--output", filepath.Join(t.TempDir(), "run.json")}
	drop := func(flag string) []string {
		out := []string{"reference", "run"}
		for i := 0; i < len(base); i += 2 {
			if base[i] != flag {
				out = append(out, base[i], base[i+1])
			}
		}
		return out
	}
	cases := map[string][]string{
		"no server":              drop("--server"),
		"no gateway":             drop("--gateway"),
		"no rate":                drop("--rate"),
		"no seed":                drop("--seed"),
		"no run id":              drop("--run-id"),
		"no output":              drop("--output"),
		"bad instrumentation":    append(append([]string{"reference", "run"}, base...), "--instrumentation", "maybe"),
		"history page too large": append(append([]string{"reference", "run"}, base...), "--history-page-limit", "20000"),
	}
	for name, args := range cases {
		var stdout, stderr bytes.Buffer
		cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
		cmd.SetArgs(args)
		err := cmd.Execute()
		if err == nil || exitCode(t, err) != command.ExitConfig {
			t.Fatalf("%s: err = %v, want a configuration exit", name, err)
		}
	}
	if called {
		t.Fatal("the executor must not run on an invalid configuration")
	}
}

func TestBenchReferenceRunReportsAnInterruptedRunWithExitThree(t *testing.T) {
	orig := executeReference
	t.Cleanup(func() { executeReference = orig })
	executeReference = func(_ context.Context, cfg referenceConfig) (*reference.RunResult, error) {
		return stubReferenceRun(cfg.RatePerSecond, "interrupted"), nil
	}
	output := filepath.Join(t.TempDir(), "run.json")
	var stdout, stderr bytes.Buffer
	cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "run", "--server", "http://127.0.0.1:5001", "--gateway", "127.0.0.1:5100", "--rate", "100", "--seed", "s", "--run-id", "r", "--output", output})
	err := cmd.Execute()
	if err == nil || exitCode(t, err) != referenceExitIncomplete {
		t.Fatalf("err = %v, want exit %d", err, referenceExitIncomplete)
	}
	if _, statErr := os.Stat(output); statErr != nil {
		t.Fatal("the run result is still written for an interrupted run")
	}
}

func TestBenchReferenceEmitAssemblesTheDocumentAndRefusesDrift(t *testing.T) {
	dir := t.TempDir()
	runPath := filepath.Join(dir, "run.json")
	encoded, err := json.Marshal(stubReferenceRun(150, "passed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(runPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	provenance := filepath.Join("testdata", "reference", "provenance.json")
	output := filepath.Join(dir, "document.json")
	var stdout, stderr bytes.Buffer
	cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "emit", "--run", runPath, "--provenance", provenance, "--output", output})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v stderr %q", err, stderr.String())
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document["schema"] != reference.DocumentSchema || document["document_id"] != "wukongim-common-150-seq06" || document["product"] != "wukongim" {
		t.Fatalf("document identity = %v %v %v", document["schema"], document["document_id"], document["product"])
	}
	if !strings.HasPrefix(stdout.String(), "reference emit: document_id=wukongim-common-150-seq06 bytes=") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	driftedRun := filepath.Join(dir, "drifted.json")
	if err := os.WriteFile(driftedRun, []byte(strings.Replace(string(encoded), `"run_id"`, `"run_identifier"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"unknown run field": {"reference", "emit", "--run", driftedRun, "--provenance", provenance, "--output", output},
		"missing flags":     {"reference", "emit", "--run", runPath},
		"missing file":      {"reference", "emit", "--run", filepath.Join(dir, "absent.json"), "--provenance", provenance, "--output", output},
	} {
		cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
		cmd.SetArgs(args)
		err := cmd.Execute()
		if err == nil || exitCode(t, err) != command.ExitConfig {
			t.Fatalf("%s: err = %v, want a configuration exit", name, err)
		}
	}
	wrongRate := filepath.Join(dir, "wrong-rate.json")
	if err := os.WriteFile(wrongRate, []byte(strings.Replace(string(encoded), `"rate_per_second":150`, `"rate_per_second":175`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd = NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "emit", "--run", wrongRate, "--provenance", provenance, "--output", output})
	err = cmd.Execute()
	if err == nil || exitCode(t, err) != command.ExitConfig || !strings.Contains(err.Error(), "rate") {
		t.Fatalf("a run whose rate differs from the provenance is refused: %v", err)
	}
}

func TestBenchReferenceRunWritesTheAcknowledgedLedgerWhenAsked(t *testing.T) {
	orig := executeReference
	t.Cleanup(func() { executeReference = orig })
	executeReference = func(_ context.Context, cfg referenceConfig) (*reference.RunResult, error) {
		run := stubReferenceRun(cfg.RatePerSecond, "passed")
		run.Schedule = &reference.ScheduleResult{Records: []reference.Record{
			{Index: 0, Phase: "warmup", ClientMsgNo: "ref-a-run-1-00000000", SenderUID: "s0", RecipientUID: "r0", ChannelID: "s0@r0", PayloadDigest: "d", Class: reference.ClassAcknowledged, MessageID: 1, MessageSeq: 1},
			{Index: 1, Phase: "warmup", ClientMsgNo: "ref-a-run-1-00000001", SenderUID: "s1", RecipientUID: "r1", ChannelID: "s1@r1", PayloadDigest: "d", Class: reference.ClassRejected},
		}}
		return run, nil
	}
	dir := t.TempDir()
	ledger := filepath.Join(dir, "acknowledged-ledger.jsonl")
	var stdout, stderr bytes.Buffer
	cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "run", "--server", "http://127.0.0.1:5001", "--gateway", "127.0.0.1:5100", "--rate", "100", "--seed", "s", "--run-id", "r", "--output", filepath.Join(dir, "run.json"), "--ledger", ledger})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v stderr %q", err, stderr.String())
	}
	expected, _, err := reference.ReadAcknowledgedLedger(ledger)
	if err != nil || len(expected) != 1 || expected[0].ClientMsgNo != "ref-a-run-1-00000000" {
		t.Fatalf("ledger = %v %+v", err, expected)
	}
	if !strings.Contains(stdout.String(), "ledger_records=1 ledger_sha256=") || strings.Contains(stdout.String(), ledger) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestBenchReferenceReauditWritesTheResultAndReportsIncorrectHistoryWithExitThree(t *testing.T) {
	orig := executeReaudit
	t.Cleanup(func() { executeReaudit = orig })
	var captured reauditConfig
	executeReaudit = func(_ context.Context, cfg reauditConfig) (reference.ReauditResult, error) {
		captured = cfg
		return reference.ReauditResult{Schema: reference.ReauditSchema, Expected: 10, History: reference.HistoryResult{Audited: true, Expected: 10, Matched: 9, Missing: 1, Complete: true}}, nil
	}
	dir := t.TempDir()
	output := filepath.Join(dir, "reaudit.json")
	var stdout, stderr bytes.Buffer
	cmd := NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "reaudit", "--server", "http://127.0.0.1:5001,http://127.0.0.1:5002", "--ledger", filepath.Join(dir, "ledger.jsonl"), "--output", output})
	err := cmd.Execute()
	if err == nil || exitCode(t, err) != referenceExitIncomplete {
		t.Fatalf("an incorrect re-audit exits %d: %v", referenceExitIncomplete, err)
	}
	if len(splitValues(captured.ServerAddrs)) != 2 || captured.HistoryPageLimit != 1000 {
		t.Fatalf("captured = %+v", captured)
	}
	data, err := os.ReadFile(output)
	if err != nil || !strings.Contains(string(data), reference.ReauditSchema) {
		t.Fatalf("result written: %v %s", err, data)
	}
	if !strings.HasPrefix(stdout.String(), "reference reaudit: expected=10 matched=9 missing=1") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	cmd = NewCommand(command.Deps{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"reference", "reaudit", "--ledger", "x"})
	if err := cmd.Execute(); err == nil || exitCode(t, err) != command.ExitConfig {
		t.Fatalf("missing inputs are a configuration exit: %v", err)
	}
}

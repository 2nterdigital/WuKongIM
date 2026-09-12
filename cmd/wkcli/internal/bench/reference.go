package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/cmd/wkcli/internal/command"
	"github.com/WuKongIM/WuKongIM/internal/bench/reference"
	benchtarget "github.com/WuKongIM/WuKongIM/internal/bench/target"
	"github.com/spf13/cobra"
)

const (
	referenceMaxInputBytes    = 16 << 20
	referenceMaxDocumentBytes = 512 << 10
	referenceExitIncomplete   = 3
	referenceStatusPassed     = "passed"
	referenceInstrumentOn     = "on"
	referenceInstrumentOff    = "off"
)

var executeReference = executeReferenceConfig

// referenceConfig is one fixed reference point: the workload shape is not
// configurable, only the rate, identities, endpoints and observation switch.
type referenceConfig struct {
	ServerAddrs      []string
	GatewayAddrs     []string
	BenchToken       string
	RatePerSecond    uint64
	IdentitySeed     string
	RunID            string
	UIDPrefix        string
	AckTimeout       time.Duration
	DrainBound       time.Duration
	RecvSettle       time.Duration
	Instrumentation  string
	HistoryPageLimit int
	Output           string
	Ledger           string
	Log              func(string)
}

// reauditConfig re-reads one run's acknowledged ledger from history after a
// cold restart; it sends nothing.
type reauditConfig struct {
	ServerAddrs      []string
	BenchToken       string
	Ledger           string
	Output           string
	HistoryPageLimit int
	Log              func(string)
}

var executeReaudit = executeReauditConfig

func newReferenceCommand(deps command.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reference",
		Short: "Run and emit the fixed ech0/WuKongIM reference comparison workload",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			return command.Exit{Code: command.ExitConfig}
		},
	}
	cmd.AddCommand(newReferenceRunCommand(deps), newReferenceReauditCommand(deps), newReferenceEmitCommand(deps))
	return cmd
}

func newReferenceRunCommand(deps command.Deps) *cobra.Command {
	cfg := referenceConfig{
		UIDPrefix:        "ref",
		AckTimeout:       35 * time.Second,
		DrainBound:       60 * time.Second,
		RecvSettle:       5 * time.Second,
		Instrumentation:  referenceInstrumentOff,
		HistoryPageLimit: 1000,
	}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run one reference point (273 connections, 17 Person pairs, 256-byte payloads, fixed phases) and write its run result",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateReferenceConfig(cfg); err != nil {
				return command.Exit{Code: command.ExitConfig, Message: err.Error()}
			}
			cfg.Log = func(line string) { _, _ = fmt.Fprintln(deps.Stderr, "reference: "+line) }
			result, err := executeReference(cmd.Context(), cfg)
			if err != nil {
				return command.Exit{Code: command.ExitUnavailable, Message: err.Error()}
			}
			encoded, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			if err := os.WriteFile(cfg.Output, append(encoded, '\n'), 0o600); err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			ledgerNote := ""
			if cfg.Ledger != "" {
				if result.Schedule == nil {
					return command.Exit{Code: command.ExitInternal, Message: "the run kept no schedule records; the acknowledged ledger cannot be written"}
				}
				count, sum, err := reference.WriteAcknowledgedLedger(cfg.Ledger, result.Schedule.Records)
				if err != nil {
					return command.Exit{Code: command.ExitInternal, Message: "acknowledged ledger: " + err.Error()}
				}
				ledgerNote = fmt.Sprintf(" ledger_records=%d ledger_sha256=%s", count, sum)
			}
			total := result.Accounting.Total
			_, _ = fmt.Fprintf(deps.Stdout, "reference run: rate=%d/s scheduled=%d dispatched=%d acknowledged=%d rejected=%d timed_out=%d indeterminate=%d unissued=%d history_correct=%t online_complete=%t status=%s%s\n",
				result.RatePerSecond, total.Scheduled, total.Dispatched, total.Acknowledged, total.Rejected, total.TimedOut, total.Indeterminate, total.Unissued,
				result.History.Correct(), result.Online.Complete, result.Lifecycle.DeclaredStatus, ledgerNote)
			if result.Lifecycle.DeclaredStatus != referenceStatusPassed {
				return command.Exit{Code: referenceExitIncomplete}
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&cfg.ServerAddrs, "server", nil, "Node HTTP API base address in node order; repeat or use comma-separated values")
	cmd.Flags().StringArrayVar(&cfg.GatewayAddrs, "gateway", nil, "WKProto TCP gateway address; repeat or use comma-separated values")
	cmd.Flags().StringVar(&cfg.BenchToken, "bench-token", "", "Optional bearer token for bench/v1 preparation APIs")
	cmd.Flags().Uint64Var(&cfg.RatePerSecond, "rate", 0, "Measured-phase arrival rate in SENDs per second")
	cmd.Flags().StringVar(&cfg.IdentitySeed, "seed", "", "Identity seed shared by every point of the campaign")
	cmd.Flags().StringVar(&cfg.RunID, "run-id", "", "Run identity; fresh per launch and part of every ClientMsgNo")
	cmd.Flags().StringVar(&cfg.UIDPrefix, "uid-prefix", cfg.UIDPrefix, "Prefix for generated UIDs")
	cmd.Flags().DurationVar(&cfg.AckTimeout, "ack-timeout", cfg.AckTimeout, "SENDACK wait bound per SEND")
	cmd.Flags().DurationVar(&cfg.DrainBound, "drain-bound", cfg.DrainBound, "Bound for in-flight SENDs after the last arrival")
	cmd.Flags().DurationVar(&cfg.RecvSettle, "recv-settle", cfg.RecvSettle, "Wall-clock settle for online delivery after the drain")
	cmd.Flags().StringVar(&cfg.Instrumentation, "instrumentation", cfg.Instrumentation, "Scrape node metrics at phase boundaries: on or off")
	cmd.Flags().IntVar(&cfg.HistoryPageLimit, "history-page-limit", cfg.HistoryPageLimit, "Rows per authenticated history page (1..10000)")
	cmd.Flags().StringVar(&cfg.Output, "output", "", "Run result file to write (JSON)")
	cmd.Flags().StringVar(&cfg.Ledger, "ledger", "", "Optional private acknowledged-identity ledger to write for a later cold-restart re-audit (mode 0600, never part of a document)")
	return cmd
}

func newReferenceReauditCommand(deps command.Deps) *cobra.Command {
	cfg := reauditConfig{HistoryPageLimit: 1000}
	cmd := &cobra.Command{
		Use:   "reaudit",
		Short: "Re-read every acknowledged identity of a run from history after a cold restart; sends nothing",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var problems []string
			if len(splitValues(cfg.ServerAddrs)) == 0 {
				problems = append(problems, "--server is required")
			}
			if strings.TrimSpace(cfg.Ledger) == "" || strings.TrimSpace(cfg.Output) == "" {
				problems = append(problems, "--ledger and --output are required")
			}
			if cfg.HistoryPageLimit <= 0 || cfg.HistoryPageLimit > 10000 {
				problems = append(problems, "--history-page-limit must be within 1..10000")
			}
			if len(problems) > 0 {
				return command.Exit{Code: command.ExitConfig, Message: strings.Join(problems, "; ")}
			}
			cfg.Log = func(line string) { _, _ = fmt.Fprintln(deps.Stderr, "reaudit: "+line) }
			result, err := executeReaudit(cmd.Context(), cfg)
			if err != nil {
				return command.Exit{Code: command.ExitUnavailable, Message: err.Error()}
			}
			encoded, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			if err := os.WriteFile(cfg.Output, append(encoded, '\n'), 0o600); err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			_, _ = fmt.Fprintf(deps.Stdout, "reference reaudit: expected=%d matched=%d missing=%d duplicate=%d conflicting=%d wrong_channel=%d wrong_content=%d misordered=%d unexpected=%d complete=%t correct=%t\n",
				result.Expected, result.History.Matched, result.History.Missing, result.History.Duplicate, result.History.Conflicting, result.History.WrongChannel,
				result.History.WrongContent, result.History.Misordered, result.History.UnexpectedRows, result.History.Complete, result.History.Correct())
			if !result.History.Correct() {
				return command.Exit{Code: referenceExitIncomplete}
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&cfg.ServerAddrs, "server", nil, "Node HTTP API base address; repeat or use comma-separated values")
	cmd.Flags().StringVar(&cfg.BenchToken, "bench-token", "", "Optional bearer token for bench/v1 preparation APIs")
	cmd.Flags().StringVar(&cfg.Ledger, "ledger", "", "Acknowledged-identity ledger written by `bench reference run --ledger`")
	cmd.Flags().StringVar(&cfg.Output, "output", "", "Re-audit result file to write (JSON)")
	cmd.Flags().IntVar(&cfg.HistoryPageLimit, "history-page-limit", cfg.HistoryPageLimit, "Rows per authenticated history page (1..10000)")
	return cmd
}

func executeReauditConfig(ctx context.Context, cfg reauditConfig) (reference.ReauditResult, error) {
	servers := dedupeValues(splitValues(cfg.ServerAddrs))
	for _, addr := range servers {
		probe := benchtarget.NewClient(benchtarget.Config{APIAddrs: []string{addr}, Token: cfg.BenchToken})
		if err := probe.Healthz(ctx); err != nil {
			return reference.ReauditResult{}, fmt.Errorf("target %s /healthz failed: %w", addr, err)
		}
		if err := probe.Readyz(ctx); err != nil {
			return reference.ReauditResult{}, fmt.Errorf("target %s /readyz failed: %w", addr, err)
		}
	}
	expected, sum, err := reference.ReadAcknowledgedLedger(cfg.Ledger)
	if err != nil {
		return reference.ReauditResult{}, err
	}
	history := reference.NewHistoryClient(reference.HistoryClientConfig{APIAddrs: servers, PageLimit: cfg.HistoryPageLimit})
	return reference.Reaudit(ctx, history, expected, sum, cfg.Log)
}

func validateReferenceConfig(cfg referenceConfig) error {
	var problems []string
	if len(splitValues(cfg.ServerAddrs)) == 0 {
		problems = append(problems, "--server is required")
	}
	if len(splitValues(cfg.GatewayAddrs)) == 0 {
		problems = append(problems, "--gateway is required")
	}
	if cfg.RatePerSecond == 0 {
		problems = append(problems, "--rate must be positive")
	}
	if strings.TrimSpace(cfg.IdentitySeed) == "" {
		problems = append(problems, "--seed is required")
	}
	if strings.TrimSpace(cfg.RunID) == "" {
		problems = append(problems, "--run-id is required")
	}
	if strings.TrimSpace(cfg.Output) == "" {
		problems = append(problems, "--output is required")
	}
	if cfg.Instrumentation != referenceInstrumentOn && cfg.Instrumentation != referenceInstrumentOff {
		problems = append(problems, "--instrumentation must be on or off")
	}
	if cfg.HistoryPageLimit <= 0 || cfg.HistoryPageLimit > 10000 {
		problems = append(problems, "--history-page-limit must be within 1..10000")
	}
	if cfg.AckTimeout <= 0 || cfg.DrainBound <= 0 || cfg.RecvSettle < 0 {
		problems = append(problems, "--ack-timeout and --drain-bound must be positive and --recv-settle non-negative")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func executeReferenceConfig(ctx context.Context, cfg referenceConfig) (*reference.RunResult, error) {
	plan, err := reference.NewPlan(reference.PlanConfig{
		RatePerSecond: cfg.RatePerSecond,
		IdentitySeed:  cfg.IdentitySeed,
		RunID:         cfg.RunID,
		UIDPrefix:     cfg.UIDPrefix,
		AckTimeout:    cfg.AckTimeout,
		DrainBound:    cfg.DrainBound,
	})
	if err != nil {
		return nil, err
	}
	servers := dedupeValues(splitValues(cfg.ServerAddrs))
	gateways := dedupeValues(splitValues(cfg.GatewayAddrs))
	for _, addr := range servers {
		probe := benchtarget.NewClient(benchtarget.Config{APIAddrs: []string{addr}, Token: cfg.BenchToken})
		if err := probe.Healthz(ctx); err != nil {
			return nil, fmt.Errorf("target %s /healthz failed: %w", addr, err)
		}
		if err := probe.Readyz(ctx); err != nil {
			return nil, fmt.Errorf("target %s /readyz failed: %w", addr, err)
		}
	}
	api := benchtarget.NewClient(benchtarget.Config{APIAddrs: servers, Token: cfg.BenchToken})
	if err := reference.RegisterTokens(ctx, api, plan); err != nil {
		return nil, fmt.Errorf("register identities: %w", err)
	}
	sessions, err := reference.OpenSessions(ctx, plan, reference.SessionConfig{GatewayAddrs: gateways, AckTimeout: cfg.AckTimeout})
	if err != nil {
		return nil, err
	}
	var scraper reference.Scraper
	if cfg.Instrumentation == referenceInstrumentOn {
		nodes := make([]reference.NodeAddress, len(servers))
		for i, addr := range servers {
			nodes[i] = reference.NodeAddress{Name: fmt.Sprintf("node-%d", i+1), APIAddr: addr}
		}
		scraper = reference.NodeScraper{Nodes: nodes}
	}
	history := reference.NewHistoryClient(reference.HistoryClientConfig{APIAddrs: servers, PageLimit: cfg.HistoryPageLimit})
	return reference.Run(ctx, reference.RunConfig{
		Plan:       plan,
		Sessions:   sessions,
		Scraper:    scraper,
		History:    history,
		RecvSettle: cfg.RecvSettle,
		Log:        cfg.Log,
	})
}

func newReferenceEmitCommand(deps command.Deps) *cobra.Command {
	var runPath, provenancePath, output string
	cmd := &cobra.Command{
		Use:   "emit",
		Short: "Assemble the comparison document from a run result and the launcher's provenance",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(runPath) == "" || strings.TrimSpace(provenancePath) == "" || strings.TrimSpace(output) == "" {
				return command.Exit{Code: command.ExitConfig, Message: "--run, --provenance and --output are required"}
			}
			var run reference.RunResult
			if err := readStrictJSON(runPath, &run); err != nil {
				return command.Exit{Code: command.ExitConfig, Message: "run result: " + err.Error()}
			}
			if run.Schema != reference.RunSchema {
				return command.Exit{Code: command.ExitConfig, Message: fmt.Sprintf("run result schema %q is not %q", run.Schema, reference.RunSchema)}
			}
			var provenance reference.Provenance
			if err := readStrictJSON(provenancePath, &provenance); err != nil {
				return command.Exit{Code: command.ExitConfig, Message: "provenance: " + err.Error()}
			}
			document, err := reference.Emit(&run, provenance)
			if err != nil {
				return command.Exit{Code: command.ExitConfig, Message: err.Error()}
			}
			encoded, err := json.MarshalIndent(document, "", "  ")
			if err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			if len(encoded) > referenceMaxDocumentBytes {
				return command.Exit{Code: command.ExitConfig, Message: fmt.Sprintf("document is %d bytes; the bound is %d", len(encoded), referenceMaxDocumentBytes)}
			}
			if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
				return command.Exit{Code: command.ExitInternal, Message: err.Error()}
			}
			_, _ = fmt.Fprintf(deps.Stdout, "reference emit: document_id=%v bytes=%d\n", document["document_id"], len(encoded))
			return nil
		},
	}
	cmd.Flags().StringVar(&runPath, "run", "", "Run result written by `bench reference run`")
	cmd.Flags().StringVar(&provenancePath, "provenance", "", "Launcher provenance file (wukongim-reference-provenance/1)")
	cmd.Flags().StringVar(&output, "output", "", "Comparison document to write (ech0-wukongim-reference-comparison/1)")
	return cmd
}

// readStrictJSON decodes one bounded file and refuses unknown fields, so a
// drifted run result or provenance shape fails instead of emitting silently.
func readStrictJSON(path string, into any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, referenceMaxInputBytes+1))
	if err != nil {
		return err
	}
	if len(data) > referenceMaxInputBytes {
		return fmt.Errorf("file exceeds the %d-byte bound", referenceMaxInputBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing content after the JSON document")
	}
	return nil
}

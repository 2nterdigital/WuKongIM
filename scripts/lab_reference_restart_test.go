package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLabReferenceRestartReauditsARetainedRootOnceAndKeepsThePreRestartDocument(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "scripts", "lab-reference", "restart.sh")
	script := readFile(t, path)
	if output, err := exec.Command("bash", "-n", path).CombinedOutput(); err != nil {
		t.Fatalf("bash syntax failed: %v\n%s", err, output)
	}
	if !strings.HasPrefix(script, "#!/usr/bin/env bash\nset -Eeuo pipefail\n") || !strings.Contains(script, `source "$LAB_SCRIPT_DIR/point.sh"`) {
		t.Fatal("the restart runner fails closed and shares the point runner's functions")
	}
	ordered := []string{
		`validate_retained_root "$task_root" "$run_root"`,
		`refuse_prior_state "$docker_context" "$wkcli"`,
		`verify_frozen_identities "$source_dir" "$facts/source.json" "$facts/build.json"`,
		`cmp -s "$revidence/compose-config.yaml" "$run_root/evidence/compose-config.yaml"`,
		`stopped_at_utc="$(cat "$run_root/evidence/stopped-at-utc")"`,
		`started_ms="$(epoch_ms)"`,
		`up -d --no-build --no-recreate`,
		`wait_nodes_ready "$LAB_READY_TIMEOUT_SECONDS"`,
		`ready_ms="$(epoch_ms)"`,
		`bench reference reaudit`,
		`log_event "restart cleanup $cleanup_status"`,
		`"included_in_throughput_latency": False`,
		`--restart "$restart_dir/restart.json"`,
		`cp -- "$run_root/comparison.json" "$run_root/comparison.pre-restart.json"`,
		`bench reference emit`,
		`cp -- "$restart_dir/comparison.json" "$run_root/comparison.json"`,
	}
	last := -1
	for _, marker := range ordered {
		index := strings.Index(script, marker)
		if index < 0 {
			t.Fatalf("restart runner lacks %q", marker)
		}
		if index <= last {
			t.Fatalf("restart runner step %q is out of order", marker)
		}
		last = index
	}
	for _, forbidden := range []string{"rm -rf", "rm -r ", "down -v", "volume rm", "system prune", "--bench-token", "git reset", "git checkout", "--rate", "bench reference run"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("restart runner must not contain %q", forbidden)
		}
	}
	for _, required := range []string{
		`[[ ! -e "$run_root/comparison.pre-restart.json" ]] || die`,
		`taskset -c "$LAB_CLIENT_CPUS"`,
		`timeout "$deadline_seconds"`,
		`"reaudit": reaudit["history"]`,
		`the re-audit exited $reaudit_exit`,
		`the restart cleanup left residual state`,
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("restart runner lacks %q", required)
		}
	}
	point := readFile(t, filepath.Join(root, "scripts", "lab-reference", "point.sh"))
	if !strings.Contains(point, `[[ ! -e "$run_root/restart" ]] || die "the retained root was already restarted once`) {
		t.Fatal("a retained root is restarted at most once")
	}
}

func TestLabReferenceRestartRefusesAnIncompleteRetainedRoot(t *testing.T) {
	root := repoRoot(t)
	point := filepath.Join(root, "scripts", "lab-reference", "point.sh")
	taskRoot := t.TempDir()
	retained := filepath.Join(taskRoot, "runs", "wukongim-common-150-seq06")
	for _, name := range []string{"client", "evidence", "facts"} {
		if err := os.MkdirAll(filepath.Join(retained, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	check := func() ([]byte, error) {
		return exec.Command("bash", "-c", `source "$1"; validate_retained_root "$2" "$3"`, "_", point, taskRoot, retained).CombinedOutput()
	}
	if out, err := check(); err == nil || !strings.Contains(string(out), "lacks comparison.json") {
		t.Fatalf("an empty retained root is refused by its first missing file: %v %s", err, out)
	}
	for _, name := range []string{"comparison.json", "provenance.json", "client/acknowledged-ledger.jsonl", "evidence/stopped-at-utc", "evidence/compose-config.yaml", "facts/campaign.json", "facts/source.json", "facts/build.json", "facts/host.json", "facts/deployment.json", "facts/docker-info.json", "facts/docker-inspect.json", "facts/isolation.json", "facts/resources.json", "facts/cleanup.json", "facts/networks.txt"} {
		if err := os.WriteFile(filepath.Join(retained, name), []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if out, err := check(); err != nil {
		t.Fatalf("a complete retained root passes: %v %s", err, out)
	}
	if err := os.MkdirAll(filepath.Join(retained, "restart"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := check(); err == nil || !strings.Contains(string(out), "already restarted once") {
		t.Fatalf("a second restart is refused: %v %s", err, out)
	}
}

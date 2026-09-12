package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func labReferencePointScript(t *testing.T) (string, string) {
	t.Helper()
	path := filepath.Join(repoRoot(t), "scripts", "lab-reference", "point.sh")
	return path, readFile(t, path)
}

func TestLabReferencePointRunnerKeepsTheDeclaredOrderAndNeverDeletesOrLeaks(t *testing.T) {
	path, script := labReferencePointScript(t)
	if output, err := exec.Command("bash", "-n", path).CombinedOutput(); err != nil {
		t.Fatalf("bash syntax failed: %v\n%s", err, output)
	}
	if !strings.HasPrefix(script, "#!/usr/bin/env bash\nset -Eeuo pipefail\n") {
		t.Fatal("the runner fails closed under set -Eeuo pipefail")
	}
	ordered := []string{
		`validate_task_root "$task_root"`,
		`validate_run_root "$task_root" "$run_root"`,
		`created_root=1`,
		`docker_containment "$docker_context" "$task_root" "$allow_rootful"`,
		`refuse_prior_state "$docker_context" "$wkcli"`,
		`verify_frozen_identities "$source_dir" "$source_file" "$build_file" "$image"`,
		`compose -f "$compose_file" config`,
		`gate_compose_config "$evidence/compose-config.yaml"`,
		`launched=1`,
		`compose -f "$compose_file" up -d --no-build`,
		`wait_nodes_ready "$LAB_READY_TIMEOUT_SECONDS"`,
		`inspect_project "$docker_context" "$project" "$facts"`,
		`systemd-run --user --scope`,
		`collector.py`,
		`: >"$go_file"`,
		`wait "$collector_pid" || true` + "\n    collector_pid=\"\"",
		`cleanup_status="$(compose_down_and_inventory "$docker_context" "$compose_file" "$project" "$run_root" "$evidence" "$facts" "$wkcli" "${compose_vars[@]}")"`,
		`provenance.py`,
		`bench reference emit`,
	}
	last := -1
	for _, marker := range ordered {
		index := strings.Index(script, marker)
		if index < 0 {
			t.Fatalf("runner lacks %q", marker)
		}
		if index <= last {
			t.Fatalf("runner step %q is out of order", marker)
		}
		last = index
	}
	downIndex := strings.Index(script, `down --remove-orphans --timeout "$LAB_DOWN_TIMEOUT_SECONDS"`)
	inventoryIndex := strings.Index(script, `residual-containers.txt`)
	if downIndex < 0 || inventoryIndex < 0 || downIndex > inventoryIndex {
		t.Fatal("bounded compose down precedes the residual inventory")
	}
	for _, forbidden := range []string{"rm -rf", "rm -r ", "down -v", "--volumes\" ", "volume rm", "volume prune", "system prune", "WK_BENCH_API_TOKEN", "--bench-token", "git reset", "git checkout", "git clean", "docker login", "--privileged", "network=host", "/var/lib/docker", "/tmp/"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("runner must not contain %q", forbidden)
		}
	}
	for _, required := range []string{
		`readonly LAB_NODE_CPUSETS=("0-3" "4-7" "8-11")`,
		`readonly LAB_CLIENT_CPUS="12,13"`,
		`readonly LAB_API_PORTS=(25001 25002 25003)`,
		`readonly LAB_TCP_PORTS=(25100 25101 25102)`,
		`readonly LAB_STOP_GRACE="30s"`,
		`-p MemoryMax="$client_memory_max" -p MemorySwapMax=0 -p CPUQuota="$LAB_CLIENT_CPU_QUOTA"`,
		`taskset -c "$LAB_CLIENT_CPUS"`,
		`--sample-marker "$facts/collector.marker"`,
		`--ledger "$client_dir/acknowledged-ledger.jsonl"`,
		`utc_now >"$evidence/stopped-at-utc"`,
		`--daemon "pid:$dockerd_pid"`,
		`the daemon is not rootless`,
		`below_task_root "$task_root" "$socket_file" || die "docker socket escapes the task root`,
		`realpath -e "${socket_path#unix://}"`,
		`git -C "$source_dir" status --porcelain`,
		`is not the frozen commit`,
		`not the frozen $expected_image_id`,
		`wkcli sha256 differs from the frozen build`,
		`the project created a volume`,
		`"prior_process_live": False`,
		`cleanup left residual state; the next launch is blocked`,
		`if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then`,
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("runner lacks %q", required)
		}
	}
	if strings.Count(script, `env "${compose_vars[@]}"`) != 2 || strings.Count(script, `"${compose_vars[@]}"`) < 4 {
		t.Fatal("config, up and both down paths share one Compose environment")
	}
}

func TestLabReferencePointRunnerFunctionsFailClosed(t *testing.T) {
	path, _ := labReferencePointScript(t)
	taskRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(taskRoot, "runs", "taken"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"relative run root": `validate_run_root "$1" "runs/x"`,
		"run root outside":  `validate_run_root "$1" "/srv/elsewhere/runs/x"`,
		"nested run root":   `validate_run_root "$1" "$1/runs/a/b"`,
		"existing run root": `validate_run_root "$1" "$1/runs/taken"`,
		"dotdot run root":   `validate_run_root "$1" "$1/runs/../runs/x"`,
		"bad node index":    `cpuset_for_node 4`,
		"bad metrics":       `compose_env "$1/runs/x" "$1/conf" img proj maybe 8g 0:0`,
	} {
		cmd := exec.Command("bash", "-c", "source \"$1\"; shift; "+body, "_", path, taskRoot)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("%s: must fail: %s", name, output)
		}
		if !strings.Contains(string(output), "ERROR:") {
			t.Fatalf("%s: failure is named: %s", name, output)
		}
	}
	cmd := exec.Command("bash", "-c", `source "$1"; validate_run_root "$2" "$2/runs/fresh" && cpuset_for_node 2 && api_port_for_node 3 && tcp_port_for_node 1`, "_", path, taskRoot)
	output, err := cmd.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "4-7\n25003\n25100" {
		t.Fatalf("fresh root and constants: %v %q", err, output)
	}
	cmd = exec.Command("bash", "-c", `source "$1"; compose_env "$2/runs/fresh" "$2/src/docker/lab/reference/conf" wukongim-lab:x wk-ref-seq05 off 8g 0:0`, "_", path, taskRoot)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compose_env: %v %s", err, output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 17 {
		t.Fatalf("compose_env prints 17 variables, got %d:\n%s", len(lines), output)
	}
	for _, want := range []string{"WK_LAB_METRICS_ENABLE=false", "WK_LAB_NODE2_CPUSET=4-7", "WK_LAB_NODE3_TCP_PORT=25102", "WK_LAB_ROOT=" + taskRoot + "/runs/fresh", "WK_LAB_STOP_GRACE=30s", "WK_LAB_PROJECT=wk-ref-seq05"} {
		if !strings.Contains(string(output), want+"\n") {
			t.Fatalf("compose_env lacks %q:\n%s", want, output)
		}
	}
}

func TestLabReferencePointRunnerGatesTheRenderedComposeConfiguration(t *testing.T) {
	path, _ := labReferencePointScript(t)
	dir := t.TempDir()
	runRoot := "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/runs/wukongim-common-150-seq06"
	confDir := "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/sources/wukongim/docker/lab/reference/conf"
	service := func(n string) string {
		return "  wk-node" + n + ":\n    image: wukongim-lab:test\n    volumes:\n      - type: bind\n        source: " + confDir + "/node" + n + ".toml\n        target: /etc/wukongim/wukongim.toml\n        read_only: true\n      - type: bind\n        source: " + runRoot + "/node" + n + "/data\n        target: /var/lib/wukongim\n      - type: bind\n        source: " + runRoot + "/node" + n + "/logs\n        target: /var/log/wukongim\n"
	}
	good := "name: wk-ref-seq06\nservices:\n" + service("1") + service("2") + service("3")
	rendered := filepath.Join(dir, "good.yaml")
	if err := os.WriteFile(rendered, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	gate := func(file string) ([]byte, error) {
		return exec.Command("bash", "-c", `source "$1"; gate_compose_config "$2" "$3" "$4"`, "_", path, file, runRoot, confDir).CombinedOutput()
	}
	if out, err := gate(rendered); err != nil {
		t.Fatalf("a contained rendering passes: %v %s", err, out)
	}
	for name, mutated := range map[string]string{
		"escaping bind":    strings.Replace(good, runRoot+"/node2/data", "/var/lib/docker/volumes/x", 1),
		"top-level volume": good + "volumes:\n  data: {}\n",
		"fourth service":   good + service("4"),
		"foreign mount":    strings.Replace(good, "type: bind\n        source: "+runRoot+"/node3/logs", "type: volume\n        source: "+runRoot+"/node3/logs", 1),
	} {
		file := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".yaml")
		if err := os.WriteFile(file, []byte(mutated), 0o600); err != nil {
			t.Fatal(err)
		}
		if out, err := gate(file); err == nil {
			t.Fatalf("%s must be refused: %s", name, out)
		}
	}
}

// The pre-launch refusal only fires when something is live: with no container,
// no product process and no bound laboratory port it must succeed, even though
// pgrep exits non-zero when nothing matches (pipefail must not turn "nothing
// is live" into a silent exit).
func TestLabReferencePointRunnerRefusesPriorStateOnlyWhenSomethingIsLive(t *testing.T) {
	path, _ := labReferencePointScript(t)
	// Plain statements: inside a && list bash would suppress errexit and hide the defect.
	quiet := exec.Command("bash", "-c", "source \"$1\"; docker() { :; }; pgrep() { return 1; }; ss() { :; }\nrefuse_prior_state ctx wkcli\necho CLEAN", "_", path)
	output, err := quiet.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "CLEAN") {
		t.Fatalf("a clean host passes the prior-state refusal: %v %s", err, output)
	}
	live := exec.Command("bash", "-c", `source "$1"; docker() { :; }; pgrep() { echo 4242; }; ss() { :; }; refuse_prior_state ctx wkcli`, "_", path)
	output, err = live.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "product process(es) are live") {
		t.Fatalf("a live product process is refused by name: %v %s", err, output)
	}
}

// The EXIT trap runs after main's frame is gone when errexit fires inside
// main, so every variable it reads must be global: a trap that trips over an
// unbound local writes no receipt and skips the compose teardown.
func TestLabReferencePointRunnerExitTrapUsesNoFunctionLocalState(t *testing.T) {
	root := repoRoot(t)
	for _, entry := range []struct{ file, main string }{
		{"point.sh", "\nmain() {"},
		{"restart.sh", "\nrestart_main() {"},
	} {
		script := readFile(t, filepath.Join(root, "scripts", "lab-reference", entry.file))
		assertExitTrapReadsOnlyGlobals(t, entry.file, script, entry.main)
	}
}

func assertExitTrapReadsOnlyGlobals(t *testing.T, file, script, mainMarker string) {
	t.Helper()
	start := strings.Index(script, "    finish() {")
	if start < 0 {
		t.Fatalf("%s: the runner installs its finish trap inside main", file)
	}
	end := strings.Index(script[start:], "    trap finish EXIT")
	if end < 0 {
		t.Fatalf("%s: the runner installs its finish trap inside main", file)
	}
	body := script[start : start+end]
	mainStart := strings.Index(script, mainMarker)
	if mainStart < 0 {
		t.Fatalf("%s: main is missing", file)
	}
	mainBody := script[mainStart:]
	seen := map[string]bool{}
	for _, field := range strings.FieldsFunc(body, func(r rune) bool {
		return !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '$' || r == '{' || r == '#')
	}) {
		name := strings.TrimLeft(field, "${#")
		if !strings.HasPrefix(field, "$") || name == "" || name == "status" || seen[name] {
			continue
		}
		seen[name] = true
		for _, line := range strings.Split(mainBody, "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "local ") {
				continue
			}
			for _, decl := range strings.Fields(strings.TrimPrefix(trimmed, "local ")) {
				decl = strings.TrimPrefix(decl, "-a")
				if decl == name || strings.HasPrefix(decl, name+"=") {
					t.Fatalf("%s: the finish trap reads %q, which main declares local: %s", file, name, trimmed)
				}
			}
		}
	}
	if len(seen) < 5 {
		t.Fatalf("%s: the finish trap reads the run state, saw %v", file, seen)
	}
}

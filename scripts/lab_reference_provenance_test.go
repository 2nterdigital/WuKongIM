package scripts_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const labReferenceTaskRoot = "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison"

func writeJSON(t *testing.T, path string, value any) string {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func labReferenceFacts(t *testing.T, dir string) map[string]string {
	t.Helper()
	hex := func(c string) string { return strings.Repeat(c, 64) }
	role := func(cpus ...int) map[string]any {
		return map[string]any{"cpus": cpus, "cpu_quota_percent": 100 * len(cpus), "memory_max_bytes": 8 << 30, "cgroup": nil}
	}
	sample := map[string]any{"cpu_ms": 1000, "max_rss_bytes": 1 << 20, "memory_peak_bytes": 1 << 20, "read_bytes": 0, "write_bytes": 1 << 20, "samples": 200, "throttled_ms": 0}
	imageID := "sha256:" + hex("e")
	container := func(n int) map[string]any {
		return map[string]any{
			"Name": "/wk-ref-seq05-wk-node" + string(rune('0'+n)) + "-1", "Image": imageID, "LogPath": labReferenceTaskRoot + "/docker/data/containers/" + string(rune('0'+n)) + "-json.log",
			"HostConfig": map[string]any{"CpusetCpus": []string{"0-3", "4-7", "8-11"}[n-1], "NanoCpus": 0, "Memory": 8 << 30},
			"Mounts": []map[string]any{
				{"Type": "bind", "Source": labReferenceTaskRoot + "/runs/wukongim-common-150-seq05/node1/data", "Destination": "/var/lib/wukongim"},
				{"Type": "tmpfs", "Source": "", "Destination": "/run/wukongim"},
			},
		}
	}
	facts := map[string]string{}
	facts["campaign"] = writeJSON(t, filepath.Join(dir, "campaign.json"), map[string]any{"change": "establish-ech0-wukongim-reference-load-comparison", "manifest_sha256": hex("a"), "launch_sequence": 5, "point_kind": "common", "ladder": "common", "rate_per_second": 150, "repeat_of": nil, "root": labReferenceTaskRoot + "/runs/wukongim-common-150-seq05", "identity_seed": "seed-1", "launched_at_utc": "2026-09-13T01:00:00Z", "deadline_ms": 3600000})
	facts["source"] = writeJSON(t, filepath.Join(dir, "source.json"), map[string]any{"repository": "2nterdigital/WuKongIM", "remote_url": "git@github.com:2nterdigital/WuKongIM.git", "git_ref": "ref/metric", "commit": strings.Repeat("4", 40), "tree": strings.Repeat("5", 40), "parent": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "dirty": false, "dependency_manifest": map[string]any{"name": "go.sum", "sha256": hex("b")}, "planning_base": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "instrumentation": map[string]any{"base_commit": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "diff_sha256": hex("c"), "changed_paths": []string{"internal/bench/reference"}}})
	facts["build"] = writeJSON(t, filepath.Join(dir, "build.json"), map[string]any{"host_toolchain": "go1.25.11 linux/amd64", "profile": "docker build", "features": []string{}, "compose_version": "v5.5.1", "binaries": []map[string]any{{"name": "wkcli", "sha256": hex("d"), "size_bytes": 1}}, "images": []map[string]any{{"name": "wukongim-lab:ref-metric", "image_id": imageID, "repo_digest": nil, "base_images": []map[string]any{}, "build_args": map[string]any{}, "buildkit_version": "v0.26.0", "compiler_identity": "go1.26.7 linux/amd64"}}})
	facts["host"] = writeJSON(t, filepath.Join(dir, "host.json"), map[string]any{"teleport_node": "ech0-message-lab3", "hostname": "h", "labels": map[string]any{"project": "tornado-message", "purpose": "ech0-message-lab3"}, "login": "ecs-user", "logical_cpus": 16, "memory_bytes": 64 << 30, "kernel": "k", "os": "o", "task_root": labReferenceTaskRoot, "backing_device": "/dev/nvme0n1p3", "filesystem": "ext4", "mount_options": "rw", "free_bytes_at_launch": 1 << 40, "clock_synchronized": true})
	facts["deployment"] = writeJSON(t, filepath.Join(dir, "deployment.json"), map[string]any{"role_quotas": map[string]any{"node-1": role(0, 1, 2, 3), "node-2": role(4, 5, 6, 7), "node-3": role(8, 9, 10, 11), "client": role(12, 13), "coordinator": role(14, 15)}, "configuration_sha256": map[string]any{"node-1": hex("f"), "node-2": hex("f"), "node-3": hex("f")}})
	facts["docker-info"] = writeJSON(t, filepath.Join(dir, "docker-info.json"), map[string]any{"ServerVersion": "29.8.0", "DockerRootDir": labReferenceTaskRoot + "/docker/data", "ExecRoot": labReferenceTaskRoot + "/docker/exec", "SocketPath": "unix://" + labReferenceTaskRoot + "/docker/run/docker.sock", "SecurityOptions": []string{"name=seccomp,profile=builtin", "name=rootless"}, "ClientInfo": map[string]any{"ComposeVersion": "v5.5.1"}})
	facts["docker-inspect"] = writeJSON(t, filepath.Join(dir, "inspect.json"), []map[string]any{container(1), container(2), container(3)})
	facts["isolation"] = writeJSON(t, filepath.Join(dir, "isolation.json"), map[string]any{"quiet_host": true, "load1_at_launch": 0.1, "foreign_processes_at_launch": []string{}, "foreign_compile_seen_during_run": false, "clock_synchronized": true, "prior_process_live": false, "prior_containers_live": false, "host_overlap": false})
	facts["resources"] = writeJSON(t, filepath.Join(dir, "resources.json"), map[string]any{"schema": "wukongim-reference-collector/1", "sample_interval_ms": 1000, "samples": 200, "roles": map[string]any{"node-1": sample, "node-2": sample, "node-3": sample, "client": sample, "coordinator": sample}, "disk": map[string]any{"consumed_max_bytes": 1 << 30, "free_at_start_bytes": 1 << 40, "free_min_bytes": 1 << 39}, "docker_daemon": map[string]any{"cpu_ms": 100, "max_rss_bytes": 1 << 20}})
	facts["cleanup"] = writeJSON(t, filepath.Join(dir, "cleanup.json"), map[string]any{"status": "clean", "detail": nil, "residual": map[string]any{"processes": 0, "containers": 0, "ports": 0, "cgroups": 0, "networks": 0, "mounts": 0, "volumes": 0, "open_files": 0}})
	return facts
}

func runProvenance(t *testing.T, facts map[string]string, output string, extra ...string) ([]byte, error) {
	t.Helper()
	root := repoRoot(t)
	args := []string{filepath.Join(root, "scripts", "lab-reference", "provenance.py")}
	for name, path := range facts {
		args = append(args, "--"+name, path)
	}
	for n := 1; n <= 3; n++ {
		args = append(args, "--node-config", filepath.Join(root, "docker", "lab", "reference", "conf", "node"+string(rune('0'+n))+".toml"))
	}
	args = append(args, "--log-volume-bytes", "12345", "--networks", "wk-ref-seq05_default", "--output", output)
	args = append(args, extra...)
	return exec.Command("python3", args...).CombinedOutput()
}

func TestLabReferenceProvenanceProjectsTheFactsIntoTheDocumentShape(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not installed here")
	}
	dir := t.TempDir()
	facts := labReferenceFacts(t, dir)
	output := filepath.Join(dir, "provenance.json")
	if out, err := runProvenance(t, facts, output); err != nil {
		t.Fatalf("provenance: %v\n%s", err, out)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document["schema"] != "wukongim-reference-provenance/1" || document["identity_seed"] != "seed-1" || document["deadline_ms"] != float64(3600000) {
		t.Fatalf("identity = %v %v %v", document["schema"], document["identity_seed"], document["deadline_ms"])
	}
	for _, key := range []string{"campaign", "source", "build", "host", "deployment", "isolation", "resources", "restart", "cleanup", "launched_at_utc"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("provenance lacks %s", key)
		}
	}
	deployment := document["deployment"].(map[string]any)
	topology := deployment["topology"].(map[string]any)
	wk := topology["wukongim"].(map[string]any)
	if topology["kind"] != "wukongim_slots" || wk["hash_slot_count"] != float64(256) || wk["slot_replica_n"] != float64(3) || wk["channel_append_batch_max_wait"] != "250us" || wk["commit_coordinator_max_bytes"] != float64(131072) {
		t.Fatalf("topology read from the lab node configuration = %v", wk)
	}
	docker := deployment["docker"].(map[string]any)
	containers := docker["containers"].([]any)
	if docker["rootless"] != true || docker["engine_version"] != "29.8.0" || docker["compose_version"] != "v5.5.1" || len(containers) != 3 {
		t.Fatalf("docker = %v", docker)
	}
	first := containers[0].(map[string]any)
	if first["name"] != "wk-ref-seq05-wk-node1-1" || first["cpuset"] != "0-3" || first["cpu_quota_percent"] != float64(400) || first["memory_limit_bytes"] != float64(8<<30) {
		t.Fatalf("container projection = %v", first)
	}
	mounts := first["mounts"].([]any)
	if len(mounts) != 2 || mounts[1].(map[string]any)["source"] != "tmpfs" {
		t.Fatalf("mounts = %v", mounts)
	}
	resources := document["resources"].(map[string]any)
	roles := resources["roles"].(map[string]any)
	if len(roles) != 4 || resources["log_volume_bytes"] != float64(12345) || resources["sample_interval_ms"] != float64(1000) {
		t.Fatalf("resources = %v", resources)
	}
	if _, ok := resources["network"].(map[string]any)["unavailable"]; !ok {
		t.Fatalf("network is explicitly unavailable: %v", resources["network"])
	}
	if _, ok := document["restart"].(map[string]any)["unavailable"]; !ok {
		t.Fatalf("restart defaults to unavailable: %v", document["restart"])
	}
	if !strings.Contains(string(data), `"networks": [`) || !strings.Contains(string(data), "wk-ref-seq05_default") {
		t.Fatal("project networks are recorded")
	}
	info, _ := os.Stat(output)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %o", info.Mode().Perm())
	}
}

func TestLabReferenceProvenanceRefusesMissingOrEscapingFacts(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not installed here")
	}
	dir := t.TempDir()
	output := filepath.Join(dir, "provenance.json")
	factsIn := func(name string) map[string]string {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
		return labReferenceFacts(t, filepath.Join(dir, name))
	}
	rewrite := func(path string, edit func(map[string]any)) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		edit(value)
		writeJSON(t, path, value)
	}
	cases := map[string]func(map[string]string){
		"missing client sample": func(facts map[string]string) {
			rewrite(facts["resources"], func(v map[string]any) { delete(v["roles"].(map[string]any), "client") })
		},
		"docker data root outside the task root": func(facts map[string]string) {
			rewrite(facts["docker-info"], func(v map[string]any) { v["DockerRootDir"] = "/var/lib/docker" })
		},
		"a fourth role quota": func(facts map[string]string) {
			rewrite(facts["deployment"], func(v map[string]any) { v["role_quotas"].(map[string]any)["extra"] = map[string]any{} })
		},
		"credential-like string": func(facts map[string]string) {
			rewrite(facts["host"], func(v map[string]any) { v["hostname"] = "ghp_abcdefghijklmnop" })
		},
		"foreign collector schema": func(facts map[string]string) {
			rewrite(facts["resources"], func(v map[string]any) { v["schema"] = "other/1" })
		},
		"container without memory limit": func(facts map[string]string) {
			data, _ := os.ReadFile(facts["docker-inspect"])
			var items []map[string]any
			_ = json.Unmarshal(data, &items)
			items[1]["HostConfig"].(map[string]any)["Memory"] = 0
			writeJSON(t, facts["docker-inspect"], items)
		},
	}
	for name, corrupt := range cases {
		facts := factsIn(name)
		corrupt(facts)
		out, err := runProvenance(t, facts, output)
		if err == nil {
			t.Fatalf("%s: provenance must refuse: %s", name, out)
		}
		if !strings.HasPrefix(strings.TrimSpace(string(out)), "provenance:") {
			t.Fatalf("%s: refusal is named, not a traceback: %s", name, out)
		}
	}
	if _, err := os.Stat(output); err == nil {
		t.Fatal("a refused assembly writes nothing")
	}
	facts := factsIn("missing")
	delete(facts, "cleanup")
	if out, err := runProvenance(t, facts, output); err == nil {
		t.Fatalf("a missing fact file is refused: %s", out)
	}
}

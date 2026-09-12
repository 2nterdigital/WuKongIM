package reference

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func sampleProvenance() []byte {
	hex := func(c string) string { return strings.Repeat(c, 64) }
	role := func(cpus ...int) map[string]any {
		return map[string]any{"cpus": cpus, "cpu_quota_percent": 100 * len(cpus), "memory_max_bytes": 8 << 30, "cgroup": nil}
	}
	sampled := func(cpuMs int) map[string]any {
		return map[string]any{"cpu_ms": cpuMs, "max_rss_bytes": 1 << 20, "memory_peak_bytes": 1 << 20, "read_bytes": 0, "write_bytes": 1 << 20, "samples": 200, "throttled_ms": 0}
	}
	container := func(n int) map[string]any {
		return map[string]any{
			"name": fmt.Sprintf("wk-ref-node%d", n), "image_id": "sha256:" + hex("e"), "cpuset": fmt.Sprintf("%d-%d", (n-1)*4, n*4-1), "cpu_quota_percent": 400, "memory_limit_bytes": 8 << 30,
			"mounts":   []map[string]any{{"source": fmt.Sprintf("/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/runs/wukongim-common-150-seq05/node%d/data", n), "destination": "/var/lib/wukongim", "kind": "bind"}},
			"log_path": fmt.Sprintf("/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/docker/data/containers/%d-json.log", n),
		}
	}
	provenance := map[string]any{
		"schema":   ProvenanceSchema,
		"campaign": map[string]any{"change": "establish-ech0-wukongim-reference-load-comparison", "manifest_sha256": hex("a"), "launch_sequence": 5, "point_kind": "common", "ladder": "common", "rate_per_second": 150, "repeat_of": nil, "root": "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/runs/wukongim-common-150-seq05"},
		"source":   map[string]any{"repository": "2nterdigital/WuKongIM", "remote_url": "git@github.com:2nterdigital/WuKongIM.git", "git_ref": "ref/metric", "commit": strings.Repeat("4", 40), "tree": strings.Repeat("5", 40), "parent": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "dirty": false, "dependency_manifest": map[string]any{"name": "go.sum", "sha256": hex("b")}, "planning_base": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "instrumentation": map[string]any{"base_commit": "47bc77ee34d6677a78dd6f8674e4293409e8f9a2", "diff_sha256": hex("c"), "changed_paths": []string{"internal/bench/reference"}}},
		"build":    map[string]any{"host_toolchain": "go1.25.11 linux/amd64", "profile": "docker build", "features": []string{}, "binaries": []map[string]any{{"name": "wkcli", "sha256": hex("d"), "size_bytes": 1}}, "images": []map[string]any{{"name": "wukongim-lab:ref-metric", "image_id": "sha256:" + hex("e"), "repo_digest": nil, "base_images": []map[string]any{{"name": "golang:1.26.7-bookworm", "digest": "sha256:" + hex("1")}, {"name": "alpine:3.24.1", "digest": "sha256:" + hex("2")}}, "build_args": map[string]any{}, "buildkit_version": "v0.26.0", "compiler_identity": "go1.26.7 linux/amd64 (image builder stage)"}}},
		"host":     map[string]any{"teleport_node": "ech0-message-lab3", "hostname": "h", "labels": map[string]any{"project": "tornado-message", "purpose": "ech0-message-lab3"}, "login": "ecs-user", "logical_cpus": 16, "memory_bytes": 64 << 30, "kernel": "k", "os": "o", "task_root": "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison", "backing_device": "/dev/nvme0n1p3", "filesystem": "ext4", "mount_options": "rw", "free_bytes_at_launch": 1 << 40, "clock_synchronized": true},
		"deployment": map[string]any{
			"role_quotas":          map[string]any{"node-1": role(0, 1, 2, 3), "node-2": role(4, 5, 6, 7), "node-3": role(8, 9, 10, 11), "client": role(12, 13), "coordinator": role(14, 15)},
			"topology":             map[string]any{"kind": "wukongim_slots", "ech0": nil, "wukongim": map[string]any{"hash_slot_count": 256, "initial_slot_count": 12, "slot_replica_n": 3, "channel_append_batch_max_records": 128, "channel_append_batch_max_wait": "250us", "commit_coordinator_flush_window": "1ms", "commit_coordinator_max_requests": 0, "commit_coordinator_max_records": 0, "commit_coordinator_max_bytes": 131072, "commit_coordinator_shards": 1}},
			"configuration_sha256": map[string]any{"node-1": hex("f"), "node-2": hex("f"), "node-3": hex("f")},
			"docker":               map[string]any{"engine_version": "29.8.0", "compose_version": "v5.5.1", "buildkit_version": nil, "rootless": true, "data_root": "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/docker/data", "exec_root": "/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/docker/exec", "socket": "unix:///srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/docker/run/docker.sock", "containers": []map[string]any{container(1), container(2), container(3)}, "volumes": []string{}, "networks": []string{"wk-ref-seq05_default"}, "daemon_overhead": map[string]any{"cpu_ms": 100, "max_rss_bytes": 1 << 20}},
		},
		"isolation":       map[string]any{"quiet_host": true, "load1_at_launch": 0.0, "foreign_processes_at_launch": []string{}, "foreign_compile_seen_during_run": false, "clock_synchronized": true, "prior_process_live": false, "prior_containers_live": false, "host_overlap": false},
		"resources":       map[string]any{"sample_interval_ms": 1000, "roles": map[string]any{"node-1": sampled(1000), "node-2": sampled(1000), "node-3": sampled(1000), "client": sampled(500)}, "disk": map[string]any{"consumed_max_bytes": 1 << 30, "free_at_start_bytes": 1 << 40, "free_min_bytes": 1 << 39}, "network": map[string]any{"unavailable": "loopback traffic is not attributed per role"}, "log_volume_bytes": 10, "docker_daemon": map[string]any{"cpu_ms": 100, "max_rss_bytes": 1 << 20}},
		"restart":         map[string]any{"unavailable": "not selected for this root"},
		"cleanup":         map[string]any{"status": "clean", "detail": nil, "residual": map[string]any{"processes": 0, "containers": 0, "ports": 0, "cgroups": 0, "networks": 0, "mounts": 0, "volumes": 0, "open_files": 0}},
		"identity_seed":   "reference-comparison-seed-1",
		"launched_at_utc": "2026-09-13T01:00:00Z",
		"deadline_ms":     3600000,
	}
	encoded, err := json.Marshal(provenance)
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestEmitAssemblesTheComparisonDocumentFromRunAndProvenance(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 150)
	cluster := newFakeCluster(plan)
	var logs []string
	run := runFake(t, plan, cluster, true, &logs)
	var provenance Provenance
	if err := json.Unmarshal(sampleProvenance(), &provenance); err != nil {
		t.Fatal(err)
	}
	document, err := Emit(run, provenance)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if document["schema"] != DocumentSchema || document["product"] != "wukongim" || document["document_id"] != "wukongim-common-150-seq05" {
		t.Fatalf("document identity = %v / %v / %v", document["schema"], document["product"], document["document_id"])
	}
	for _, key := range []string{"campaign", "source", "build", "host", "deployment", "workload", "lifecycle", "accounting", "latency", "history", "online_delivery", "resources", "physical_work", "native", "observation", "restart", "isolation", "cleanup"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("document lacks %s", key)
		}
	}
	if len(document) != 21 {
		t.Fatalf("document has %d top-level keys, want exactly the 21 of the schema", len(document))
	}
	deployment := document["deployment"].(map[string]any)
	if deployment["shape"] != "docker_containers" || deployment["nodes"] != uint64(3) || deployment["replication_factor"] != uint64(3) || deployment["gateway_endpoints"] != uint64(3) {
		t.Fatalf("deployment = %v", deployment)
	}
	workload := document["workload"].(WorkloadDocument)
	if workload.IdentitySeed != "reference-comparison-seed-1" || workload.Connections != 273 {
		t.Fatalf("workload = %+v", workload)
	}
	lifecycle := document["lifecycle"].(LifecycleDocument)
	if lifecycle.LaunchedAtUTC != "2026-09-13T01:00:00Z" || lifecycle.DeadlineMs != 3600000 || lifecycle.DeclaredFailure != nil {
		t.Fatalf("lifecycle = %+v", lifecycle)
	}
	accounting := document["accounting"].(AccountingDocument)
	if accounting.Total.Acknowledged != plan.TotalScheduled() || len(accounting.TimedOutByReason) != 0 {
		t.Fatalf("accounting = %+v", accounting)
	}
	native := document["native"].(map[string]any)["wukongim_native"].(map[string]any)
	if native["drained_in_flight"] == nil || native["counters"] == nil || native["online"] == nil {
		t.Fatalf("native = %v", native)
	}
	if _, present := document["native"].(map[string]any)["ech0_native"]; !present {
		t.Fatalf("the ech0 native slot is present and null")
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if dump := os.Getenv("WK_REFERENCE_EMIT_DUMP"); dump != "" {
		// Cross-product evidence only: the ech0 validator reads this file.
		if err := os.WriteFile(dump, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if dump := os.Getenv("WK_REFERENCE_EMIT_DUMP_RESTART"); dump != "" {
		// The same point after a cold restart and re-audit, in the restart.sh shape.
		restarted := provenance
		restart := map[string]any{
			"performed": true, "stop_model": "bounded SIGTERM compose down after the point's drain, then a cold start of the same three containers on the retained data roots",
			"stopped_at_utc": "2026-09-13T01:20:00Z", "started_at_utc": "2026-09-13T01:21:00Z", "ready_at_utc": "2026-09-13T01:21:09Z", "restart_ms": 9000,
			"reaudit": run.History, "included_in_throughput_latency": false,
		}
		raw, err := json.Marshal(restart)
		if err != nil {
			t.Fatal(err)
		}
		restarted.Restart = raw
		document, err := Emit(run, restarted)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dump, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if len(encoded) > 512*1024 {
		t.Fatalf("document is %d bytes; it must stay below the 512 KiB bound", len(encoded))
	}
	text := string(encoded)
	for _, forbidden := range []string{plan.Senders[0].UID, plan.Senders[0].Token, plan.ClientMsgNo(0), "\"uid\"", "\"payload\""} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("document must not carry %q", forbidden)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	online := decoded["online_delivery"].(map[string]any)
	for _, key := range []string{"observed", "received", "receive_acknowledged", "distinct", "duplicates", "messages_without_delivery", "withheld", "complete"} {
		if _, ok := online[key]; !ok {
			t.Fatalf("online_delivery lacks %s", key)
		}
	}
	if len(online) != 8 {
		t.Fatalf("online_delivery carries %d keys; the extra native counts live under native", len(online))
	}
	resources := decoded["resources"].(map[string]any)
	if resources["sample_interval_ms"] == nil || resources["roles"] == nil {
		t.Fatalf("resources = %v", resources)
	}
}

func TestEmitRefusesAMissingProvenanceSectionOrSchema(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	run := runFake(t, plan, cluster, false, &logs)
	var provenance Provenance
	if err := json.Unmarshal(sampleProvenance(), &provenance); err != nil {
		t.Fatal(err)
	}
	wrongSchema := provenance
	wrongSchema.Schema = "wukongim-reference-provenance/2"
	if _, err := Emit(run, wrongSchema); err == nil {
		t.Fatalf("a different provenance schema is refused")
	}
	noRoles := provenance
	noRoles.Resources.Roles = nil
	if _, err := Emit(run, noRoles); err == nil || !strings.Contains(err.Error(), "roles") {
		t.Fatalf("missing cgroup role samples are refused, never zero-filled: %v", err)
	}
	mismatch := provenanceWithRate(t, 175)
	if _, err := Emit(run, mismatch); err == nil || !strings.Contains(err.Error(), "rate") {
		t.Fatalf("a provenance rate that differs from the run is refused: %v", err)
	}
	if _, err := Emit(nil, provenance); err == nil {
		t.Fatalf("a nil run is refused")
	}
}

func TestRunResultRoundTripsThroughJSONBeforeEmit(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	run := runFake(t, plan, cluster, true, &logs)
	encoded, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var decoded RunResult
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("a run result decodes strictly from its own JSON: %v", err)
	}
	provenance := provenanceWithRate(t, 100)
	direct, err := Emit(run, provenance)
	if err != nil {
		t.Fatal(err)
	}
	viaJSON, err := Emit(&decoded, provenance)
	if err != nil {
		t.Fatal(err)
	}
	directBytes, _ := json.Marshal(direct)
	viaBytes, _ := json.Marshal(viaJSON)
	if string(directBytes) != string(viaBytes) {
		t.Fatalf("the document emitted from the decoded run differs from the direct one")
	}
	if decoded.Work.Fsyncs.Unavailable == "" || decoded.Work.PhysicalCommits.Unavailable != "" {
		t.Fatalf("measured fields survive the round trip: %+v", decoded.Work)
	}
	var wrongBounds Histogram
	if err := json.Unmarshal([]byte(`{"bounds_ms":[1,2,3],"buckets":[0],"count":0,"max_ms":0}`), &wrongBounds); err == nil {
		t.Fatal("foreign histogram bounds are refused")
	}
}

// provenanceWithRate is the sample provenance with the campaign rate replaced.
func provenanceWithRate(t *testing.T, rate uint64) Provenance {
	t.Helper()
	var provenance Provenance
	if err := json.Unmarshal(sampleProvenance(), &provenance); err != nil {
		t.Fatal(err)
	}
	var campaign map[string]any
	if err := json.Unmarshal(provenance.Campaign, &campaign); err != nil {
		t.Fatal(err)
	}
	campaign["rate_per_second"] = rate
	encoded, err := json.Marshal(campaign)
	if err != nil {
		t.Fatal(err)
	}
	provenance.Campaign = encoded
	return provenance
}

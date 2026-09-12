package scripts_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFakeCgroup(t *testing.T, dir string, usageUsec, throttledUsec, current, peak, anon, rbytes, wbytes int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"cpu.stat":       "usage_usec " + itoa(usageUsec) + "\nuser_usec 1\nsystem_usec 1\nnr_throttled 0\nthrottled_usec " + itoa(throttledUsec) + "\n",
		"memory.current": itoa(current) + "\n",
		"memory.peak":    itoa(peak) + "\n",
		"memory.stat":    "anon " + itoa(anon) + "\nfile 10\n",
		"io.stat":        "259:0 rbytes=" + itoa(rbytes) + " wbytes=" + itoa(wbytes) + " rios=1 wios=1\n259:1 rbytes=5 wbytes=5 rios=1 wios=1\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func itoa(n int) string {
	var digits []byte
	if n == 0 {
		return "0"
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func TestLabReferenceCollectorSummarizesCgroupDeltasWithoutRetainingProcessDetail(t *testing.T) {
	root := repoRoot(t)
	collector := filepath.Join(root, "scripts", "lab-reference", "collector.py")
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not installed here")
	}
	dir := t.TempDir()
	node := filepath.Join(dir, "cg", "node-1")
	client := filepath.Join(dir, "cg", "client")
	writeFakeCgroup(t, node, 1_000_000, 0, 100, 150, 120, 1000, 2000)
	writeFakeCgroup(t, client, 500_000, 20_000, 50, 60, 40, 10, 20)
	output := filepath.Join(dir, "resources.json")
	marker := filepath.Join(dir, "samples.marker")
	// Two samples: the first at start, the second after the fake counters moved.
	cmd := exec.Command("python3", collector, "--output", output, "--disk-path", dir, "--interval-ms", "300", "--max-samples", "2",
		"--sample-marker", marker, "--role", "node-1=cgroup:"+node, "--role", "client=cgroup:"+client)
	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()
	advanced := false
	for i := 0; i < 200 && !advanced; i++ {
		if data, err := os.ReadFile(marker); err == nil && strings.TrimSpace(string(data)) == "1" {
			writeFakeCgroup(t, node, 4_000_000, 0, 130, 170, 160, 1500, 2600)
			writeFakeCgroup(t, client, 700_000, 50_000, 55, 70, 45, 10, 20)
			advanced = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := <-done; err != nil {
		t.Fatalf("collector: %v", err)
	}
	if !advanced {
		t.Fatal("the first sample marker never appeared")
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Schema  string `json:"schema"`
		Samples int    `json:"samples"`
		Roles   map[string]struct {
			CPUMs       int `json:"cpu_ms"`
			MaxRSS      int `json:"max_rss_bytes"`
			MemoryPeak  int `json:"memory_peak_bytes"`
			ReadBytes   int `json:"read_bytes"`
			WriteBytes  int `json:"write_bytes"`
			Samples     int `json:"samples"`
			ThrottledMs int `json:"throttled_ms"`
		} `json:"roles"`
		Disk struct {
			ConsumedMax int `json:"consumed_max_bytes"`
			FreeAtStart int `json:"free_at_start_bytes"`
			FreeMin     int `json:"free_min_bytes"`
		} `json:"disk"`
		Daemon map[string]any `json:"docker_daemon"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode: %v\n%s", err, data)
	}
	if document.Schema != "wukongim-reference-collector/1" || document.Samples != 2 {
		t.Fatalf("schema/samples = %s/%d", document.Schema, document.Samples)
	}
	node1 := document.Roles["node-1"]
	if node1.CPUMs != 3000 || node1.MaxRSS != 160 || node1.MemoryPeak != 170 || node1.ReadBytes != 500 || node1.WriteBytes != 600 || node1.Samples != 2 || node1.ThrottledMs != 0 {
		t.Fatalf("node-1 = %+v, want deltas of the fake counters", node1)
	}
	if document.Roles["client"].ThrottledMs != 30 || document.Roles["client"].CPUMs != 200 {
		t.Fatalf("client = %+v", document.Roles["client"])
	}
	if document.Disk.FreeAtStart == 0 || document.Disk.FreeMin > document.Disk.FreeAtStart || document.Disk.ConsumedMax < 0 {
		t.Fatalf("disk = %+v", document.Disk)
	}
	if _, ok := document.Daemon["unavailable"]; !ok {
		t.Fatalf("no daemon pid was named, so the daemon cost is unavailable: %v", document.Daemon)
	}
	text := string(data)
	for _, forbidden := range []string{dir, "cmdline", "/proc/"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("the collector output retains %q", forbidden)
		}
	}
	info, _ := os.Stat(output)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %o", info.Mode().Perm())
	}
}

func TestLabReferenceCollectorFailsClosedOnBadArguments(t *testing.T) {
	root := repoRoot(t)
	collector := filepath.Join(root, "scripts", "lab-reference", "collector.py")
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not installed here")
	}
	dir := t.TempDir()
	for name, args := range map[string][]string{
		"no role":            {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir},
		"relative output":    {"--output", "o.json", "--disk-path", dir, "--role", "node-1=cgroup:" + dir},
		"relative cgroup":    {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir, "--role", "node-1=cgroup:relative"},
		"malformed role":     {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir, "--role", "node-1"},
		"interval too small": {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir, "--role", "node-1=cgroup:" + dir, "--interval-ms", "10"},
		"duplicate role":     {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir, "--role", "node-1=cgroup:" + dir, "--role", "node-1=cgroup:" + dir, "--max-samples", "1"},
		"bad daemon":         {"--output", filepath.Join(dir, "o.json"), "--disk-path", dir, "--role", "node-1=cgroup:" + dir, "--daemon", "name:dockerd", "--max-samples", "1"},
	} {
		out, err := exec.Command("python3", append([]string{collector}, args...)...).CombinedOutput()
		if err == nil {
			t.Fatalf("%s: the collector must refuse: %s", name, out)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "o.json")); err == nil {
		t.Fatal("a refused invocation writes nothing")
	}
}

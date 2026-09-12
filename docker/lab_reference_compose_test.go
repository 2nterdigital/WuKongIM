package docker_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/app"
	productconfig "github.com/WuKongIM/WuKongIM/internal/config"
	"gopkg.in/yaml.v3"
)

type labComposeService struct {
	Image           string            `yaml:"image"`
	PullPolicy      string            `yaml:"pull_policy"`
	User            string            `yaml:"user"`
	Restart         string            `yaml:"restart"`
	StopSignal      string            `yaml:"stop_signal"`
	StopGracePeriod string            `yaml:"stop_grace_period"`
	Cpuset          string            `yaml:"cpuset"`
	MemLimit        string            `yaml:"mem_limit"`
	Environment     map[string]string `yaml:"environment"`
	Volumes         []string          `yaml:"volumes"`
	Tmpfs           []string          `yaml:"tmpfs"`
	Ports           []string          `yaml:"ports"`
	Build           any               `yaml:"build"`
	DependsOn       any               `yaml:"depends_on"`
	Profiles        any               `yaml:"profiles"`
	Logging         any               `yaml:"logging"`
}

type labCompose struct {
	Name     string                       `yaml:"name"`
	Services map[string]labComposeService `yaml:"services"`
	Volumes  map[string]any               `yaml:"volumes"`
	Networks map[string]any               `yaml:"networks"`
}

func labReferenceComposePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(dockerRepoRoot(t), "docker", "lab", "reference", "compose.yml")
}

func loadLabReferenceCompose(t *testing.T) (labCompose, string) {
	t.Helper()
	data, err := os.ReadFile(labReferenceComposePath(t))
	if err != nil {
		t.Fatalf("read lab compose: %v", err)
	}
	var compose labCompose
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatalf("decode lab compose: %v", err)
	}
	return compose, string(data)
}

func TestLabReferenceComposeDeclaresExactlyThreeNodesAndNothingElse(t *testing.T) {
	compose, raw := loadLabReferenceCompose(t)
	if len(compose.Services) != 3 {
		t.Fatalf("services = %d, want exactly wk-node1..3", len(compose.Services))
	}
	if compose.Name != "${WK_LAB_PROJECT:?WK_LAB_PROJECT is required}" {
		t.Fatalf("project name = %q, want a required interpolation", compose.Name)
	}
	if compose.Volumes != nil || compose.Networks != nil {
		t.Fatalf("no top-level volumes or networks: %v %v", compose.Volumes, compose.Networks)
	}
	if strings.Contains(raw, ":-") {
		t.Fatal("no interpolation default: every path, port, CPU set and switch is named by the launcher")
	}
	for _, forbidden := range []string{"/var/lib/docker", "/tmp", "prometheus", "grafana", "wk-sim", "./docker/dev-cluster"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("lab compose must not mention %q", forbidden)
		}
	}
	for n := 1; n <= 3; n++ {
		name := "wk-node" + string(rune('0'+n))
		service, ok := compose.Services[name]
		if !ok {
			t.Fatalf("service %s missing", name)
		}
		if service.Image != "${WK_LAB_IMAGE:?WK_LAB_IMAGE is required}" || service.PullPolicy != "never" || service.Build != nil {
			t.Fatalf("%s runs only the frozen image: image %q pull_policy %q build %v", name, service.Image, service.PullPolicy, service.Build)
		}
		if service.Restart != "no" || service.StopSignal != "SIGTERM" || !strings.HasPrefix(service.StopGracePeriod, "${WK_LAB_STOP_GRACE:?") {
			t.Fatalf("%s shutdown is bounded and never restarts: %+v", name, service)
		}
		if service.User != "${WK_LAB_CONTAINER_USER:?WK_LAB_CONTAINER_USER is required}" {
			t.Fatalf("%s user = %q", name, service.User)
		}
		cpusetVar := "${WK_LAB_NODE" + string(rune('0'+n)) + "_CPUSET:?"
		if !strings.HasPrefix(service.Cpuset, cpusetVar) || !strings.HasPrefix(service.MemLimit, "${WK_LAB_NODE_MEMORY:?") {
			t.Fatalf("%s cpuset %q mem_limit %q must be named by the launcher", name, service.Cpuset, service.MemLimit)
		}
		if service.Environment["WK_METRICS_ENABLE"] != "${WK_LAB_METRICS_ENABLE:?WK_LAB_METRICS_ENABLE is required}" {
			t.Fatalf("%s metrics switch = %q", name, service.Environment["WK_METRICS_ENABLE"])
		}
		tcpVar := "WK_LAB_NODE" + string(rune('0'+n)) + "_TCP_PORT"
		if service.Environment["WK_EXTERNAL_TCPADDR"] != "127.0.0.1:${"+tcpVar+":?"+tcpVar+" is required}" {
			t.Fatalf("%s external tcp addr = %q", name, service.Environment["WK_EXTERNAL_TCPADDR"])
		}
		if len(service.Environment) != 2 {
			t.Fatalf("%s environment carries only the two launcher switches: %v", name, service.Environment)
		}
		wantVolumes := []string{
			"${WK_LAB_CONF_DIR:?WK_LAB_CONF_DIR is required}/node" + string(rune('0'+n)) + ".toml:/etc/wukongim/wukongim.toml:ro",
			"${WK_LAB_ROOT:?WK_LAB_ROOT is required}/node" + string(rune('0'+n)) + "/data:/var/lib/wukongim",
			"${WK_LAB_ROOT:?WK_LAB_ROOT is required}/node" + string(rune('0'+n)) + "/logs:/var/log/wukongim",
		}
		if strings.Join(service.Volumes, "\n") != strings.Join(wantVolumes, "\n") {
			t.Fatalf("%s volumes = %v, want exactly the read-only config and the two bind-mounted roots", name, service.Volumes)
		}
		if len(service.Tmpfs) != 1 || service.Tmpfs[0] != "/run/wukongim:size=16m" {
			t.Fatalf("%s tmpfs = %v", name, service.Tmpfs)
		}
		apiVar := "WK_LAB_NODE" + string(rune('0'+n)) + "_API_PORT"
		wantPorts := []string{
			"127.0.0.1:${" + apiVar + ":?" + apiVar + " is required}:5001",
			"127.0.0.1:${" + tcpVar + ":?" + tcpVar + " is required}:5100",
		}
		if strings.Join(service.Ports, "\n") != strings.Join(wantPorts, "\n") {
			t.Fatalf("%s ports = %v, want loopback-only API and WKProto endpoints", name, service.Ports)
		}
		if service.DependsOn != nil || service.Profiles != nil || service.Logging != nil {
			t.Fatalf("%s carries no dependency, profile or logging override", name)
		}
	}
}

func loadLabReferenceNodeConfig(t *testing.T, node string) app.Config {
	t.Helper()
	cfg, err := productconfig.Load(productconfig.Options{
		Args:    []string{"-config", filepath.Join(dockerRepoRoot(t), "docker", "lab", "reference", "conf", node)},
		Environ: []string{"PATH=" + os.Getenv("PATH")},
	})
	if err != nil {
		t.Fatalf("load lab %s: %v", node, err)
	}
	return cfg
}

func TestLabReferenceNodeConfigsMatchTheDevelopmentProfileExceptLabPaths(t *testing.T) {
	for n, node := range []string{"node1.toml", "node2.toml", "node3.toml"} {
		t.Run(node, func(t *testing.T) {
			lab := loadLabReferenceNodeConfig(t, node)
			dev := loadDockerNodeConfig(t, node)
			if lab.Cluster.Slots.HashSlotCount != dev.Cluster.Slots.HashSlotCount || lab.Cluster.Slots.InitialSlotCount != dev.Cluster.Slots.InitialSlotCount || lab.Cluster.Slots.ReplicaCount != dev.Cluster.Slots.ReplicaCount {
				t.Fatalf("%s slot topology differs from the development profile", node)
			}
			if lab.Cluster.Channel.AppendBatchMaxRecords != dev.Cluster.Channel.AppendBatchMaxRecords || lab.Cluster.Channel.AppendBatchMaxWait != dev.Cluster.Channel.AppendBatchMaxWait {
				t.Fatalf("%s append batching differs from the development profile", node)
			}
			if lab.Cluster.Storage.CommitFlushWindow != dev.Cluster.Storage.CommitFlushWindow || lab.Cluster.Storage.CommitMaxRequests != dev.Cluster.Storage.CommitMaxRequests || lab.Cluster.Storage.CommitMaxRecords != dev.Cluster.Storage.CommitMaxRecords || lab.Cluster.Storage.CommitMaxBytes != dev.Cluster.Storage.CommitMaxBytes || lab.Cluster.Storage.CommitShards != dev.Cluster.Storage.CommitShards {
				t.Fatalf("%s commit coordination differs from the development profile", node)
			}
			if lab.Gateway.Runtime.AsyncSendWorkers != dev.Gateway.Runtime.AsyncSendWorkers || lab.Gateway.Transport.Gnet.NumEventLoop != dev.Gateway.Transport.Gnet.NumEventLoop || lab.Gateway.Transport.Gnet.Multicore != dev.Gateway.Transport.Gnet.Multicore {
				t.Fatalf("%s gateway runtime differs from the development profile", node)
			}
			if !lab.Bench.APIEnabled || !lab.Observability.MetricsEnabled || !lab.Gateway.TokenAuthOn {
				t.Fatalf("%s needs the bench preparation API, metrics (the launcher toggles them) and token auth: %+v %+v", node, lab.Bench, lab.Observability.MetricsEnabled)
			}
			if lab.NodeID != uint64(n+1) || lab.DataDir != "/var/lib/wukongim" || lab.Log.Dir != "/var/log/wukongim" || lab.Plugin.SocketPath != "/run/wukongim/plugin.sock" {
				t.Fatalf("%s paths = data %q log %q plugin %q id %d", node, lab.DataDir, lab.Log.Dir, lab.Plugin.SocketPath, lab.NodeID)
			}
			if pathIsWithin(lab.DataDir, lab.Log.Dir) || pathIsWithin(lab.DataDir, lab.Plugin.SocketPath) {
				t.Fatalf("%s log dir and plugin socket live outside the data root", node)
			}
			if lab.Observability.Prometheus.QueryBaseURL != "" || len(lab.Observability.Diagnostics.DebugMatches) != 0 || dev.Observability.Prometheus.QueryBaseURL == "" {
				t.Fatalf("%s must not point at a Prometheus service or match a debug channel: %q %d", node, lab.Observability.Prometheus.QueryBaseURL, len(lab.Observability.Diagnostics.DebugMatches))
			}
			_ = dev
		})
	}
}

func TestLabReferenceComposeConfigFailsWithoutALaboratoryRoot(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker CLI is not installed here; the interpolation contract is exercised on the laboratory host")
	}
	if out, err := exec.Command(docker, "compose", "version").CombinedOutput(); err != nil {
		t.Skipf("docker compose is unavailable here: %v %s", err, out)
	}
	compose := labReferenceComposePath(t)
	full := []string{
		"WK_LAB_PROJECT=wk-ref-test", "WK_LAB_IMAGE=wukongim-lab:test", "WK_LAB_CONTAINER_USER=0:0", "WK_LAB_STOP_GRACE=30s",
		"WK_LAB_NODE1_CPUSET=0-3", "WK_LAB_NODE2_CPUSET=4-7", "WK_LAB_NODE3_CPUSET=8-11", "WK_LAB_NODE_MEMORY=8g", "WK_LAB_METRICS_ENABLE=false",
		"WK_LAB_NODE1_API_PORT=25001", "WK_LAB_NODE2_API_PORT=25002", "WK_LAB_NODE3_API_PORT=25003",
		"WK_LAB_NODE1_TCP_PORT=25100", "WK_LAB_NODE2_TCP_PORT=25101", "WK_LAB_NODE3_TCP_PORT=25102",
		"WK_LAB_CONF_DIR=/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/sources/wukongim/docker/lab/reference/conf",
		"WK_LAB_ROOT=/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/runs/wukongim-common-150-seq06",
	}
	render := func(env []string) (string, error) {
		cmd := exec.Command(docker, "compose", "-f", compose, "config")
		// The compose plugin is discovered below the real HOME; only the lab variables vary.
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, env...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	rendered, err := render(full)
	if err != nil {
		t.Fatalf("a fully named laboratory root renders: %v\n%s", err, rendered)
	}
	for _, want := range []string{"/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison/runs/wukongim-common-150-seq06/node1/data", "cpuset: 0-3", "127.0.0.1:25100", "WK_METRICS_ENABLE: \"false\""} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered config lacks %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "/var/lib/docker") || strings.Contains(rendered, "\nvolumes:\n") {
		t.Fatalf("rendered config must not use daemon-default storage or named volumes:\n%s", rendered)
	}
	for _, missing := range []string{"WK_LAB_ROOT", "WK_LAB_CONF_DIR", "WK_LAB_METRICS_ENABLE", "WK_LAB_NODE2_CPUSET"} {
		var env []string
		for _, entry := range full {
			if !strings.HasPrefix(entry, missing+"=") {
				env = append(env, entry)
			}
		}
		out, err := render(env)
		if err == nil || !strings.Contains(out, missing) {
			t.Fatalf("config without %s must fail and name it: err %v\n%s", missing, err, out)
		}
	}
}

package reference

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// DocumentSchema is the shared comparison document schema owned by ech0.
	DocumentSchema = "ech0-wukongim-reference-comparison/1"
	// ProvenanceSchema names the launcher-supplied provenance file.
	ProvenanceSchema = "wukongim-reference-provenance/1"
)

// Provenance is what the launcher knows and the run does not: identities,
// host, deployment, isolation, host-side resource samples, restart, cleanup.
// Every section is carried in the comparison document's exact shape.
type Provenance struct {
	Schema        string          `json:"schema"`
	Campaign      json.RawMessage `json:"campaign"`
	Source        json.RawMessage `json:"source"`
	Build         json.RawMessage `json:"build"`
	Host          json.RawMessage `json:"host"`
	Deployment    ProvDeployment  `json:"deployment"`
	Isolation     json.RawMessage `json:"isolation"`
	Resources     ProvResources   `json:"resources"`
	Restart       json.RawMessage `json:"restart"`
	Cleanup       json.RawMessage `json:"cleanup"`
	IdentitySeed  string          `json:"identity_seed"`
	LaunchedAtUTC string          `json:"launched_at_utc"`
	DeadlineMs    uint64          `json:"deadline_ms"`
}

// ProvDeployment is the launcher-owned part of the deployment section.
type ProvDeployment struct {
	RoleQuotas          json.RawMessage `json:"role_quotas"`
	Topology            json.RawMessage `json:"topology"`
	ConfigurationSha256 json.RawMessage `json:"configuration_sha256"`
	Docker              json.RawMessage `json:"docker"`
}

// ProvResources is the launcher's host-side resource sampling.
type ProvResources struct {
	SampleIntervalMs uint64          `json:"sample_interval_ms"`
	Roles            json.RawMessage `json:"roles"`
	Disk             json.RawMessage `json:"disk"`
	Network          json.RawMessage `json:"network"`
	LogVolumeBytes   json.RawMessage `json:"log_volume_bytes"`
	DockerDaemon     json.RawMessage `json:"docker_daemon"`
}

// WorkloadDocument is the workload section with the campaign seed.
type WorkloadDocument struct {
	Family                          string      `json:"family"`
	Connections                     uint64      `json:"connections"`
	Senders                         uint64      `json:"senders"`
	Recipients                      uint64      `json:"recipients"`
	PersonChannelsMax               uint64      `json:"person_channels_max"`
	PayloadBytes                    uint64      `json:"payload_bytes"`
	OperationsInFlightPerConnection uint64      `json:"operations_in_flight_per_connection"`
	IdentitySeed                    string      `json:"identity_seed"`
	Endpoints                       uint64      `json:"endpoints"`
	Phases                          []PhaseJSON `json:"phases"`
	MeasuredPhase                   string      `json:"measured_phase"`
}

// LifecycleDocument is the lifecycle section.
type LifecycleDocument struct {
	LaunchedAtUTC   string  `json:"launched_at_utc"`
	StartedAtUTC    string  `json:"started_at_utc"`
	MeasuredStartMs uint64  `json:"measured_window_start_offset_ms"`
	MeasuredMs      uint64  `json:"measured_window_ms"`
	EndedAtUTC      string  `json:"ended_at_utc"`
	ElapsedMs       uint64  `json:"elapsed_ms"`
	DeadlineMs      uint64  `json:"deadline_ms"`
	ExitCode        int64   `json:"exit_code"`
	DeclaredStatus  string  `json:"declared_status"`
	DeclaredFailure *string `json:"declared_failure"`
}

// AccountingDocument is the accounting section (the run's ledgers only).
type AccountingDocument struct {
	Phases                []PhaseCountsJSON `json:"phases"`
	Total                 Counts            `json:"total"`
	Fresh                 uint64            `json:"fresh"`
	Retries               uint64            `json:"retries"`
	Resubmitted           uint64            `json:"resubmitted"`
	UnissuedByReason      map[string]uint64 `json:"unissued_by_reason"`
	RejectedByReason      map[string]uint64 `json:"rejected_by_reason"`
	TimedOutByReason      map[string]uint64 `json:"timed_out_by_reason"`
	IndeterminateByReason map[string]uint64 `json:"indeterminate_by_reason"`
	ReasonMapping         map[string]string `json:"reason_mapping"`
	GeneratorLimited      uint64            `json:"generator_limited"`
	MaxDispatchLagMs      uint64            `json:"max_dispatch_lag_ms"`
}

// OnlineDocument is the online_delivery section.
type OnlineDocument struct {
	Observed                bool   `json:"observed"`
	Received                uint64 `json:"received"`
	ReceiveAcknowledged     uint64 `json:"receive_acknowledged"`
	Distinct                uint64 `json:"distinct"`
	Duplicates              uint64 `json:"duplicates"`
	MessagesWithoutDelivery uint64 `json:"messages_without_delivery"`
	Withheld                uint64 `json:"withheld"`
	Complete                bool   `json:"complete"`
}

type campaignView struct {
	RatePerSecond  uint64 `json:"rate_per_second"`
	LaunchSequence uint32 `json:"launch_sequence"`
	PointKind      string `json:"point_kind"`
}

func nonEmpty(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null"
}

func rawOrNull(raw json.RawMessage) json.RawMessage {
	if !nonEmpty(raw) {
		return json.RawMessage("null")
	}
	return raw
}

// Emit assembles the comparison document from the run result and the
// launcher's provenance, in the schema's exact top-level shape. Nothing from
// the run's identities, payloads or tokens enters the document.
func Emit(run *RunResult, provenance Provenance) (map[string]any, error) {
	if run == nil {
		return nil, errors.New("emit: run result is required")
	}
	if provenance.Schema != ProvenanceSchema {
		return nil, fmt.Errorf("emit: provenance schema %q is not %q", provenance.Schema, ProvenanceSchema)
	}
	for name, raw := range map[string]json.RawMessage{
		"campaign": provenance.Campaign, "source": provenance.Source, "build": provenance.Build, "host": provenance.Host,
		"deployment.role_quotas": provenance.Deployment.RoleQuotas, "deployment.topology": provenance.Deployment.Topology,
		"deployment.configuration_sha256": provenance.Deployment.ConfigurationSha256, "deployment.docker": provenance.Deployment.Docker,
		"isolation": provenance.Isolation, "resources.roles": provenance.Resources.Roles, "resources.disk": provenance.Resources.Disk,
		"resources.network": provenance.Resources.Network, "resources.log_volume_bytes": provenance.Resources.LogVolumeBytes,
		"resources.docker_daemon": provenance.Resources.DockerDaemon, "restart": provenance.Restart, "cleanup": provenance.Cleanup,
	} {
		if !nonEmpty(raw) {
			return nil, fmt.Errorf("emit: provenance lacks %s; it is never zero-filled", name)
		}
	}
	if provenance.IdentitySeed == "" || provenance.LaunchedAtUTC == "" || provenance.DeadlineMs == 0 {
		return nil, errors.New("emit: provenance lacks identity_seed, launched_at_utc or deadline_ms")
	}
	var campaign campaignView
	if err := json.Unmarshal(provenance.Campaign, &campaign); err != nil {
		return nil, fmt.Errorf("emit: campaign does not decode: %w", err)
	}
	if campaign.RatePerSecond != run.RatePerSecond {
		return nil, fmt.Errorf("emit: provenance rate %d/s differs from the run's %d/s", campaign.RatePerSecond, run.RatePerSecond)
	}
	kind := strings.ReplaceAll(campaign.PointKind, "_", "-")
	if kind == "" {
		return nil, errors.New("emit: campaign.point_kind is required")
	}
	sampleInterval := provenance.Resources.SampleIntervalMs
	if sampleInterval == 0 {
		sampleInterval = 1000
	}
	document := map[string]any{
		"schema":      DocumentSchema,
		"document_id": fmt.Sprintf("wukongim-%s-%d-seq%02d", kind, campaign.RatePerSecond, campaign.LaunchSequence),
		"product":     "wukongim",
		"campaign":    provenance.Campaign,
		"source":      provenance.Source,
		"build":       provenance.Build,
		"host":        provenance.Host,
		"deployment": map[string]any{
			"shape":                "docker_containers",
			"nodes":                uint64(3),
			"replication_factor":   uint64(3),
			"gateway_endpoints":    run.Workload.Endpoints,
			"role_quotas":          provenance.Deployment.RoleQuotas,
			"topology":             provenance.Deployment.Topology,
			"configuration_sha256": provenance.Deployment.ConfigurationSha256,
			"docker":               provenance.Deployment.Docker,
		},
		"workload": WorkloadDocument{
			Family:                          run.Workload.Family,
			Connections:                     run.Workload.Connections,
			Senders:                         run.Workload.Senders,
			Recipients:                      run.Workload.Recipients,
			PersonChannelsMax:               run.Workload.PersonChannelsMax,
			PayloadBytes:                    run.Workload.PayloadBytes,
			OperationsInFlightPerConnection: run.Workload.OperationsInFlightPerConnection,
			IdentitySeed:                    provenance.IdentitySeed,
			Endpoints:                       run.Workload.Endpoints,
			Phases:                          run.Workload.Phases,
			MeasuredPhase:                   run.Workload.MeasuredPhase,
		},
		"lifecycle": LifecycleDocument{
			LaunchedAtUTC:   provenance.LaunchedAtUTC,
			StartedAtUTC:    run.Lifecycle.StartedAtUTC,
			MeasuredStartMs: run.Lifecycle.MeasuredStartMs,
			MeasuredMs:      run.Lifecycle.MeasuredMs,
			EndedAtUTC:      run.Lifecycle.EndedAtUTC,
			ElapsedMs:       run.Lifecycle.ElapsedMs,
			DeadlineMs:      provenance.DeadlineMs,
			ExitCode:        run.Lifecycle.ExitCode,
			DeclaredStatus:  run.Lifecycle.DeclaredStatus,
			DeclaredFailure: nil,
		},
		"accounting": AccountingDocument{
			Phases:                run.Accounting.Phases,
			Total:                 run.Accounting.Total,
			Fresh:                 run.Accounting.Fresh,
			Retries:               run.Accounting.Retries,
			Resubmitted:           run.Accounting.Resubmitted,
			UnissuedByReason:      run.Accounting.UnissuedByReason,
			RejectedByReason:      run.Accounting.RejectedByReason,
			TimedOutByReason:      run.Accounting.TimedOutByReason,
			IndeterminateByReason: run.Accounting.IndeterminateByReason,
			ReasonMapping:         run.Accounting.ReasonMapping,
			GeneratorLimited:      run.Accounting.GeneratorLimited,
			MaxDispatchLagMs:      run.Accounting.MaxDispatchLagMs,
		},
		"latency": run.Latency,
		"history": run.History,
		"online_delivery": OnlineDocument{
			Observed:                run.Online.Observed,
			Received:                run.Online.Received,
			ReceiveAcknowledged:     run.Online.ReceiveAcknowledged,
			Distinct:                run.Online.Distinct,
			Duplicates:              run.Online.Duplicates,
			MessagesWithoutDelivery: run.Online.MessagesWithoutDelivery,
			Withheld:                run.Online.Withheld,
			Complete:                run.Online.Complete,
		},
		"resources": map[string]any{
			"sample_interval_ms": sampleInterval,
			"roles":              provenance.Resources.Roles,
			"disk":               provenance.Resources.Disk,
			"network":            provenance.Resources.Network,
			"log_volume_bytes":   provenance.Resources.LogVolumeBytes,
			"docker_daemon":      provenance.Resources.DockerDaemon,
		},
		"physical_work": run.Work,
		"native": map[string]any{
			"ech0_native": nil,
			"wukongim_native": map[string]any{
				"reason_codes": map[string]any{
					"rejected":      run.Accounting.RejectedByReason,
					"timed_out":     run.Accounting.TimedOutByReason,
					"indeterminate": run.Accounting.IndeterminateByReason,
					"unissued":      run.Accounting.UnissuedByReason,
				},
				"counters":          run.Native,
				"process_resources": run.Process,
				"drained_in_flight": run.Accounting.DrainedInFlight,
				"ledger_dropped":    run.Accounting.LedgerDropped,
				"online": map[string]any{
					"unexpected":       run.Online.Unexpected,
					"wrong_sender":     run.Online.WrongSender,
					"payload_mismatch": run.Online.PayloadMismatch,
					"dropped":          run.Online.Dropped,
				},
				"observation_notes": run.Observation.Notes,
				"run_schema":        run.Schema,
			},
		},
		"observation": run.Observation,
		"restart":     rawOrNull(provenance.Restart),
		"isolation":   provenance.Isolation,
		"cleanup":     provenance.Cleanup,
	}
	return document, nil
}

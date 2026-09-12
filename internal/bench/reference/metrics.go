package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	benchmetrics "github.com/WuKongIM/WuKongIM/internal/bench/metrics"
)

const (
	defaultScrapeBound   = 16 * 1024 * 1024
	defaultScrapeTimeout = 30 * time.Second

	familyCommitStageDuration = "wukongim_storage_commit_batch_duration_seconds"
	familyCommitRecords       = "wukongim_storage_commit_batch_records"
	familyCommitBytes         = "wukongim_storage_commit_batch_bytes"
	familyCommitRequests      = "wukongim_storage_commit_batch_requests"
	familyAppendBatchRecords  = "wukongim_channelv2_append_batch_records"
	familyReplicationStage    = "wukongim_channelv2_replication_stage_total"
	familyProcessCPU          = "process_cpu_seconds_total"
	familyProcessRSS          = "process_resident_memory_bytes"

	fsyncUnavailable = "no fsync counter is exposed; every grouped commit calls Commit(true) (pkg/db/internal/commit/coordinator.go:221), so physical_commits bounds the fsync count without inferring it"
)

// Snapshot is one parsed /metrics exposition.
type Snapshot = benchmetrics.PrometheusSnapshot

// NodeScrapes pairs the before and after scrapes of one node.
type NodeScrapes struct {
	Node   string
	Before Snapshot
	After  Snapshot
}

// BatchStat counts batches with their optional record and byte totals.
type BatchStat struct {
	Count   uint64  `json:"count"`
	Records *uint64 `json:"records"`
	Bytes   *uint64 `json:"bytes"`
}

// Distribution is one histogram delta with explicit bounds and an open last bucket.
type Distribution struct {
	Unit    string   `json:"unit"`
	Bounds  []uint64 `json:"bounds"`
	Buckets []uint64 `json:"buckets"`
	Count   uint64   `json:"count"`
	// Max is the largest finite bound whose bucket holds samples (0 when only
	// the open bucket does): a bound, not an exact maximum.
	Max uint64 `json:"max"`
	Sum uint64 `json:"sum"`
}

// MeasuredBatch is a batch statistic the run measured or explicitly could not.
type MeasuredBatch struct {
	Value       BatchStat
	Unavailable string
}

// MarshalJSON encodes the value, or `{"unavailable": reason}`.
func (m MeasuredBatch) MarshalJSON() ([]byte, error) {
	if m.Unavailable != "" {
		return json.Marshal(map[string]string{"unavailable": m.Unavailable})
	}
	return json.Marshal(m.Value)
}

// UnmarshalJSON decodes a value or `{"unavailable": reason}`.
func (m *MeasuredBatch) UnmarshalJSON(data []byte) error {
	if reason, ok, err := unavailableReason(data); err != nil || ok {
		m.Unavailable = reason
		return err
	}
	m.Unavailable = ""
	return json.Unmarshal(data, &m.Value)
}

// MeasuredDistribution is a distribution the run measured or explicitly could not.
type MeasuredDistribution struct {
	Value       Distribution
	Unavailable string
}

// MarshalJSON encodes the value, or `{"unavailable": reason}`.
func (m MeasuredDistribution) MarshalJSON() ([]byte, error) {
	if m.Unavailable != "" {
		return json.Marshal(map[string]string{"unavailable": m.Unavailable})
	}
	return json.Marshal(m.Value)
}

// UnmarshalJSON decodes a value or `{"unavailable": reason}`.
func (m *MeasuredDistribution) UnmarshalJSON(data []byte) error {
	if reason, ok, err := unavailableReason(data); err != nil || ok {
		m.Unavailable = reason
		return err
	}
	m.Unavailable = ""
	return json.Unmarshal(data, &m.Value)
}

// unavailableReason reports whether data is exactly `{"unavailable": reason}`.
func unavailableReason(data []byte) (string, bool, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return "", false, err
	}
	raw, ok := probe["unavailable"]
	if !ok {
		return "", false, nil
	}
	if len(probe) != 1 {
		return "", false, errors.New("measured value: unavailable must be the only key")
	}
	var reason string
	if err := json.Unmarshal(raw, &reason); err != nil {
		return "", false, err
	}
	if reason == "" {
		return "", false, errors.New("measured value: unavailable needs a reason")
	}
	return reason, true, nil
}

// PhysicalWork mirrors the comparison document's physical_work object.
type PhysicalWork struct {
	ProposalBatches          MeasuredBatch        `json:"proposal_batches"`
	RecordsPerProposal       MeasuredDistribution `json:"records_per_proposal"`
	LogicalCommitBatches     MeasuredBatch        `json:"logical_commit_batches"`
	PhysicalCommits          MeasuredBatch        `json:"physical_commits"`
	RecordsPerPhysicalCommit MeasuredDistribution `json:"records_per_physical_commit"`
	BytesPerPhysicalCommit   MeasuredDistribution `json:"bytes_per_physical_commit"`
	Fsyncs                   MeasuredBatch        `json:"fsyncs"`
	ReplicationRPCs          MeasuredBatch        `json:"replication_rpcs"`
	BatchCollectionDelayUs   MeasuredDistribution `json:"batch_collection_delay_us"`
}

// ProcessResource is the node's own view of its CPU and memory.
type ProcessResource struct {
	CPUMs       uint64 `json:"cpu_ms"`
	MaxRSSBytes uint64 `json:"max_rss_bytes"`
}

// nativeAllowlist is the fixed set of families the native object may carry;
// every value is a delta or a maximum keyed by a bounded label, never a node
// name or an identity.
var nativeAllowlist = map[string]bool{
	"wukongim_storage_commit_queue_depth":                       true,
	"wukongim_gateway_sendacks_total":                           true,
	"wukongim_gateway_async_send_queue_depth":                   true,
	"wukongim_channelappend_router_total":                       true,
	"wukongim_channelappend_router_group_inflight":              true,
	"wukongim_channelappend_writer_admission_depth":             true,
	"wukongim_channelappend_post_commit_handoff_depth":          true,
	"wukongim_channelappend_post_commit_retry_queue_depth":      true,
	"wukongim_channel_append_total":                             true,
	"wukongim_channelv2_reactor_mailbox_depth":                  true,
	"wukongim_channelv2_worker_queue_depth":                     true,
	"wukongim_channelv2_replication_stage_total":                true,
	"wukongim_delivery_retry_total":                             true,
	"wukongim_delivery_recipient_worker_queue_depth":            true,
	"wukongim_message_append_errors_total":                      true,
	"wukongim_storage_commit_batch_duration_seconds_count":      true,
	"wukongim_channelappend_router_backpressured_total":         true,
	"wukongim_channelappend_effect_pool_saturated_total":        true,
	"wukongim_gateway_messages_received_total":                  true,
	"wukongim_channelv2_append_wait_stage_duration_seconds_cnt": false,
}

// nativeGauges are reported as the maximum observed value; every other
// allowlisted family is reported as a delta between the scrapes.
var nativeGauges = map[string]bool{
	"wukongim_storage_commit_queue_depth":                  true,
	"wukongim_gateway_async_send_queue_depth":              true,
	"wukongim_channelappend_router_group_inflight":         true,
	"wukongim_channelappend_writer_admission_depth":        true,
	"wukongim_channelappend_post_commit_handoff_depth":     true,
	"wukongim_channelappend_post_commit_retry_queue_depth": true,
	"wukongim_channelv2_reactor_mailbox_depth":             true,
	"wukongim_channelv2_worker_queue_depth":                true,
	"wukongim_delivery_recipient_worker_queue_depth":       true,
}

// nativeKeyLabels are the bounded labels a native counter may be keyed by.
var nativeKeyLabels = []string{"reason", "stage", "result", "lane", "store", "class"}

// ParseScrape parses one Prometheus text exposition.
func ParseScrape(text []byte) (Snapshot, error) {
	return benchmetrics.ParsePrometheusText(bytes.NewReader(text))
}

// Scrape reads /metrics from one node API base address within bound bytes
// (default 16 MiB); a larger answer is refused rather than truncated.
func Scrape(ctx context.Context, apiAddr string, bound int64) (Snapshot, error) {
	if bound <= 0 {
		bound = defaultScrapeBound
	}
	url := strings.TrimRight(apiAddr, "/")
	if !strings.HasSuffix(url, "/metrics") {
		url += "/metrics"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Snapshot{}, err
	}
	client := &http.Client{Timeout: defaultScrapeTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return Snapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Snapshot{}, fmt.Errorf("metrics scrape: %s answered HTTP %d", apiAddr, resp.StatusCode)
	}
	encoded, err := io.ReadAll(io.LimitReader(resp.Body, bound+1))
	if err != nil {
		return Snapshot{}, err
	}
	if int64(len(encoded)) > bound {
		return Snapshot{}, fmt.Errorf("metrics scrape: %s exceeds the %d-byte bound", apiAddr, bound)
	}
	return ParseScrape(encoded)
}

type sampleKey struct {
	name string
	le   string
}

func matches(labels map[string]string, want map[string]string) bool {
	for key, value := range want {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// sum adds every sample of a family (and suffix) whose labels match, keyed by
// the `le` label so histogram buckets stay separate.
func sum(snapshot Snapshot, name string, want map[string]string) map[string]float64 {
	out := map[string]float64{}
	for _, sample := range snapshot.Samples {
		if sample.Name != name || !matches(sample.Labels, want) {
			continue
		}
		out[sample.Labels["le"]] += sample.Value
	}
	return out
}

func present(snapshot Snapshot, name string) bool {
	for _, sample := range snapshot.Samples {
		if sample.Name == name {
			return true
		}
	}
	return false
}

func nonNegative(value float64) uint64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return uint64(math.Round(value))
}

// counterDelta sums a counter family across nodes and returns after minus before.
func counterDelta(nodes []NodeScrapes, name string, want map[string]string) (uint64, bool) {
	var total float64
	found := false
	for _, node := range nodes {
		if !present(node.After, name) {
			continue
		}
		found = true
		total += sum(node.After, name, want)[""] - sum(node.Before, name, want)[""]
	}
	return nonNegative(total), found
}

// histogramDelta turns cumulative bucket deltas across nodes into one
// distribution with explicit finite bounds and an open last bucket.
func histogramDelta(nodes []NodeScrapes, family string, want map[string]string, unit string, scale float64) (Distribution, bool) {
	boundSet := map[float64]struct{}{}
	cumulative := map[float64]float64{}
	var open float64
	var count, total float64
	found := false
	for _, node := range nodes {
		if !present(node.After, family+"_count") {
			continue
		}
		found = true
		after := sum(node.After, family+"_bucket", want)
		before := sum(node.Before, family+"_bucket", want)
		for le, value := range after {
			delta := value - before[le]
			if le == "+Inf" {
				open += delta
				continue
			}
			bound, err := strconv.ParseFloat(le, 64)
			if err != nil {
				continue
			}
			boundSet[bound] = struct{}{}
			cumulative[bound] += delta
		}
		count += sum(node.After, family+"_count", want)[""] - sum(node.Before, family+"_count", want)[""]
		total += sum(node.After, family+"_sum", want)[""] - sum(node.Before, family+"_sum", want)[""]
	}
	if !found {
		return Distribution{}, false
	}
	bounds := make([]float64, 0, len(boundSet))
	for bound := range boundSet {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	distribution := Distribution{Unit: unit, Count: nonNegative(count), Sum: nonNegative(total * scale)}
	var previous float64
	for _, bound := range bounds {
		bucket := cumulative[bound] - previous
		previous = cumulative[bound]
		distribution.Bounds = append(distribution.Bounds, nonNegative(bound*scale))
		distribution.Buckets = append(distribution.Buckets, nonNegative(bucket))
		if bucket > 0 {
			distribution.Max = nonNegative(bound * scale)
		}
	}
	distribution.Buckets = append(distribution.Buckets, nonNegative(open-previous))
	return distribution, true
}

func unavailableBatch(reason string) MeasuredBatch {
	return MeasuredBatch{Unavailable: reason}
}

func unavailableDistribution(reason string) MeasuredDistribution {
	return MeasuredDistribution{Unavailable: reason}
}

// PhysicalWorkDelta derives the physical-work object from the runtime's own
// counters between two scrapes. Every family the nodes did not expose stays
// unavailable with its reason; nothing is derived from message counts.
func PhysicalWorkDelta(nodes []NodeScrapes) PhysicalWork {
	if len(nodes) == 0 {
		reason := "no scrape was taken"
		return PhysicalWork{
			ProposalBatches:          unavailableBatch(reason),
			RecordsPerProposal:       unavailableDistribution(reason),
			LogicalCommitBatches:     unavailableBatch(reason),
			PhysicalCommits:          unavailableBatch(reason),
			RecordsPerPhysicalCommit: unavailableDistribution(reason),
			BytesPerPhysicalCommit:   unavailableDistribution(reason),
			Fsyncs:                   unavailableBatch(reason),
			ReplicationRPCs:          unavailableBatch(reason),
			BatchCollectionDelayUs:   unavailableDistribution(reason),
		}
	}
	absent := func(family string) string {
		return fmt.Sprintf("%s was not present in the scraped exposition", family)
	}
	work := PhysicalWork{Fsyncs: unavailableBatch(fsyncUnavailable)}
	commit := map[string]string{"stage": "commit"}
	if commits, ok := counterDelta(nodes, familyCommitStageDuration+"_count", commit); ok {
		stat := BatchStat{Count: commits}
		if records, ok := counterDelta(nodes, familyCommitRecords+"_sum", nil); ok {
			stat.Records = &records
		}
		if bytesTotal, ok := counterDelta(nodes, familyCommitBytes+"_sum", nil); ok {
			stat.Bytes = &bytesTotal
		}
		work.PhysicalCommits = MeasuredBatch{Value: stat}
	} else {
		work.PhysicalCommits = unavailableBatch(absent(familyCommitStageDuration))
	}
	if requests, ok := counterDelta(nodes, familyCommitRequests+"_count", nil); ok {
		stat := BatchStat{Count: requests}
		if logical, ok := counterDelta(nodes, familyCommitRequests+"_sum", nil); ok {
			stat.Records = &logical
		}
		work.LogicalCommitBatches = MeasuredBatch{Value: stat}
	} else {
		work.LogicalCommitBatches = unavailableBatch(absent(familyCommitRequests))
	}
	if distribution, ok := histogramDelta(nodes, familyCommitRecords, nil, "records", 1); ok {
		work.RecordsPerPhysicalCommit = MeasuredDistribution{Value: distribution}
	} else {
		work.RecordsPerPhysicalCommit = unavailableDistribution(absent(familyCommitRecords))
	}
	if distribution, ok := histogramDelta(nodes, familyCommitBytes, nil, "bytes", 1); ok {
		work.BytesPerPhysicalCommit = MeasuredDistribution{Value: distribution}
	} else {
		work.BytesPerPhysicalCommit = unavailableDistribution(absent(familyCommitBytes))
	}
	if distribution, ok := histogramDelta(nodes, familyCommitStageDuration, map[string]string{"stage": "collect"}, "us", 1_000_000); ok {
		work.BatchCollectionDelayUs = MeasuredDistribution{Value: distribution}
	} else {
		work.BatchCollectionDelayUs = unavailableDistribution(absent(familyCommitStageDuration + `{stage="collect"}`))
	}
	if appends, ok := counterDelta(nodes, familyAppendBatchRecords+"_count", nil); ok {
		stat := BatchStat{Count: appends}
		if records, ok := counterDelta(nodes, familyAppendBatchRecords+"_sum", nil); ok {
			stat.Records = &records
		}
		work.ProposalBatches = MeasuredBatch{Value: stat}
		if distribution, ok := histogramDelta(nodes, familyAppendBatchRecords, nil, "records", 1); ok {
			work.RecordsPerProposal = MeasuredDistribution{Value: distribution}
		} else {
			work.RecordsPerProposal = unavailableDistribution(absent(familyAppendBatchRecords))
		}
	} else {
		reason := absent(familyAppendBatchRecords)
		work.ProposalBatches = unavailableBatch(reason)
		work.RecordsPerProposal = unavailableDistribution(reason)
	}
	if present(nodes[0].After, familyReplicationStage) {
		var exchanges uint64
		for _, stage := range []string{"peer_foreground_exchange", "peer_background_exchange"} {
			delta, _ := counterDelta(nodes, familyReplicationStage, map[string]string{"stage": stage})
			exchanges += delta
		}
		work.ReplicationRPCs = MeasuredBatch{Value: BatchStat{Count: exchanges}}
	} else {
		work.ReplicationRPCs = unavailableBatch(absent(familyReplicationStage) + " (the sampled stage histogram is not a count)")
	}
	return work
}

// NativeCounters projects the allowlisted families into a bounded object:
// deltas keyed by one bounded label for counters, maxima for gauges.
func NativeCounters(nodes []NodeScrapes) map[string]any {
	native := map[string]any{}
	for family, allowed := range nativeAllowlist {
		if !allowed {
			continue
		}
		found := false
		if nativeGauges[family] {
			var maximum float64
			for _, node := range nodes {
				for _, snapshot := range []Snapshot{node.Before, node.After} {
					for _, sample := range snapshot.Samples {
						if sample.Name != family {
							continue
						}
						found = true
						if sample.Value > maximum {
							maximum = sample.Value
						}
					}
				}
			}
			if found {
				native[family] = map[string]any{"max": maximum}
			}
			continue
		}
		entries := map[string]any{}
		for _, node := range nodes {
			byKey := map[string]float64{}
			collect := func(snapshot Snapshot, sign float64) {
				for _, sample := range snapshot.Samples {
					if sample.Name != family {
						continue
					}
					found = true
					key := "total"
					for _, label := range nativeKeyLabels {
						if value, ok := sample.Labels[label]; ok {
							key = label + "=" + value
							break
						}
					}
					byKey[key] += sign * sample.Value
				}
			}
			collect(node.After, 1)
			collect(node.Before, -1)
			for key, value := range byKey {
				current, _ := entries[key].(float64)
				entries[key] = current + value
			}
		}
		if found {
			native[family] = entries
		}
	}
	return native
}

// ProcessResources reads each node's own process CPU and RSS from its scrapes.
func ProcessResources(nodes []NodeScrapes) map[string]ProcessResource {
	out := map[string]ProcessResource{}
	for _, node := range nodes {
		cpu := sum(node.After, familyProcessCPU, nil)[""] - sum(node.Before, familyProcessCPU, nil)[""]
		var rss float64
		for _, snapshot := range []Snapshot{node.Before, node.After} {
			for _, sample := range snapshot.Samples {
				if sample.Name == familyProcessRSS && sample.Value > rss {
					rss = sample.Value
				}
			}
		}
		out[node.Node] = ProcessResource{CPUMs: nonNegative(cpu * 1000), MaxRSSBytes: nonNegative(rss)}
	}
	return out
}

var errNoNodes = errors.New("reference metrics: no node addresses")

// ScrapeAll scrapes every node API address into named node snapshots.
func ScrapeAll(ctx context.Context, apiAddrs []string) ([]Snapshot, error) {
	if len(apiAddrs) == 0 {
		return nil, errNoNodes
	}
	snapshots := make([]Snapshot, 0, len(apiAddrs))
	for _, addr := range apiAddrs {
		snapshot, err := Scrape(ctx, addr, 0)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

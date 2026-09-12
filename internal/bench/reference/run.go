package reference

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// RunSchema names the run-owned result document written by `wkcli bench reference run`.
const RunSchema = "wukongim-reference-run/1"

const (
	defaultRecvSettle   = 5 * time.Second
	historyReadWorkers  = 8
	instrumentationKind = "bench_metrics"
)

// Recipient owns one receiving connection.
type Recipient interface {
	UID() string
	Recv(ctx context.Context) (*frame.RecvPacket, error)
	RecvAck(ctx context.Context, messageID int64, messageSeq uint64) error
}

// Sessions owns every connection of one run and closes them together.
type Sessions interface {
	Senders() []Sender
	Recipients() []Recipient
	Close() error
}

// Scraper reads /metrics from every node.
type Scraper interface {
	ScrapeAll(ctx context.Context) ([]NodeScrapes, error)
}

// HistoryReader reads one person channel's committed history.
type HistoryReader interface {
	ReadPersonChannel(ctx context.Context, loginUID, peerUID string) ([]HistoryRow, uint64, error)
}

// RunConfig configures one reference run.
type RunConfig struct {
	Plan     *Plan
	Sessions Sessions
	// Scraper is nil when observation is disabled: nothing is scraped and
	// every physical-work field stays unavailable.
	Scraper    Scraper
	History    HistoryReader
	Clock      Clock
	RecvSettle time.Duration
	// Log receives bounded summary lines; never a per-message line.
	Log func(line string)
}

// Timeline locates the phases of the run.
type Timeline struct {
	StartedAtUTC        string        `json:"started_at_utc"`
	EndedAtUTC          string        `json:"ended_at_utc"`
	ElapsedMs           uint64        `json:"elapsed_ms"`
	MeasuredStartOffset time.Duration `json:"-"`
	MeasuredDuration    time.Duration `json:"-"`
	MeasuredStartMs     uint64        `json:"measured_window_start_offset_ms"`
	MeasuredMs          uint64        `json:"measured_window_ms"`
}

// PhaseJSON is one phase in the document's shape.
type PhaseJSON struct {
	Name          string `json:"name"`
	RatePerSecond uint64 `json:"rate_per_second"`
	DurationMs    uint64 `json:"duration_ms"`
}

// WorkloadJSON is the workload section in the document's shape.
type WorkloadJSON struct {
	Family                          string      `json:"family"`
	Connections                     uint64      `json:"connections"`
	Senders                         uint64      `json:"senders"`
	Recipients                      uint64      `json:"recipients"`
	PersonChannelsMax               uint64      `json:"person_channels_max"`
	PayloadBytes                    uint64      `json:"payload_bytes"`
	OperationsInFlightPerConnection uint64      `json:"operations_in_flight_per_connection"`
	Endpoints                       uint64      `json:"endpoints"`
	Phases                          []PhaseJSON `json:"phases"`
	MeasuredPhase                   string      `json:"measured_phase"`
}

// PhaseCountsJSON is one phase's accounting in the document's shape.
type PhaseCountsJSON struct {
	Name   string `json:"name"`
	Counts Counts `json:"counts"`
}

// AccountingJSON is the accounting section in the document's shape.
type AccountingJSON struct {
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
	DrainedInFlight       uint64            `json:"drained_in_flight"`
	LedgerDropped         uint64            `json:"ledger_dropped"`
}

// LatencyJSON is the latency section in the document's shape.
type LatencyJSON struct {
	ClockDomain string     `json:"clock_domain"`
	Measured    LatencySet `json:"measured"`
	AllPhases   LatencySet `json:"all_phases"`
}

// ObservationJSON is the observation section in the document's shape.
type ObservationJSON struct {
	InstrumentationEnabled bool     `json:"instrumentation_enabled"`
	Kind                   string   `json:"kind"`
	Notes                  []string `json:"notes"`
}

// LifecycleJSON is the run-owned half of the lifecycle section.
type LifecycleJSON struct {
	StartedAtUTC    string `json:"started_at_utc"`
	EndedAtUTC      string `json:"ended_at_utc"`
	ElapsedMs       uint64 `json:"elapsed_ms"`
	MeasuredStartMs uint64 `json:"measured_window_start_offset_ms"`
	MeasuredMs      uint64 `json:"measured_window_ms"`
	ExitCode        int64  `json:"exit_code"`
	DeclaredStatus  string `json:"declared_status"`
}

// RunResult is everything one run owns, bounded and identity-free.
type RunResult struct {
	Schema                 string                     `json:"schema"`
	RunID                  string                     `json:"run_id"`
	RatePerSecond          uint64                     `json:"rate_per_second"`
	InstrumentationEnabled bool                       `json:"instrumentation_enabled"`
	Timeline               Timeline                   `json:"timeline"`
	Workload               WorkloadJSON               `json:"workload"`
	Lifecycle              LifecycleJSON              `json:"lifecycle"`
	Accounting             AccountingJSON             `json:"accounting"`
	Latency                LatencyJSON                `json:"latency"`
	History                HistoryResult              `json:"history"`
	Online                 ReceiveSummary             `json:"online_delivery"`
	Work                   PhysicalWork               `json:"physical_work"`
	Native                 map[string]any             `json:"native"`
	Process                map[string]ProcessResource `json:"process_resources"`
	Observation            ObservationJSON            `json:"observation"`
	Schedule               *ScheduleResult            `json:"-"`
}

type scrapeSet struct {
	mu        sync.Mutex
	wg        sync.WaitGroup
	snapshots map[string][]NodeScrapes
	failures  []string
}

func (s *scrapeSet) take(ctx context.Context, scraper Scraper, label string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		nodes, err := scraper.ScrapeAll(ctx)
		s.mu.Lock()
		defer s.mu.Unlock()
		if err != nil {
			s.failures = append(s.failures, label+": "+err.Error())
			return
		}
		s.snapshots[label] = nodes
	}()
}

// window pairs the "after" snapshots of two labels into before/after node scrapes.
func (s *scrapeSet) window(from, to string) ([]NodeScrapes, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before, okBefore := s.snapshots[from]
	after, okAfter := s.snapshots[to]
	if !okBefore || !okAfter || len(before) != len(after) {
		return nil, false
	}
	nodes := make([]NodeScrapes, len(after))
	for i := range after {
		nodes[i] = NodeScrapes{Node: after[i].Node, Before: before[i].After, After: after[i].After}
	}
	return nodes, true
}

// Run executes one reference point: connect the recipient readers, drive the
// schedule, scrape at the phase boundaries when observation is enabled,
// settle receives, reconcile history for every acknowledged pair, close and
// join every session, and return the bounded run result.
func Run(ctx context.Context, cfg RunConfig) (*RunResult, error) {
	if cfg.Plan == nil || cfg.Sessions == nil || cfg.History == nil {
		return nil, errors.New("reference run: plan, sessions and history reader are required")
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.RecvSettle <= 0 {
		cfg.RecvSettle = defaultRecvSettle
	}
	logf := func(format string, args ...any) {
		if cfg.Log != nil {
			cfg.Log(fmt.Sprintf(format, args...))
		}
	}
	plan := cfg.Plan
	senders := cfg.Sessions.Senders()
	recipients := cfg.Sessions.Recipients()
	if len(senders) != SenderCount || len(recipients) != RecipientCount {
		return nil, fmt.Errorf("reference run: %d senders and %d recipients, want %d and %d", len(senders), len(recipients), SenderCount, RecipientCount)
	}
	ledger := NewReceiveLedger(plan.TotalScheduled() * 2)
	recvCtx, cancelRecv := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	for _, recipient := range recipients {
		readers.Add(1)
		go func(recipient Recipient) {
			defer readers.Done()
			for {
				recv, err := recipient.Recv(recvCtx)
				if err != nil {
					return
				}
				acked := recipient.RecvAck(recvCtx, recv.MessageID, recv.MessageSeq) == nil
				ledger.Record(recipient.UID(), recv, acked)
			}
		}(recipient)
	}
	scrapes := &scrapeSet{snapshots: map[string][]NodeScrapes{}}
	enabled := cfg.Scraper != nil
	if enabled {
		scrapes.take(ctx, cfg.Scraper, "start")
	}
	logf("run %s: %d connections, %d arrivals over %d phases, observation=%t", plan.Config.RunID, plan.Connections(), plan.TotalScheduled(), len(plan.Phases()), enabled)
	hooks := ScheduleHooks{
		OnPhaseStart: func(phase string) {
			logf("phase %s starts", phase)
			if enabled && (phase == phaseMeasure || phase == phaseReduction) {
				scrapes.take(ctx, cfg.Scraper, phase)
			}
		},
		OnDrained: func() {
			if enabled {
				scrapes.take(ctx, cfg.Scraper, "end")
			}
		},
	}
	schedule, scheduleErr := RunScheduleWithHooks(ctx, plan, senders, cfg.Clock, hooks)
	total := schedule.Total()
	logf("schedule done: scheduled=%d dispatched=%d unissued=%d acknowledged=%d rejected=%d timed_out=%d indeterminate=%d drained_in_flight=%d",
		total.Scheduled, total.Dispatched, total.Unissued, total.Acknowledged, total.Rejected, total.TimedOut, total.Indeterminate, schedule.DrainedInFlight)
	// Receives settle on the wall clock: a fake clock never delays delivery.
	time.Sleep(cfg.RecvSettle)
	cancelRecv()
	readers.Wait()
	scrapes.wg.Wait()

	expected := make([]ExpectedMessage, 0, total.Acknowledged)
	for _, record := range schedule.Records {
		if record.Class != ClassAcknowledged {
			continue
		}
		expected = append(expected, ExpectedMessage{
			ClientMsgNo:   record.ClientMsgNo,
			SenderUID:     record.SenderUID,
			RecipientUID:  record.RecipientUID,
			ChannelID:     record.ChannelID,
			PayloadDigest: record.PayloadDigest,
			MessageID:     record.MessageID,
			MessageSeq:    record.MessageSeq,
			Phase:         record.Phase,
		})
	}
	online := ledger.Summary(expected)
	logf("online delivery: received=%d acknowledged=%d distinct=%d duplicates=%d without_delivery=%d unexpected=%d dropped=%d",
		online.Received, online.ReceiveAcknowledged, online.Distinct, online.Duplicates, online.MessagesWithoutDelivery, online.Unexpected, online.Dropped)
	history := reconcileHistory(ctx, cfg.History, expected, logf)
	closeErr := cfg.Sessions.Close()
	if closeErr != nil {
		logf("session close reported: %v", closeErr)
	}

	work := PhysicalWork{}
	native := map[string]any{}
	process := map[string]ProcessResource{}
	notes := []string{}
	if enabled {
		if measured, ok := scrapes.window(phaseMeasure, phaseReduction); ok {
			work = PhysicalWorkDelta(measured)
			native = NativeCounters(measured)
		} else {
			work = PhysicalWorkDelta(nil)
			notes = append(notes, "the measured-window scrapes are incomplete; physical work is unavailable")
		}
		if whole, ok := scrapes.window("start", "end"); ok {
			process = ProcessResources(whole)
		}
		scrapes.mu.Lock()
		notes = append(notes, scrapes.failures...)
		scrapes.mu.Unlock()
	} else {
		reason := "observation disabled for this control root: no scrape was taken"
		work = PhysicalWork{
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
	if scheduleErr != nil {
		notes = append(notes, "the schedule was cancelled: "+scheduleErr.Error())
	}

	phases := make([]PhaseJSON, 0, 3)
	counts := make([]PhaseCountsJSON, 0, 3)
	for _, phase := range plan.Phases() {
		phases = append(phases, PhaseJSON{Name: phase.Name, RatePerSecond: phase.RatePerSecond, DurationMs: uint64(phase.Duration / time.Millisecond)})
		counts = append(counts, PhaseCountsJSON{Name: phase.Name, Counts: schedule.Phase(phase.Name)})
	}
	elapsed := schedule.EndedAt.Sub(schedule.StartedAt)
	window := plan.MeasuredWindow()
	status := "passed"
	if scheduleErr != nil {
		status = "interrupted"
	}
	result := &RunResult{
		Schema:                 RunSchema,
		RunID:                  plan.Config.RunID,
		RatePerSecond:          plan.RatePerSecond,
		InstrumentationEnabled: enabled,
		Timeline: Timeline{
			StartedAtUTC:        schedule.StartedAt.UTC().Format(time.RFC3339),
			EndedAtUTC:          schedule.EndedAt.UTC().Format(time.RFC3339),
			ElapsedMs:           uint64(elapsed / time.Millisecond),
			MeasuredStartOffset: window.Start,
			MeasuredDuration:    window.Duration,
			MeasuredStartMs:     uint64(window.Start / time.Millisecond),
			MeasuredMs:          uint64(window.Duration / time.Millisecond),
		},
		Workload: WorkloadJSON{
			Family:                          "person_send",
			Connections:                     uint64(plan.Connections()),
			Senders:                         SenderCount,
			Recipients:                      RecipientCount,
			PersonChannelsMax:               plan.PersonChannelsMax(),
			PayloadBytes:                    uint64(plan.PayloadBytes),
			OperationsInFlightPerConnection: 1,
			Endpoints:                       3,
			Phases:                          phases,
			MeasuredPhase:                   phaseMeasure,
		},
		Lifecycle: LifecycleJSON{
			StartedAtUTC:    schedule.StartedAt.UTC().Format(time.RFC3339),
			EndedAtUTC:      schedule.EndedAt.UTC().Format(time.RFC3339),
			ElapsedMs:       uint64(elapsed / time.Millisecond),
			MeasuredStartMs: uint64(window.Start / time.Millisecond),
			MeasuredMs:      uint64(window.Duration / time.Millisecond),
			ExitCode:        0,
			DeclaredStatus:  status,
		},
		Accounting: AccountingJSON{
			Phases:                counts,
			Total:                 total,
			Fresh:                 schedule.Fresh,
			Retries:               schedule.Retries,
			Resubmitted:           0,
			UnissuedByReason:      copyReasons(schedule.UnissuedByReason),
			RejectedByReason:      copyReasons(schedule.RejectedByReason),
			TimedOutByReason:      copyReasons(schedule.TimedOutByReason),
			IndeterminateByReason: copyReasons(schedule.IndeterminateByReason),
			ReasonMapping:         copyMapping(schedule.ReasonMapping),
			GeneratorLimited:      schedule.GeneratorLimited,
			MaxDispatchLagMs:      uint64(schedule.MaxDispatchLag / time.Millisecond),
			DrainedInFlight:       schedule.DrainedInFlight,
			LedgerDropped:         online.Dropped,
		},
		Latency: LatencyJSON{
			ClockDomain: "client-process-monotonic",
			Measured:    schedule.Latency(phaseMeasure),
			AllPhases:   schedule.Latency(allPhases),
		},
		History:     history,
		Online:      online,
		Work:        work,
		Native:      native,
		Process:     process,
		Observation: ObservationJSON{InstrumentationEnabled: enabled, Kind: instrumentationKind, Notes: notes},
		Schedule:    schedule,
	}
	return result, nil
}

func copyReasons(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyMapping(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type pair struct {
	sender    string
	recipient string
}

// reconcileHistory reads every acknowledged pair's channel and reconciles
// the rows against the acknowledged identities. Rows are read in the
// sender's view, so a peer-UID channel is normalized to the canonical pair.
func reconcileHistory(ctx context.Context, reader HistoryReader, expected []ExpectedMessage, logf func(string, ...any)) HistoryResult {
	seen := map[pair]struct{}{}
	pairs := make([]pair, 0, 64)
	for _, message := range expected {
		key := pair{sender: message.SenderUID, recipient: message.RecipientUID}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		pairs = append(pairs, key)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].sender != pairs[j].sender {
			return pairs[i].sender < pairs[j].sender
		}
		return pairs[i].recipient < pairs[j].recipient
	})
	var mu sync.Mutex
	var rows []HistoryRow
	var pages uint64
	var failures []string
	work := make(chan pair)
	var wg sync.WaitGroup
	for i := 0; i < historyReadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range work {
				read, count, err := reader.ReadPersonChannel(ctx, key.sender, key.recipient)
				canonical := channelid.EncodePersonChannel(key.sender, key.recipient)
				for i := range read {
					if read[i].ChannelType == frame.ChannelTypePerson {
						if normalized, err := channelid.NormalizePersonChannel(key.sender, read[i].ChannelID); err == nil {
							read[i].ChannelID = normalized
						}
					}
					if read[i].ChannelID == "" {
						read[i].ChannelID = canonical
					}
				}
				mu.Lock()
				rows = append(rows, read...)
				pages += count
				if err != nil {
					failures = append(failures, err.Error())
				}
				mu.Unlock()
			}
		}()
	}
	for _, key := range pairs {
		work <- key
	}
	close(work)
	wg.Wait()
	stats := HistoryReadStats{ChannelsRead: uint64(len(pairs)), PagesRead: pages, Complete: len(failures) == 0}
	if len(failures) > 0 {
		stats.IncompleteReason = fmt.Sprintf("%d channel read(s) failed; first: %s", len(failures), failures[0])
	}
	result := Reconcile(expected, rows, stats)
	logf("history: channels=%d pages=%d expected=%d matched=%d missing=%d duplicate=%d conflicting=%d wrong_channel=%d wrong_content=%d misordered=%d unexpected=%d complete=%t",
		result.ChannelsRead, result.PagesRead, result.Expected, result.Matched, result.Missing, result.Duplicate, result.Conflicting, result.WrongChannel, result.WrongContent, result.Misordered, result.UnexpectedRows, result.Complete)
	return result
}

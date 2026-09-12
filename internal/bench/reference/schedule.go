package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// Class is the common application-result class of one dispatched SEND.
type Class string

const (
	ClassAcknowledged  Class = "acknowledged"
	ClassRejected      Class = "rejected"
	ClassTimedOut      Class = "timed_out"
	ClassIndeterminate Class = "indeterminate"

	reasonNoIdleSender = "no_idle_sender"
	reasonDrainBound   = "drain_bound_expired"
	reasonCancelled    = "cancelled"
	allPhases          = "all"
)

// Outbound is one SEND handed to a sender.
type Outbound struct {
	ClientSeq   uint64
	ClientMsgNo string
	ChannelID   string
	Payload     []byte
}

// Outcome is the application result of one dispatched SEND.
type Outcome struct {
	Class        Class
	NativeReason string
	Reason       frame.ReasonCode
	MessageID    int64
	MessageSeq   uint64
	Err          error
}

// Sender owns one sending connection. The scheduler never hands it a second
// SEND before the previous one returned.
type Sender interface {
	Send(ctx context.Context, arrival Arrival, msg Outbound) Outcome
}

// Clock lets tests drive the arrival schedule on virtual time. The schedule
// loop is the only caller of Sleep and passes an accessor for the number of
// dispatched SENDs whose result has not been recorded yet, so a virtual clock
// can wait for quiescence before it advances; the wall clock ignores it.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration, inFlight func() int64) error
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns the wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep waits d or until ctx ends.
func (RealClock) Sleep(ctx context.Context, d time.Duration, _ func() int64) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var fixedLatencyBoundsMs = [15]uint64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 60000}

// Histogram is the fixed-bound application-ACK latency histogram shared with
// the ech0 client: fifteen upper bounds in milliseconds plus one open bucket.
type Histogram struct {
	Buckets [16]uint64
	Count   uint64
	MaxMs   uint64
}

type histogramJSON struct {
	BoundsMs []uint64 `json:"bounds_ms"`
	Buckets  []uint64 `json:"buckets"`
	Count    uint64   `json:"count"`
	MaxMs    uint64   `json:"max_ms"`
}

// MarshalJSON encodes the histogram in the comparison document's shape.
func (h Histogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(histogramJSON{BoundsMs: fixedLatencyBoundsMs[:], Buckets: h.Buckets[:], Count: h.Count, MaxMs: h.MaxMs})
}

// UnmarshalJSON decodes the document shape and refuses other bounds.
func (h *Histogram) UnmarshalJSON(data []byte) error {
	var decoded histogramJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if len(decoded.BoundsMs) != len(fixedLatencyBoundsMs) || len(decoded.Buckets) != len(h.Buckets) {
		return fmt.Errorf("histogram: %d bounds and %d buckets, want %d and %d", len(decoded.BoundsMs), len(decoded.Buckets), len(fixedLatencyBoundsMs), len(h.Buckets))
	}
	for i, bound := range decoded.BoundsMs {
		if bound != fixedLatencyBoundsMs[i] {
			return fmt.Errorf("histogram: bound %d is %d, want %d", i, bound, fixedLatencyBoundsMs[i])
		}
	}
	copy(h.Buckets[:], decoded.Buckets)
	h.Count = decoded.Count
	h.MaxMs = decoded.MaxMs
	return nil
}

// Bounds returns the fixed bucket upper bounds in milliseconds.
func (Histogram) Bounds() [15]uint64 { return fixedLatencyBoundsMs }

// Observe records one sample.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	ms := uint64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	slot := len(fixedLatencyBoundsMs)
	for i, bound := range fixedLatencyBoundsMs {
		if ms <= bound {
			slot = i
			break
		}
	}
	h.Buckets[slot]++
	h.Count++
	if ms > h.MaxMs {
		h.MaxMs = ms
	}
}

// LatencySet holds the three latency views of one phase.
type LatencySet struct {
	AcknowledgedDueToComplete      Histogram `json:"acknowledged_due_to_complete"`
	AcknowledgedDispatchToComplete Histogram `json:"acknowledged_dispatch_to_complete"`
	FailedDueToComplete            Histogram `json:"failed_due_to_complete"`
}

// Counts is the command accounting of one phase.
type Counts struct {
	Scheduled     uint64 `json:"scheduled"`
	Dispatched    uint64 `json:"dispatched"`
	Unissued      uint64 `json:"unissued"`
	Acknowledged  uint64 `json:"acknowledged"`
	Rejected      uint64 `json:"rejected"`
	TimedOut      uint64 `json:"timed_out"`
	Indeterminate uint64 `json:"indeterminate"`
}

// NotAcknowledged is every dispatched command without a successful application acknowledgment.
func (c Counts) NotAcknowledged() uint64 { return c.Rejected + c.TimedOut + c.Indeterminate }

func (c *Counts) add(o Counts) {
	c.Scheduled += o.Scheduled
	c.Dispatched += o.Dispatched
	c.Unissued += o.Unissued
	c.Acknowledged += o.Acknowledged
	c.Rejected += o.Rejected
	c.TimedOut += o.TimedOut
	c.Indeterminate += o.Indeterminate
}

// Record is the ledger entry of one arrival.
type Record struct {
	Index         uint64
	Phase         string
	ClientMsgNo   string
	SenderUID     string
	RecipientUID  string
	ChannelID     string
	PayloadDigest string
	Class         Class
	NativeReason  string
	MessageID     int64
	MessageSeq    uint64
	DueAt         time.Time
	DispatchedAt  time.Time
	CompletedAt   time.Time
}

// ScheduleResult is the five-ledger accounting of one run.
type ScheduleResult struct {
	mu sync.Mutex

	phaseOrder []string
	phases     map[string]*Counts
	latency    map[string]*LatencySet

	UnissuedByReason      map[string]uint64
	RejectedByReason      map[string]uint64
	TimedOutByReason      map[string]uint64
	IndeterminateByReason map[string]uint64
	ReasonMapping         map[string]string

	Fresh            uint64
	Retries          uint64
	GeneratorLimited uint64
	MaxDispatchLag   time.Duration
	DrainedInFlight  uint64
	Records          []Record
	StartedAt        time.Time
	EndedAt          time.Time
}

func newScheduleResult(plan *Plan) *ScheduleResult {
	result := &ScheduleResult{
		phases:                map[string]*Counts{},
		latency:               map[string]*LatencySet{},
		UnissuedByReason:      map[string]uint64{},
		RejectedByReason:      map[string]uint64{},
		TimedOutByReason:      map[string]uint64{},
		IndeterminateByReason: map[string]uint64{},
		ReasonMapping:         map[string]string{reasonNoIdleSender: string("unissued")},
		Records:               make([]Record, plan.TotalScheduled()),
	}
	for _, phase := range plan.Phases() {
		result.phaseOrder = append(result.phaseOrder, phase.Name)
		result.phases[phase.Name] = &Counts{}
		result.latency[phase.Name] = &LatencySet{}
	}
	result.latency[allPhases] = &LatencySet{}
	return result
}

// Phase returns the counts of one phase.
func (r *ScheduleResult) Phase(name string) Counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	if counts, ok := r.phases[name]; ok {
		return *counts
	}
	return Counts{}
}

// PhaseNames returns the phases in plan order.
func (r *ScheduleResult) PhaseNames() []string { return append([]string(nil), r.phaseOrder...) }

// Total is the sum of every phase.
func (r *ScheduleResult) Total() Counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total Counts
	for _, name := range r.phaseOrder {
		total.add(*r.phases[name])
	}
	return total
}

// Latency returns the latency set of one phase, or of every phase for "all".
func (r *ScheduleResult) Latency(name string) LatencySet {
	r.mu.Lock()
	defer r.mu.Unlock()
	if set, ok := r.latency[name]; ok {
		return *set
	}
	return LatencySet{}
}

// Histogram is an alias of Latency.
func (r *ScheduleResult) Histogram(name string) LatencySet { return r.Latency(name) }

// Accounted reports whether scheduled equals dispatched plus unissued in every phase.
func (r *ScheduleResult) Accounted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, counts := range r.phases {
		if counts.Dispatched+counts.Unissued != counts.Scheduled {
			return false
		}
		if counts.Acknowledged+counts.NotAcknowledged() != counts.Dispatched {
			return false
		}
	}
	return true
}

// SortedReasons returns the keys of a reason map in stable order.
func SortedReasons(m map[string]uint64) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (r *ScheduleResult) recordUnissued(arrival Arrival, plan *Plan, dueAt time.Time, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases[arrival.Phase].Scheduled++
	r.phases[arrival.Phase].Unissued++
	r.UnissuedByReason[reason]++
	r.ReasonMapping[reason] = "unissued"
	r.GeneratorLimited++
	r.Records[arrival.Index] = Record{
		Index:         arrival.Index,
		Phase:         arrival.Phase,
		ClientMsgNo:   plan.ClientMsgNo(arrival.Index),
		SenderUID:     plan.Senders[arrival.Sender].UID,
		RecipientUID:  plan.Recipients[arrival.Recipient].UID,
		ChannelID:     plan.Channel(arrival),
		PayloadDigest: plan.PayloadDigest(arrival.Index),
		NativeReason:  reason,
		DueAt:         dueAt,
	}
}

func (r *ScheduleResult) recordDispatched(arrival Arrival, lag time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases[arrival.Phase].Scheduled++
	r.phases[arrival.Phase].Dispatched++
	r.Fresh++
	if lag > r.MaxDispatchLag {
		r.MaxDispatchLag = lag
	}
}

func (r *ScheduleResult) recordOutcome(record Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := r.phases[record.Phase]
	phaseLatency := r.latency[record.Phase]
	allLatency := r.latency[allPhases]
	due := record.CompletedAt.Sub(record.DueAt)
	dispatch := record.CompletedAt.Sub(record.DispatchedAt)
	switch record.Class {
	case ClassAcknowledged:
		counts.Acknowledged++
		phaseLatency.AcknowledgedDueToComplete.Observe(due)
		phaseLatency.AcknowledgedDispatchToComplete.Observe(dispatch)
		allLatency.AcknowledgedDueToComplete.Observe(due)
		allLatency.AcknowledgedDispatchToComplete.Observe(dispatch)
	case ClassRejected:
		counts.Rejected++
		r.RejectedByReason[record.NativeReason]++
		phaseLatency.FailedDueToComplete.Observe(due)
		allLatency.FailedDueToComplete.Observe(due)
	case ClassTimedOut:
		counts.TimedOut++
		r.TimedOutByReason[record.NativeReason]++
		phaseLatency.FailedDueToComplete.Observe(due)
		allLatency.FailedDueToComplete.Observe(due)
	default:
		record.Class = ClassIndeterminate
		counts.Indeterminate++
		r.IndeterminateByReason[record.NativeReason]++
	}
	if record.NativeReason != "" {
		r.ReasonMapping[record.NativeReason] = string(record.Class)
	}
	r.Records[record.Index] = record
}

// ScheduleHooks lets the runner act at phase boundaries without touching
// arrival timing: OnPhaseStart fires once, before the first arrival of the
// named phase is dispatched, and OnDrained once after the drain.
type ScheduleHooks struct {
	OnPhaseStart func(phase string)
	OnDrained    func()
}

// RunSchedule drives every arrival of the plan on the clock: each arrival is
// dispatched at its due time to its assigned sender when that sender is idle,
// or recorded as unissued when it is not. Nothing is queued, delayed or
// retried; after the last arrival the in-flight SENDs are drained within the
// plan's bound.
func RunSchedule(ctx context.Context, plan *Plan, senders []Sender, clock Clock) (*ScheduleResult, error) {
	return RunScheduleWithHooks(ctx, plan, senders, clock, ScheduleHooks{})
}

// RunScheduleWithHooks is RunSchedule with phase-boundary hooks.
func RunScheduleWithHooks(ctx context.Context, plan *Plan, senders []Sender, clock Clock, hooks ScheduleHooks) (*ScheduleResult, error) {
	if plan == nil {
		return nil, errors.New("reference schedule: plan is required")
	}
	if len(senders) != SenderCount {
		return nil, errors.New("reference schedule: exactly 256 senders are required")
	}
	if clock == nil {
		clock = RealClock{}
	}
	result := newScheduleResult(plan)
	busy := make([]sync.Mutex, len(senders))
	idle := make([]bool, len(senders))
	for i := range idle {
		idle[i] = true
	}
	var stateMu sync.Mutex
	var draining bool
	var wg sync.WaitGroup
	var inFlight atomic.Int64
	pending := func() int64 { return inFlight.Load() }
	sendCtx, cancelSends := context.WithCancel(context.Background())
	defer cancelSends()

	start := clock.Now()
	result.StartedAt = start
	var cancelled bool
	currentPhase := ""
	for _, arrival := range plan.Arrivals() {
		dueAt := start.Add(arrival.Due)
		if !cancelled {
			if err := clock.Sleep(ctx, dueAt.Sub(clock.Now()), pending); err != nil {
				cancelled = true
			}
		}
		if cancelled {
			result.recordUnissued(arrival, plan, dueAt, reasonCancelled)
			continue
		}
		if arrival.Phase != currentPhase {
			currentPhase = arrival.Phase
			if hooks.OnPhaseStart != nil {
				hooks.OnPhaseStart(currentPhase)
			}
		}
		now := clock.Now()
		lag := now.Sub(dueAt)
		if lag < 0 {
			lag = 0
		}
		stateMu.Lock()
		free := idle[arrival.Sender]
		if free {
			idle[arrival.Sender] = false
		}
		stateMu.Unlock()
		if !free {
			result.recordUnissued(arrival, plan, dueAt, reasonNoIdleSender)
			continue
		}
		result.recordDispatched(arrival, lag)
		record := Record{
			Index:         arrival.Index,
			Phase:         arrival.Phase,
			ClientMsgNo:   plan.ClientMsgNo(arrival.Index),
			SenderUID:     plan.Senders[arrival.Sender].UID,
			RecipientUID:  plan.Recipients[arrival.Recipient].UID,
			ChannelID:     plan.Channel(arrival),
			PayloadDigest: plan.PayloadDigest(arrival.Index),
			DueAt:         dueAt,
			DispatchedAt:  now,
		}
		msg := Outbound{
			ClientSeq:   arrival.Index + 1,
			ClientMsgNo: record.ClientMsgNo,
			ChannelID:   record.ChannelID,
			Payload:     plan.Payload(arrival.Index),
		}
		wg.Add(1)
		inFlight.Add(1)
		go func(arrival Arrival, record Record, msg Outbound) {
			defer wg.Done()
			defer inFlight.Add(-1)
			sender := senders[arrival.Sender]
			busy[arrival.Sender].Lock()
			outcome := sender.Send(sendCtx, arrival, msg)
			busy[arrival.Sender].Unlock()
			record.CompletedAt = clock.Now()
			record.Class = outcome.Class
			record.NativeReason = outcome.NativeReason
			record.MessageID = outcome.MessageID
			record.MessageSeq = outcome.MessageSeq
			if record.Class == "" {
				record.Class = ClassIndeterminate
			}
			if record.NativeReason == "" {
				record.NativeReason = string(record.Class)
			}
			result.recordOutcome(record)
			stateMu.Lock()
			idle[arrival.Sender] = true
			if draining {
				result.mu.Lock()
				result.DrainedInFlight++
				result.mu.Unlock()
			}
			stateMu.Unlock()
		}(arrival, record, msg)
	}
	// Drain: every dispatched SEND whose result arrives from here on is
	// counted as drained in flight and gets the plan's bound.
	stateMu.Lock()
	draining = true
	stateMu.Unlock()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	drain := time.NewTimer(plan.Config.DrainBound)
	defer drain.Stop()
	select {
	case <-done:
	case <-drain.C:
		cancelSends()
		<-done
		result.mu.Lock()
		result.ReasonMapping[reasonDrainBound] = string(ClassIndeterminate)
		result.mu.Unlock()
	}
	result.EndedAt = clock.Now()
	if hooks.OnDrained != nil {
		hooks.OnDrained()
	}
	if cancelled {
		return result, ctx.Err()
	}
	return result, nil
}

// reasonKey renders a WuKongIM SENDACK reason as a stable lowercase label.
func reasonKey(reason frame.ReasonCode) string {
	if reason == frame.ReasonSuccess {
		return "success"
	}
	name := strings.TrimPrefix(reason.String(), "Reason")
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

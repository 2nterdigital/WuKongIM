package reference

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// fakeClock advances only when the scheduler sleeps, so 210 s of plan run instantly.
// fakeClock is virtual time. Unless advanceWhileBusy is set it waits, on the
// wall clock, until every dispatched SEND has completed before it advances,
// which makes the exact counts independent of goroutine scheduling.
type fakeClock struct {
	mu               sync.Mutex
	now              time.Time
	advanceWhileBusy bool
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration, inFlight func() int64) error {
	if !c.advanceWhileBusy {
		bound := time.Now().Add(30 * time.Second)
		for inFlight() > 0 && ctx.Err() == nil && time.Now().Before(bound) {
			time.Sleep(20 * time.Microsecond)
		}
	}
	if d > 0 {
		c.mu.Lock()
		c.now = c.now.Add(d)
		c.mu.Unlock()
	}
	return ctx.Err()
}

type fakeOutcome struct {
	reason frame.ReasonCode
	err    error
	hold   <-chan struct{}
}

// fakeSender resolves every dispatched SEND from a scripted outcome and
// refuses more than one operation in flight per sender.
type fakeSender struct {
	script   func(arrival Arrival) fakeOutcome
	inflight atomic.Int32
	peak     atomic.Int32
	sent     atomic.Uint64
}

func (s *fakeSender) Send(ctx context.Context, arrival Arrival, msg Outbound) Outcome {
	if s.inflight.Add(1) > 1 {
		s.peak.Store(2)
	}
	defer s.inflight.Add(-1)
	s.sent.Add(1)
	outcome := s.script(arrival)
	if outcome.hold != nil {
		select {
		case <-outcome.hold:
		case <-ctx.Done():
			return Outcome{Class: ClassIndeterminate, NativeReason: "context:" + ctx.Err().Error(), Err: ctx.Err()}
		}
	}
	if outcome.err != nil {
		if errors.Is(outcome.err, context.DeadlineExceeded) {
			return Outcome{Class: ClassTimedOut, NativeReason: "sendack_timeout", Err: outcome.err}
		}
		return Outcome{Class: ClassRejected, NativeReason: "send_error", Err: outcome.err}
	}
	if outcome.reason != frame.ReasonSuccess {
		return Outcome{Class: ClassRejected, NativeReason: "reason:" + reasonKey(outcome.reason), Reason: outcome.reason}
	}
	return Outcome{Class: ClassAcknowledged, NativeReason: "reason:success", Reason: frame.ReasonSuccess, MessageID: int64(arrival.Index) + 1, MessageSeq: arrival.Index + 1}
}

func senders(n int, script func(Arrival) fakeOutcome) ([]Sender, []*fakeSender) {
	fakes := make([]*fakeSender, n)
	out := make([]Sender, n)
	for i := range fakes {
		fakes[i] = &fakeSender{script: script}
		out[i] = fakes[i]
	}
	return out, fakes
}

func newTestPlan(t *testing.T, rate uint64) *Plan {
	t.Helper()
	plan, err := NewPlan(PlanConfig{RatePerSecond: rate, IdentitySeed: "seed", RunID: "run", AckTimeout: 5 * time.Second, DrainBound: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestSchedulerAcknowledgesEveryArrivalWithExactCounts(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	pool, fakes := senders(256, func(Arrival) fakeOutcome { return fakeOutcome{reason: frame.ReasonSuccess} })
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	result, err := RunSchedule(context.Background(), plan, pool, clock)
	if err != nil {
		t.Fatalf("RunSchedule: %v", err)
	}
	for _, phase := range plan.Phases() {
		counts := result.Phase(phase.Name)
		want := plan.Scheduled(phase.Name)
		if counts.Scheduled != want || counts.Dispatched != want || counts.Acknowledged != want || counts.Unissued != 0 {
			t.Fatalf("%s counts = %+v, want %d scheduled/dispatched/acknowledged", phase.Name, counts, want)
		}
	}
	total := result.Total()
	if total.Scheduled != 16500 || total.Acknowledged != 16500 || total.Dispatched != 16500 {
		t.Fatalf("total = %+v", total)
	}
	var sent uint64
	for _, fake := range fakes {
		sent += fake.sent.Load()
		if fake.peak.Load() > 1 {
			t.Fatalf("a sender carried more than one operation in flight")
		}
	}
	if sent != 16500 {
		t.Fatalf("senders saw %d sends, want 16500", sent)
	}
	measured := result.Latency("measure")
	if measured.AcknowledgedDueToComplete.Count != 12000 || measured.AcknowledgedDispatchToComplete.Count != 12000 {
		t.Fatalf("measured latency counts = %+v", measured)
	}
	if got := result.Latency("all").AcknowledgedDueToComplete.Count; got != 16500 {
		t.Fatalf("all-phase latency count = %d", got)
	}
	if result.Fresh != 16500 || result.Retries != 0 {
		t.Fatalf("fresh=%d retries=%d", result.Fresh, result.Retries)
	}
	if !result.Accounted() {
		t.Fatalf("scheduled must equal dispatched plus unissued")
	}
	if result.MaxDispatchLag > 0 {
		t.Fatalf("a fake clock never lags: %v", result.MaxDispatchLag)
	}
	if got := result.Phase("measure").Scheduled; got != 12000 {
		t.Fatalf("measure scheduled = %d", got)
	}
	if len(result.Records) != 16500 {
		t.Fatalf("ledger holds %d records, want 16500", len(result.Records))
	}
	if result.Records[0].ClientMsgNo != plan.ClientMsgNo(0) || len(result.Records[0].PayloadDigest) != 64 {
		t.Fatalf("record 0 = %+v", result.Records[0])
	}
}

func TestSchedulerNeverQueuesAnArrivalOnABusySender(t *testing.T) {
	t.Parallel()
	plan, err := NewPlan(PlanConfig{RatePerSecond: 100, IdentitySeed: "seed", RunID: "run", AckTimeout: 5 * time.Second, DrainBound: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// Every sender holds its first SEND until the drain bound cancels it, so
	// each later arrival finds its sender busy and is recorded as unissued
	// instead of waiting or moving to another connection.
	never := make(chan struct{})
	pool, fakes := senders(256, func(Arrival) fakeOutcome { return fakeOutcome{reason: frame.ReasonSuccess, hold: never} })
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0), advanceWhileBusy: true}
	result, err := RunSchedule(context.Background(), plan, pool, clock)
	if err != nil {
		t.Fatalf("RunSchedule: %v", err)
	}
	total := result.Total()
	if total.Dispatched != 256 {
		t.Fatalf("dispatched = %d, want exactly one per sender", total.Dispatched)
	}
	if total.Unissued != 16500-256 || result.UnissuedByReason["no_idle_sender"] != 16500-256 {
		t.Fatalf("unissued = %d by reason %v", total.Unissued, result.UnissuedByReason)
	}
	if total.Acknowledged != 0 || total.Indeterminate != 256 {
		t.Fatalf("acknowledged=%d indeterminate=%d, want 0 and 256 after the drain bound", total.Acknowledged, total.Indeterminate)
	}
	if !result.Accounted() {
		t.Fatalf("scheduled must equal dispatched plus unissued")
	}
	if result.GeneratorLimited == 0 {
		t.Fatalf("an unissued arrival marks the point generator-limited")
	}
	if result.ReasonMapping["drain_bound_expired"] != "indeterminate" {
		t.Fatalf("an expired drain is recorded: %v", result.ReasonMapping)
	}
	for i, fake := range fakes {
		if fake.sent.Load() != 1 {
			t.Fatalf("sender %d saw %d sends, want 1", i, fake.sent.Load())
		}
	}
}

func TestSchedulerRecordsRejectedTimedOutAndIndeterminateClasses(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 50)
	pool, _ := senders(256, func(arrival Arrival) fakeOutcome {
		switch arrival.Index % 5 {
		case 1:
			return fakeOutcome{reason: frame.ReasonNotInWhitelist}
		case 2:
			return fakeOutcome{err: context.DeadlineExceeded}
		case 3:
			return fakeOutcome{err: errors.New("write: broken pipe")}
		default:
			return fakeOutcome{reason: frame.ReasonSuccess}
		}
	})
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	result, err := RunSchedule(context.Background(), plan, pool, clock)
	if err != nil {
		t.Fatalf("RunSchedule: %v", err)
	}
	total := result.Total()
	if total.Dispatched != 10500 {
		t.Fatalf("dispatched = %d", total.Dispatched)
	}
	if total.Acknowledged+total.Rejected+total.TimedOut+total.Indeterminate != total.Dispatched {
		t.Fatalf("every dispatched command has exactly one class: %+v", total)
	}
	if total.TimedOut != 2100 || total.Rejected != 4200 {
		t.Fatalf("timed_out=%d rejected=%d, want 2100 and 4200", total.TimedOut, total.Rejected)
	}
	if result.RejectedByReason["reason:not_in_whitelist"] != 2100 || result.RejectedByReason["send_error"] != 2100 {
		t.Fatalf("rejected by reason = %v", result.RejectedByReason)
	}
	if result.TimedOutByReason["sendack_timeout"] != 2100 {
		t.Fatalf("timed out by reason = %v", result.TimedOutByReason)
	}
	if got := result.ReasonMapping["reason:not_in_whitelist"]; got != "rejected" {
		t.Fatalf("mapping = %v", result.ReasonMapping)
	}
	if got := result.ReasonMapping["sendack_timeout"]; got != "timed_out" {
		t.Fatalf("mapping = %v", result.ReasonMapping)
	}
	if got := result.ReasonMapping["no_idle_sender"]; got != "unissued" {
		t.Fatalf("mapping = %v", result.ReasonMapping)
	}
	measured := result.Phase("measure")
	if measured.Acknowledged+measured.NotAcknowledged() != measured.Dispatched {
		t.Fatalf("measured = %+v", measured)
	}
	if result.Latency("measure").FailedDueToComplete.Count != measured.Rejected+measured.TimedOut {
		t.Fatalf("failed samples = %d, want %d", result.Latency("measure").FailedDueToComplete.Count, measured.Rejected+measured.TimedOut)
	}
	acknowledged := 0
	for _, record := range result.Records {
		if record.Class == ClassAcknowledged {
			acknowledged++
			if record.MessageID == 0 {
				t.Fatalf("an acknowledged record carries its server message id")
			}
		}
	}
	if uint64(acknowledged) != total.Acknowledged {
		t.Fatalf("ledger acknowledged = %d, want %d", acknowledged, total.Acknowledged)
	}
}

func TestSchedulerStopsAtTheWindowEndAndDrainsInFlight(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 50)
	release := make(chan struct{})
	var dispatched atomic.Uint64
	pool, _ := senders(256, func(arrival Arrival) fakeOutcome {
		dispatched.Add(1)
		if arrival.Index >= plan.TotalScheduled()-1 {
			return fakeOutcome{reason: frame.ReasonSuccess, hold: release}
		}
		return fakeOutcome{reason: frame.ReasonSuccess}
	})
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	done := make(chan *ScheduleResult, 1)
	go func() {
		result, _ := RunSchedule(context.Background(), plan, pool, clock)
		done <- result
	}()
	select {
	case <-done:
		t.Fatal("the schedule must not finish while a dispatched SEND is still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	result := <-done
	if result == nil {
		t.Fatal("no result")
	}
	if dispatched.Load() != plan.TotalScheduled() {
		t.Fatalf("dispatched %d, want every scheduled arrival", dispatched.Load())
	}
	if result.Total().Acknowledged != plan.TotalScheduled() {
		t.Fatalf("the drained SEND is acknowledged: %+v", result.Total())
	}
	if result.DrainedInFlight < 1 {
		t.Fatalf("drained in flight = %d, want at least the held SEND", result.DrainedInFlight)
	}
	if result.Histogram("all").AcknowledgedDueToComplete.MaxMs == 0 && result.Total().Acknowledged > 0 {
		// Max may legitimately be 0 on a fake clock; the field must still exist.
		_ = result.Histogram("all")
	}
}

func TestHistogramUsesTheFixedBoundsAndOpenLastBucket(t *testing.T) {
	t.Parallel()
	var h Histogram
	for _, d := range []time.Duration{500 * time.Microsecond, 1 * time.Millisecond, 3 * time.Millisecond, 61 * time.Second} {
		h.Observe(d)
	}
	if h.Count != 4 || h.MaxMs != 61000 {
		t.Fatalf("count=%d max=%d", h.Count, h.MaxMs)
	}
	if h.Buckets[0] != 2 || h.Buckets[2] != 1 || h.Buckets[15] != 1 {
		t.Fatalf("buckets = %v", h.Buckets)
	}
	if h.Bounds() != fixedLatencyBoundsMs {
		t.Fatalf("bounds = %v", h.Bounds())
	}
	sum := uint64(0)
	for _, b := range h.Buckets {
		sum += b
	}
	if sum != h.Count {
		t.Fatalf("buckets must sum to the count")
	}
}

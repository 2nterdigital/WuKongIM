package replication

import (
	"sync"
	"testing"
	"time"
)

type replicationStageObservation struct {
	stage  string
	result string
	d      time.Duration
}

type recordingReplicationStageObserver struct {
	mu     sync.Mutex
	events []replicationStageObservation
}

func (o *recordingReplicationStageObserver) ObserveReplicationStage(stage string, result string, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, replicationStageObservation{stage: stage, result: result, d: d})
}

func (o *recordingReplicationStageObserver) snapshot() []replicationStageObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]replicationStageObservation(nil), o.events...)
}

func hasReplicationStage(events []replicationStageObservation, stage string, result string) bool {
	for _, event := range events {
		if event.stage == stage && event.result == result && event.d >= 0 {
			return true
		}
	}
	return false
}

func TestSampledStageObserverKeepsOneBoundedSeriesSamplePerInterval(t *testing.T) {
	sink := &recordingReplicationStageObserver{}
	observer := newSampledStageObserver(sink, 3)
	for index := 0; index < 7; index++ {
		observer.ObserveReplicationStage(stagePeerForegroundQueue, "ok", time.Duration(index))
	}
	events := sink.snapshot()
	if len(events) != 3 || events[0].d != 0 || events[1].d != 3 || events[2].d != 6 {
		t.Fatalf("sampled events = %+v, want observations 0, 3, and 6", events)
	}
}

func TestReplicationStageIndexIncludesEveryRuntimeStage(t *testing.T) {
	t.Parallel()

	stages := []string{
		stageQuorumLocalQueue,
		stageQuorumLocalStore,
		stageQuorumLocalEndToEnd,
		stagePeerForegroundQueue,
		stagePeerForegroundExchange,
		stagePeerForegroundEndToEnd,
		stagePeerBackgroundQueue,
		stagePeerBackgroundExchange,
		stagePeerBackgroundEndToEnd,
		stageFollowerForegroundStore,
		stageFollowerBackgroundStore,
	}
	seen := make(map[int]string, len(stages))
	for _, stage := range stages {
		index := replicationStageIndex(stage)
		if index < 0 {
			t.Fatalf("replicationStageIndex(%q) = %d, want registered stage", stage, index)
		}
		if previous, exists := seen[index]; exists {
			t.Fatalf("stages %q and %q share sample counter %d", previous, stage, index)
		}
		seen[index] = stage
	}
}

type countingReplicationStageObserver struct {
	recordingReplicationStageObserver
	mu     sync.Mutex
	counts map[string]uint64
}

func (o *countingReplicationStageObserver) CountReplicationStage(stage string, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts = map[string]uint64{}
	}
	o.counts[stage+"/"+result]++
}

func TestSampledStageObserverCountsEveryStageBeforeSampling(t *testing.T) {
	sink := &countingReplicationStageObserver{}
	observer := newSampledStageObserver(sink, 3)
	for index := 0; index < 7; index++ {
		observer.ObserveReplicationStage(stagePeerForegroundExchange, "ok", time.Duration(index))
	}
	observer.ObserveReplicationStage("not-a-stage", "ok", time.Second)
	if got := sink.counts[stagePeerForegroundExchange+"/ok"]; got != 7 {
		t.Fatalf("count = %d, want every one of the 7 completions", got)
	}
	if _, ok := sink.counts["not-a-stage/ok"]; ok {
		t.Fatalf("an unknown stage is never counted")
	}
	if events := sink.snapshot(); len(events) != 3 {
		t.Fatalf("sampled events = %d, want the latency histogram to stay sampled", len(events))
	}
	unsampled := newSampledStageObserver(sink, 1)
	unsampled.ObserveReplicationStage(stageQuorumLocalStore, "err", time.Millisecond)
	if got := sink.counts[stageQuorumLocalStore+"/err"]; got != 1 {
		t.Fatalf("an unsampled observer still counts: %d", got)
	}
	if !hasReplicationStage(sink.snapshot(), stageQuorumLocalStore, "err") {
		t.Fatalf("an unsampled observer forwards every observation")
	}
}

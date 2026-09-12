package reference

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// fakeRecipient delivers one RECV per acknowledged SEND addressed to it and
// records the RECVACKs the workload writes.
type fakeRecipient struct {
	uid    string
	inbox  chan *frame.RecvPacket
	acked  atomic.Uint64
	closed atomic.Bool
}

func (r *fakeRecipient) UID() string { return r.uid }

func (r *fakeRecipient) Recv(ctx context.Context) (*frame.RecvPacket, error) {
	select {
	case recv := <-r.inbox:
		return recv, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *fakeRecipient) RecvAck(context.Context, int64, uint64) error {
	r.acked.Add(1)
	return nil
}

func (r *fakeRecipient) Close() error {
	r.closed.Store(true)
	return nil
}

// fakeCluster is a whole fake product: senders whose acknowledgments deliver
// RECVs to the recipient sessions and whose history answers from the same
// ledger.
type fakeCluster struct {
	plan       *Plan
	mu         sync.Mutex
	recipients map[string]*fakeRecipient
	history    map[string][]HistoryRow
	scrapes    atomic.Int32
	closed     atomic.Bool
	dropRecv   func(arrival Arrival) bool
}

func newFakeCluster(plan *Plan) *fakeCluster {
	cluster := &fakeCluster{plan: plan, recipients: map[string]*fakeRecipient{}, history: map[string][]HistoryRow{}}
	for _, identity := range plan.Recipients {
		cluster.recipients[identity.UID] = &fakeRecipient{uid: identity.UID, inbox: make(chan *frame.RecvPacket, 64)}
	}
	return cluster
}

func (c *fakeCluster) Senders() []Sender {
	out := make([]Sender, SenderCount)
	for i := range out {
		out[i] = &fakeClusterSender{cluster: c}
	}
	return out
}

func (c *fakeCluster) Recipients() []Recipient {
	out := make([]Recipient, 0, len(c.recipients))
	for _, identity := range c.plan.Recipients {
		out = append(out, c.recipients[identity.UID])
	}
	return out
}

func (c *fakeCluster) Close() error {
	c.closed.Store(true)
	for _, recipient := range c.recipients {
		_ = recipient.Close()
	}
	return nil
}

func (c *fakeCluster) ScrapeAll(context.Context) ([]NodeScrapes, error) {
	c.scrapes.Add(1)
	before, _ := ParseScrape([]byte(scrapeBefore))
	after, _ := ParseScrape([]byte(scrapeAfter()))
	return []NodeScrapes{{Node: "node-1", Before: before, After: after}}, nil
}

func (c *fakeCluster) ReadPersonChannel(_ context.Context, loginUID, peerUID string) ([]HistoryRow, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows := c.history[loginUID+"|"+peerUID]
	return append([]HistoryRow(nil), rows...), 1, nil
}

type fakeClusterSender struct {
	cluster *fakeCluster
}

func (s *fakeClusterSender) Send(_ context.Context, arrival Arrival, msg Outbound) Outcome {
	plan := s.cluster.plan
	sender := plan.Senders[arrival.Sender]
	recipient := plan.Recipients[arrival.Recipient]
	s.cluster.mu.Lock()
	key := sender.UID + "|" + recipient.UID
	seq := uint64(len(s.cluster.history[key]) + 1)
	id := int64(arrival.Index) + 1
	s.cluster.history[key] = append(s.cluster.history[key], HistoryRow{
		ClientMsgNo: msg.ClientMsgNo, MessageID: id, MessageSeq: seq, FromUID: sender.UID, ChannelID: msg.ChannelID,
		ChannelType: frame.ChannelTypePerson, PayloadDigest: DigestHex(msg.Payload), PayloadBytes: len(msg.Payload),
	})
	s.cluster.mu.Unlock()
	if s.cluster.dropRecv == nil || !s.cluster.dropRecv(arrival) {
		s.cluster.recipients[recipient.UID].inbox <- &frame.RecvPacket{
			ClientMsgNo: msg.ClientMsgNo, MessageID: id, MessageSeq: seq, FromUID: sender.UID, ChannelID: sender.UID,
			ChannelType: frame.ChannelTypePerson, Payload: msg.Payload,
		}
	}
	return Outcome{Class: ClassAcknowledged, NativeReason: "reason:success", Reason: frame.ReasonSuccess, MessageID: id, MessageSeq: seq}
}

func runFake(t *testing.T, plan *Plan, cluster *fakeCluster, enabled bool, logs *[]string) *RunResult {
	t.Helper()
	var scraper Scraper
	if enabled {
		scraper = cluster
	}
	result, err := Run(context.Background(), RunConfig{
		Plan:       plan,
		Sessions:   cluster,
		Scraper:    scraper,
		History:    cluster,
		Clock:      &fakeClock{now: time.Unix(1_700_000_000, 0)},
		RecvSettle: 200 * time.Millisecond,
		Log: func(line string) {
			*logs = append(*logs, line)
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return result
}

func TestRunReconcilesHistoryAndOnlineDeliverySeparatelyAndJoinsEverything(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 50)
	cluster := newFakeCluster(plan)
	cluster.dropRecv = func(arrival Arrival) bool { return arrival.Index%100 == 7 }
	var logs []string
	result := runFake(t, plan, cluster, true, &logs)
	total := result.Schedule.Total()
	if total.Acknowledged != plan.TotalScheduled() {
		t.Fatalf("acknowledged = %d", total.Acknowledged)
	}
	if !result.History.Correct() || result.History.Expected != plan.TotalScheduled() {
		t.Fatalf("history = %+v", result.History)
	}
	if result.History.ChannelsRead == 0 || result.History.PagesRead < result.History.ChannelsRead {
		t.Fatalf("history read stats = %+v", result.History)
	}
	if result.Online.MessagesWithoutDelivery != 105 || result.Online.Complete {
		t.Fatalf("online gap = %+v (105 dropped receives must never become durable loss)", result.Online)
	}
	if result.Online.ReceiveAcknowledged != result.Online.Received {
		t.Fatalf("every observed RECV was acknowledged: %+v", result.Online)
	}
	if result.Work.PhysicalCommits.Unavailable != "" || result.Work.Fsyncs.Unavailable == "" {
		t.Fatalf("physical work = %+v", result.Work)
	}
	if cluster.scrapes.Load() != 4 {
		t.Fatalf("scrapes = %d, want before warmup, at measured start, at measured end and after drain", cluster.scrapes.Load())
	}
	if !cluster.closed.Load() {
		t.Fatalf("sessions must be closed and joined at the end")
	}
	if result.Timeline.MeasuredStartOffset != 30*time.Second || result.Timeline.MeasuredDuration != 120*time.Second {
		t.Fatalf("timeline = %+v", result.Timeline)
	}
	if !result.InstrumentationEnabled {
		t.Fatalf("instrumentation was enabled")
	}
}

func TestDisabledObservationScrapesNothingAndLeavesPhysicalWorkUnavailable(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 50)
	cluster := newFakeCluster(plan)
	var logs []string
	result := runFake(t, plan, cluster, false, &logs)
	if cluster.scrapes.Load() != 0 {
		t.Fatalf("a disabled observation must not scrape")
	}
	if result.Work.PhysicalCommits.Unavailable == "" || !strings.Contains(result.Work.PhysicalCommits.Unavailable, "disabled") {
		t.Fatalf("physical work = %+v", result.Work.PhysicalCommits)
	}
	if result.InstrumentationEnabled || len(result.Native) != 0 {
		t.Fatalf("no native counters without observation: %+v", result.Native)
	}
	if !result.History.Correct() {
		t.Fatalf("history is audited regardless of observation: %+v", result.History)
	}
}

func TestRunLogsAreBoundedSummariesNeverPerMessageLines(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	result := runFake(t, plan, cluster, true, &logs)
	if len(logs) == 0 || len(logs) > 64 {
		t.Fatalf("logs = %d lines; a run emits bounded summaries only", len(logs))
	}
	joined := strings.Join(logs, "\n")
	for _, identity := range plan.Senders[:4] {
		if strings.Contains(joined, identity.UID) || strings.Contains(joined, identity.Token) {
			t.Fatalf("logs must not carry identities or tokens: %s", joined)
		}
	}
	if strings.Contains(joined, plan.ClientMsgNo(0)) || strings.Contains(joined, string(plan.Payload(0)[:32])) {
		t.Fatalf("logs must not carry client message numbers or payloads")
	}
	_ = result
}

func TestRunResultEncodesBoundedAndRedacted(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	result := runFake(t, plan, cluster, true, &logs)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 256*1024 {
		t.Fatalf("run result is %d bytes; it must stay bounded", len(encoded))
	}
	text := string(encoded)
	for _, forbidden := range []string{plan.Senders[0].UID, plan.Recipients[0].UID, plan.Senders[0].Token, plan.ClientMsgNo(1), string(plan.Payload(1)[:24]), "/srv/", "@"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("run result must not carry %q", forbidden)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["schema"] != RunSchema {
		t.Fatalf("schema = %v", decoded["schema"])
	}
	for _, key := range []string{"workload", "lifecycle", "accounting", "latency", "history", "online_delivery", "physical_work", "native", "observation"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("run result lacks %s", key)
		}
	}
	if decoded["accounting"].(map[string]any)["ledger_dropped"] == nil {
		t.Fatalf("dropped ledger entries must be accounted explicitly")
	}
}

package reference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	benchtarget "github.com/WuKongIM/WuKongIM/internal/bench/target"
	"github.com/WuKongIM/WuKongIM/pkg/bench/model"
	wkclient "github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

type scriptedFuture struct {
	result wkclient.SendResult
	err    error
}

func (f scriptedFuture) Wait(context.Context) (wkclient.SendResult, error) { return f.result, f.err }

type scriptedClient struct {
	admitErr error
	future   scriptedFuture
	last     wkclient.Message
}

func (c *scriptedClient) SendAsync(_ context.Context, msg wkclient.Message) (sendFuture, error) {
	c.last = msg
	if c.admitErr != nil {
		return nil, c.admitErr
	}
	return c.future, nil
}

func TestClientSenderClassifiesSentinelErrorsAndReasonCodesWithoutRawText(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		client   *scriptedClient
		class    Class
		reason   string
		withID   bool
		rawError string
	}{
		{name: "queue full before admission", client: &scriptedClient{admitErr: wkclient.ErrSendQueueFull}, class: ClassRejected, reason: "client:send_queue_full"},
		{name: "closed before admission", client: &scriptedClient{admitErr: wkclient.ErrClosed}, class: ClassRejected, reason: "client:closed"},
		{name: "sendack timeout", client: &scriptedClient{future: scriptedFuture{err: wkclient.ErrAckTimeout}}, class: ClassTimedOut, reason: reasonSendackTimeout},
		{name: "closed while pending", client: &scriptedClient{future: scriptedFuture{err: wkclient.ErrClosed}}, class: ClassIndeterminate, reason: "client:closed"},
		{name: "cancelled while pending", client: &scriptedClient{future: scriptedFuture{err: context.Canceled}}, class: ClassIndeterminate, reason: "context:canceled"},
		{name: "unknown error while pending", client: &scriptedClient{future: scriptedFuture{err: errors.New("write tcp 10.0.0.9:5100: broken pipe")}}, class: ClassIndeterminate, reason: "client:error", rawError: "10.0.0.9"},
		{name: "unknown error before admission", client: &scriptedClient{admitErr: errors.New("dial tcp 10.0.0.9:5100: refused")}, class: ClassRejected, reason: "client:error", rawError: "10.0.0.9"},
		{name: "server reason code", client: &scriptedClient{future: scriptedFuture{result: wkclient.SendResult{ReasonCode: frame.ReasonNotInWhitelist}}}, class: ClassRejected, reason: "reason:not_in_whitelist"},
		{name: "acknowledged", client: &scriptedClient{future: scriptedFuture{result: wkclient.SendResult{ReasonCode: frame.ReasonSuccess, MessageID: 77, MessageSeq: 9}}}, class: ClassAcknowledged, reason: "reason:success", withID: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender := &clientSender{client: test.client}
			outcome := sender.Send(context.Background(), Arrival{}, Outbound{ClientSeq: 3, ClientMsgNo: "ref-x-1", ChannelID: "a@b", Payload: []byte("p")})
			if outcome.Class != test.class || outcome.NativeReason != test.reason {
				t.Fatalf("outcome = %s/%s, want %s/%s", outcome.Class, outcome.NativeReason, test.class, test.reason)
			}
			if test.withID && (outcome.MessageID != 77 || outcome.MessageSeq != 9) {
				t.Fatalf("acknowledged outcome lacks the server ids: %+v", outcome)
			}
			if test.rawError != "" && strings.Contains(outcome.NativeReason, test.rawError) {
				t.Fatalf("raw error text leaked into the reason %q", outcome.NativeReason)
			}
			if test.client.last.ChannelType != frame.ChannelTypePerson || test.client.last.ClientMsgNo != "ref-x-1" || test.client.last.ClientSeq != 3 {
				t.Fatalf("message sent = %+v", test.client.last)
			}
		})
	}
}

func TestEndpointAssignmentSpreadsTheConnectionsOverThreeGateways(t *testing.T) {
	t.Parallel()
	addrs := []string{"a:5100", "b:5100", "c:5100"}
	counts := map[string]int{}
	for i := 0; i < ConnectionCount; i++ {
		counts[endpointFor(i, addrs)]++
	}
	if counts["a:5100"] != 91 || counts["b:5100"] != 91 || counts["c:5100"] != 91 {
		t.Fatalf("connections per endpoint = %v, want 91 each", counts)
	}
	if endpointFor(SenderCount, addrs) != "b:5100" {
		t.Fatalf("the first recipient continues the round robin after the senders")
	}
	if got := dedupeAddrs([]string{" a:5100 ", "a:5100", "", "b:5100"}); len(got) != 2 {
		t.Fatalf("dedupe = %v", got)
	}
}

func TestOpenSessionsFailsClosedWithoutAGateway(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	if _, err := OpenSessions(context.Background(), plan, SessionConfig{}); err == nil {
		t.Fatal("no gateway must fail before any connection")
	}
	if _, err := OpenSessions(context.Background(), nil, SessionConfig{GatewayAddrs: []string{"a"}}); err == nil {
		t.Fatal("a nil plan must fail")
	}
}

func TestRegisterTokensPostsEveryIdentityInOneIdempotentBatch(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	var got model.BatchTokensRequest
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	api := benchtarget.NewClient(benchtarget.Config{APIAddrs: []string{server.URL}})
	if err := RegisterTokens(context.Background(), api, plan); err != nil {
		t.Fatalf("RegisterTokens: %v", err)
	}
	if path != "/bench/v1/users/tokens" || !got.Upsert || got.RunID != "run" || len(got.Users) != ConnectionCount {
		t.Fatalf("request = path %s upsert %v run %q users %d", path, got.Upsert, got.RunID, len(got.Users))
	}
	if got.Users[0].UID != plan.Senders[0].UID || got.Users[0].Token != plan.Senders[0].Token || got.Users[SenderCount].UID != plan.Recipients[0].UID {
		t.Fatalf("identities are registered in plan order: %+v", got.Users[:2])
	}
}

func TestNodeScraperNamesNodesInOrderAndFailsOnAnyNode(t *testing.T) {
	t.Parallel()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("# HELP process_cpu_seconds_total x\n# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total 1.5\n"))
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer bad.Close()
	scraper := NodeScraper{Nodes: []NodeAddress{{Name: "node-1", APIAddr: good.URL}, {Name: "node-2", APIAddr: good.URL}}}
	nodes, err := scraper.ScrapeAll(context.Background())
	if err != nil {
		t.Fatalf("ScrapeAll: %v", err)
	}
	if len(nodes) != 2 || nodes[0].Node != "node-1" || nodes[1].Node != "node-2" || len(nodes[1].After.Samples) == 0 {
		t.Fatalf("nodes = %+v", nodes)
	}
	if _, err := (NodeScraper{Nodes: []NodeAddress{{Name: "node-1", APIAddr: good.URL}, {Name: "node-3", APIAddr: bad.URL}}}).ScrapeAll(context.Background()); err == nil || !strings.Contains(err.Error(), "node-3") {
		t.Fatalf("one failing node fails the scrape by name: %v", err)
	}
	if _, err := (NodeScraper{}).ScrapeAll(context.Background()); err == nil {
		t.Fatal("no nodes must fail")
	}
}

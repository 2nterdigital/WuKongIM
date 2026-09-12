package reference

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func expectedSet(plan *Plan, acknowledged int) []ExpectedMessage {
	arrivals := plan.Arrivals()
	out := make([]ExpectedMessage, 0, acknowledged)
	for i := 0; i < acknowledged; i++ {
		arrival := arrivals[i]
		out = append(out, ExpectedMessage{
			ClientMsgNo:   plan.ClientMsgNo(arrival.Index),
			SenderUID:     plan.Senders[arrival.Sender].UID,
			RecipientUID:  plan.Recipients[arrival.Recipient].UID,
			ChannelID:     plan.Channel(arrival),
			PayloadDigest: plan.PayloadDigest(arrival.Index),
			MessageID:     int64(arrival.Index) + 1000,
			MessageSeq:    uint64(arrival.Index/SenderCount) + 1,
			Phase:         arrival.Phase,
		})
	}
	return out
}

func rowsOf(expected []ExpectedMessage) []HistoryRow {
	rows := make([]HistoryRow, 0, len(expected))
	for _, message := range expected {
		rows = append(rows, HistoryRow{
			ClientMsgNo:   message.ClientMsgNo,
			MessageID:     message.MessageID,
			MessageSeq:    message.MessageSeq,
			FromUID:       message.SenderUID,
			ChannelID:     message.ChannelID,
			ChannelType:   frame.ChannelTypePerson,
			PayloadDigest: message.PayloadDigest,
		})
	}
	return rows
}

func TestReconcileMatchesEveryAcknowledgedIdentityExactlyOnce(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	expected := expectedSet(plan, 600)
	result := Reconcile(expected, rowsOf(expected), HistoryReadStats{ChannelsRead: 600, PagesRead: 600, Complete: true})
	if result.Expected != 600 || result.Matched != 600 {
		t.Fatalf("expected=%d matched=%d", result.Expected, result.Matched)
	}
	if result.Missing != 0 || result.Duplicate != 0 || result.Conflicting != 0 || result.WrongChannel != 0 || result.WrongContent != 0 || result.Misordered != 0 || result.UnexpectedRows != 0 {
		t.Fatalf("clean history reports counterexamples: %+v", result)
	}
	if !result.Correct() || !result.Complete {
		t.Fatalf("a clean complete audit is correct: %+v", result)
	}
	if result.Method == "" || !strings.Contains(result.Method, "messagesync") {
		t.Fatalf("the method names the read path: %q", result.Method)
	}
}

func TestReconcileNamesMissingDuplicateConflictingWrongAndUnexpectedRows(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	expected := expectedSet(plan, 8)
	rows := rowsOf(expected)
	rows = rows[:7]                                             // index 7 is missing
	rows = append(rows, rows[1])                                // index 1 is duplicated
	rows[2].MessageID = 999_999                                 // index 2 conflicts on its server identity
	rows[3].ChannelID = "someone-else@" + expected[3].SenderUID // index 3 in the wrong channel
	rows[4].PayloadDigest = strings.Repeat("0", 64)             // index 4 has the wrong content
	rows[5].FromUID = "impostor"                                // index 5 conflicts on its sender
	rows = append(rows, HistoryRow{ClientMsgNo: "not-ours", MessageID: 1, MessageSeq: 1, FromUID: "x", ChannelID: "x@y", ChannelType: frame.ChannelTypePerson, PayloadDigest: strings.Repeat("1", 64)})
	result := Reconcile(expected, rows, HistoryReadStats{ChannelsRead: 8, PagesRead: 8, Complete: true})
	if result.Expected != 8 {
		t.Fatalf("expected = %d", result.Expected)
	}
	if result.Matched != 3 {
		t.Fatalf("matched = %d, want indices 0, 1 and 6 (a duplicate row never un-matches its first row)", result.Matched)
	}
	if result.Missing != 1 || result.Duplicate != 1 || result.Conflicting != 2 || result.WrongChannel != 1 || result.WrongContent != 1 || result.UnexpectedRows != 1 {
		t.Fatalf("counterexamples = %+v", result)
	}
	if result.Correct() {
		t.Fatalf("a history with counterexamples is never correct")
	}
	// A duplicate never repairs a missing identity and matched never double counts.
	if result.Matched+result.Missing+result.Conflicting+result.WrongChannel+result.WrongContent != result.Expected {
		t.Fatalf("every expected identity has exactly one disposition: %+v", result)
	}
}

func TestReconcileDetectsMisorderedSequencesWithinAChannel(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	// Arrivals 0 and 256 share sender 0 and recipient 0 (256 mod 17 = 1, so use 0 and 4352).
	arrivals := plan.Arrivals()
	first := arrivals[0]
	second := arrivals[4352]
	if plan.Channel(first) != plan.Channel(second) {
		t.Fatalf("test arrivals must share a channel")
	}
	expected := []ExpectedMessage{
		{ClientMsgNo: plan.ClientMsgNo(first.Index), SenderUID: plan.Senders[first.Sender].UID, RecipientUID: plan.Recipients[first.Recipient].UID, ChannelID: plan.Channel(first), PayloadDigest: plan.PayloadDigest(first.Index), MessageID: 10, MessageSeq: 1},
		{ClientMsgNo: plan.ClientMsgNo(second.Index), SenderUID: plan.Senders[second.Sender].UID, RecipientUID: plan.Recipients[second.Recipient].UID, ChannelID: plan.Channel(second), PayloadDigest: plan.PayloadDigest(second.Index), MessageID: 11, MessageSeq: 2},
	}
	rows := rowsOf(expected)
	rows[0], rows[1] = rows[1], rows[0]
	result := Reconcile(expected, rows, HistoryReadStats{ChannelsRead: 1, PagesRead: 1, Complete: true})
	if result.Misordered != 1 {
		t.Fatalf("misordered = %d, want 1", result.Misordered)
	}
	if result.Matched != 2 {
		t.Fatalf("misordering is not a content or identity failure: %+v", result)
	}
	if result.Correct() {
		t.Fatalf("a misordered channel is not correct")
	}
	incomplete := Reconcile(expected, rowsOf(expected), HistoryReadStats{ChannelsRead: 1, PagesRead: 1, Complete: false, IncompleteReason: "read-back bound expired"})
	if incomplete.Correct() || incomplete.Complete || incomplete.IncompleteReason == nil || *incomplete.IncompleteReason == "" {
		t.Fatalf("an incomplete audit is never correct: %+v", incomplete)
	}
}

func TestHistoryClientPagesUntilNoMoreRowsAndDecodesPayloads(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/channel/messagesync" || r.Method != http.MethodPost {
			http.Error(w, "unexpected route", http.StatusNotFound)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req["login_uid"] != "sender-a" || req["channel_id"] != "recipient-b" || req["channel_type"] != float64(1) || req["pull_mode"] != float64(1) || req["limit"] != float64(1000) {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		call := calls.Add(1)
		start := uint64(req["start_message_seq"].(float64))
		w.Header().Set("Content-Type", "application/json")
		switch call {
		case 1:
			if start != 0 {
				http.Error(w, "first page must start at 0", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"start_message_seq":0,"end_message_seq":0,"more":1,"messages":[` +
				`{"header":{"no_persist":0,"red_dot":1,"sync_once":0},"setting":0,"message_id":101,"message_idstr":"101","client_msg_no":"m-1","message_seq":1,"from_uid":"sender-a","channel_id":"recipient-b","channel_type":1,"expire":0,"timestamp":1,"payload":"` + base64.StdEncoding.EncodeToString([]byte("hello")) + `"},` +
				`{"header":{"no_persist":0,"red_dot":1,"sync_once":0},"setting":0,"message_id":102,"message_idstr":"102","client_msg_no":"m-2","message_seq":2,"from_uid":"sender-a","channel_id":"recipient-b","channel_type":1,"expire":0,"timestamp":1,"payload":"` + base64.StdEncoding.EncodeToString([]byte("world")) + `"}]}`))
		case 2:
			if start != 3 {
				http.Error(w, "second page must continue after the last sequence", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"start_message_seq":3,"end_message_seq":0,"more":0,"messages":[` +
				`{"header":{"no_persist":0,"red_dot":1,"sync_once":0},"setting":0,"message_id":103,"message_idstr":"103","client_msg_no":"m-3","message_seq":3,"from_uid":"sender-a","channel_id":"recipient-b","channel_type":1,"expire":0,"timestamp":1,"payload":"` + base64.StdEncoding.EncodeToString([]byte("!")) + `"}]}`))
		default:
			http.Error(w, "too many pages", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := NewHistoryClient(HistoryClientConfig{APIAddrs: []string{server.URL}, PageLimit: 1000, MaxPages: 10})
	rows, pages, err := client.ReadPersonChannel(context.Background(), "sender-a", "recipient-b")
	if err != nil {
		t.Fatalf("ReadPersonChannel: %v", err)
	}
	if pages != 2 || len(rows) != 3 {
		t.Fatalf("pages=%d rows=%d", pages, len(rows))
	}
	if rows[0].ClientMsgNo != "m-1" || rows[0].MessageID != 101 || rows[0].MessageSeq != 1 || rows[0].FromUID != "sender-a" || rows[0].ChannelType != frame.ChannelTypePerson {
		t.Fatalf("row 0 = %+v", rows[0])
	}
	if rows[0].PayloadDigest != DigestHex([]byte("hello")) || rows[2].PayloadDigest != DigestHex([]byte("!")) {
		t.Fatalf("payload digests must be taken from the decoded payload bytes: %+v", rows)
	}
	if rows[0].PayloadBytes != 5 {
		t.Fatalf("payload byte counts are retained, payloads are not: %+v", rows[0])
	}
	bounded := NewHistoryClient(HistoryClientConfig{APIAddrs: []string{server.URL}, PageLimit: 1000, MaxPages: 1})
	calls.Store(0)
	if _, _, err := bounded.ReadPersonChannel(context.Background(), "sender-a", "recipient-b"); err == nil {
		t.Fatalf("a page bound that ends before more=0 is an incomplete read, not silent truncation")
	}
}

func TestReceiveLedgerKeepsOnlineDeliveryApartFromDurableCorrectness(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	expected := expectedSet(plan, 5)
	ledger := NewReceiveLedger(uint64(len(expected)) + 8)
	recv := func(index int, message ExpectedMessage, id int64) *frame.RecvPacket {
		return &frame.RecvPacket{ClientMsgNo: message.ClientMsgNo, MessageID: id, MessageSeq: message.MessageSeq, FromUID: message.SenderUID, ChannelID: message.SenderUID, ChannelType: frame.ChannelTypePerson, Payload: plan.Payload(uint64(index))}
	}
	for i, message := range expected[:4] {
		ledger.Record(message.RecipientUID, recv(i, message, int64(1000+i)), true)
	}
	// One duplicate delivery of message 0 and one unknown message.
	ledger.Record(expected[0].RecipientUID, recv(0, expected[0], 1000), true)
	ledger.Record(expected[0].RecipientUID, &frame.RecvPacket{ClientMsgNo: "stranger", MessageID: 5, FromUID: "someone", ChannelType: frame.ChannelTypePerson}, false)
	summary := ledger.Summary(expected)
	if summary.Received != 6 || summary.ReceiveAcknowledged != 5 || summary.Distinct != 4 || summary.Duplicates != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.PayloadMismatch != 0 {
		t.Fatalf("matching payloads never count as a mismatch: %+v", summary)
	}
	if summary.MessagesWithoutDelivery != 1 || summary.Unexpected != 1 || summary.Complete {
		t.Fatalf("an undelivered message is an online gap, never durable loss: %+v", summary)
	}
	if summary.WrongSender != 0 {
		t.Fatalf("wrong sender = %d", summary.WrongSender)
	}
	if ledger.Encoded() > 64*1024 {
		t.Fatalf("the ledger summary must stay bounded")
	}
	// The bound is a hard ceiling: entries beyond it are counted as dropped, never retained.
	tiny := NewReceiveLedger(2)
	for i := range expected {
		tiny.Record(expected[i].RecipientUID, recv(i, expected[i], int64(i)), false)
	}
	if got := tiny.Summary(expected).Dropped; got != 3 {
		t.Fatalf("dropped = %d, want 3 beyond the bound", got)
	}
}

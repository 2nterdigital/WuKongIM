package reference

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

// HistoryMethod names the read path the reconciliation uses.
const HistoryMethod = "wukongim POST /channel/messagesync paginated by the sender's login_uid (membership-authorized)"

// ExpectedMessage is one successfully acknowledged logical SEND, by identity.
type ExpectedMessage struct {
	ClientMsgNo   string
	SenderUID     string
	RecipientUID  string
	ChannelID     string
	PayloadDigest string
	MessageID     int64
	MessageSeq    uint64
	Phase         string
}

// HistoryRow is one row read back from history, with its payload reduced to a digest.
type HistoryRow struct {
	ClientMsgNo   string
	MessageID     int64
	MessageSeq    uint64
	FromUID       string
	ChannelID     string
	ChannelType   uint8
	PayloadDigest string
	PayloadBytes  int
}

// HistoryReadStats describes how far the read-back got.
type HistoryReadStats struct {
	ChannelsRead     uint64
	PagesRead        uint64
	Complete         bool
	IncompleteReason string
}

// HistoryResult is the exact reconciliation of acknowledged identities
// against the rows read back, in the comparison document's vocabulary.
type HistoryResult struct {
	Audited          bool    `json:"audited"`
	Method           string  `json:"method"`
	ChannelsRead     uint64  `json:"channels_read"`
	PagesRead        uint64  `json:"pages_read"`
	Expected         uint64  `json:"expected"`
	Matched          uint64  `json:"matched"`
	Missing          uint64  `json:"missing"`
	Duplicate        uint64  `json:"duplicate"`
	Conflicting      uint64  `json:"conflicting"`
	WrongChannel     uint64  `json:"wrong_channel"`
	WrongContent     uint64  `json:"wrong_content"`
	Misordered       uint64  `json:"misordered"`
	UnexpectedRows   uint64  `json:"unexpected_rows"`
	Complete         bool    `json:"complete"`
	IncompleteReason *string `json:"incomplete_reason"`
}

// Correct reports whether every acknowledged identity was read back exactly
// once with the right channel, content and order and nothing unexpected appeared.
func (r HistoryResult) Correct() bool {
	return r.Audited && r.Complete && r.Matched == r.Expected && r.Missing == 0 && r.Duplicate == 0 &&
		r.Conflicting == 0 && r.WrongChannel == 0 && r.WrongContent == 0 && r.Misordered == 0 && r.UnexpectedRows == 0
}

// Reconcile judges every row against the acknowledged set. Each expected
// identity receives exactly one disposition (matched, missing, conflicting,
// wrong channel or wrong content); a second row for the same identity is a
// duplicate, a row for an unknown identity is unexpected, and a sequence
// that goes backwards inside one channel is misordered.
func Reconcile(expected []ExpectedMessage, rows []HistoryRow, stats HistoryReadStats) HistoryResult {
	result := HistoryResult{
		Audited:      true,
		Method:       HistoryMethod,
		ChannelsRead: stats.ChannelsRead,
		PagesRead:    stats.PagesRead,
		Expected:     uint64(len(expected)),
		Complete:     stats.Complete,
	}
	if !stats.Complete {
		reason := stats.IncompleteReason
		if reason == "" {
			reason = "history read-back did not complete"
		}
		result.IncompleteReason = &reason
	}
	index := make(map[string]int, len(expected))
	for i, message := range expected {
		index[message.ClientMsgNo] = i
	}
	seen := make([]bool, len(expected))
	lastSeq := make(map[string]uint64, 64)
	for _, row := range rows {
		position, known := index[row.ClientMsgNo]
		if !known {
			result.UnexpectedRows++
			continue
		}
		if last, ok := lastSeq[row.ChannelID]; ok && row.MessageSeq <= last {
			result.Misordered++
		}
		lastSeq[row.ChannelID] = row.MessageSeq
		if seen[position] {
			result.Duplicate++
			continue
		}
		seen[position] = true
		message := expected[position]
		switch {
		case row.ChannelID != message.ChannelID:
			result.WrongChannel++
		case row.PayloadDigest != message.PayloadDigest:
			result.WrongContent++
		case row.FromUID != message.SenderUID || (message.MessageID > 0 && row.MessageID != message.MessageID) ||
			(message.MessageSeq > 0 && row.MessageSeq != message.MessageSeq):
			result.Conflicting++
		default:
			result.Matched++
		}
	}
	for _, ok := range seen {
		if !ok {
			result.Missing++
		}
	}
	return result
}

// ReceiveSummary is the online-delivery oracle, kept apart from history.
type ReceiveSummary struct {
	Observed                bool   `json:"observed"`
	Received                uint64 `json:"received"`
	ReceiveAcknowledged     uint64 `json:"receive_acknowledged"`
	Distinct                uint64 `json:"distinct"`
	Duplicates              uint64 `json:"duplicates"`
	MessagesWithoutDelivery uint64 `json:"messages_without_delivery"`
	Withheld                uint64 `json:"withheld"`
	Unexpected              uint64 `json:"unexpected"`
	WrongSender             uint64 `json:"wrong_sender"`
	PayloadMismatch         uint64 `json:"payload_mismatch"`
	Dropped                 uint64 `json:"dropped"`
	Complete                bool   `json:"complete"`
}

type receiveEntry struct {
	count         uint64
	acknowledged  uint64
	fromUID       string
	payloadDigest string
}

// ReceiveLedger records every RECV a recipient session observed and every
// RECVACK the workload wrote, keyed by client message number and bounded by
// the number of distinct identities it may retain.
type ReceiveLedger struct {
	mu      sync.Mutex
	bound   uint64
	entries map[string]*receiveEntry
	dropped uint64
}

// NewReceiveLedger creates a ledger that retains at most bound distinct identities.
func NewReceiveLedger(bound uint64) *ReceiveLedger {
	return &ReceiveLedger{bound: bound, entries: make(map[string]*receiveEntry)}
}

// Record notes one RECV and whether a RECVACK was written for it.
func (l *ReceiveLedger) Record(recipientUID string, recv *frame.RecvPacket, acknowledged bool) {
	if recv == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[recv.ClientMsgNo]
	if !ok {
		if uint64(len(l.entries)) >= l.bound {
			l.dropped++
			return
		}
		entry = &receiveEntry{fromUID: recv.FromUID, payloadDigest: DigestHex(recv.Payload)}
		l.entries[recv.ClientMsgNo] = entry
	}
	entry.count++
	if acknowledged {
		entry.acknowledged++
	}
	_ = recipientUID
}

// Summary judges the ledger against the acknowledged set.
func (l *ReceiveLedger) Summary(expected []ExpectedMessage) ReceiveSummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	summary := ReceiveSummary{Observed: true, Dropped: l.dropped}
	known := make(map[string]ExpectedMessage, len(expected))
	for _, message := range expected {
		known[message.ClientMsgNo] = message
	}
	for clientMsgNo, entry := range l.entries {
		summary.Received += entry.count
		summary.ReceiveAcknowledged += entry.acknowledged
		message, ok := known[clientMsgNo]
		if !ok {
			summary.Unexpected++
			continue
		}
		summary.Distinct++
		summary.Duplicates += entry.count - 1
		if entry.fromUID != message.SenderUID {
			summary.WrongSender++
		}
		if entry.payloadDigest != message.PayloadDigest {
			summary.PayloadMismatch++
		}
	}
	for _, message := range expected {
		if _, ok := l.entries[message.ClientMsgNo]; !ok {
			summary.MessagesWithoutDelivery++
		}
	}
	summary.Complete = summary.MessagesWithoutDelivery == 0 && summary.Unexpected == 0 && summary.Dropped == 0 &&
		summary.WrongSender == 0 && summary.PayloadMismatch == 0
	return summary
}

// Encoded is the size of the ledger's bounded JSON projection: counts per
// identity, never payloads.
func (l *ReceiveLedger) Encoded() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]string, 0, len(l.entries))
	for key := range l.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	projection := make(map[string][2]uint64, len(keys))
	for _, key := range keys {
		projection[key] = [2]uint64{l.entries[key].count, l.entries[key].acknowledged}
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return 0
	}
	return len(encoded)
}

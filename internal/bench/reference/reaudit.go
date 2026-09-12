package reference

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const (
	// AcknowledgedLedgerSchema names the private ledger of acknowledged
	// identities a run writes for the later cold-restart re-audit. It is
	// workload state, not an observation: it carries identities, lives in the
	// run root at mode 0600 and never enters a run result or document.
	AcknowledgedLedgerSchema = "wukongim-reference-acknowledged-ledger/1"
	// ReauditSchema names the re-audit result.
	ReauditSchema = "wukongim-reference-reaudit/1"
	// maxLedgerBytes bounds a ledger read; a larger file is refused.
	maxLedgerBytes = 64 << 20
)

type ledgerHeader struct {
	Schema  string `json:"schema"`
	Records uint64 `json:"records"`
}

type ledgerLine struct {
	Index         uint64 `json:"index"`
	Phase         string `json:"phase"`
	ClientMsgNo   string `json:"client_msg_no"`
	SenderUID     string `json:"sender_uid"`
	RecipientUID  string `json:"recipient_uid"`
	ChannelID     string `json:"channel_id"`
	PayloadDigest string `json:"payload_digest"`
	MessageID     int64  `json:"message_id"`
	MessageSeq    uint64 `json:"message_seq"`
}

// WriteAcknowledgedLedger writes every acknowledged record as one JSON line
// after a header line, atomically and at mode 0600. It returns the record
// count and the file's sha256.
func WriteAcknowledgedLedger(path string, records []Record) (uint64, string, error) {
	if path == "" {
		return 0, "", errors.New("acknowledged ledger: path is required")
	}
	var count uint64
	for _, record := range records {
		if record.Class == ClassAcknowledged {
			count++
		}
	}
	var buffer strings.Builder
	encoder := json.NewEncoder(&buffer)
	if err := encoder.Encode(ledgerHeader{Schema: AcknowledgedLedgerSchema, Records: count}); err != nil {
		return 0, "", err
	}
	for _, record := range records {
		if record.Class != ClassAcknowledged {
			continue
		}
		if err := encoder.Encode(ledgerLine{
			Index: record.Index, Phase: record.Phase, ClientMsgNo: record.ClientMsgNo, SenderUID: record.SenderUID,
			RecipientUID: record.RecipientUID, ChannelID: record.ChannelID, PayloadDigest: record.PayloadDigest,
			MessageID: record.MessageID, MessageSeq: record.MessageSeq,
		}); err != nil {
			return 0, "", err
		}
	}
	encoded := []byte(buffer.String())
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, encoded, 0o600); err != nil {
		return 0, "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, "", err
	}
	sum := sha256.Sum256(encoded)
	return count, hex.EncodeToString(sum[:]), nil
}

// ReadAcknowledgedLedger reads a ledger written by WriteAcknowledgedLedger
// within the byte bound and returns the expected messages and the file's
// sha256. A header mismatch or a short file is refused.
func ReadAcknowledgedLedger(path string) ([]ExpectedMessage, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxLedgerBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(data) > maxLedgerBytes {
		return nil, "", fmt.Errorf("acknowledged ledger exceeds the %d-byte bound", maxLedgerBytes)
	}
	sum := sha256.Sum256(data)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	if !scanner.Scan() {
		return nil, "", errors.New("acknowledged ledger: empty file")
	}
	var header ledgerHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil || header.Schema != AcknowledgedLedgerSchema {
		return nil, "", fmt.Errorf("acknowledged ledger: header is not %s", AcknowledgedLedgerSchema)
	}
	expected := make([]ExpectedMessage, 0, header.Records)
	for scanner.Scan() {
		if len(strings.TrimSpace(scanner.Text())) == 0 {
			continue
		}
		var line ledgerLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			return nil, "", fmt.Errorf("acknowledged ledger: line %d does not decode", len(expected)+2)
		}
		if line.ClientMsgNo == "" || line.SenderUID == "" || line.RecipientUID == "" || line.ChannelID == "" || line.PayloadDigest == "" {
			return nil, "", fmt.Errorf("acknowledged ledger: line %d lacks an identity", len(expected)+2)
		}
		expected = append(expected, ExpectedMessage{
			ClientMsgNo: line.ClientMsgNo, SenderUID: line.SenderUID, RecipientUID: line.RecipientUID, ChannelID: line.ChannelID,
			PayloadDigest: line.PayloadDigest, MessageID: line.MessageID, MessageSeq: line.MessageSeq, Phase: line.Phase,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, "", err
	}
	if uint64(len(expected)) != header.Records {
		return nil, "", fmt.Errorf("acknowledged ledger: header declares %d records, file holds %d", header.Records, len(expected))
	}
	return expected, hex.EncodeToString(sum[:]), nil
}

// ReauditResult is the outcome of re-reading every acknowledged identity from
// history, after a cold restart, against the run's own acknowledged ledger.
type ReauditResult struct {
	Schema       string        `json:"schema"`
	StartedAtUTC string        `json:"started_at_utc"`
	EndedAtUTC   string        `json:"ended_at_utc"`
	LedgerSha256 string        `json:"ledger_sha256"`
	Expected     uint64        `json:"expected"`
	History      HistoryResult `json:"history"`
}

// Reaudit reconciles the expected messages against a fresh paginated history
// read; it sends nothing and adds no load.
func Reaudit(ctx context.Context, reader HistoryReader, expected []ExpectedMessage, ledgerSha256 string, log func(string)) (ReauditResult, error) {
	if reader == nil {
		return ReauditResult{}, errors.New("reaudit: history reader is required")
	}
	if len(expected) == 0 {
		return ReauditResult{}, errors.New("reaudit: the acknowledged ledger is empty; nothing to re-audit")
	}
	logf := func(format string, args ...any) {
		if log != nil {
			log(fmt.Sprintf(format, args...))
		}
	}
	started := time.Now().UTC()
	history := reconcileHistory(ctx, reader, expected, logf)
	return ReauditResult{
		Schema:       ReauditSchema,
		StartedAtUTC: started.Format(time.RFC3339),
		EndedAtUTC:   time.Now().UTC().Format(time.RFC3339),
		LedgerSha256: ledgerSha256,
		Expected:     uint64(len(expected)),
		History:      history,
	}, nil
}

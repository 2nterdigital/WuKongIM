package reference

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcknowledgedLedgerRoundTripsOnlyAcknowledgedIdentitiesAtOwnerOnlyMode(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	run := runFake(t, plan, cluster, false, &logs)
	if run.Schedule == nil || len(run.Schedule.Records) == 0 {
		t.Fatal("the run keeps its schedule records in memory for the ledger")
	}
	records := append([]Record(nil), run.Schedule.Records...)
	records = append(records, Record{Index: 999_999, Class: ClassRejected, ClientMsgNo: "rejected-one", SenderUID: "s", RecipientUID: "r", ChannelID: "c", PayloadDigest: "d"})
	path := filepath.Join(t.TempDir(), "acknowledged-ledger.jsonl")
	count, sum, err := WriteAcknowledgedLedger(path, records)
	if err != nil {
		t.Fatal(err)
	}
	if count != run.Accounting.Total.Acknowledged || len(sum) != 64 {
		t.Fatalf("count=%d sha=%q, want %d acknowledged", count, sum, run.Accounting.Total.Acknowledged)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ledger mode = %o", info.Mode().Perm())
	}
	expected, readSum, err := ReadAcknowledgedLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(expected)) != count || readSum != sum {
		t.Fatalf("read %d records sha %q, want %d %q", len(expected), readSum, count, sum)
	}
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "rejected-one") || strings.Contains(string(data), "\"payload\"") {
		t.Fatal("only acknowledged identities and digests are written")
	}
	if expected[0].ClientMsgNo != plan.ClientMsgNo(0) || expected[0].SenderUID != plan.Senders[0].UID {
		t.Fatalf("first expected = %+v", expected[0])
	}
	// A header that disagrees with the body is refused.
	corrupt := strings.Replace(string(data), `"records":`+itoaU(count), `"records":`+itoaU(count+1), 1)
	if err := os.WriteFile(path, []byte(corrupt), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadAcknowledgedLedger(path); err == nil {
		t.Fatal("a ledger whose header disagrees with its body is refused")
	}
	if _, _, err := ReadAcknowledgedLedger(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("a missing ledger is refused")
	}
	if _, _, err := WriteAcknowledgedLedger("", records); err == nil {
		t.Fatal("a ledger needs a path")
	}
}

func itoaU(n uint64) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func TestReauditReconcilesTheLedgerAgainstHistoryAfterTheRun(t *testing.T) {
	t.Parallel()
	plan := newTestPlan(t, 100)
	cluster := newFakeCluster(plan)
	var logs []string
	run := runFake(t, plan, cluster, false, &logs)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if _, _, err := WriteAcknowledgedLedger(path, run.Schedule.Records); err != nil {
		t.Fatal(err)
	}
	expected, sum, err := ReadAcknowledgedLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	// The fake cluster's history survives "the restart": the same rows answer.
	result, err := Reaudit(context.Background(), cluster, expected, sum, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if result.Schema != ReauditSchema || result.Expected != uint64(len(expected)) || result.LedgerSha256 != sum {
		t.Fatalf("result identity = %+v", result)
	}
	if !result.History.Correct() || result.History.Matched != uint64(len(expected)) || result.History.Missing != 0 {
		t.Fatalf("history after restart = %+v", result.History)
	}
	// A row lost by the restart is missing, never silently matched.
	cluster.mu.Lock()
	for key, rows := range cluster.history {
		if len(rows) > 0 {
			cluster.history[key] = rows[:len(rows)-1]
			break
		}
	}
	cluster.mu.Unlock()
	lossy, err := Reaudit(context.Background(), cluster, expected, sum, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lossy.History.Correct() || lossy.History.Missing != 1 {
		t.Fatalf("a lost row is reported missing: %+v", lossy.History)
	}
	if _, err := Reaudit(context.Background(), cluster, nil, sum, nil); err == nil {
		t.Fatal("an empty ledger cannot be re-audited")
	}
	if _, err := Reaudit(context.Background(), nil, expected, sum, nil); err == nil {
		t.Fatal("a reader is required")
	}
}

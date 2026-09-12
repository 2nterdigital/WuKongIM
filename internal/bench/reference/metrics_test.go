package reference

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const scrapeBefore = `# HELP wukongim_storage_commit_batch_duration_seconds Grouped storage commit stage latency in seconds.
# TYPE wukongim_storage_commit_batch_duration_seconds histogram
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="commit",result="ok",le="0.001"} 100
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="commit",result="ok",le="0.01"} 150
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="commit",result="ok",le="+Inf"} 160
wukongim_storage_commit_batch_duration_seconds_sum{node_id="1",node_name="n1",store="message",stage="commit",result="ok"} 0.5
wukongim_storage_commit_batch_duration_seconds_count{node_id="1",node_name="n1",store="message",stage="commit",result="ok"} 160
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="collect",result="ok",le="0.0005"} 120
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="collect",result="ok",le="0.001"} 158
wukongim_storage_commit_batch_duration_seconds_bucket{node_id="1",node_name="n1",store="message",stage="collect",result="ok",le="+Inf"} 160
wukongim_storage_commit_batch_duration_seconds_sum{node_id="1",node_name="n1",store="message",stage="collect",result="ok"} 0.1
wukongim_storage_commit_batch_duration_seconds_count{node_id="1",node_name="n1",store="message",stage="collect",result="ok"} 160
# TYPE wukongim_storage_commit_batch_records histogram
wukongim_storage_commit_batch_records_bucket{node_id="1",node_name="n1",store="message",le="1"} 100
wukongim_storage_commit_batch_records_bucket{node_id="1",node_name="n1",store="message",le="2"} 140
wukongim_storage_commit_batch_records_bucket{node_id="1",node_name="n1",store="message",le="4"} 160
wukongim_storage_commit_batch_records_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160
wukongim_storage_commit_batch_records_sum{node_id="1",node_name="n1",store="message"} 260
wukongim_storage_commit_batch_records_count{node_id="1",node_name="n1",store="message"} 160
# TYPE wukongim_storage_commit_batch_bytes histogram
wukongim_storage_commit_batch_bytes_bucket{node_id="1",node_name="n1",store="message",le="1024"} 150
wukongim_storage_commit_batch_bytes_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160
wukongim_storage_commit_batch_bytes_sum{node_id="1",node_name="n1",store="message"} 80000
wukongim_storage_commit_batch_bytes_count{node_id="1",node_name="n1",store="message"} 160
# TYPE wukongim_storage_commit_batch_requests histogram
wukongim_storage_commit_batch_requests_bucket{node_id="1",node_name="n1",store="message",le="1"} 120
wukongim_storage_commit_batch_requests_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160
wukongim_storage_commit_batch_requests_sum{node_id="1",node_name="n1",store="message"} 220
wukongim_storage_commit_batch_requests_count{node_id="1",node_name="n1",store="message"} 160
# TYPE wukongim_channelv2_append_batch_records histogram
wukongim_channelv2_append_batch_records_bucket{node_id="1",node_name="n1",le="1"} 200
wukongim_channelv2_append_batch_records_bucket{node_id="1",node_name="n1",le="8"} 210
wukongim_channelv2_append_batch_records_bucket{node_id="1",node_name="n1",le="+Inf"} 210
wukongim_channelv2_append_batch_records_sum{node_id="1",node_name="n1"} 240
wukongim_channelv2_append_batch_records_count{node_id="1",node_name="n1"} 210
# TYPE wukongim_channelv2_replication_stage_total counter
wukongim_channelv2_replication_stage_total{node_id="1",node_name="n1",stage="peer_foreground_exchange",result="ok"} 300
wukongim_channelv2_replication_stage_total{node_id="1",node_name="n1",stage="quorum_local_store",result="ok"} 210
# TYPE wukongim_storage_commit_queue_depth gauge
wukongim_storage_commit_queue_depth{node_id="1",node_name="n1",store="message"} 3
# TYPE wukongim_gateway_sendacks_total counter
wukongim_gateway_sendacks_total{node_id="1",node_name="n1",reason="success",source="send",class="none"} 1000
wukongim_gateway_sendacks_total{node_id="1",node_name="n1",reason="node_not_match",source="send",class="timeout"} 2
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total{node_id="1",node_name="n1"} 12.5
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes{node_id="1",node_name="n1"} 150000000
`

func bump(text string, replacements map[string]string) string {
	for old, updated := range replacements {
		text = strings.Replace(text, old, updated, 1)
	}
	return text
}

func scrapeAfter() string {
	return bump(scrapeBefore, map[string]string{
		`stage="commit",result="ok",le="0.001"} 100`:                                        `stage="commit",result="ok",le="0.001"} 1100`,
		`stage="commit",result="ok",le="0.01"} 150`:                                         `stage="commit",result="ok",le="0.01"} 1160`,
		`stage="commit",result="ok",le="+Inf"} 160`:                                         `stage="commit",result="ok",le="+Inf"} 1180`,
		`stage="commit",result="ok"} 160`:                                                   `stage="commit",result="ok"} 1180`,
		`stage="collect",result="ok",le="0.0005"} 120`:                                      `stage="collect",result="ok",le="0.0005"} 1000`,
		`stage="collect",result="ok",le="0.001"} 158`:                                       `stage="collect",result="ok",le="0.001"} 1170`,
		`stage="collect",result="ok",le="+Inf"} 160`:                                        `stage="collect",result="ok",le="+Inf"} 1180`,
		`stage="collect",result="ok"} 0.1`:                                                  `stage="collect",result="ok"} 0.9`,
		`stage="collect",result="ok"} 160`:                                                  `stage="collect",result="ok"} 1180`,
		`store="message",le="1"} 100`:                                                       `store="message",le="1"} 700`,
		`store="message",le="2"} 140`:                                                       `store="message",le="2"} 1000`,
		`store="message",le="4"} 160`:                                                       `store="message",le="4"} 1180`,
		`records_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160`:          `records_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 1180`,
		`records_sum{node_id="1",node_name="n1",store="message"} 260`:                       `records_sum{node_id="1",node_name="n1",store="message"} 2300`,
		`records_count{node_id="1",node_name="n1",store="message"} 160`:                     `records_count{node_id="1",node_name="n1",store="message"} 1180`,
		`bytes_bucket{node_id="1",node_name="n1",store="message",le="1024"} 150`:            `bytes_bucket{node_id="1",node_name="n1",store="message",le="1024"} 1100`,
		`bytes_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160`:            `bytes_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 1180`,
		`bytes_sum{node_id="1",node_name="n1",store="message"} 80000`:                       `bytes_sum{node_id="1",node_name="n1",store="message"} 600000`,
		`bytes_count{node_id="1",node_name="n1",store="message"} 160`:                       `bytes_count{node_id="1",node_name="n1",store="message"} 1180`,
		`requests_bucket{node_id="1",node_name="n1",store="message",le="1"} 120`:            `requests_bucket{node_id="1",node_name="n1",store="message",le="1"} 900`,
		`requests_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 160`:         `requests_bucket{node_id="1",node_name="n1",store="message",le="+Inf"} 1180`,
		`requests_sum{node_id="1",node_name="n1",store="message"} 220`:                      `requests_sum{node_id="1",node_name="n1",store="message"} 1600`,
		`requests_count{node_id="1",node_name="n1",store="message"} 160`:                    `requests_count{node_id="1",node_name="n1",store="message"} 1180`,
		`append_batch_records_bucket{node_id="1",node_name="n1",le="1"} 200`:                `append_batch_records_bucket{node_id="1",node_name="n1",le="1"} 1500`,
		`append_batch_records_bucket{node_id="1",node_name="n1",le="8"} 210`:                `append_batch_records_bucket{node_id="1",node_name="n1",le="8"} 1600`,
		`append_batch_records_bucket{node_id="1",node_name="n1",le="+Inf"} 210`:             `append_batch_records_bucket{node_id="1",node_name="n1",le="+Inf"} 1600`,
		`append_batch_records_sum{node_id="1",node_name="n1"} 240`:                          `append_batch_records_sum{node_id="1",node_name="n1"} 1900`,
		`append_batch_records_count{node_id="1",node_name="n1"} 210`:                        `append_batch_records_count{node_id="1",node_name="n1"} 1600`,
		`stage="peer_foreground_exchange",result="ok"} 300`:                                 `stage="peer_foreground_exchange",result="ok"} 3100`,
		`stage="quorum_local_store",result="ok"} 210`:                                       `stage="quorum_local_store",result="ok"} 1600`,
		`wukongim_storage_commit_queue_depth{node_id="1",node_name="n1",store="message"} 3`: `wukongim_storage_commit_queue_depth{node_id="1",node_name="n1",store="message"} 9`,
		`reason="success",source="send",class="none"} 1000`:                                 `reason="success",source="send",class="none"} 2390`,
		`process_cpu_seconds_total{node_id="1",node_name="n1"} 12.5`:                        `process_cpu_seconds_total{node_id="1",node_name="n1"} 40.0`,
		`process_resident_memory_bytes{node_id="1",node_name="n1"} 150000000`:               `process_resident_memory_bytes{node_id="1",node_name="n1"} 190000000`,
	})
}

func TestPhysicalWorkFromTwoScrapesUsesOnlyRuntimeCounters(t *testing.T) {
	t.Parallel()
	before, err := ParseScrape([]byte(scrapeBefore))
	if err != nil {
		t.Fatal(err)
	}
	after, err := ParseScrape([]byte(scrapeAfter()))
	if err != nil {
		t.Fatal(err)
	}
	work := PhysicalWorkDelta([]NodeScrapes{{Node: "node-1", Before: before, After: after}})
	if work.PhysicalCommits.Unavailable != "" || work.PhysicalCommits.Value.Count != 1020 || work.PhysicalCommits.Value.Records == nil || *work.PhysicalCommits.Value.Records != 2040 || *work.PhysicalCommits.Value.Bytes != 520000 {
		t.Fatalf("physical commits = %+v", work.PhysicalCommits)
	}
	if work.LogicalCommitBatches.Value.Count != 1020 || *work.LogicalCommitBatches.Value.Records != 1380 {
		t.Fatalf("logical commit batches = %+v (requests per grouped commit)", work.LogicalCommitBatches)
	}
	records := work.RecordsPerPhysicalCommit.Value
	if work.RecordsPerPhysicalCommit.Unavailable != "" || records.Count != 1020 || records.Sum != 2040 || records.Unit != "records" {
		t.Fatalf("records per physical commit = %+v", work.RecordsPerPhysicalCommit)
	}
	if len(records.Bounds) != 3 || records.Bounds[0] != 1 || records.Bounds[2] != 4 || len(records.Buckets) != 4 {
		t.Fatalf("records distribution bounds/buckets = %v / %v", records.Bounds, records.Buckets)
	}
	if records.Buckets[0] != 600 || records.Buckets[1] != 260 || records.Buckets[2] != 160 || records.Buckets[3] != 0 || records.Max != 4 {
		t.Fatalf("records distribution buckets = %v max %d", records.Buckets, records.Max)
	}
	if work.BytesPerPhysicalCommit.Value.Sum != 520000 || work.BytesPerPhysicalCommit.Value.Unit != "bytes" {
		t.Fatalf("bytes per physical commit = %+v", work.BytesPerPhysicalCommit)
	}
	delay := work.BatchCollectionDelayUs.Value
	if work.BatchCollectionDelayUs.Unavailable != "" || delay.Unit != "us" || delay.Count != 1020 || delay.Bounds[0] != 500 || delay.Bounds[1] != 1000 || delay.Buckets[0] != 880 || delay.Buckets[1] != 132 || delay.Buckets[2] != 8 {
		t.Fatalf("collection delay = %+v", work.BatchCollectionDelayUs)
	}
	if work.ProposalBatches.Value.Count != 1390 || *work.ProposalBatches.Value.Records != 1660 {
		t.Fatalf("proposal batches (append requests) = %+v", work.ProposalBatches)
	}
	if work.RecordsPerProposal.Value.Count != 1390 || work.RecordsPerProposal.Value.Buckets[0] != 1300 {
		t.Fatalf("records per proposal = %+v", work.RecordsPerProposal)
	}
	if work.ReplicationRPCs.Value.Count != 2800 {
		t.Fatalf("replication RPCs = %+v (peer exchange stages only)", work.ReplicationRPCs)
	}
	if work.Fsyncs.Unavailable == "" || !strings.Contains(work.Fsyncs.Unavailable, "Commit(true)") {
		t.Fatalf("fsyncs must stay unavailable with the source note, never inferred: %+v", work.Fsyncs)
	}
	native := NativeCounters([]NodeScrapes{{Node: "node-1", Before: before, After: after}})
	if native["wukongim_storage_commit_queue_depth"].(map[string]any)["max"] != 9.0 {
		t.Fatalf("queue depth max = %v", native["wukongim_storage_commit_queue_depth"])
	}
	sendacks := native["wukongim_gateway_sendacks_total"].(map[string]any)
	if sendacks["reason=success"] != 1390.0 || sendacks["reason=node_not_match"] != 0.0 {
		t.Fatalf("sendacks = %v", sendacks)
	}
	resources := ProcessResources([]NodeScrapes{{Node: "node-1", Before: before, After: after}})
	if resources["node-1"].CPUMs != 27500 || resources["node-1"].MaxRSSBytes != 190000000 {
		t.Fatalf("process resources = %+v", resources)
	}
}

func TestPhysicalWorkIsUnavailableWhenAFamilyIsAbsent(t *testing.T) {
	t.Parallel()
	before, _ := ParseScrape([]byte("process_cpu_seconds_total{node_id=\"1\"} 1\n"))
	after, _ := ParseScrape([]byte("process_cpu_seconds_total{node_id=\"1\"} 2\n"))
	work := PhysicalWorkDelta([]NodeScrapes{{Node: "node-1", Before: before, After: after}})
	for name, field := range map[string]string{
		"physical_commits":            work.PhysicalCommits.Unavailable,
		"records_per_physical_commit": work.RecordsPerPhysicalCommit.Unavailable,
		"bytes_per_physical_commit":   work.BytesPerPhysicalCommit.Unavailable,
		"logical_commit_batches":      work.LogicalCommitBatches.Unavailable,
		"batch_collection_delay_us":   work.BatchCollectionDelayUs.Unavailable,
		"proposal_batches":            work.ProposalBatches.Unavailable,
		"records_per_proposal":        work.RecordsPerProposal.Unavailable,
		"replication_rpcs":            work.ReplicationRPCs.Unavailable,
		"fsyncs":                      work.Fsyncs.Unavailable,
	} {
		if field == "" {
			t.Fatalf("%s must be unavailable with a reason when its family was not scraped", name)
		}
	}
	none := PhysicalWorkDelta(nil)
	if none.PhysicalCommits.Unavailable == "" || !strings.Contains(none.PhysicalCommits.Unavailable, "no scrape") {
		t.Fatalf("no scrapes at all = %+v", none.PhysicalCommits)
	}
}

func TestScrapeIsBoundedAndParsesTheTextExposition(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(scrapeBefore))
	}))
	defer server.Close()
	snapshot, err := Scrape(context.Background(), server.URL, 0)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if len(snapshot.Samples) == 0 {
		t.Fatalf("no samples parsed")
	}
	if _, err := Scrape(context.Background(), server.URL, 64); err == nil {
		t.Fatalf("a scrape above its byte bound is refused, not truncated")
	}
	if _, err := Scrape(context.Background(), server.URL+"/missing", 0); err == nil {
		t.Fatalf("a non-200 answer is an error")
	}
}

func TestNativeCountersAreAFixedAllowlistWithoutIdentities(t *testing.T) {
	t.Parallel()
	before, _ := ParseScrape([]byte(scrapeBefore + "wukongim_channel_secret{uid=\"u1\",channel_id=\"u1@u2\"} 1\n"))
	after, _ := ParseScrape([]byte(scrapeAfter() + "wukongim_channel_secret{uid=\"u1\",channel_id=\"u1@u2\"} 2\n"))
	native := NativeCounters([]NodeScrapes{{Node: "node-1", Before: before, After: after}})
	for key := range native {
		if !nativeAllowlist[key] {
			t.Fatalf("native key %q is not on the fixed allowlist", key)
		}
	}
	encoded := nativeEncoded(t, native)
	for _, forbidden := range []string{"uid=", "channel_id", "u1@u2", "n1"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("native counters must not carry identities or node names: %s", encoded)
		}
	}
	if len(encoded) > 64*1024 {
		t.Fatalf("native counters must stay bounded")
	}
}

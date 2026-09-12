package reference

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
)

func TestPlanDeclaresTheFixedPersonProfile(t *testing.T) {
	t.Parallel()
	plan, err := NewPlan(PlanConfig{RatePerSecond: 150, IdentitySeed: "reference-comparison-seed-1", RunID: "wukongim-common-150-seq05"})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	if got := plan.Connections(); got != 273 {
		t.Fatalf("connections = %d, want 273", got)
	}
	if len(plan.Senders) != 256 || len(plan.Recipients) != 17 {
		t.Fatalf("senders=%d recipients=%d, want 256 and 17", len(plan.Senders), len(plan.Recipients))
	}
	if plan.PayloadBytes != 256 {
		t.Fatalf("payload bytes = %d, want 256", plan.PayloadBytes)
	}
	phases := plan.Phases()
	want := []Phase{
		{Name: "warmup", RatePerSecond: 50, Duration: 30 * time.Second},
		{Name: "measure", RatePerSecond: 150, Duration: 120 * time.Second},
		{Name: "reduction", RatePerSecond: 50, Duration: 60 * time.Second},
	}
	if len(phases) != len(want) {
		t.Fatalf("phases = %+v, want %+v", phases, want)
	}
	for i := range want {
		if phases[i] != want[i] {
			t.Fatalf("phase %d = %+v, want %+v", i, phases[i], want[i])
		}
	}
	for i, count := range []uint64{1500, 18000, 3000} {
		if got := plan.Scheduled(phases[i].Name); got != count {
			t.Fatalf("scheduled(%s) = %d, want %d", phases[i].Name, got, count)
		}
	}
	if got := plan.TotalScheduled(); got != 22500 {
		t.Fatalf("total scheduled = %d, want 22500", got)
	}
	if plan.PersonChannelsMax() != 4352 {
		t.Fatalf("person channels max = %d, want 4352", plan.PersonChannelsMax())
	}
	if _, err := NewPlan(PlanConfig{RatePerSecond: 0, IdentitySeed: "s", RunID: "r"}); err == nil {
		t.Fatalf("a zero rate must be refused")
	}
	if _, err := NewPlan(PlanConfig{RatePerSecond: 150, IdentitySeed: "", RunID: "r"}); err == nil {
		t.Fatalf("an empty identity seed must be refused")
	}
}

func TestIdentitiesAndPayloadsAreDeterministicFromTheSeed(t *testing.T) {
	t.Parallel()
	first, err := NewPlan(PlanConfig{RatePerSecond: 100, IdentitySeed: "seed-a", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPlan(PlanConfig{RatePerSecond: 100, IdentitySeed: "seed-a", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewPlan(PlanConfig{RatePerSecond: 100, IdentitySeed: "seed-b", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range first.Senders {
		if first.Senders[i] != second.Senders[i] {
			t.Fatalf("sender %d differs for the same seed", i)
		}
		if first.Senders[i].Token == "" || first.Senders[i].DeviceID == "" {
			t.Fatalf("sender %d lacks a token or device", i)
		}
	}
	if first.Senders[0].UID == other.Senders[0].UID || first.Senders[0].Token == other.Senders[0].Token {
		t.Fatalf("a different seed must produce different identities")
	}
	seen := map[string]struct{}{}
	for _, identity := range append(append([]Identity(nil), first.Senders...), first.Recipients...) {
		if _, dup := seen[identity.UID]; dup {
			t.Fatalf("uid %s is not unique", identity.UID)
		}
		seen[identity.UID] = struct{}{}
	}
	payload := first.Payload(7)
	if len(payload) != 256 {
		t.Fatalf("payload len = %d, want 256", len(payload))
	}
	if string(payload) != string(second.Payload(7)) {
		t.Fatalf("payload 7 differs for the same seed")
	}
	if string(payload) == string(first.Payload(8)) {
		t.Fatalf("payloads must differ per arrival")
	}
	if first.ClientMsgNo(7) == first.ClientMsgNo(8) || first.ClientMsgNo(7) != second.ClientMsgNo(7) {
		t.Fatalf("client message numbers must be unique per arrival and deterministic")
	}
	if first.ClientMsgNo(7) == other.ClientMsgNo(7) {
		t.Fatalf("client message numbers must differ across seeds")
	}
}

func TestArrivalsAreIndependentlyScheduledWithExactCounts(t *testing.T) {
	t.Parallel()
	plan, err := NewPlan(PlanConfig{RatePerSecond: 175, IdentitySeed: "seed", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	arrivals := plan.Arrivals()
	if uint64(len(arrivals)) != plan.TotalScheduled() {
		t.Fatalf("arrivals = %d, want %d", len(arrivals), plan.TotalScheduled())
	}
	counts := map[string]uint64{}
	channels := map[string]struct{}{}
	var previous time.Duration = -1
	for i, arrival := range arrivals {
		if arrival.Index != uint64(i) {
			t.Fatalf("arrival %d carries index %d", i, arrival.Index)
		}
		if arrival.Due < previous {
			t.Fatalf("arrival %d is due before its predecessor", i)
		}
		previous = arrival.Due
		counts[arrival.Phase]++
		if int(arrival.Sender) != i%256 {
			t.Fatalf("arrival %d sender = %d, want round-robin %d", i, arrival.Sender, i%256)
		}
		if int(arrival.Recipient) != i%17 {
			t.Fatalf("arrival %d recipient = %d, want %d", i, arrival.Recipient, i%17)
		}
		sender := plan.Senders[arrival.Sender]
		recipient := plan.Recipients[arrival.Recipient]
		channel := plan.Channel(arrival)
		if channel != channelid.EncodePersonChannel(sender.UID, recipient.UID) {
			t.Fatalf("arrival %d channel %q is not the canonical person channel", i, channel)
		}
		channels[channel] = struct{}{}
	}
	if counts["warmup"] != 1500 || counts["measure"] != 21000 || counts["reduction"] != 3000 {
		t.Fatalf("phase counts = %v", counts)
	}
	if arrivals[1499].Due >= 30*time.Second || arrivals[1500].Due < 30*time.Second {
		t.Fatalf("the measured window must start exactly at 30 s: %v / %v", arrivals[1499].Due, arrivals[1500].Due)
	}
	if arrivals[22499].Due >= 150*time.Second || arrivals[22500].Due < 150*time.Second {
		t.Fatalf("the reduction must start exactly at 150 s: %v / %v", arrivals[22499].Due, arrivals[22500].Due)
	}
	if last := arrivals[len(arrivals)-1].Due; last >= 210*time.Second {
		t.Fatalf("the last arrival %v must fall inside the 210 s plan", last)
	}
	if len(channels) != 4352 {
		t.Fatalf("distinct person channels = %d, want 4352 (256 senders x 17 recipients)", len(channels))
	}
	if plan.MeasuredWindow() != (Window{Start: 30 * time.Second, Duration: 120 * time.Second}) {
		t.Fatalf("measured window = %+v", plan.MeasuredWindow())
	}
	_ = context.Background()
}

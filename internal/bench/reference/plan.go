// Package reference implements the ech0/WuKongIM reference-comparison Person
// workload for the WuKongIM side: the fixed 273-connection profile, an
// independently scheduled three-phase arrival stream with exact counts, a
// recipient-side receive ledger, paginated history reconciliation, bounded
// metrics scrapes and the normalized comparison document.
//
// The package reproduces external semantics only. It changes no server
// behavior and never infers a physical counter from a message count.
package reference

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
)

const (
	// SenderCount is the number of sending connections.
	SenderCount = 256
	// RecipientCount is the number of receiving connections (Person targets).
	RecipientCount = 17
	// ConnectionCount is every connection the workload opens.
	ConnectionCount = SenderCount + RecipientCount
	// PayloadBytes is the fixed deterministic payload size.
	PayloadBytes = 256
	// WarmupRate and ReductionRate are the shoulder rates in messages per second.
	WarmupRate    = 50
	ReductionRate = 50
	// WarmupDuration, MeasureDuration and ReductionDuration are the fixed phases.
	WarmupDuration    = 30 * time.Second
	MeasureDuration   = 120 * time.Second
	ReductionDuration = 60 * time.Second

	phaseWarmup    = "warmup"
	phaseMeasure   = "measure"
	phaseReduction = "reduction"

	defaultUIDPrefix  = "ref"
	defaultAckTimeout = 35 * time.Second
	defaultDrainBound = 60 * time.Second
)

// PlanConfig declares one reference point.
type PlanConfig struct {
	// RatePerSecond is the measured-window offered rate.
	RatePerSecond uint64
	// IdentitySeed derives every UID, token, device and payload deterministically.
	IdentitySeed string
	// RunID names the launch; it is part of every client message number.
	RunID string
	// UIDPrefix prefixes generated UIDs (default "ref").
	UIDPrefix string
	// AckTimeout bounds one SENDACK wait (default 35 s, the ech0 operation timeout).
	AckTimeout time.Duration
	// DrainBound bounds the wait for in-flight SENDs after the last arrival.
	DrainBound time.Duration
}

// Identity is one deterministic connection identity.
type Identity struct {
	UID      string
	DeviceID string
	Token    string
}

// Phase is one arrival-rate window of the plan.
type Phase struct {
	Name          string
	RatePerSecond uint64
	Duration      time.Duration
}

// Window locates the measured window inside the plan.
type Window struct {
	Start    time.Duration
	Duration time.Duration
}

// Arrival is one independently scheduled logical SEND.
type Arrival struct {
	// Index is the global arrival index across every phase.
	Index uint64
	// Phase names the phase the arrival belongs to, by its due time.
	Phase string
	// Due is the planned offset from the run start.
	Due time.Duration
	// Sender and Recipient index the plan's identities.
	Sender    uint32
	Recipient uint32
}

// Plan is one validated reference point.
type Plan struct {
	Config        PlanConfig
	RatePerSecond uint64
	PayloadBytes  int
	Senders       []Identity
	Recipients    []Identity
	phases        []Phase
	seedTag       string
}

// NewPlan validates the declaration and derives the deterministic identities.
func NewPlan(cfg PlanConfig) (*Plan, error) {
	if cfg.RatePerSecond == 0 {
		return nil, errors.New("reference plan: rate_per_second must be greater than zero")
	}
	if strings.TrimSpace(cfg.IdentitySeed) == "" {
		return nil, errors.New("reference plan: identity_seed is required")
	}
	if strings.TrimSpace(cfg.RunID) == "" {
		return nil, errors.New("reference plan: run_id is required")
	}
	if cfg.UIDPrefix == "" {
		cfg.UIDPrefix = defaultUIDPrefix
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	if cfg.DrainBound <= 0 {
		cfg.DrainBound = defaultDrainBound
	}
	seed := sha256.Sum256([]byte("reference-identity|" + cfg.IdentitySeed))
	tag := hex.EncodeToString(seed[:4])
	plan := &Plan{
		Config:        cfg,
		RatePerSecond: cfg.RatePerSecond,
		PayloadBytes:  PayloadBytes,
		seedTag:       tag,
		phases: []Phase{
			{Name: phaseWarmup, RatePerSecond: WarmupRate, Duration: WarmupDuration},
			{Name: phaseMeasure, RatePerSecond: cfg.RatePerSecond, Duration: MeasureDuration},
			{Name: phaseReduction, RatePerSecond: ReductionRate, Duration: ReductionDuration},
		},
	}
	plan.Senders = make([]Identity, SenderCount)
	for i := range plan.Senders {
		plan.Senders[i] = plan.identity(fmt.Sprintf("%s-%s-s%03d", cfg.UIDPrefix, tag, i))
	}
	plan.Recipients = make([]Identity, RecipientCount)
	for i := range plan.Recipients {
		plan.Recipients[i] = plan.identity(fmt.Sprintf("%s-%s-r%02d", cfg.UIDPrefix, tag, i))
	}
	return plan, nil
}

func (p *Plan) identity(uid string) Identity {
	token := sha256.Sum256([]byte("reference-token|" + p.Config.IdentitySeed + "|" + uid))
	return Identity{
		UID:      uid,
		DeviceID: "ref-dev-" + uid,
		Token:    hex.EncodeToString(token[:16]),
	}
}

// Connections is every connection the workload opens.
func (p *Plan) Connections() int { return len(p.Senders) + len(p.Recipients) }

// Phases returns the three fixed phases in order.
func (p *Plan) Phases() []Phase { return append([]Phase(nil), p.phases...) }

// Scheduled is the exact number of arrivals in the named phase: rate times
// whole seconds, never rounded from wall-clock progress.
func (p *Plan) Scheduled(phase string) uint64 {
	for _, item := range p.phases {
		if item.Name == phase {
			return item.RatePerSecond * uint64(item.Duration/time.Second)
		}
	}
	return 0
}

// TotalScheduled is the exact number of arrivals across every phase.
func (p *Plan) TotalScheduled() uint64 {
	var total uint64
	for _, item := range p.phases {
		total += p.Scheduled(item.Name)
	}
	return total
}

// PersonChannelsMax is the number of distinct sender-recipient pairs.
func (p *Plan) PersonChannelsMax() uint64 { return SenderCount * RecipientCount }

// MeasuredWindow locates the 120 s comparison window.
func (p *Plan) MeasuredWindow() Window {
	return Window{Start: WarmupDuration, Duration: MeasureDuration}
}

// Arrivals lays every phase end-to-end on one origin and assigns each
// arrival its sender (round-robin over 256) and recipient (round-robin over
// 17), so every pair and payload is decided before the run starts.
func (p *Plan) Arrivals() []Arrival {
	arrivals := make([]Arrival, 0, p.TotalScheduled())
	var index uint64
	var phaseStart time.Duration
	for _, phase := range p.phases {
		count := p.Scheduled(phase.Name)
		for i := uint64(0); i < count; i++ {
			offset := time.Duration(i * uint64(time.Second) / phase.RatePerSecond)
			arrivals = append(arrivals, Arrival{
				Index:     index,
				Phase:     phase.Name,
				Due:       phaseStart + offset,
				Sender:    uint32(index % SenderCount),
				Recipient: uint32(index % RecipientCount),
			})
			index++
		}
		phaseStart += phase.Duration
	}
	return arrivals
}

// Channel is the canonical WuKongIM person channel of an arrival's pair.
func (p *Plan) Channel(arrival Arrival) string {
	return channelid.EncodePersonChannel(p.Senders[arrival.Sender].UID, p.Recipients[arrival.Recipient].UID)
}

// ClientMsgNo is the immutable logical identity of one arrival.
func (p *Plan) ClientMsgNo(index uint64) string {
	return fmt.Sprintf("ref-%s-%s-%08d", p.seedTag, p.Config.RunID, index)
}

// Payload is the deterministic 256-byte payload of one arrival.
func (p *Plan) Payload(index uint64) []byte {
	marker := fmt.Sprintf("ref seed=%s run=%s i=%d|", p.seedTag, p.Config.RunID, index)
	fill := sha256.Sum256([]byte(fmt.Sprintf("reference-payload|%s|%d", p.Config.IdentitySeed, index)))
	payload := make([]byte, 0, p.PayloadBytes)
	payload = append(payload, marker...)
	hexFill := hex.EncodeToString(fill[:])
	for len(payload) < p.PayloadBytes {
		remaining := p.PayloadBytes - len(payload)
		if remaining >= len(hexFill) {
			payload = append(payload, hexFill...)
			continue
		}
		payload = append(payload, hexFill[:remaining]...)
	}
	return payload[:p.PayloadBytes]
}

// PayloadDigest is the SHA-256 of an arrival's payload as lowercase hex.
func (p *Plan) PayloadDigest(index uint64) string {
	return DigestHex(p.Payload(index))
}

// DigestHex is the SHA-256 of bytes as lowercase hex.
func DigestHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

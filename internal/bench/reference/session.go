package reference

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	benchtarget "github.com/WuKongIM/WuKongIM/internal/bench/target"
	"github.com/WuKongIM/WuKongIM/pkg/bench/model"
	wkclient "github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

const (
	defaultOperationTimeout = 10 * time.Second
	reasonSendackTimeout    = "sendack_timeout"
)

// SessionConfig opens the plan's connections against real WKProto gateways.
type SessionConfig struct {
	// GatewayAddrs are the WKProto TCP endpoints; connection i uses endpoint
	// i modulo the endpoint count, senders first and recipients after them.
	GatewayAddrs []string
	// AckTimeout bounds the SENDACK wait; zero uses the plan's value.
	AckTimeout time.Duration
	// OperationTimeout bounds connect, write and close (default 10s).
	OperationTimeout time.Duration
	// Dialer overrides the network dialer for tests.
	Dialer wkclient.Dialer
}

// sendFuture is the part of *wkclient.SendFuture a sender waits on.
type sendFuture interface {
	Wait(ctx context.Context) (wkclient.SendResult, error)
}

// sendClient is the part of *wkclient.Client a sender uses.
type sendClient interface {
	SendAsync(ctx context.Context, msg wkclient.Message) (sendFuture, error)
}

type realSendClient struct{ client *wkclient.Client }

func (c realSendClient) SendAsync(ctx context.Context, msg wkclient.Message) (sendFuture, error) {
	future, err := c.client.SendAsync(ctx, msg)
	if err != nil {
		return nil, err
	}
	return future, nil
}

// clientSender sends one SEND at a time on its own connection and classifies
// the outcome from sentinel errors and the SENDACK reason code only; raw error
// text never becomes a reason.
type clientSender struct {
	client sendClient
}

// Send implements Sender over one WKProto connection.
func (s *clientSender) Send(ctx context.Context, _ Arrival, msg Outbound) Outcome {
	future, err := s.client.SendAsync(ctx, wkclient.Message{
		ClientSeq:   msg.ClientSeq,
		ClientMsgNo: msg.ClientMsgNo,
		ChannelID:   msg.ChannelID,
		ChannelType: frame.ChannelTypePerson,
		Payload:     msg.Payload,
	})
	if err != nil {
		return classifySendError(err, false)
	}
	result, err := future.Wait(ctx)
	if err != nil {
		return classifySendError(err, true)
	}
	if result.ReasonCode != frame.ReasonSuccess {
		return Outcome{Class: ClassRejected, NativeReason: "reason:" + reasonKey(result.ReasonCode), Reason: result.ReasonCode}
	}
	return Outcome{
		Class:        ClassAcknowledged,
		NativeReason: "reason:success",
		Reason:       frame.ReasonSuccess,
		MessageID:    result.MessageID,
		MessageSeq:   result.MessageSeq,
	}
}

// classifySendError maps a client error to one class and a closed reason
// vocabulary. Before admission nothing left the process, so the SEND is
// rejected locally; once pending, only a SENDACK timeout is a timeout and
// every other loss of the connection or context is indeterminate.
func classifySendError(err error, pending bool) Outcome {
	if pending {
		switch {
		case errors.Is(err, wkclient.ErrAckTimeout):
			return Outcome{Class: ClassTimedOut, NativeReason: reasonSendackTimeout, Err: err}
		case errors.Is(err, context.Canceled):
			return Outcome{Class: ClassIndeterminate, NativeReason: "context:canceled", Err: err}
		case errors.Is(err, context.DeadlineExceeded):
			return Outcome{Class: ClassIndeterminate, NativeReason: "context:deadline_exceeded", Err: err}
		case errors.Is(err, wkclient.ErrClosed), errors.Is(err, wkclient.ErrNotConnected):
			return Outcome{Class: ClassIndeterminate, NativeReason: "client:closed", Err: err}
		default:
			return Outcome{Class: ClassIndeterminate, NativeReason: "client:error", Err: err}
		}
	}
	reason := "client:error"
	for sentinel, name := range map[error]string{
		wkclient.ErrSendQueueFull:        "client:send_queue_full",
		wkclient.ErrPayloadTooLarge:      "client:payload_too_large",
		wkclient.ErrInvalidMessage:       "client:invalid_message",
		wkclient.ErrDuplicatePendingSend: "client:duplicate_pending_send",
		wkclient.ErrClientSeqExhausted:   "client:client_seq_exhausted",
		wkclient.ErrTerminalFenceActive:  "client:terminal_fence",
		wkclient.ErrNotConnected:         "client:not_connected",
		wkclient.ErrClosed:               "client:closed",
		context.Canceled:                 "client:cancelled",
		context.DeadlineExceeded:         "client:cancelled",
	} {
		if errors.Is(err, sentinel) {
			reason = name
			break
		}
	}
	return Outcome{Class: ClassRejected, NativeReason: reason, Err: err}
}

// clientRecipient owns one receiving connection; it never acknowledges on its
// own, the run's reader does after it records the RECV.
type clientRecipient struct {
	uid    string
	client *wkclient.Client
}

func (r *clientRecipient) UID() string { return r.uid }

// Recv returns the next RECV frame, skipping other inbound frames.
func (r *clientRecipient) Recv(ctx context.Context) (*frame.RecvPacket, error) {
	for {
		f, err := r.client.ReadFrame(ctx)
		if err != nil {
			return nil, err
		}
		if recv, ok := f.(*frame.RecvPacket); ok {
			return recv, nil
		}
	}
}

func (r *clientRecipient) RecvAck(ctx context.Context, messageID int64, messageSeq uint64) error {
	return r.client.RecvAck(ctx, messageID, messageSeq)
}

type clientSessions struct {
	senders    []Sender
	recipients []Recipient
	clients    []*wkclient.Client
}

func (s *clientSessions) Senders() []Sender       { return s.senders }
func (s *clientSessions) Recipients() []Recipient { return s.recipients }

// Close closes every connection and joins the errors.
func (s *clientSessions) Close() error {
	var errs []error
	for _, client := range s.clients {
		if err := client.Close(); err != nil && !errors.Is(err, wkclient.ErrClosed) {
			errs = append(errs, err)
		}
	}
	s.clients = nil
	return errors.Join(errs...)
}

// endpointFor assigns connection index to one endpoint round-robin.
func endpointFor(index int, addrs []string) string {
	return addrs[index%len(addrs)]
}

func dedupeAddrs(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// OpenSessions connects every identity of the plan on its own connection:
// the 256 senders first, then the 17 recipients, each on endpoint index mod
// count. Senders discard inbound RECVs so their reader serves SENDACKs only;
// recipients keep RECVs and acknowledge explicitly. A single failed connect
// closes what was opened and fails the run.
func OpenSessions(ctx context.Context, plan *Plan, cfg SessionConfig) (Sessions, error) {
	if plan == nil {
		return nil, errors.New("reference sessions: plan is required")
	}
	addrs := dedupeAddrs(cfg.GatewayAddrs)
	if len(addrs) == 0 {
		return nil, errors.New("reference sessions: at least one gateway address is required")
	}
	ackTimeout := cfg.AckTimeout
	if ackTimeout <= 0 {
		ackTimeout = plan.Config.AckTimeout
	}
	opTimeout := cfg.OperationTimeout
	if opTimeout <= 0 {
		opTimeout = defaultOperationTimeout
	}
	sessions := &clientSessions{}
	connect := func(index int, identity Identity, sender bool) (*wkclient.Client, error) {
		client, err := wkclient.New(wkclient.Config{
			Addr:               endpointFor(index, addrs),
			Token:              identity.Token,
			Dialer:             cfg.Dialer,
			OperationTimeout:   opTimeout,
			AckTimeout:         ackTimeout,
			DiscardInboundRecv: sender,
			AutoRecvAck:        false,
		})
		if err != nil {
			return nil, err
		}
		connack, err := client.Connect(ctx, wkclient.ConnectOptions{UID: identity.UID, DeviceID: identity.DeviceID, DeviceFlag: frame.APP, Token: identity.Token})
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		if connack != nil && connack.ReasonCode != frame.ReasonSuccess {
			_ = client.Close()
			return nil, fmt.Errorf("connect refused: reason:%s", reasonKey(connack.ReasonCode))
		}
		return client, nil
	}
	for i, identity := range plan.Senders {
		client, err := connect(i, identity, true)
		if err != nil {
			_ = sessions.Close()
			return nil, fmt.Errorf("reference sessions: sender connection %d: %w", i, err)
		}
		sessions.clients = append(sessions.clients, client)
		sessions.senders = append(sessions.senders, &clientSender{client: realSendClient{client: client}})
	}
	for i, identity := range plan.Recipients {
		client, err := connect(len(plan.Senders)+i, identity, false)
		if err != nil {
			_ = sessions.Close()
			return nil, fmt.Errorf("reference sessions: recipient connection %d: %w", i, err)
		}
		sessions.clients = append(sessions.clients, client)
		sessions.recipients = append(sessions.recipients, &clientRecipient{uid: identity.UID, client: client})
	}
	return sessions, nil
}

// RegisterTokens registers every plan identity's CONNECT token through the
// bench preparation API in one idempotent batch.
func RegisterTokens(ctx context.Context, api *benchtarget.Client, plan *Plan) error {
	if api == nil || plan == nil {
		return errors.New("reference tokens: api client and plan are required")
	}
	users := make([]model.UserTokenItem, 0, plan.Connections())
	for _, identity := range plan.Senders {
		users = append(users, model.UserTokenItem{UID: identity.UID, Token: identity.Token})
	}
	for _, identity := range plan.Recipients {
		users = append(users, model.UserTokenItem{UID: identity.UID, Token: identity.Token})
	}
	return api.UpsertTokens(ctx, model.BatchTokensRequest{
		RunID:   plan.Config.RunID,
		BatchID: plan.Config.RunID + "-identities",
		Upsert:  true,
		Users:   users,
	})
}

// NodeAddress names one node's HTTP API base address for scraping.
type NodeAddress struct {
	Name    string
	APIAddr string
}

// NodeScraper scrapes /metrics from every named node in order.
type NodeScraper struct {
	Nodes []NodeAddress
	// Bound limits one exposition's bytes (default 16 MiB).
	Bound int64
}

// ScrapeAll implements Scraper: each node's current exposition is returned as
// that node's After snapshot; the run pairs successive takes itself.
func (s NodeScraper) ScrapeAll(ctx context.Context) ([]NodeScrapes, error) {
	if len(s.Nodes) == 0 {
		return nil, errNoNodes
	}
	out := make([]NodeScrapes, 0, len(s.Nodes))
	for _, node := range s.Nodes {
		snapshot, err := Scrape(ctx, node.APIAddr, s.Bound)
		if err != nil {
			return nil, fmt.Errorf("scrape %s: %w", node.Name, err)
		}
		out = append(out, NodeScrapes{Node: node.Name, After: snapshot})
	}
	return out, nil
}

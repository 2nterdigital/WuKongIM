package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

const (
	defaultHistoryPageLimit = 1000
	defaultHistoryMaxPages  = 4096
	maxHistoryResponseBytes = 16 * 1024 * 1024
	defaultHistoryTimeout   = 30 * time.Second
)

// HistoryClientConfig configures the paginated history reader.
type HistoryClientConfig struct {
	// APIAddrs are the node HTTP API base addresses tried in order.
	APIAddrs []string
	// PageLimit is the rows requested per page (default 1000, at most 10000).
	PageLimit int
	// MaxPages bounds the pages read for one channel (default 4096).
	MaxPages int
	// HTTPClient overrides the default client for tests.
	HTTPClient *http.Client
}

// HistoryClient reads one person channel's committed history page by page.
type HistoryClient struct {
	cfg  HistoryClientConfig
	http *http.Client
}

// NewHistoryClient builds a reader with bounded pages and response sizes.
func NewHistoryClient(cfg HistoryClientConfig) *HistoryClient {
	if cfg.PageLimit <= 0 || cfg.PageLimit > 10000 {
		cfg.PageLimit = defaultHistoryPageLimit
	}
	if cfg.MaxPages <= 0 {
		cfg.MaxPages = defaultHistoryMaxPages
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHistoryTimeout}
	}
	return &HistoryClient{cfg: cfg, http: client}
}

type historySyncRequest struct {
	LoginUID        string `json:"login_uid"`
	ChannelID       string `json:"channel_id"`
	ChannelType     uint8  `json:"channel_type"`
	StartMessageSeq uint64 `json:"start_message_seq"`
	EndMessageSeq   uint64 `json:"end_message_seq"`
	Limit           int    `json:"limit"`
	PullMode        int    `json:"pull_mode"`
}

type historySyncMessage struct {
	MessageID   int64  `json:"message_id"`
	ClientMsgNo string `json:"client_msg_no"`
	MessageSeq  uint64 `json:"message_seq"`
	FromUID     string `json:"from_uid"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	Payload     []byte `json:"payload"`
}

type historySyncResponse struct {
	More     int                  `json:"more"`
	Messages []historySyncMessage `json:"messages"`
}

// ReadPersonChannel reads every committed row of the person channel between
// the sender (login) and the recipient, oldest first, and returns the rows
// with payloads reduced to digests plus the number of pages it took.
func (c *HistoryClient) ReadPersonChannel(ctx context.Context, loginUID, peerUID string) ([]HistoryRow, uint64, error) {
	if len(c.cfg.APIAddrs) == 0 {
		return nil, 0, errors.New("history client: no API address configured")
	}
	var rows []HistoryRow
	var pages uint64
	start := uint64(0)
	for {
		if int(pages) >= c.cfg.MaxPages {
			return rows, pages, fmt.Errorf("history client: %s reached the %d-page bound before the last page", peerUID, c.cfg.MaxPages)
		}
		response, err := c.page(ctx, historySyncRequest{
			LoginUID:        loginUID,
			ChannelID:       peerUID,
			ChannelType:     frame.ChannelTypePerson,
			StartMessageSeq: start,
			Limit:           c.cfg.PageLimit,
			PullMode:        1,
		})
		if err != nil {
			return rows, pages, err
		}
		pages++
		var last uint64
		for _, message := range response.Messages {
			rows = append(rows, HistoryRow{
				ClientMsgNo:   message.ClientMsgNo,
				MessageID:     message.MessageID,
				MessageSeq:    message.MessageSeq,
				FromUID:       message.FromUID,
				ChannelID:     message.ChannelID,
				ChannelType:   message.ChannelType,
				PayloadDigest: DigestHex(message.Payload),
				PayloadBytes:  len(message.Payload),
			})
			if message.MessageSeq > last {
				last = message.MessageSeq
			}
		}
		if response.More == 0 || len(response.Messages) == 0 {
			return rows, pages, nil
		}
		if last < start {
			return rows, pages, fmt.Errorf("history client: %s page did not advance past sequence %d", peerUID, start)
		}
		start = last + 1
	}
}

func (c *HistoryClient) page(ctx context.Context, request historySyncRequest) (historySyncResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return historySyncResponse{}, err
	}
	var lastErr error
	for _, addr := range c.cfg.APIAddrs {
		url := strings.TrimRight(addr, "/") + "/channel/messagesync"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return historySyncResponse{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		encoded, err := io.ReadAll(io.LimitReader(resp.Body, maxHistoryResponseBytes+1))
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if len(encoded) > maxHistoryResponseBytes {
			return historySyncResponse{}, errors.New("history client: response exceeds the 16 MiB bound")
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("history client: %s returned HTTP %d", addr, resp.StatusCode)
			continue
		}
		var out historySyncResponse
		if err := json.Unmarshal(encoded, &out); err != nil {
			return historySyncResponse{}, fmt.Errorf("history client: response does not decode: %w", err)
		}
		return out, nil
	}
	if lastErr == nil {
		lastErr = errors.New("history client: no address answered")
	}
	return historySyncResponse{}, lastErr
}

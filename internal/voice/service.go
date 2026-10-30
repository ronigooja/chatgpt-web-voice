package voice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/tokenutil"
)

const (
	wmURL           = "https://chatgpt.com/realtime/wm?dcid=0"
	settingsUserURL = "https://chatgpt.com/backend-api/settings/user"
	// conversationURLPrefix is used to load chatgpt.com conversation metadata
	// (including the generated title) with the sticky account token.
	conversationURLPrefix = "https://chatgpt.com/backend-api/conversation/"
	// Probe is a quick liveness check. Keep it short so the accounts panel
	// does not sit on "checking…" while an unreachable upstream path dials chatgpt.com.
	probeTimeout      = 12 * time.Second
	probeDialTimeout  = 8 * time.Second
	probeTLSTimeout   = 8 * time.Second
	probeBodyLimit    = 64 << 10
	titleFetchTimeout = 15 * time.Second
	titleBodyLimit    = 2 << 20
)

// AccountRepository is the account-pool surface required by the voice gateway.
// Concrete type is *accounts.Pool; the interface keeps this package free of
// storage-construction details.
type AccountRepository interface {
	Pick(preferredToken string, excluded map[string]struct{}) (string, accounts.Account, error)
	PickByID(id int64, excluded map[string]struct{}) (string, accounts.Account, error)
	MarkInvalid(token string) error
	Get(id int64) (accounts.Account, error)
}

// CallSessionStore persists gateway voice-session metadata (no chat content)
// for admin visibility and sticky account resume after restarts.
type CallSessionStore interface {
	Upsert(item callsessions.Session) error
	UpdateUpstream(owner, voiceSessionID string, accountID int64, upstreamConversationID, upstreamParentMessageID, upstreamVoiceSessionID string) (callsessions.Session, error)
	MarkReleased(owner, voiceSessionID string) error
	Get(owner, voiceSessionID string) (callsessions.Session, error)
}

// MediaEvidenceStore persists media sessions and gateway-attested termination
// evidence. Concrete type is *mediaevidence.Store.
type MediaEvidenceStore interface {
	Register(owner, relaySession string, id mediaevidence.Identity) error
	MarkClosing(id mediaevidence.Identity, reason string) error
	Close(id mediaevidence.Identity, c mediaevidence.Components, reason string) (mediaevidence.MediaSession, error)
	Current(voiceSessionID string) (mediaevidence.MediaSession, error)
	Get(voiceSessionID string, generation int64) (mediaevidence.MediaSession, error)
	Evidence(id mediaevidence.Identity) (mediaevidence.Evidence, error)
	NextGeneration(voiceSessionID string) (int64, error)
	RecordLegSwap(id mediaevidence.Identity, newAccountID int64, clean bool) error
}

// relayAcceptTimeout bounds applying the upstream answer and gathering peer A candidates.
const relayAcceptTimeout = 20 * time.Second

// ServiceError is a typed gateway error with HTTP status.
type ServiceError struct {
	Message    string
	StatusCode int
	Detail     any
}

func (e *ServiceError) Error() string { return e.Message }

// UpstreamContext is the chatgpt.com conversation continuity state learned from
// DataChannel events or supplied by a reconnecting client.
type UpstreamContext struct {
	ConversationID         string `json:"upstream_conversation_id,omitempty"`
	ParentMessageID        string `json:"upstream_parent_message_id,omitempty"`
	UpstreamVoiceSessionID string `json:"upstream_voice_session_id,omitempty"`
}

type sessionBinding struct {
	Owner                  string
	AccountID              int64
	AccessToken            string
	Proxy                  string
	DeviceID               string
	UpstreamVoiceSessionID string
	ConversationID         string
	ParentMessageID        string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	// media is the gateway-terminated media session, nil in client-direct mode.
	media relay.Session
	// mediaID is the evidence identity of media; zero when evidence is not tracked.
	mediaID mediaevidence.Identity
	// call holds in-memory per-call data for rebuilds; nil for client-direct media.
	call *callState
}

// Service is the ChatGPT web voice gateway.
type Service struct {
	cfg         config.Config
	httpOptions httpclient.Options
	pool        AccountRepository
	records     CallSessionStore
	logger      *slog.Logger

	// realtimeURL overrides the voice endpoint in tests.
	realtimeURL string
	// settingsUserURL overrides the account probe endpoint in tests.
	settingsUserURL string
	// conversationURLPrefix overrides the conversation metadata endpoint in tests.
	conversationURLPrefix string
	// filesAPIURL overrides POST /backend-api/files in tests (image upload credentials).
	filesAPIURL string

	// relay, when set, terminates media in the gateway (peers A and B).
	relay relay.Relay
	// evidence, when set, records media sessions and termination evidence.
	evidence MediaEvidenceStore

	mu       sync.Mutex
	bindings map[string]*sessionBinding
}

// New creates a voice service.
func New(cfg config.Config, pool AccountRepository, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		cfg:                   cfg,
		httpOptions:           httpclient.FromConfig(cfg),
		pool:                  pool,
		logger:                logger,
		settingsUserURL:       settingsUserURL,
		conversationURLPrefix: conversationURLPrefix,
		bindings:              make(map[string]*sessionBinding),
	}
}

// WithRelay enables gateway media termination. A nil relay keeps client-direct mode.
func (s *Service) WithRelay(r relay.Relay) *Service {
	if s != nil {
		s.relay = r
	}
	return s
}

// WithMediaEvidence enables durable media-session and termination-evidence records.
func (s *Service) WithMediaEvidence(store MediaEvidenceStore) *Service {
	if s != nil {
		s.evidence = store
	}
	return s
}

// relayRoute returns the proxy the relay's upstream peer should use for media.
// Signaling keeps using the account proxy regardless of this setting.
func (s *Service) relayRoute(account accounts.Account) relay.Route {
	if s.cfg.RelayMediaRoute == config.RelayMediaRouteDirect {
		return relay.Route{}
	}
	if strings.TrimSpace(account.Proxy) == httpclient.ProxyDirect {
		return relay.Route{}
	}
	return relay.Route{ProxyURL: account.Proxy}
}

// WithCallSessions attaches durable call-session metadata storage.
func (s *Service) WithCallSessions(records CallSessionStore) *Service {
	if s != nil {
		s.records = records
	}
	return s
}

// SessionResult is returned by CreateSession.
type SessionResult struct {
	AnswerSDP              string `json:"answer_sdp"`
	Voice                  string `json:"voice"`
	VoiceMode              string `json:"voice_mode"`
	LanguageCode           string `json:"language_code"`
	SessionID              string `json:"session_id"`
	VoiceSessionID         string `json:"voice_session_id"`
	AccountID              int64  `json:"account_id,omitempty"`
	AccountLeaseID         string `json:"account_lease_id,omitempty"`
	AccountLeaseGeneration int64  `json:"account_lease_generation,omitempty"`
	// VoiceSessionGeneration is the media generation when termination evidence
	// is tracked; zero otherwise.
	VoiceSessionGeneration  int64  `json:"voice_session_generation,omitempty"`
	UpstreamVoiceSessionID  string `json:"upstream_voice_session_id,omitempty"`
	UpstreamConversationID  string `json:"upstream_conversation_id,omitempty"`
	UpstreamParentMessageID string `json:"upstream_parent_message_id,omitempty"`
}

func normalizeVoice(voice string) string {
	clean := strings.ToLower(strings.TrimSpace(voice))
	if clean == "" {
		return defaultVoice
	}
	if alias, ok := realtimeVoiceAliases[clean]; ok {
		clean = alias
	}
	if _, ok := allowedRealtimeVoices[clean]; ok {
		return clean
	}
	return defaultVoice
}

func newVoiceSessionID() string {
	return "vs_" + strings.ReplaceAll(uuid.New().String(), "-", "")
}

func (s *Service) cleanupBindingsLocked(now time.Time) {
	ttl := time.Duration(s.cfg.SessionTTLSeconds) * time.Second
	type expiredBinding struct {
		owner     string
		sessionID string
		media     relay.Session
		mediaID   mediaevidence.Identity
		call      *callState
	}
	var expired []expiredBinding
	for id, item := range s.bindings {
		base := item.UpdatedAt
		if base.IsZero() {
			base = item.CreatedAt
		}
		if now.Sub(base) > ttl {
			// Media-backed bindings live as long as their peers; the close
			// handler reclaims them. Idle TTL must not cut a live call.
			if item.media != nil {
				if _, closed := item.media.Termination(); !closed {
					continue
				}
			}
			expired = append(expired, expiredBinding{owner: item.Owner, sessionID: id, media: item.media, mediaID: item.mediaID, call: item.call})
			delete(s.bindings, id)
		}
	}
	// Persist expiry so admin call_sessions does not remain "active" after the
	// in-memory binding is reaped by TTL.
	for _, item := range expired {
		if item.media != nil {
			if item.call != nil {
				item.call.halt()
			}
			go s.closeTracked(item.media, item.mediaID, "binding_ttl")
		}
	}
	if s.records == nil {
		return
	}
	for _, item := range expired {
		if err := s.records.MarkReleased(item.owner, item.sessionID); err != nil {
			s.logger.Warn("call_session_ttl_release_failed", "voice_session_id", item.sessionID, "error", err)
		}
	}
}

func (s *Service) bindVoiceSession(owner, sessionID, token string, account accounts.Account, upstream UpstreamContext) string {
	return s.bindVoiceSessionMedia(owner, sessionID, token, account, upstream, nil, mediaevidence.Identity{}, nil)
}

func (s *Service) bindVoiceSessionMedia(owner, sessionID, token string, account accounts.Account, upstream UpstreamContext, media relay.Session, mediaID mediaevidence.Identity, call *callState) string {
	owner = normalizeSessionOwner(owner)
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		sessionID = newVoiceSessionID()
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupBindingsLocked(now)

	// Preserve continuity fields across SDP re-exchanges unless the caller
	// supplies newer values.
	prev := s.bindings[sessionID]
	createdAt := now
	merged := upstream
	if prev != nil && prev.Owner == owner {
		createdAt = prev.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		merged = mergeUpstreamContext(prev.upstreamContext(), upstream)
	}
	if strings.TrimSpace(merged.UpstreamVoiceSessionID) == "" {
		merged.UpstreamVoiceSessionID = newUpstreamVoiceSessionID()
	}

	var replaced relay.Session
	var replacedID mediaevidence.Identity
	var replacedCall *callState
	if prev != nil && prev.media != nil && prev.media != media {
		replaced, replacedID, replacedCall = prev.media, prev.mediaID, prev.call
	}
	s.bindings[sessionID] = &sessionBinding{
		media:                  media,
		mediaID:                mediaID,
		call:                   call,
		Owner:                  owner,
		AccountID:              account.ID,
		AccessToken:            token,
		Proxy:                  account.Proxy,
		DeviceID:               account.DeviceID,
		UpstreamVoiceSessionID: merged.UpstreamVoiceSessionID,
		ConversationID:         merged.ConversationID,
		ParentMessageID:        merged.ParentMessageID,
		CreatedAt:              createdAt,
		UpdatedAt:              now,
	}
	if replaced != nil {
		// A re-exchange supersedes the previous media; do not leak its peers.
		if replacedCall != nil {
			replacedCall.halt()
		}
		go s.closeTracked(replaced, replacedID, "superseded")
	}
	if media != nil {
		media.SetCloseHandler(func(reason string) { s.onMediaClosed(owner, sessionID, media, mediaID, reason) })
	}
	return sessionID
}

// onMediaClosed runs once when a media session closes for any reason. It first
// records the local termination observation, then drops the binding if it still
// points at this media (a binding already re-pointed at newer media is kept).
func (s *Service) onMediaClosed(owner, sessionID string, media relay.Session, mediaID mediaevidence.Identity, reason string) {
	s.finalizeMedia(media, mediaID, reason)
	s.mu.Lock()
	item, ok := s.bindings[sessionID]
	current := ok && item.Owner == owner && item.media == media
	var call *callState
	if current {
		call = item.call
		delete(s.bindings, sessionID)
	}
	s.mu.Unlock()
	if call != nil {
		call.halt()
	}
	if !current {
		return
	}
	s.logger.Info("relay_media_closed", "voice_session_id", sessionID, "media_session_id", media.ID(), "reason", reason)
	if s.records != nil {
		if err := s.records.MarkReleased(owner, sessionID); err != nil {
			s.logger.Warn("call_session_release_persist_failed", "voice_session_id", sessionID, "error", err)
		}
	}
}

// finalizeMedia turns the relay's close record into a durable evidence row. The
// row is a gateway-local observation; any failure leaves the lease blocked.
func (s *Service) finalizeMedia(media relay.Session, mediaID mediaevidence.Identity, reason string) {
	if s.evidence == nil || mediaID.VoiceSessionID == "" {
		return
	}
	var comps mediaevidence.Components
	if term, ok := media.Termination(); ok {
		comps = mediaevidence.Components{
			PeerAClosed:        term.PeerA.Closed && term.PeerA.Err == nil,
			PeerBClosed:        term.PeerB.Closed && term.PeerB.Err == nil,
			DataChannelsClosed: term.DataChannels.Closed && term.DataChannels.Err == nil,
			TunnelsClosed:      term.Tunnels.Closed && term.Tunnels.Err == nil,
		}
		if term.Reason != "" {
			reason = term.Reason
		}
	}
	row, err := s.evidence.Close(mediaID, comps, reason)
	if err != nil {
		s.logger.Error("media_evidence_close_failed", "voice_session_id", mediaID.VoiceSessionID, "media_generation", mediaID.MediaGeneration, "error", err)
		return
	}
	s.logger.Info("media_termination_recorded", "voice_session_id", mediaID.VoiceSessionID, "media_generation", mediaID.MediaGeneration, "state", row.State, "reason", reason)
}

// closeTracked marks a tracked media session closing (so a crash mid-close is
// recoverable as unknown) and closes it. Evidence is written by the close handler.
func (s *Service) closeTracked(media relay.Session, mediaID mediaevidence.Identity, reason string) {
	if media == nil {
		return
	}
	if s.evidence != nil && mediaID.VoiceSessionID != "" {
		if err := s.evidence.MarkClosing(mediaID, reason); err != nil {
			s.logger.Warn("media_mark_closing_failed", "voice_session_id", mediaID.VoiceSessionID, "error", err)
		}
	}
	s.closeMedia(media, reason)
}

func (s *Service) closeMedia(media relay.Session, reason string) {
	if media == nil {
		return
	}
	if err := media.CloseWithReason(reason); err != nil {
		s.logger.Warn("relay_media_close_failed", "media_session_id", media.ID(), "reason", reason, "error", err)
	}
}

func (b *sessionBinding) upstreamContext() UpstreamContext {
	if b == nil {
		return UpstreamContext{}
	}
	return UpstreamContext{
		ConversationID:         b.ConversationID,
		ParentMessageID:        b.ParentMessageID,
		UpstreamVoiceSessionID: b.UpstreamVoiceSessionID,
	}
}

func mergeUpstreamContext(base, patch UpstreamContext) UpstreamContext {
	out := base
	if v := strings.TrimSpace(patch.ConversationID); v != "" {
		out.ConversationID = v
	}
	if v := strings.TrimSpace(patch.ParentMessageID); v != "" {
		out.ParentMessageID = v
	}
	if v := strings.TrimSpace(patch.UpstreamVoiceSessionID); v != "" {
		out.UpstreamVoiceSessionID = v
	}
	return out
}

func newUpstreamVoiceSessionID() string {
	return strings.ToUpper(uuid.New().String())
}

// UpdateSessionContext records upstream conversation identifiers learned from
// the DataChannel after the media plane is connected. Only the binding owner
// may update the context. When the in-memory binding was released after hangup,
// durable call_sessions rows still accept continuity updates for the same owner.
func (s *Service) UpdateSessionContext(owner, voiceSessionID string, patch UpstreamContext) (UpstreamContext, error) {
	owner = normalizeSessionOwner(owner)
	sessionID := strings.TrimSpace(voiceSessionID)
	if sessionID == "" {
		return UpstreamContext{}, &ServiceError{Message: "voice session id is required", StatusCode: 400}
	}
	now := time.Now()
	s.mu.Lock()
	s.cleanupBindingsLocked(now)
	item := s.bindings[sessionID]
	if item != nil && item.Owner != owner {
		s.mu.Unlock()
		return UpstreamContext{}, &ServiceError{Message: "voice session does not belong to caller", StatusCode: 403}
	}
	if item != nil {
		merged := mergeUpstreamContext(item.upstreamContext(), patch)
		item.ConversationID = merged.ConversationID
		item.ParentMessageID = merged.ParentMessageID
		if strings.TrimSpace(merged.UpstreamVoiceSessionID) != "" {
			item.UpstreamVoiceSessionID = merged.UpstreamVoiceSessionID
		}
		item.UpdatedAt = now
		out := item.upstreamContext()
		accountID := item.AccountID
		s.mu.Unlock()
		if s.records != nil {
			if _, err := s.records.UpdateUpstream(owner, sessionID, accountID, out.ConversationID, out.ParentMessageID, out.UpstreamVoiceSessionID); err != nil {
				s.logger.Warn("call_session_upstream_persist_failed", "voice_session_id", sessionID, "error", err)
			}
		}
		return out, nil
	}
	s.mu.Unlock()

	// Memory binding gone (typical after hangup): update durable metadata only.
	if s.records == nil {
		return UpstreamContext{}, &ServiceError{Message: "voice session not found", StatusCode: 404}
	}
	row, err := s.records.Get(owner, sessionID)
	if err != nil {
		if errors.Is(err, callsessions.ErrNotFound) {
			return UpstreamContext{}, &ServiceError{Message: "voice session not found", StatusCode: 404}
		}
		return UpstreamContext{}, &ServiceError{Message: "voice session lookup failed", StatusCode: 500}
	}
	updated, err := s.records.UpdateUpstream(
		owner,
		sessionID,
		row.AccountID,
		strings.TrimSpace(patch.ConversationID),
		strings.TrimSpace(patch.ParentMessageID),
		strings.TrimSpace(patch.UpstreamVoiceSessionID),
	)
	if err != nil {
		if errors.Is(err, callsessions.ErrNotFound) {
			return UpstreamContext{}, &ServiceError{Message: "voice session not found", StatusCode: 404}
		}
		return UpstreamContext{}, &ServiceError{Message: "voice session update failed", StatusCode: 500}
	}
	return UpstreamContext{
		ConversationID:         updated.UpstreamConversationID,
		ParentMessageID:        updated.UpstreamParentMessageID,
		UpstreamVoiceSessionID: updated.UpstreamVoiceSessionID,
	}, nil
}

// SessionContext returns the stored upstream continuity fields for an owned binding.
func (s *Service) SessionContext(owner, voiceSessionID string) (UpstreamContext, error) {
	binding, ownedByOther := s.boundVoiceSession(owner, voiceSessionID)
	if ownedByOther {
		return UpstreamContext{}, &ServiceError{Message: "voice session does not belong to caller", StatusCode: 403}
	}
	if binding == nil {
		return UpstreamContext{}, &ServiceError{Message: "voice session not found", StatusCode: 404}
	}
	return binding.upstreamContext(), nil
}

// UpstreamTitleResult is a redacted conversation title fetched from chatgpt.com.
// It never includes the access token or full conversation mapping.
type UpstreamTitleResult struct {
	VoiceSessionID         string `json:"voice_session_id"`
	UpstreamConversationID string `json:"upstream_conversation_id"`
	Title                  string `json:"title"`
	HasTitle               bool   `json:"has_title"`
	StatusCode             int    `json:"status_code,omitempty"`
}

// FetchUpstreamTitle loads the chatgpt.com conversation title using the sticky
// account bound to voiceSessionID. conversationID may be empty when the binding
// or durable call_sessions row already stores UpstreamConversationID.
func (s *Service) FetchUpstreamTitle(owner, voiceSessionID, conversationID string) (*UpstreamTitleResult, error) {
	owner = normalizeSessionOwner(owner)
	sessionID := strings.TrimSpace(voiceSessionID)
	if sessionID == "" {
		return nil, &ServiceError{Message: "voice session id is required", StatusCode: 400}
	}
	binding, ownedByOther := s.boundVoiceSession(owner, sessionID)
	if ownedByOther {
		return nil, &ServiceError{Message: "voice session does not belong to caller", StatusCode: 403}
	}

	if binding != nil {
		var err error
		binding, err = s.refreshBindingAccount(binding)
		if err != nil {
			return nil, err
		}
	}

	// Rehydrate sticky account from durable metadata when memory was released.
	var accessToken, proxy, deviceID string
	var accountID int64
	var storedConversationID string
	if binding != nil {
		accessToken = binding.AccessToken
		proxy = binding.Proxy
		deviceID = binding.DeviceID
		accountID = binding.AccountID
		storedConversationID = binding.ConversationID
	} else if s.records != nil {
		row, err := s.records.Get(owner, sessionID)
		if err != nil {
			if errors.Is(err, callsessions.ErrNotFound) {
				return nil, &ServiceError{Message: "voice session not found", StatusCode: 404}
			}
			return nil, &ServiceError{Message: "voice session lookup failed", StatusCode: 500}
		}
		if row.AccountID <= 0 {
			return nil, &ServiceError{Message: "voice session has no sticky account", StatusCode: 404}
		}
		token, account, err := s.pool.PickByID(row.AccountID, nil)
		if err != nil {
			if ae, ok := err.(*accounts.Error); ok {
				return nil, &ServiceError{Message: ae.Message, StatusCode: 503}
			}
			return nil, &ServiceError{Message: err.Error(), StatusCode: 503}
		}
		accessToken = token
		proxy = account.Proxy
		deviceID = account.DeviceID
		accountID = account.ID
		storedConversationID = row.UpstreamConversationID
	} else {
		return nil, &ServiceError{Message: "voice session not found", StatusCode: 404}
	}

	upstreamID := strings.TrimSpace(conversationID)
	if upstreamID == "" {
		upstreamID = strings.TrimSpace(storedConversationID)
	}
	if upstreamID == "" {
		return nil, &ServiceError{Message: "upstream conversation id is required", StatusCode: 400}
	}
	if !validUpstreamConversationID(upstreamID) {
		return nil, &ServiceError{Message: "upstream conversation id is invalid", StatusCode: 400}
	}

	// Keep continuity in sync when the client supplies a newer id.
	if upstreamID != strings.TrimSpace(storedConversationID) {
		if _, err := s.UpdateSessionContext(owner, sessionID, UpstreamContext{ConversationID: upstreamID}); err != nil {
			return nil, err
		}
	}

	status, contentType, body, err := s.getConversationOnce(accessToken, proxy, upstreamID, deviceID)
	if err != nil {
		s.logger.Warn("upstream_title_fetch_failed", "voice_session_id", sessionID, "error", err)
		return nil, &ServiceError{
			Message:    "upstream conversation request failed",
			StatusCode: 502,
			Detail:     truncate(err.Error(), 300),
		}
	}

	result := &UpstreamTitleResult{
		VoiceSessionID:         sessionID,
		UpstreamConversationID: upstreamID,
		StatusCode:             status,
	}
	switch {
	case status == http.StatusUnauthorized:
		health := s.reportOutcome(accessToken, time.Time{}, status, contentType, body, "", nil)
		s.logger.Warn("upstream_title_unauthorized", "account_id", accountID, "marked_invalid", health.Invalidated)
		return nil, &ServiceError{Message: "account token invalid", StatusCode: 401, Detail: truncate(body, 300)}
	case status == http.StatusNotFound:
		return nil, &ServiceError{Message: "upstream conversation not found", StatusCode: 404}
	case status == http.StatusTooManyRequests:
		return nil, &ServiceError{Message: "upstream conversation rate limited", StatusCode: 429}
	case status != http.StatusOK:
		kind := classifyProbeBody(contentType, body)
		detail := truncate(body, 300)
		if kind == "html" {
			detail = "upstream returned HTML challenge or block page"
		}
		return nil, &ServiceError{
			Message:    fmt.Sprintf("upstream conversation failed status=%d", status),
			StatusCode: 502,
			Detail:     detail,
		}
	}

	title, ok := extractConversationTitle(body)
	if !ok {
		// Conversation exists but title is not generated yet (common early in a call).
		result.HasTitle = false
		result.Title = ""
		s.logger.Info("upstream_title_empty", "voice_session_id", sessionID, "upstream_conversation_id", upstreamID)
		return result, nil
	}
	result.Title = title
	result.HasTitle = true
	s.logger.Info("upstream_title_fetched", "voice_session_id", sessionID, "upstream_conversation_id", upstreamID, "title_len", len(title))
	return result, nil
}

func (s *Service) getConversationOnce(token, proxy, conversationID, deviceID string) (status int, contentType, text string, err error) {
	client := httpclient.New(s.httpOptions, proxy)
	client.Timeout = titleFetchTimeout
	prefix := s.conversationURLPrefix
	if prefix == "" {
		prefix = conversationURLPrefix
	}
	endpoint := strings.TrimRight(prefix, "/") + "/" + conversationID
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", "", err
	}
	req.Header = s.authHeaders(token, deviceID, map[string]string{
		"accept": "application/json, text/plain, */*",
	})
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, titleBodyLimit))
	return resp.StatusCode, resp.Header.Get("content-type"), string(raw), nil
}

func validUpstreamConversationID(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 160 {
		return false
	}
	// Conversation ids are opaque strings from chatgpt.com; reject path traversal
	// and whitespace so they can be safely appended to the URL path.
	if strings.ContainsAny(id, "/\\?# \t\r\n") {
		return false
	}
	return true
}

// extractConversationTitle pulls the user-visible title from a conversation JSON
// payload. Empty / placeholder titles are treated as not ready.
func extractConversationTitle(body string) (string, bool) {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return "", false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return "", false
	}
	title := strings.TrimSpace(fmt.Sprint(payload["title"]))
	if title == "" || title == "<nil>" {
		return "", false
	}
	if isPlaceholderConversationTitle(title) {
		return "", false
	}
	// Cap length for UI / local storage consumers.
	runes := []rune(title)
	if len(runes) > 120 {
		title = string(runes[:120])
	}
	return title, true
}

func isPlaceholderConversationTitle(title string) bool {
	switch strings.ToLower(strings.TrimSpace(title)) {
	case "", "new chat", "new conversation", "untitled", "untitled conversation",
		"新聊天", "新对话", "新会话", "未命名会话", "未命名对话":
		return true
	default:
		return false
	}
}

// ReleaseSession unbinds a voice_session_id.
func (s *Service) ReleaseSession(owner, voiceSessionID string) bool {
	owner = normalizeSessionOwner(owner)
	sessionID := strings.TrimSpace(voiceSessionID)
	if sessionID == "" {
		return false
	}
	s.mu.Lock()
	item, ok := s.bindings[sessionID]
	released := false
	var media relay.Session
	var mediaID mediaevidence.Identity
	var call *callState
	if ok && item.Owner == owner {
		media, mediaID, call = item.media, item.mediaID, item.call
		delete(s.bindings, sessionID)
		released = true
	}
	s.mu.Unlock()
	if call != nil {
		call.halt()
	}
	s.closeTracked(media, mediaID, "released")
	if s.records != nil {
		if err := s.records.MarkReleased(owner, sessionID); err != nil {
			s.logger.Warn("call_session_release_persist_failed", "voice_session_id", sessionID, "error", err)
		}
	}
	return released
}

func (s *Service) boundVoiceSession(owner, voiceSessionID string) (binding *sessionBinding, ownedByOther bool) {
	owner = normalizeSessionOwner(owner)
	sessionID := strings.TrimSpace(voiceSessionID)
	if sessionID == "" {
		return nil, false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupBindingsLocked(now)
	item := s.bindings[sessionID]
	if item == nil {
		return nil, false
	}
	if item.Owner != owner {
		return nil, true
	}
	item.UpdatedAt = now
	cp := *item
	return &cp, false
}

// refreshBindingAccount reads current credentials and transport policy so edits
// cannot leave live sessions using an obsolete device ID or proxy route.
func (s *Service) refreshBindingAccount(binding *sessionBinding) (*sessionBinding, error) {
	if s.pool == nil {
		return binding, nil
	}
	account, err := s.pool.Get(binding.AccountID)
	if err != nil {
		return nil, &ServiceError{Message: "voice session account unavailable", StatusCode: 503}
	}
	refreshed := *binding
	refreshed.AccessToken = account.AccessToken
	refreshed.Proxy = account.Proxy
	refreshed.DeviceID = account.DeviceID
	return &refreshed, nil
}

func normalizeSessionOwner(owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "internal"
	}
	return owner
}

func encodeMultipart(fields [][2]string) (body []byte, contentType string) {
	boundary := "----WebKitFormBoundary" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	var buf bytes.Buffer
	for _, kv := range fields {
		fmt.Fprintf(&buf, "--%s\r\n", boundary)
		fmt.Fprintf(&buf, "Content-Disposition: form-data; name=\"%s\"\r\n\r\n", kv[0])
		buf.WriteString(kv[1])
		buf.WriteString("\r\n")
	}
	fmt.Fprintf(&buf, "--%s--\r\n", boundary)
	return buf.Bytes(), "multipart/form-data; boundary=" + boundary
}

func normalizeSDP(offerSDP string) (string, error) {
	text := strings.TrimSpace(offerSDP)
	if !strings.HasPrefix(text, "v=0") {
		return "", &ServiceError{Message: "offer_sdp invalid; must be WebRTC offer SDP text", StatusCode: 400}
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	text = strings.ReplaceAll(text, "\n", "\r\n")
	if !strings.HasSuffix(text, "\r\n") {
		text += "\r\n"
	}
	return text, nil
}

func buildSessionJSON(voice, voiceMode, languageCode string, upstream UpstreamContext) string {
	sid := strings.TrimSpace(upstream.UpstreamVoiceSessionID)
	if sid == "" {
		sid = newUpstreamVoiceSessionID()
	}
	// ChatGPT web uses an uppercase UUID for voice_session_id in the wm form.
	sid = strings.ToUpper(sid)
	payload := map[string]any{
		"backend_reasoning_effort":      "instant",
		"language_code":                 languageCode,
		"requested_default_model":       "",
		"voice":                         normalizeVoice(voice),
		"voice_session_id":              sid,
		"voice_status_request_id":       sid,
		"timezone_offset_min":           -480,
		"timezone":                      "Etc/GMT-8",
		"voice_mode":                    voiceMode,
		"model_slug":                    "",
		"model_slug_advanced":           "",
		"client_tools":                  []any{},
		"history_and_training_disabled": false,
		"conversation_mode":             map[string]any{"kind": "primary_assistant"},
		"enable_message_streaming":      true,
	}
	// When present, these fields ask the upstream voice path to continue the
	// existing chatgpt.com conversation instead of starting a blank thread.
	// Field names follow the web conversation protocol; if upstream ignores an
	// unknown field the call still works as a fresh session.
	if conversationID := strings.TrimSpace(upstream.ConversationID); conversationID != "" {
		payload["conversation_id"] = conversationID
	}
	if parentID := strings.TrimSpace(upstream.ParentMessageID); parentID != "" {
		payload["parent_message_id"] = parentID
	}
	data, _ := json.Marshal(payload)
	return string(data)
}

func (s *Service) authHeaders(token, deviceID string, extra map[string]string) http.Header {
	// Header set aligned with ChatGPT2API-GO UpstreamClient.headers (Edge 143 persona).
	h := http.Header{}
	h.Set("accept", "*/*")
	h.Set("accept-language", "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7")
	h.Set("cache-control", "no-cache")
	h.Set("pragma", "no-cache")
	h.Set("priority", "u=1, i")
	h.Set("origin", "https://chatgpt.com")
	h.Set("referer", "https://chatgpt.com/")
	h.Set("user-agent", s.cfg.DefaultUA)
	h.Set("sec-ch-ua", s.cfg.SecCHUA)
	h.Set("sec-ch-ua-arch", s.cfg.SecCHUAArch)
	h.Set("sec-ch-ua-bitness", s.cfg.SecCHUABitness)
	h.Set("sec-ch-ua-full-version", s.cfg.SecCHUAFullVersion)
	h.Set("sec-ch-ua-full-version-list", s.cfg.SecCHUAFullVersionList)
	h.Set("sec-ch-ua-mobile", s.cfg.SecCHUAMobile)
	h.Set("sec-ch-ua-model", s.cfg.SecCHUAModel)
	h.Set("sec-ch-ua-platform", s.cfg.SecCHUAPlatform)
	h.Set("sec-ch-ua-platform-version", s.cfg.SecCHUAPlatformVersion)
	h.Set("sec-fetch-dest", "empty")
	h.Set("sec-fetch-mode", "cors")
	h.Set("sec-fetch-site", "same-origin")
	h.Set("oai-device-id", firstNonEmpty(deviceID, s.cfg.DeviceID))
	h.Set("oai-session-id", s.cfg.SessionID)
	h.Set("oai-language", "zh-CN")
	h.Set("oai-client-version", s.cfg.ClientVersion)
	h.Set("oai-client-build-number", s.cfg.ClientBuildNumber)
	h.Set("authorization", "Bearer "+token)
	for k, v := range extra {
		h.Set(k, v)
	}
	return h
}

func (s *Service) postWMOnce(token, offerSDP, sessionJSON, proxy, deviceID string) (status int, contentType, text string, err error) {
	status, contentType, text, _, err = s.postWMResponse(token, offerSDP, sessionJSON, proxy, deviceID)
	return status, contentType, text, err
}

func (s *Service) postWMResponse(token, offerSDP, sessionJSON, proxy, deviceID string) (status int, contentType, text, retryAfter string, err error) {
	client := httpclient.New(s.httpOptions, proxy)
	body, ct := encodeMultipart([][2]string{
		{"sdp", offerSDP},
		{"session", sessionJSON},
	})
	req, err := http.NewRequest(http.MethodPost, firstNonEmpty(s.realtimeURL, wmURL), bytes.NewReader(body))
	if err != nil {
		return 0, "", "", "", err
	}
	req.Header = s.authHeaders(token, deviceID, map[string]string{"content-type": ct})

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return resp.StatusCode, resp.Header.Get("content-type"), string(raw), resp.Header.Get("Retry-After"), nil
}

// ProbeResult is the outcome of a lightweight account liveness check.
type ProbeResult struct {
	AccountID        int64  `json:"account_id"`
	Alive            bool   `json:"alive"`
	Status           string `json:"status"`
	StatusCode       int    `json:"status_code,omitempty"`
	Detail           string `json:"detail,omitempty"`
	ContentKind      string `json:"content_kind,omitempty"`
	MarkedInvalid    bool   `json:"marked_invalid,omitempty"`
	TokenHasExp      bool   `json:"token_has_exp"`
	TokenExp         int64  `json:"token_exp,omitempty"`
	ExpiresInSeconds *int64 `json:"expires_in_seconds,omitempty"`
	TokenExpired     bool   `json:"token_expired,omitempty"`
}

// ProbeAccountToken GETs backend-api/settings/user to check whether the stored
// access token is still accepted. A JSON 401 marks the account invalid; Cloudflare
// HTML / network errors are reported as unknown and do not disable the account.
func (s *Service) ProbeAccountToken(accountID int64) (*ProbeResult, error) {
	account, err := s.pool.Get(accountID)
	if err != nil {
		return nil, err
	}
	result := &ProbeResult{
		AccountID: account.ID,
		Status:    "unknown",
	}
	if exp, expErr := tokenutil.ParseAccessTokenExpiry(account.AccessToken); expErr == nil {
		result.TokenHasExp = exp.HasExp
		result.TokenExp = exp.Exp
		result.TokenExpired = exp.Expired
		if exp.HasExp {
			seconds := exp.ExpiresInSeconds
			result.ExpiresInSeconds = &seconds
		}
	}

	probeStarted := time.Now()
	status, contentType, body, err := s.getSettingsUserOnce(account.AccessToken, account.Proxy, account.DeviceID)
	health := s.reportOutcome(account.AccessToken, probeStarted, status, contentType, body, "", err)
	if err != nil {
		result.Detail = probeNetworkDetail(err, account.Proxy)
		s.logger.Warn("account_probe_failed", "account_id", account.ID, "proxy", account.Proxy != "" && account.Proxy != httpclient.ProxyDirect, "error", err)
		return result, nil
	}
	result.StatusCode = status
	result.ContentKind = classifyProbeBody(contentType, body)

	switch {
	case status == http.StatusUnauthorized && result.ContentKind != "html":
		result.Status = "unauthorized"
		result.Alive = false
		result.Detail = truncate(probeDetail(body), 300)
		result.MarkedInvalid = health.Invalidated
		s.logger.Warn("account_probe_unauthorized", "account_id", account.ID, "status", status)
	case status == http.StatusOK && result.ContentKind == "json":
		result.Status = "alive"
		result.Alive = true
		result.Detail = "settings/user accepted token"
		s.logger.Info("account_probe_alive", "account_id", account.ID)
	default:
		result.Status = "unknown"
		result.Alive = false
		if result.ContentKind == "html" {
			result.Detail = "upstream returned HTML challenge or block page"
		} else {
			result.Detail = truncate(probeDetail(body), 300)
		}
		s.logger.Warn("account_probe_unknown", "account_id", account.ID, "status", status, "content_kind", result.ContentKind)
	}
	return result, nil
}

func (s *Service) getSettingsUserOnce(token, proxy, deviceID string) (status int, contentType, text string, err error) {
	client := httpclient.New(s.httpOptions, proxy)
	client.Timeout = probeTimeout
	if transport, ok := client.Transport.(*http.Transport); ok {
		// Clone so session clients keep their longer dial budget.
		transport = transport.Clone()
		transport.DialContext = (&net.Dialer{
			Timeout:   probeDialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext
		transport.TLSHandshakeTimeout = probeTLSTimeout
		transport.ResponseHeaderTimeout = probeTimeout
		client.Transport = transport
	}
	endpoint := s.settingsUserURL
	if endpoint == "" {
		endpoint = settingsUserURL
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", "", err
	}
	req.Header = s.authHeaders(token, deviceID, map[string]string{
		"accept": "application/json, text/plain, */*",
	})
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, probeBodyLimit))
	return resp.StatusCode, resp.Header.Get("content-type"), string(raw), nil
}

func probeNetworkDetail(err error, accountProxy string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	timedOut := strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "i/o timeout")
	if timedOut {
		if strings.TrimSpace(accountProxy) == httpclient.ProxyDirect {
			return "upstream timeout on explicit direct connection; check direct network reachability"
		}
		if strings.TrimSpace(accountProxy) != "" {
			return "upstream timeout via account proxy; check proxy reachability"
		}
		return "upstream timeout; check HTTP_PROXY/HTTPS_PROXY/NO_PROXY or set this account's proxy if chatgpt.com is blocked"
	}
	return truncate(msg, 300)
}

func classifyProbeBody(contentType, body string) string {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	trimmed := strings.TrimSpace(body)
	lower := strings.ToLower(trimmed)
	if strings.Contains(ct, "text/html") ||
		strings.Contains(lower, "cf-mitigated") ||
		strings.Contains(lower, "challenge-platform") ||
		strings.Contains(lower, "just a moment") ||
		strings.HasPrefix(lower, "<!doctype html") ||
		strings.HasPrefix(lower, "<html") {
		return "html"
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return "json"
	}
	if trimmed == "" {
		return "empty"
	}
	return "other"
}

func probeDetail(body string) string {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "{") {
		var payload map[string]any
		if err := json.Unmarshal([]byte(trimmed), &payload); err == nil {
			if detail, ok := payload["detail"]; ok {
				switch value := detail.(type) {
				case string:
					return value
				case map[string]any:
					if message, ok := value["message"]; ok {
						return fmt.Sprint(message)
					}
				}
				return fmt.Sprint(detail)
			}
			if errObj, ok := payload["error"].(map[string]any); ok {
				if message, ok := errObj["message"]; ok {
					return fmt.Sprint(message)
				}
			}
			if message, ok := payload["message"]; ok {
				return fmt.Sprint(message)
			}
		}
	}
	return trimmed
}

// CreateSessionRequest is the input for CreateSession.
type CreateSessionRequest struct {
	Owner          string
	OfferSDP       string
	Voice          string
	VoiceMode      string
	LanguageCode   string
	VoiceSessionID string
	// AccountID pins the pool account for this conversation. Prefer this over
	// LRU pick when resuming an upstream thread after a gateway restart.
	AccountID int64
	// Optional continuity fields. When VoiceSessionID already has a binding,
	// values stored on the binding win unless the request supplies non-empty
	// replacements (for example after DataChannel updates persisted client-side).
	UpstreamConversationID  string
	UpstreamParentMessageID string
	UpstreamVoiceSessionID  string
}

// pickAccountForSession preserves stable account identity across token edits.
// A token retained by a live binding may have been reassigned to another account.
func (s *Service) pickAccountForSession(preferredToken string, preferredAccountID int64, excluded map[string]struct{}) (string, accounts.Account, error) {
	if preferredAccountID > 0 {
		return s.pool.PickByID(preferredAccountID, excluded)
	}
	return s.pool.Pick(strings.TrimSpace(preferredToken), excluded)
}

// CreateSession POSTs offer SDP to /realtime/wm and returns answer SDP.
func (s *Service) CreateSession(req CreateSessionRequest) (*SessionResult, error) {
	offerSDP, err := normalizeSDP(req.OfferSDP)
	if err != nil {
		return nil, err
	}
	options, err := normalizeSessionOptions(req.Voice, req.VoiceMode, req.LanguageCode)
	if err != nil {
		return nil, err
	}
	voice := options.Voice
	owner := normalizeSessionOwner(req.Owner)
	bound, ownedByOther := s.boundVoiceSession(owner, req.VoiceSessionID)
	if ownedByOther {
		return nil, &ServiceError{Message: "voice session does not belong to caller", StatusCode: 403}
	}

	// After a gateway restart the memory map is empty. Rebuild sticky preference
	// from durable call_sessions metadata when the client still holds vs_...
	var durable *callsessions.Session
	if bound == nil && strings.TrimSpace(req.VoiceSessionID) != "" && s.records != nil {
		if row, err := s.records.Get(owner, req.VoiceSessionID); err == nil {
			durable = &row
		} else if !errors.Is(err, callsessions.ErrNotFound) {
			s.logger.Warn("call_session_lookup_failed", "voice_session_id", req.VoiceSessionID, "error", err)
		}
	}
	// Unknown voice_session_id with neither memory nor durable row: reject so
	// clients cannot invent session ids (except first create with empty id).
	if strings.TrimSpace(req.VoiceSessionID) != "" && bound == nil && durable == nil {
		return nil, &ServiceError{Message: "voice session not found", StatusCode: 404}
	}

	// Sticky account preference, strongest first:
	// 1) live memory binding token
	// 2) client-supplied account_id (from SQLite conversation / downstream sticky)
	// 3) durable call_sessions.account_id
	// 4) LRU pool pick
	preferredToken := ""
	preferredAccountID := req.AccountID
	if bound != nil {
		if bound.AccessToken != "" {
			preferredToken = bound.AccessToken
		}
		if preferredAccountID <= 0 && bound.AccountID > 0 {
			preferredAccountID = bound.AccountID
		}
	}
	if preferredAccountID <= 0 && durable != nil && durable.AccountID > 0 {
		preferredAccountID = durable.AccountID
	}
	// When a sticky account is known, never silently switch: the chatgpt.com
	// conversation_id is only valid for the account that created it.
	// Legacy rows with upstream ids but no account_id keep best-effort LRU pick.
	requireStickyAccount := preferredAccountID > 0 || preferredToken != ""

	requestUpstream := UpstreamContext{
		ConversationID:         strings.TrimSpace(req.UpstreamConversationID),
		ParentMessageID:        strings.TrimSpace(req.UpstreamParentMessageID),
		UpstreamVoiceSessionID: strings.TrimSpace(req.UpstreamVoiceSessionID),
	}
	upstreamForWM := requestUpstream
	if durable != nil {
		// Fill gaps from durable metadata before applying the live binding.
		upstreamForWM = mergeUpstreamContext(UpstreamContext{
			ConversationID:         durable.UpstreamConversationID,
			ParentMessageID:        durable.UpstreamParentMessageID,
			UpstreamVoiceSessionID: durable.UpstreamVoiceSessionID,
		}, upstreamForWM)
	}
	if bound != nil {
		// Prefer sticky upstream voice session id from the binding so SDP
		// re-exchanges resume the same upstream realtime session when possible.
		upstreamForWM = mergeUpstreamContext(bound.upstreamContext(), requestUpstream)
		if durable != nil {
			// Keep any durable-only fields the live binding has not learned yet.
			upstreamForWM = mergeUpstreamContext(UpstreamContext{
				ConversationID:         durable.UpstreamConversationID,
				ParentMessageID:        durable.UpstreamParentMessageID,
				UpstreamVoiceSessionID: durable.UpstreamVoiceSessionID,
			}, upstreamForWM)
			upstreamForWM = mergeUpstreamContext(upstreamForWM, requestUpstream)
		}
	}
	if strings.TrimSpace(upstreamForWM.UpstreamVoiceSessionID) == "" {
		upstreamForWM.UpstreamVoiceSessionID = newUpstreamVoiceSessionID()
	}

	sessionJSON := buildSessionJSON(voice, options.VoiceMode, options.LanguageCode, upstreamForWM)
	// Fixed before any attempt so media evidence can be keyed to the final id.
	targetSessionID := strings.TrimSpace(req.VoiceSessionID)
	if targetSessionID == "" {
		targetSessionID = newVoiceSessionID()
	}
	excluded := map[string]struct{}{}
	var lastError string
	var lastDetail any
	lastStatus := 0

	for attempt := 1; attempt <= s.cfg.MaxAccountAttempts; attempt++ {
		token, account, err := s.pickAccountForSession(preferredToken, preferredAccountID, excluded)
		if err != nil {
			if preferredToken != "" || preferredAccountID > 0 {
				s.ReleaseSession(owner, req.VoiceSessionID)
				// Sticky account gone: cannot honestly resume this upstream thread.
				if requireStickyAccount {
					if ae, ok := err.(*accounts.Error); ok {
						return nil, &ServiceError{Message: ae.Message, StatusCode: 503}
					}
					return nil, &ServiceError{Message: err.Error(), StatusCode: 503}
				}
				preferredToken = ""
				preferredAccountID = 0
				upstreamForWM.UpstreamVoiceSessionID = newUpstreamVoiceSessionID()
				// Drop upstream conversation resume fields if we no longer own the account.
				upstreamForWM.ConversationID = ""
				upstreamForWM.ParentMessageID = ""
				sessionJSON = buildSessionJSON(voice, options.VoiceMode, options.LanguageCode, upstreamForWM)
				token, account, err = s.pool.Pick("", excluded)
			}
		}
		if err != nil {
			if ae, ok := err.(*accounts.Error); ok {
				return nil, &ServiceError{Message: ae.Message, StatusCode: 503}
			}
			return nil, &ServiceError{Message: err.Error(), StatusCode: 503}
		}
		excluded[token] = struct{}{}

		explicitProxy := account.Proxy
		proxySource := "process_environment_or_direct"
		if strings.TrimSpace(explicitProxy) == httpclient.ProxyDirect {
			proxySource = "account_direct"
		} else if strings.TrimSpace(explicitProxy) != "" {
			proxySource = "account"
		}

		// With a relay, the upstream receives peer B's offer, never the client's.
		wmOffer := offerSDP
		var media relay.Session
		if s.relay != nil {
			var relayErr error
			media, wmOffer, relayErr = s.relay.NewSession(context.Background(), offerSDP, s.relayRoute(account))
			if relayErr != nil {
				s.logger.Error("relay_session_create_failed", "account_id", account.ID, "attempt", attempt, "error", relayErr)
				if errors.Is(relayErr, relay.ErrCapacity) {
					return nil, &ServiceError{Message: "media relay capacity exhausted", StatusCode: 503}
				}
				return nil, &ServiceError{Message: "media relay unavailable", StatusCode: 502, Detail: truncate(relayErr.Error(), 300)}
			}
		}
		closeMediaOnFailure := func(reason string) {
			if media != nil {
				s.closeMedia(media, reason)
				media = nil
			}
		}

		wmStarted := time.Now()
		status, contentType, text, retryAfter, err := s.postWMResponse(token, wmOffer, sessionJSON, explicitProxy, account.DeviceID)
		health := s.reportOutcome(token, wmStarted, status, contentType, text, retryAfter, err)
		if err != nil {
			closeMediaOnFailure("upstream_request_failed")
			s.logger.Error("upstream_realtime_request_failed", "account_id", account.ID, "attempt", attempt, "error", err)
			return nil, &ServiceError{
				Message:    "realtime/wm network failed",
				StatusCode: 502,
				Detail:     truncate(err.Error(), 300),
			}
		}

		if status == 401 && classifyProbeBody(contentType, text) != "html" {
			closeMediaOnFailure("upstream_unauthorized")
			lastStatus = status
			lastError = "account token invalid"
			lastDetail = truncate(text, 300)
			s.logger.Warn("upstream_account_rejected", "account_id", account.ID, "upstream_status", status, "attempt", attempt, "marked_invalid", health.Invalidated)
			if bound != nil && token == bound.AccessToken {
				s.ReleaseSession(owner, req.VoiceSessionID)
			}
			// Bound account died. Upstream conversation ids for that account are unusable.
			if requireStickyAccount {
				return nil, &ServiceError{
					Message:    "bound account token invalid",
					StatusCode: 401,
					Detail:     lastDetail,
				}
			}
			preferredToken = ""
			preferredAccountID = 0
			upstreamForWM.UpstreamVoiceSessionID = newUpstreamVoiceSessionID()
			upstreamForWM.ConversationID = ""
			upstreamForWM.ParentMessageID = ""
			sessionJSON = buildSessionJSON(voice, options.VoiceMode, options.LanguageCode, upstreamForWM)
			continue
		}

		if (status != 200 && status != 201) || !strings.HasPrefix(strings.TrimLeft(text, " \t\r\n"), "v=0") {
			closeMediaOnFailure("upstream_rejected")
			kind := classifyProbeBody(contentType, text)
			s.logger.Warn("upstream_realtime_rejected", "account_id", account.ID, "upstream_status", status, "content_kind", kind, "attempt", attempt)
			msg := fmt.Sprintf("realtime/wm failed status=%d", status)
			if kind == "html" {
				msg = "upstream blocked by Cloudflare or challenge page"
			}
			return nil, &ServiceError{
				Message:    msg,
				StatusCode: 502,
				Detail:     truncate(text, 500),
			}
		}

		answerSDP := text
		if media != nil {
			acceptCtx, cancelAccept := context.WithTimeout(context.Background(), relayAcceptTimeout)
			clientAnswer, acceptErr := media.AcceptUpstreamAnswer(acceptCtx, text)
			cancelAccept()
			if acceptErr != nil {
				closeMediaOnFailure("accept_answer_failed")
				s.logger.Error("relay_accept_answer_failed", "account_id", account.ID, "attempt", attempt, "error", acceptErr)
				return nil, &ServiceError{Message: "media relay negotiation failed", StatusCode: 502, Detail: truncate(acceptErr.Error(), 300)}
			}
			answerSDP = clientAnswer
		}

		var mediaID mediaevidence.Identity
		if media != nil && s.evidence != nil {
			generation, genErr := s.evidence.NextGeneration(targetSessionID)
			if genErr == nil {
				mediaID = mediaevidence.Identity{
					VoiceSessionID:  targetSessionID,
					MediaGeneration: generation,
					AccountID:       account.ID,
					LeaseID:         accountLeaseID(account.ID, targetSessionID),
					LeaseGeneration: 1,
				}
				genErr = s.evidence.Register(owner, media.ID(), mediaID)
			}
			if genErr != nil {
				// Without a durable record the lease could never be released
				// with evidence, so do not hand out a session.
				closeMediaOnFailure("evidence_register_failed")
				s.logger.Error("media_evidence_register_failed", "voice_session_id", targetSessionID, "error", genErr)
				return nil, &ServiceError{Message: "media evidence unavailable", StatusCode: 503}
			}
		}

		var call *callState
		if media != nil {
			call = &callState{
				voice: voice, voiceMode: options.VoiceMode, language: options.LanguageCode,
				accountID: account.ID, token: token, proxy: account.Proxy, deviceID: account.DeviceID,
				stop: make(chan struct{}),
			}
			if s.cfg.RelayHandoffEnabled {
				call.transcript = newTranscriptTail(s.cfg.RelayHandoffMaxChars)
				media.SetEventObserver(call.transcript.Observe)
			}
		}
		voiceSessionID := s.bindVoiceSessionMedia(owner, targetSessionID, token, account, upstreamForWM, media, mediaID, call)
		if media != nil {
			s.startRebuildLoop(owner, voiceSessionID, media, call)
		}
		// Re-read binding so response fields match what was actually stored.
		stored, _ := s.boundVoiceSession(owner, voiceSessionID)
		upstreamOut := upstreamForWM
		if stored != nil {
			upstreamOut = stored.upstreamContext()
		}
		s.persistCallSession(owner, voiceSessionID, account.ID, voice, options.VoiceMode, options.LanguageCode, upstreamOut)
		s.logger.Info(
			"voice_session_created",
			"voice_session_id", voiceSessionID,
			"account_id", account.ID,
			"voice", voice,
			"proxy_source", proxySource,
			"attempt", attempt,
			"resume_conversation", upstreamOut.ConversationID != "",
			"upstream_voice_session_id", upstreamOut.UpstreamVoiceSessionID,
		)
		return &SessionResult{
			AnswerSDP:               answerSDP,
			SessionID:               voiceSessionID,
			VoiceSessionID:          voiceSessionID,
			AccountID:               account.ID,
			AccountLeaseID:          accountLeaseID(account.ID, voiceSessionID),
			AccountLeaseGeneration:  1,
			VoiceSessionGeneration:  mediaID.MediaGeneration,
			Voice:                   voice,
			VoiceMode:               options.VoiceMode,
			LanguageCode:            options.LanguageCode,
			UpstreamVoiceSessionID:  upstreamOut.UpstreamVoiceSessionID,
			UpstreamConversationID:  upstreamOut.ConversationID,
			UpstreamParentMessageID: upstreamOut.ParentMessageID,
		}, nil
	}

	code := 503
	if lastStatus == 401 {
		code = 401
	}
	return nil, &ServiceError{
		Message:    orDefault(lastError, "no available web access_token"),
		StatusCode: code,
		Detail:     lastDetail,
	}
}

func accountLeaseID(accountID int64, sessionID string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", accountID, sessionID)))
	return "lease-" + fmt.Sprintf("%x", h[:16])
}

// AccountLeaseRelease is the terminal observation returned to a trusted
// downstream caller. It is valid only for the exact account/session lease.
type AccountLeaseRelease struct {
	AccountID         string `json:"account_id"`
	LeaseID           string `json:"account_lease_id"`
	Generation        int64  `json:"account_lease_generation"`
	SessionID         string `json:"voice_session_id"`
	SessionGeneration int64  `json:"voice_session_generation"`
	Terminated        bool   `json:"terminated"`
	EvidenceID        string `json:"evidence_id"`
	ObservedAt        string `json:"observed_at"`
}

// errNoTerminationEvidence is the fail-closed answer: the lease stays blocked.
func errNoTerminationEvidence(detail string) *ServiceError {
	return &ServiceError{Message: "upstream WebRTC termination evidence is unavailable", StatusCode: 503, Detail: detail}
}

// ReleaseAccountLease closes the exact media session behind a lease and returns
// its termination evidence. It reports Terminated=true only when all of these
// hold: the voice session id and media generation match a recorded media
// session; the lease id and lease generation match it; peer A, peer B, the
// DataChannel bridge, and any TCP tunnels all closed without error; and the
// evidence row was written durably before this response.
//
// The evidence is a gateway-attested local observation. It does not prove the
// upstream service reclaimed the account. Everything else, including a session
// that was never relayed, a close error, a restart, or a lost evidence write,
// stays blocked with 503.
func (s *Service) ReleaseAccountLease(owner, leaseID string, generation int64, sessionID string, sessionGeneration int64) (*AccountLeaseRelease, error) {
	leaseID, sessionID = strings.TrimSpace(leaseID), strings.TrimSpace(sessionID)
	if generation < 1 || sessionGeneration < 1 || leaseID == "" || sessionID == "" {
		return nil, &ServiceError{Message: "account lease identity is incomplete", StatusCode: 400}
	}
	if s.records == nil || s.evidence == nil {
		return nil, &ServiceError{Message: "account lease persistence is unavailable", StatusCode: 503}
	}
	owner = normalizeSessionOwner(owner)

	row, err := s.evidence.Get(sessionID, sessionGeneration)
	if err != nil {
		if errors.Is(err, mediaevidence.ErrNotFound) {
			// Client-direct media, or an unknown session: nothing to attest.
			return nil, errNoTerminationEvidence("no gateway-terminated media for this session")
		}
		s.logger.Error("media_evidence_lookup_failed", "voice_session_id", sessionID, "error", err)
		return nil, &ServiceError{Message: "account lease persistence is unavailable", StatusCode: 503}
	}
	if row.Owner != owner {
		// Same answer as an unknown session so ownership is not disclosed.
		return nil, errNoTerminationEvidence("no gateway-terminated media for this session")
	}
	if row.LeaseID != leaseID || row.LeaseGeneration != generation {
		return nil, &ServiceError{Message: "account lease identity does not match voice session", StatusCode: 409}
	}

	if row.State == mediaevidence.StateActive || row.State == mediaevidence.StateClosing {
		s.mu.Lock()
		item := s.bindings[sessionID]
		owned := item != nil && item.Owner == owner && item.mediaID.MediaGeneration == row.MediaGeneration
		s.mu.Unlock()
		if !owned {
			// Another goroutine is closing this generation (for example a
			// supersede). Do not guess; the caller may retry.
			return nil, errNoTerminationEvidence("media close is still in progress")
		}
		s.ReleaseSession(owner, sessionID)
		row, err = s.evidence.Get(sessionID, sessionGeneration)
		if err != nil {
			return nil, &ServiceError{Message: "account lease persistence is unavailable", StatusCode: 503}
		}
	}

	if row.State != mediaevidence.StateTerminated || !row.Components.Clean() || row.EvidenceID == "" {
		return nil, errNoTerminationEvidence("media close was not observed cleanly")
	}
	identity := mediaevidence.Identity{
		VoiceSessionID:  row.VoiceSessionID,
		MediaGeneration: row.MediaGeneration,
		AccountID:       row.AccountID,
		LeaseID:         row.LeaseID,
		LeaseGeneration: row.LeaseGeneration,
	}
	ev, err := s.evidence.Evidence(identity)
	if err != nil || ev.EvidenceID != row.EvidenceID {
		return nil, errNoTerminationEvidence("termination evidence row is missing")
	}
	return &AccountLeaseRelease{
		AccountID:         fmt.Sprintf("account-%d", row.AccountID),
		LeaseID:           row.LeaseID,
		Generation:        row.LeaseGeneration,
		SessionID:         row.VoiceSessionID,
		SessionGeneration: row.MediaGeneration,
		Terminated:        true,
		EvidenceID:        ev.EvidenceID,
		ObservedAt:        ev.ObservedAt,
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func orDefault(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

// persistCallSession writes metadata-only call session state for admin listing
// and sticky resume. Chat content is never stored here.
func (s *Service) persistCallSession(owner, voiceSessionID string, accountID int64, voiceName, voiceMode, languageCode string, upstream UpstreamContext) {
	if s == nil || s.records == nil {
		return
	}
	owner = normalizeSessionOwner(owner)
	kind, label, apiKeyID := classifyCaller(owner)
	item := callsessions.Session{
		VoiceSessionID:          strings.TrimSpace(voiceSessionID),
		Owner:                   owner,
		CallerKind:              kind,
		CallerLabel:             label,
		APIKeyID:                apiKeyID,
		AccountID:               accountID,
		UpstreamConversationID:  upstream.ConversationID,
		UpstreamParentMessageID: upstream.ParentMessageID,
		UpstreamVoiceSessionID:  upstream.UpstreamVoiceSessionID,
		Voice:                   voiceName,
		VoiceMode:               voiceMode,
		LanguageCode:            languageCode,
		Status:                  callsessions.StatusActive,
	}
	if err := s.records.Upsert(item); err != nil {
		s.logger.Warn("call_session_persist_failed", "voice_session_id", voiceSessionID, "error", err)
	}
}

func classifyCaller(owner string) (kind, label string, apiKeyID int64) {
	owner = strings.TrimSpace(owner)
	if strings.HasPrefix(owner, "api_key:") {
		idText := strings.TrimPrefix(owner, "api_key:")
		id, _ := strconv.ParseInt(idText, 10, 64)
		return callsessions.CallerAPIKey, "api_key:" + idText, id
	}
	// Built-in voice page / Basic Auth automation: admin:<username> or internal.
	label = callsessions.CallerAdmin
	if strings.HasPrefix(owner, "admin:") {
		user := strings.TrimSpace(strings.TrimPrefix(owner, "admin:"))
		if user != "" {
			label = "admin:" + user
		}
	}
	return callsessions.CallerAdmin, label, 0
}

package voice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

type ownedEvidenceStore interface {
	ReserveOwned(string, mediaevidence.Identity, string, int64) error
	AttachOwnedRelay(mediaevidence.Identity, string) error
	AbandonOwnedReservation(mediaevidence.Identity) error
	OwnedBinding(string, string) (string, int64, error)
	LookupTermination(string, mediaevidence.Identity) (mediaevidence.TerminationLookup, error)
}

type ClientDialRequest struct {
	RequestID    string `json:"request_id"`
	OfferSDP     string `json:"offer_sdp"`
	Voice        string `json:"voice,omitempty"`
	VoiceMode    string `json:"voice_mode,omitempty"`
	LanguageCode string `json:"language_code,omitempty"`
}

type ClientDialResult struct {
	AnswerSDP        string `json:"answer_sdp"`
	GatewaySessionID string `json:"gateway_session_id"`
	VoiceSessionID   string `json:"voice_session_id"`
	Generation       int64  `json:"generation"`
}

type CallError struct {
	Status int
	Code   string
}

func (e *CallError) Error() string { return e.Code }

var unavailableCall = &CallError{503, "voice_unavailable"}

type callStart struct {
	VoiceSessionID string         `json:"voice_session_id"`
	Generation     int64          `json:"generation"`
	RequestID      string         `json:"gateway_request_id"`
	Instructions   *string        `json:"instructions"`
	Opening        OpeningContext `json:"opening_context"`
	OpeningHash    string         `json:"opening_context_hash"`
	Error          struct {
		Code    string `json:"code"`
		Details struct {
			Credential string `json:"credential"`
		} `json:"details"`
	} `json:"error"`
}

type lifecycleIdentity struct {
	Generation       int64  `json:"generation"`
	GatewaySessionID string `json:"gateway_session_id"`
	MediaGeneration  int64  `json:"media_generation"`
	LeaseID          string `json:"account_lease_id"`
	LeaseGeneration  int64  `json:"account_lease_generation"`
}

type openingReceipt struct {
	Applied     bool   `json:"applied"`
	Silent      any    `json:"silent,omitempty"`
	Version     int    `json:"version"`
	ContentHash string `json:"content_hash,omitempty"`
}

type ownedCall struct {
	zoeID, requestID, owner string
	generation              int64
	mu                      sync.Mutex
	media                   relay.Session
	state                   *callState
	id                      mediaevidence.Identity
	answer                  *ClientDialResult
	acked, finished         bool
	closedAt                time.Time
	reason                  string
	closed, reported        chan struct{}
	endOnce                 sync.Once
}

func (c *ownedCall) identity() lifecycleIdentity {
	return lifecycleIdentity{c.generation, c.id.VoiceSessionID, c.id.MediaGeneration, c.id.LeaseID, c.id.LeaseGeneration}
}

func (s *Service) zoeCallOwner() string {
	return "api_key:" + strconv.Itoa(s.cfg.ZoeGatewayAPIKeyID)
}

func (s *Service) GatewayOwnsCalls() bool { return s.cfg.ZoeGatewayAPIKeyID > 0 }

func (s *Service) ownedReady() bool {
	return s.cfg.ZoeGatewayAPIKeyID > 0 && s.zoeToken != "" && strings.HasPrefix(s.cfg.ZoeURL, "https://")
}

const shortRetryWindow = 10 * time.Second
const zoeRequestTimeout = 2 * time.Second

// No error or response body from this credentialed client is logged or echoed.
func (s *Service) zoeJSON(ctx context.Context, method, path, bearer, instanceKey string, body, out any) int {
	ctx, cancel := context.WithTimeout(ctx, zoeRequestTimeout)
	defer cancel()
	var input io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return -1
		}
		input = bytes.NewReader(encoded)
	}
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.cfg.ZoeURL, "/")+path, input)
	if err != nil {
		return -1
	}
	r.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if instanceKey != "" {
		r.Header.Set("X-Zoe-Instance-Key", instanceKey)
	}
	res, err := s.zoeHTTP.Do(r)
	if err != nil {
		return 0
	}
	defer res.Body.Close()
	// Lifecycle retries use HTTP status for retryable responses, never their
	// bodies. Only call.start needs error details for public error mapping.
	_, isStart := out.(*callStart)
	if out != nil && (res.StatusCode < 300 || isStart && (res.StatusCode == 400 || res.StatusCode == 401 || res.StatusCode == 409 || res.StatusCode == 503)) {
		d := json.NewDecoder(io.LimitReader(res.Body, (2<<20)+1))
		if err := d.Decode(out); res.StatusCode < 300 && (err != nil || d.Decode(new(any)) != io.EOF) {
			if ctx.Err() != nil || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0
			}
			return -1
		}
	}
	return res.StatusCode
}

func shortRetry(ctx context.Context, deadline time.Time, fn func(context.Context) int) int {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	backoff, status := 100*time.Millisecond, 0
	for ctx.Err() == nil {
		status = fn(ctx)
		if status != 0 && (status < 500 || status > 599) {
			return status
		}
		wait := time.NewTimer(backoff/2 + rand.N(backoff/2))
		select {
		case <-ctx.Done():
			wait.Stop()
			return status
		case <-wait.C:
		}
		backoff = min(2*backoff, time.Second)
	}
	return status
}

// ValidInstanceBearer checks only RFC 6750 bearer syntax, not the credential.
func ValidInstanceBearer(token string) bool {
	base := strings.TrimRight(token, "=")
	return base != "" && strings.IndexFunc(base, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || strings.ContainsRune("-._~+/", r))
	}) == -1
}

func (s *Service) DialClient(ctx context.Context, key string, p ClientDialRequest) (*ClientDialResult, error) {
	if !ValidInstanceBearer(key) {
		return nil, &CallError{401, "unauthenticated"}
	}
	if strings.TrimSpace(p.RequestID) == "" || strings.TrimSpace(p.OfferSDP) == "" || len(p.OfferSDP) > 1<<20 || !utf8.ValidString(p.OfferSDP) {
		return nil, &CallError{400, "invalid_request"}
	}
	if _, err := normalizeSessionOptions(p.Voice, p.VoiceMode, p.LanguageCode); err != nil {
		return nil, &CallError{400, "invalid_request"}
	}
	claims, canClaim := s.creationOutcomes.(interface {
		ClaimOwnedVoiceCreation(string, string) (string, error)
	})
	_, hasEvidence := s.evidence.(ownedEvidenceStore)
	if !s.ownedReady() || s.cfg.VoiceUpstreamRoute != "avas" || s.relay == nil || !canClaim || !hasEvidence || s.pool == nil {
		return nil, unavailableCall
	}
	// Admission and shutdown synchronize at the start of the dial, not at media allocation.
	s.mu.Lock()
	if s.ownedStopping {
		s.mu.Unlock()
		return nil, unavailableCall
	}
	s.ownedDials.Add(1)
	s.mu.Unlock()
	defer s.ownedDials.Done()
	var start callStart
	status := s.zoeJSON(ctx, http.MethodPost, "/runner/v1/voice/calls", s.zoeToken, key, p, &start)
	code := start.Error.Code
	if status != 200 && status != 201 {
		switch {
		case status == 401 && code == "unauthenticated" && start.Error.Details.Credential == "instance_key":
			return nil, &CallError{401, "unauthenticated"}
		case status == 400 && code == "invalid_request", status == 409 && (code == "idempotency_conflict" || code == "voice_lease_busy"), status == 503 && code == "voice_pool_quarantined":
			return nil, &CallError{status, code}
		default:
			return nil, unavailableCall
		}
	}
	if strings.TrimSpace(start.VoiceSessionID) == "" || strings.TrimSpace(start.RequestID) == "" || start.Generation < 1 {
		return nil, unavailableCall
	}
	owner := s.zoeCallOwner()
	attempt, err := claims.ClaimOwnedVoiceCreation(owner, start.RequestID)
	if err != nil || attempt == "" {
		if status == 200 {
			if replay := s.liveDialReplay(start); replay != nil {
				return replay, nil
			}
		}
		return nil, unavailableCall
	}
	call := &ownedCall{owner: owner, zoeID: start.VoiceSessionID, generation: start.Generation, requestID: start.RequestID,
		closed: make(chan struct{}), reported: make(chan struct{})}
	// Before the create path takes ownership of this attempt, a malformed Zoe
	// response is a provable no-allocation outcome for Zoe's recovery read.
	validManifest := start.Instructions != nil && len(*start.Instructions) <= 6000 && start.Opening.Kind == "full" && start.Opening.Version > 0 &&
		start.Opening.Items != nil && openingContextContentHash(start.Opening) == start.OpeningHash
	_, openingErr := validateOpeningContext(start.Opening)
	_, sdpErr := normalizeSDP(p.OfferSDP)
	if !validManifest || (len(start.Opening.Items) > 0 && openingErr != nil) || sdpErr != nil {
		if err := s.creationOutcomes.UpdateVoiceCreationOutcome(owner, start.RequestID, "never_established", "", 0, attempt); err != nil {
			s.logger.Error("owned_creation_outcome_update_failed")
		}
		return nil, unavailableCall
	}
	res, err := s.CreateSession(CreateSessionRequest{Owner: owner, OfferSDP: p.OfferSDP, Voice: p.Voice, VoiceMode: p.VoiceMode,
		LanguageCode: p.LanguageCode, RequestID: start.RequestID, Instructions: *start.Instructions, OpeningContext: start.Opening, owned: call, creationAttempt: attempt})
	if err != nil {
		// media.md specifies read-only request-outcome reconciliation pre-bind.
		s.logger.Error("owned_call_creation_failed")
		if errors.Is(err, mediaevidence.ErrAccountOccupied) {
			return nil, &CallError{503, "voice_pool_quarantined"}
		}
		return nil, unavailableCall
	}
	// Complete the short binding exchange even if the dialing connection goes
	// away; a correctly authenticated retry can retrieve the still-live answer.
	return s.establishOwnedCall(context.WithoutCancel(ctx), call, res)
}

func (s *Service) liveDialReplay(start callStart) *ClientDialResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.bindings {
		if b.call == nil || b.call.owned == nil {
			continue
		}
		c := b.call.owned
		if c.zoeID != start.VoiceSessionID || c.generation != start.Generation || c.requestID != start.RequestID {
			continue
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, closed := b.media.Termination(); !closed && !c.finished && c.answer != nil {
			answer := *c.answer
			return &answer
		}
	}
	return nil
}

func (s *Service) establishOwnedCall(ctx context.Context, c *ownedCall, res *SessionResult) (*ClientDialResult, error) {
	var body struct {
		lifecycleIdentity
		Opening *openingReceipt `json:"opening_context,omitempty"`
	}
	body.lifecycleIdentity = c.identity()
	if r := res.OpeningContext; r != nil {
		body.Opening = &openingReceipt{r.Applied, r.Silent, r.Version, r.ContentHash}
	}
	status := shortRetry(ctx, time.Now().Add(shortRetryWindow), func(ctx context.Context) int {
		var ack struct {
			OK bool `json:"ok"`
		}
		status := s.zoeJSON(ctx, http.MethodPost, c.path("established"), s.zoeToken, "", body, &ack)
		if status == 200 && !ack.OK {
			return -1
		}
		return status
	})
	if status != 200 {
		s.logger.Error("owned_call_establishment_failed", "status", status, "gateway_session_id", res.VoiceSessionID)
		s.closeTracked(c.media, c.id, "failure")
		c.state.forward.abort()
		s.mu.Lock()
		delete(s.ownedCalls, c.id.VoiceSessionID)
		s.mu.Unlock()
		return nil, unavailableCall
	}
	// The ownership confirmation and forwarding start have the same gate.
	c.mu.Lock()
	c.acked = true
	c.state.startForward()
	if c.finished {
		s.startEndReport(c)
		c.mu.Unlock()
		return nil, unavailableCall
	}
	if _, closed := c.media.Termination(); closed {
		c.mu.Unlock()
		return nil, unavailableCall
	}
	answer := ClientDialResult{res.AnswerSDP, res.VoiceSessionID, c.zoeID, c.generation}
	c.answer = &answer
	c.mu.Unlock()
	return &answer, nil
}

func (c *ownedCall) path(action string) string {
	return "/runner/v1/voice/calls/" + url.PathEscape(c.zoeID) + "/" + action
}

func (s *Service) HangupClient(ctx context.Context, key, handle string) error {
	if !ValidInstanceBearer(key) {
		return &CallError{401, "unauthenticated"}
	}
	evidence, enabled := s.evidence.(ownedEvidenceStore)
	if !s.ownedReady() || !enabled {
		return unavailableCall
	}
	owner := s.zoeCallOwner()
	zoeID, generation, err := evidence.OwnedBinding(owner, handle)
	if errors.Is(err, mediaevidence.ErrNotFound) {
		zoeID = "-"
	} else if err != nil {
		return unavailableCall
	}
	var result struct {
		VoiceSessionID string `json:"voice_session_id"`
		Generation     int64  `json:"generation"`
	}
	status := s.zoeJSON(ctx, http.MethodGet, "/v1/voice/sessions/"+url.PathEscape(zoeID), key, "", nil, &result)
	switch status {
	case 401:
		return &CallError{401, "unauthenticated"}
	case 404:
		return &CallError{404, "not_found"}
	}
	if status != 200 {
		return unavailableCall
	}
	if generation < 1 {
		return &CallError{404, "not_found"}
	}
	if result.VoiceSessionID != zoeID || result.Generation != generation {
		return unavailableCall
	}
	s.mu.Lock()
	c := s.ownedCalls[handle]
	s.mu.Unlock()
	if c != nil && c.zoeID == zoeID && c.generation == generation {
		select {
		case <-c.closed:
			return nil
		default:
		}
		c.mu.Lock()
		acked := c.acked
		c.mu.Unlock()
		if !acked {
			return unavailableCall
		}
		go s.closeTracked(c.media, c.id, "user_hangup")
		select {
		case <-c.closed:
			return nil
		case <-ctx.Done():
			return unavailableCall
		}
	}
	if row, err := s.evidence.Current(handle); err == nil && row.Owner == owner && row.CloseAttemptFinished() {
		return nil
	}
	return unavailableCall
}

func (s *Service) LookupTermination(owner string, id mediaevidence.Identity) (mediaevidence.TerminationLookup, error) {
	if evidence, ok := s.evidence.(ownedEvidenceStore); ok {
		return evidence.LookupTermination(owner, id)
	}
	return mediaevidence.TerminationLookup{}, unavailableCall
}

func ownedEndReason(reason string) string {
	switch reason {
	case "user_hangup", "upstream_voice_end", "gateway_shutdown", "failure":
		return reason
	case "client_data_channel_closed":
		return "media_lost"
	default:
		return "media_lost"
	}
}

func (s *Service) finishOwned(c *ownedCall, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.finished = true
	c.answer = nil
	c.reason = ownedEndReason(reason)
	c.closedAt = time.Now().UTC()
	close(c.closed)
	if c.acked {
		s.startEndReport(c)
	}
}

// Caller holds c.mu. Ack and close race; only the second side starts the report.
func (s *Service) startEndReport(c *ownedCall) {
	c.endOnce.Do(func() {
		go func() {
			defer func() {
				close(c.reported)
				s.mu.Lock()
				delete(s.ownedCalls, c.id.VoiceSessionID)
				s.mu.Unlock()
			}()
			var body struct {
				lifecycleIdentity
				Reason   string                           `json:"reason"`
				ClosedAt time.Time                        `json:"closed_at"`
				Evidence *mediaevidence.TerminationRecord `json:"termination_evidence,omitempty"`
			}
			body.lifecycleIdentity, body.Reason, body.ClosedAt = c.identity(), c.reason, c.closedAt
			record, err := s.LookupTermination(c.owner, c.id)
			if err == nil {
				body.Evidence = record.Evidence
			}
			status := shortRetry(context.Background(), time.Now().Add(shortRetryWindow), func(ctx context.Context) int {
				var ack struct {
					OK                  bool   `json:"ok"`
					State               string `json:"state"`
					UpstreamTermination string `json:"upstream_termination"`
					PoolBlocked         *bool  `json:"pool_blocked"`
				}
				status := s.zoeJSON(ctx, http.MethodPost, c.path("ended"), s.zoeToken, "", body, &ack)
				if (status == 200 || status == 202) && (!ack.OK || ack.PoolBlocked == nil ||
					(status == 200 && (ack.State != "ended" || ack.UpstreamTermination != "verified")) ||
					(status == 202 && (ack.State != "quarantined" || ack.UpstreamTermination != "unverified"))) {
					return -1
				}
				return status
			})
			if status != 200 && status != 202 {
				s.logger.Error("owned_call_ended_report_failed", "gateway_session_id", c.id.VoiceSessionID, "status", status)
			}
		}()
	})
}

func (c *ownedCall) active() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.acked || c.finished {
		return false
	}
	_, closed := c.media.Termination()
	return !closed
}

func (s *Service) StopOwnedAdmission() {
	s.mu.Lock()
	s.ownedStopping = true
	s.mu.Unlock()
}

// ShutdownOwned closes live media concurrently, then gives pending reports the
// remaining shutdown budget. Active dial handlers can no longer admit calls.
func (s *Service) ShutdownOwned(ctx context.Context) {
	s.StopOwnedAdmission()
	done := make(chan struct{})
	go func() {
		s.ownedDials.Wait()
		s.mu.Lock()
		calls := make([]*ownedCall, 0, len(s.ownedCalls))
		for _, c := range s.ownedCalls {
			calls = append(calls, c)
		}
		s.mu.Unlock()
		var wg sync.WaitGroup
		for _, c := range calls {
			wg.Go(func() {
				s.closeTracked(c.media, c.id, "gateway_shutdown")
				select {
				case <-c.reported:
				case <-ctx.Done():
				}
			})
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

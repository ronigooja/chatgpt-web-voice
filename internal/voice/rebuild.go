package voice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

// callState is the per-call data a rebuild needs. It lives in memory beside the
// session binding and is dropped with it.
type callState struct {
	mu         sync.Mutex
	voice      string
	voiceMode  string
	language   string
	accountID  int64
	token      string
	proxy      string
	deviceID   string
	rebuilds   int
	lastReason string
	stop       chan struct{}
	stopOnce   sync.Once
	transcript *transcriptTail
}

func (c *callState) halt() {
	c.stopOnce.Do(func() { close(c.stop) })
}

// RebuildOutcome says what a rebuild attempt did.
type RebuildOutcome struct {
	Swapped        bool   `json:"swapped"`
	ChangedAccount bool   `json:"changed_account"`
	HandedOff      bool   `json:"handed_off"`
	Reason         string `json:"reason"`
}

var (
	errRebuildNoMedia    = errors.New("rebuild: session has no relayed media")
	errRebuildSuperseded = errors.New("rebuild: session changed while rebuilding")
)

// startRebuildLoop begins periodic leg replacement for a relayed call. It is a
// no-op when rebuilds are disabled.
func (s *Service) startRebuildLoop(owner, sessionID string, media relay.Session, state *callState) {
	minutes := s.cfg.RelayRebuildMinutes
	if minutes <= 0 || media == nil || state == nil {
		return
	}
	interval := time.Duration(minutes) * time.Minute
	go func() {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-state.stop:
				return
			case <-timer.C:
			}
			out, err := s.RebuildMedia(owner, sessionID, "scheduled")
			switch {
			case err == nil:
				s.logger.Info("relay_rebuild_done", "voice_session_id", sessionID, "swapped", out.Swapped, "changed_account", out.ChangedAccount, "handed_off", out.HandedOff)
			case errors.Is(err, errRebuildSuperseded), errors.Is(err, errRebuildNoMedia):
				return
			default:
				s.logger.Warn("relay_rebuild_failed", "voice_session_id", sessionID, "error", err)
			}
			timer.Reset(interval)
		}
	}()
}

// RebuildMedia replaces the upstream leg of one relayed call without touching the
// client. The new leg is built and made ready first; only then is it committed and
// the old leg closed. Any failure before the commit leaves the old leg running.
func (s *Service) RebuildMedia(owner, voiceSessionID, why string) (RebuildOutcome, error) {
	owner = normalizeSessionOwner(owner)
	s.mu.Lock()
	item := s.bindings[strings.TrimSpace(voiceSessionID)]
	if item == nil || item.Owner != owner {
		s.mu.Unlock()
		return RebuildOutcome{}, errRebuildSuperseded
	}
	media, mediaID, state := item.media, item.mediaID, item.call
	conversation := item.upstreamContext()
	s.mu.Unlock()
	if media == nil || state == nil {
		return RebuildOutcome{}, errRebuildNoMedia
	}
	if _, closed := media.Termination(); closed {
		return RebuildOutcome{}, errRebuildSuperseded
	}

	state.mu.Lock()
	current := accounts.Account{ID: state.accountID, Proxy: state.proxy, DeviceID: state.deviceID}
	currentToken := state.token
	voice, mode, lang := state.voice, state.voiceMode, state.language
	state.mu.Unlock()

	// Prefer the same account: it keeps the conversation. Fall back to another
	// one only when the current account cannot be used any more.
	token, account, err := s.pickAccountForSession(currentToken, current.ID, map[string]struct{}{})
	changed := false
	if err != nil {
		token, account, err = s.pool.Pick("", map[string]struct{}{currentToken: {}})
		if err != nil {
			return RebuildOutcome{}, fmt.Errorf("rebuild: no usable account: %w", err)
		}
		changed = true
	}
	if account.ID != current.ID {
		changed = true
	}

	upstream := UpstreamContext{UpstreamVoiceSessionID: newUpstreamVoiceSessionID()}
	if !changed {
		upstream.ConversationID = conversation.ConversationID
		upstream.ParentMessageID = conversation.ParentMessageID
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.RelayRebuildReadySeconds)*time.Second+relayAcceptTimeout)
	defer cancel()
	offer, err := media.BeginUpstreamSwap(ctx, s.relayRoute(account))
	if err != nil {
		return RebuildOutcome{}, fmt.Errorf("rebuild: begin swap: %w", err)
	}
	aborted := false
	abort := func(reason string) {
		if aborted {
			return
		}
		aborted = true
		if abortErr := media.AbortUpstreamSwap(reason); abortErr != nil {
			s.logger.Warn("relay_rebuild_abort_unclean", "voice_session_id", voiceSessionID, "error", abortErr)
		}
	}

	sessionJSON := buildSessionJSON(voice, mode, lang, upstream)
	started := time.Now()
	status, ct, text, retryAfter, postErr := s.postWMResponse(token, offer, sessionJSON, account.Proxy, account.DeviceID)
	s.reportOutcome(token, started, status, ct, text, retryAfter, postErr)
	if postErr != nil {
		abort("upstream_request_failed")
		return RebuildOutcome{}, fmt.Errorf("rebuild: upstream request: %w", postErr)
	}
	if (status != 200 && status != 201) || !strings.HasPrefix(strings.TrimLeft(text, " \t\r\n"), "v=0") {
		abort("upstream_rejected")
		return RebuildOutcome{}, fmt.Errorf("rebuild: upstream rejected the new leg (status %d, %s)", status, classifyProbeBody(ct, text))
	}
	if err := media.AcceptSwapAnswer(ctx, text); err != nil {
		abort("accept_answer_failed")
		return RebuildOutcome{}, fmt.Errorf("rebuild: accept answer: %w", err)
	}

	ready := media.SwapReady()
	if ready == nil {
		abort("not_pending")
		return RebuildOutcome{}, errors.New("rebuild: swap is no longer pending")
	}
	select {
	case <-ready:
	case <-time.After(time.Duration(s.cfg.RelayRebuildReadySeconds) * time.Second):
		abort("not_ready")
		return RebuildOutcome{}, errors.New("rebuild: new leg was not ready in time; kept the old leg")
	case <-state.stop:
		abort("call_ended")
		return RebuildOutcome{}, errRebuildSuperseded
	}

	handedOff := false
	if changed && s.cfg.RelayHandoffEnabled && state.transcript != nil {
		if text := state.transcript.Handoff(); text != "" {
			if err := media.SendPendingText(text); err != nil {
				// Without the handoff the call would silently lose its context.
				// Keep the old leg: the next scheduled rebuild can try again.
				abort("handoff_failed")
				return RebuildOutcome{}, fmt.Errorf("rebuild: context handoff failed; kept the old leg: %w", err)
			}
			handedOff = true
		}
	}

	// The call must still be the one we started from, and not already ending.
	s.mu.Lock()
	still := s.bindings[voiceSessionID]
	valid := still != nil && still.Owner == owner && still.media == media
	s.mu.Unlock()
	if !valid {
		abort("session_changed")
		return RebuildOutcome{}, errRebuildSuperseded
	}

	legClose, err := media.CommitUpstreamSwap()
	if err != nil {
		abort("commit_failed")
		return RebuildOutcome{}, fmt.Errorf("rebuild: commit: %w", err)
	}

	state.mu.Lock()
	state.accountID, state.token, state.proxy, state.deviceID = account.ID, token, account.Proxy, account.DeviceID
	state.rebuilds++
	state.lastReason = why
	state.mu.Unlock()
	s.mu.Lock()
	if cur := s.bindings[voiceSessionID]; cur != nil && cur.media == media {
		cur.AccountID, cur.AccessToken, cur.Proxy, cur.DeviceID = account.ID, token, account.Proxy, account.DeviceID
		if changed {
			// A different account cannot resume the old conversation.
			cur.ConversationID, cur.ParentMessageID = "", ""
		}
	}
	s.mu.Unlock()
	if changed && s.records != nil {
		s.persistCallSession(owner, voiceSessionID, account.ID, voice, mode, lang, UpstreamContext{UpstreamVoiceSessionID: upstream.UpstreamVoiceSessionID})
	}
	if s.evidence != nil && mediaID.VoiceSessionID != "" {
		if recErr := s.evidence.RecordLegSwap(mediaID, account.ID, legClose.Clean()); recErr != nil {
			s.logger.Error("media_leg_swap_record_failed", "voice_session_id", voiceSessionID, "error", recErr)
		}
	}
	if !legClose.Clean() {
		s.logger.Warn("relay_retired_leg_unclean", "voice_session_id", voiceSessionID)
	}
	return RebuildOutcome{Swapped: true, ChangedAccount: changed, HandedOff: handedOff, Reason: why}, nil
}

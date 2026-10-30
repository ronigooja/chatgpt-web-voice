package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
)

var (
	errNoSwapPending = errors.New("relay: no upstream swap is pending")
	errSwapPending   = errors.New("relay: an upstream swap is already pending")
	errSwapNotReady  = errors.New("relay: pending upstream leg is not ready")
	errSessionClosed = errors.New("relay: session is closed")
)

// BeginUpstreamSwap implements Session.
func (s *pionSession) BeginUpstreamSwap(ctx context.Context, route Route) (string, error) {
	select {
	case <-s.done:
		return "", errSessionClosed
	default:
	}
	s.legMu.Lock()
	if s.active == nil {
		s.legMu.Unlock()
		return "", errors.New("relay: session has no active upstream leg")
	}
	if s.pending != nil {
		s.legMu.Unlock()
		return "", errSwapPending
	}
	// Reserve the slot with a placeholder so concurrent calls fail fast.
	s.pending = &leg{name: "reserving", closed: make(chan struct{})}
	s.legMu.Unlock()

	next, offer, err := s.buildLeg(ctx, route)
	s.legMu.Lock()
	defer s.legMu.Unlock()
	if err != nil {
		s.pending = nil
		return "", err
	}
	select {
	case <-s.done:
		s.pending = nil
		go func() { _ = s.closeLegPeer(next, "session_closed") }()
		return "", errSessionClosed
	default:
	}
	s.pending = next
	// Count the replacement leg's DataChannel open as readiness; it is not yet
	// attached to the bridge.
	next.channel.OnOpen(func() { s.markChannelOpen(next) })
	if next.channel.ReadyState() == webrtc.DataChannelStateOpen {
		s.markChannelOpen(next)
	}
	return offer, nil
}

func (s *pionSession) pendingLeg() *leg {
	s.legMu.Lock()
	defer s.legMu.Unlock()
	if s.pending == nil || s.pending.pc == nil {
		return nil
	}
	return s.pending
}

// AcceptSwapAnswer implements Session.
func (s *pionSession) AcceptSwapAnswer(ctx context.Context, upstreamAnswer string) error {
	l := s.pendingLeg()
	if l == nil {
		return errNoSwapPending
	}
	if err := s.applyLegAnswer(l, upstreamAnswer); err != nil {
		_ = s.AbortUpstreamSwap("accept_answer_failed")
		return err
	}
	return nil
}

// SwapReady implements Session.
func (s *pionSession) SwapReady() <-chan struct{} {
	l := s.pendingLeg()
	if l == nil {
		return nil
	}
	return l.ready
}

// SendPendingText implements Session.
func (s *pionSession) SendPendingText(text string) error {
	l := s.pendingLeg()
	if l == nil {
		return errNoSwapPending
	}
	select {
	case <-l.ready:
	default:
		return errSwapNotReady
	}
	payload, err := userTextEvent(text)
	if err != nil {
		return err
	}
	return l.channel.SendText(payload)
}

// userTextEvent builds the same envelope the web client sends for typed text:
// a data_message wrapping a relay_message that carries one user text message.
func userTextEvent(text string) (string, error) {
	message := map[string]any{
		"id":             uuid.NewString(),
		"author":         map[string]any{"role": "user"},
		"create_time":    float64(time.Now().UnixNano()) / 1e9,
		"content":        map[string]any{"content_type": "text", "parts": []any{text}},
		"metadata":       map[string]any{"serialization_metadata": map[string]any{"custom_symbol_offsets": []any{}}},
		"clientMetadata": map[string]any{"isOptimistic": true},
	}
	inner, err := json.Marshal(map[string]any{
		"type":    "relay_message",
		"payload": map[string]any{"type": "relay_message", "message": message},
	})
	if err != nil {
		return "", err
	}
	outer, err := json.Marshal(map[string]any{"type": "data_message", "data": string(inner)})
	if err != nil {
		return "", err
	}
	return string(outer), nil
}

// CommitUpstreamSwap implements Session.
func (s *pionSession) CommitUpstreamSwap() (LegTermination, error) {
	s.legMu.Lock()
	next := s.pending
	if next == nil || next.pc == nil {
		s.legMu.Unlock()
		return LegTermination{}, errNoSwapPending
	}
	select {
	case <-next.ready:
	default:
		s.legMu.Unlock()
		return LegTermination{}, errSwapNotReady
	}
	select {
	case <-s.done:
		s.legMu.Unlock()
		return LegTermination{}, errSessionClosed
	default:
	}
	old := s.active
	s.active = next
	s.pending = nil
	s.legMu.Unlock()

	// From here the new leg carries audio and events. The old leg's tracks and
	// channel are ignored by their readers, then closed.
	retired := s.bridge.swapUpstream(next.channel)
	_ = retired
	res := s.closeLeg(old, "swapped")
	s.logger.Info("relay upstream leg swapped", "retired", old.name, "active", next.name, "clean", res.Clean())
	return res, nil
}

// AbortUpstreamSwap implements Session.
func (s *pionSession) AbortUpstreamSwap(reason string) error {
	s.legMu.Lock()
	next := s.pending
	s.pending = nil
	s.legMu.Unlock()
	if next == nil || next.pc == nil {
		return nil
	}
	res := s.closeLeg(next, reason)
	if !res.Clean() {
		return fmt.Errorf("abort swap: pending leg did not close cleanly: %w",
			errors.Join(res.PeerB.Err, res.DataChannels.Err, res.Tunnels.Err))
	}
	return nil
}

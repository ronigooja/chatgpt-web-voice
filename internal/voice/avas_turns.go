package voice

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

const failedHandoffNote = "The hand-off to the background tool failed. Tell the user the request was not confirmed; do not claim it was completed."

func delegationText(raw json.RawMessage) string {
	var parts []struct{ Type, Text string }
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var text strings.Builder
	for _, p := range parts {
		if p.Type == "input_text" {
			text.WriteString(p.Text)
		}
	}
	return text.String()
}

func (s *Service) configureAVASTurns(session string, media relay.Session, call *callState) {
	if f := call.forward; f != nil {
		f.boundOnly = call.owned != nil
		f.failed = func(id string) {
			s.mu.Lock()
			b := s.bindings[session]
			var sideband *avasSidebandWriter
			if b != nil && b.media == media && b.openDelegation == id {
				sideband = b.avasSideband
			}
			s.mu.Unlock()
			if sideband == nil || sendAVASContext(sideband, id, failedHandoffNote) != nil {
				s.logger.Warn("avas_handoff_failure_note_unavailable", "gateway_session_id", session)
			}
		}
	}
}

func (s *Service) observeAVASCallEvent(session string, media relay.Session, call *callState, data []byte) {
	var e struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
		Turn struct {
			ID, Role                  string
			Transcript, Content, Text json.RawMessage
		}
		Item struct {
			ID, Target string
			Content    json.RawMessage
		}
	}
	if json.Unmarshal(data, &e) != nil {
		return
	}
	if e.Type == "data_message" {
		var encoded string
		if json.Unmarshal(e.Data, &encoded) == nil {
			e.Data = json.RawMessage(encoded)
		}
		inner := e.Data
		if json.Unmarshal(inner, &e) != nil {
			return
		}
	}
	if e.Type == "delegation.ended" || e.Type == "delegation.done" {
		if e.Item.ID == "" {
			return
		}
		s.mu.Lock()
		if b := s.bindings[session]; b != nil && b.media == media && b.openDelegation == e.Item.ID {
			b.openDelegation = ""
		}
		s.mu.Unlock()
		return
	}
	if e.Type == "voice.end" && call.owned != nil {
		go s.closeTracked(media, call.owned.id, "upstream_voice_end")
		return
	}
	if e.Type == "turn.done" && call.owned == nil {
		text := turnText(e.Turn.Transcript)
		role := "other"
		if e.Turn.Role == "user" || e.Turn.Role == "assistant" {
			role = e.Turn.Role
		}
		s.logger.Debug("zoe_debug_message", "event", "turn.done", "role", role, "turn_id_present", e.Turn.ID != "", "byte_length", len(text), "rune_length", utf8.RuneCountInString(text))
	}
	var id, userTurnID string
	switch {
	case e.Type == "turn.created" && e.Turn.Role == "user":
		userTurnID = e.Turn.ID
	case e.Type == "delegation.created" && e.Item.Target == "client" && strings.TrimSpace(delegationText(e.Item.Content)) != "" &&
		e.Item.ID != "" && utf8.RuneCountInString(e.Item.ID) <= 256:
		id = e.Item.ID
		if call.owned == nil {
			text := delegationText(e.Item.Content)
			s.logger.Debug("zoe_debug_message", "event", "delegation.created", "target", "client", "byte_length", len(text), "rune_length", utf8.RuneCountInString(text))
		}
	default:
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.bindings[session]; b != nil && b.media == media {
		if userTurnID != "" && userTurnID == b.openUserTurn {
			return
		}
		if id != "" {
			if b.seenDelegation == nil {
				b.seenDelegation = make(map[string]bool)
			}
			if b.seenDelegation[id] {
				return
			}
			b.seenDelegation[id] = true
			b.recentDelegations = append(b.recentDelegations, id)
			if len(b.recentDelegations) > 2048 {
				delete(b.seenDelegation, b.recentDelegations[0])
				b.recentDelegations = b.recentDelegations[1:]
			}
		} else {
			b.openUserTurn = userTurnID
		}
		b.openDelegation = id
	}
}

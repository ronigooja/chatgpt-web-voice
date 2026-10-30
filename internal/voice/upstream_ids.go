package voice

import (
	"encoding/json"
	"strings"
)

// upstreamIDsFromEvent extracts the conversation continuity ids from one raw
// oai-events text frame, using the same rule as static/voice.html: look at the
// unwrapped data_message body and at its payload. Empty strings mean absent.
func upstreamIDsFromEvent(data []byte) (conversationID, parentMessageID string) {
	var outer struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &outer) != nil {
		return "", ""
	}
	body := data
	if outer.Type == "data_message" && len(outer.Data) > 0 {
		var asString string
		if json.Unmarshal(outer.Data, &asString) == nil {
			body = []byte(asString)
		} else {
			body = outer.Data
		}
	}
	type ids struct {
		ConversationID  string `json:"conversation_id"`
		ParentMessageID string `json:"parent_message_id"`
	}
	var inner struct {
		ids
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(body, &inner) != nil {
		return "", ""
	}
	conversationID, parentMessageID = strings.TrimSpace(inner.ConversationID), strings.TrimSpace(inner.ParentMessageID)
	if len(inner.Payload) > 0 {
		var p ids
		if json.Unmarshal(inner.Payload, &p) == nil {
			if v := strings.TrimSpace(p.ConversationID); v != "" {
				conversationID = v
			}
			if v := strings.TrimSpace(p.ParentMessageID); v != "" {
				parentMessageID = v
			}
		}
	}
	return conversationID, parentMessageID
}

// learnUpstreamIDs records the ids a relayed call's active upstream leg
// announces, so a rebuild can resume the conversation without depending on the
// client to report them. It only touches the binding that owns this media, and
// keeps the values in memory.
func (s *Service) learnUpstreamIDs(sessionID string, owned func(*sessionBinding) bool, data []byte) {
	conversationID, parentMessageID := upstreamIDsFromEvent(data)
	if conversationID == "" && parentMessageID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.bindings[sessionID]
	if item == nil || !owned(item) {
		return
	}
	if conversationID != "" {
		item.ConversationID = conversationID
	}
	if parentMessageID != "" {
		item.ParentMessageID = parentMessageID
	}
}

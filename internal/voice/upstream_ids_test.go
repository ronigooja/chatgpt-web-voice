package voice

import (
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"strings"
	"testing"
)

func wrapEvent(inner string) string {
	escaped := strings.ReplaceAll(inner, `"`, `\"`)
	return `{"type":"data_message","data":"` + escaped + `"}`
}

func TestUpstreamIDsFromEventShapes(t *testing.T) {
	cases := []struct {
		name         string
		frame        string
		conv, parent string
	}{
		{"top level in wrapped body", wrapEvent(`{"type":"conversation_update","conversation_id":"c1","parent_message_id":"p1"}`), "c1", "p1"},
		{"nested payload", wrapEvent(`{"type":"conversation_update","payload":{"conversation_id":"c2","parent_message_id":"p2"}}`), "c2", "p2"},
		{"payload wins over top level", wrapEvent(`{"conversation_id":"old","payload":{"conversation_id":"new"}}`), "new", ""},
		{"only conversation id", wrapEvent(`{"type":"startup_telemetry","conversation_id":"c3"}`), "c3", ""},
		{"unwrapped frame", `{"type":"conversation_update","conversation_id":"c4","parent_message_id":"p4"}`, "c4", "p4"},
		{"whitespace trimmed", wrapEvent(`{"conversation_id":"  c5  "}`), "c5", ""},
		{"no ids", wrapEvent(`{"type":"usage_update","payload":{}}`), "", ""},
		{"not json", `not json`, "", ""},
		{"wrapped body not json", `{"type":"data_message","data":"nope"}`, "", ""},
		{"empty", ``, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conv, parent := upstreamIDsFromEvent([]byte(tc.frame))
			if conv != tc.conv || parent != tc.parent {
				t.Fatalf("got %q/%q want %q/%q", conv, parent, tc.conv, tc.parent)
			}
		})
	}
}

func TestConversationIDsLearnedFromEventsWithoutClientReport(t *testing.T) {
	svc := New(config.Config{SessionTTLSeconds: 3600}, nil, nil)
	id := svc.bindVoiceSession("api_key:1", "vs_learned", "token", accounts.Account{ID: 1}, UpstreamContext{})
	svc.learnUpstreamIDs(id, func(*sessionBinding) bool { return true }, []byte(wrapEvent(`{"conversation_id":"conv-learned","parent_message_id":"msg-learned"}`)))
	ctxNow, _ := svc.SessionContext("api_key:1", id)
	if ctxNow.ConversationID != "conv-learned" || ctxNow.ParentMessageID != "msg-learned" {
		t.Fatalf("learned context: %+v", ctxNow)
	}
}

func TestLearnedIDsDoNotLeakToAnotherBinding(t *testing.T) {
	svc := New(config.Config{SessionTTLSeconds: 3600}, nil, nil)
	id := svc.bindVoiceSession("api_key:1", "vs_learned", "token", accounts.Account{ID: 1}, UpstreamContext{})
	svc.learnUpstreamIDs("vs_unknown", func(*sessionBinding) bool { return true }, []byte(wrapEvent(`{"conversation_id":"x"}`)))
	svc.learnUpstreamIDs(id, func(*sessionBinding) bool { return false }, []byte(wrapEvent(`{"conversation_id":"y"}`)))
	if ctxNow, _ := svc.SessionContext("api_key:1", id); ctxNow.ConversationID != "" {
		t.Fatalf("ids from a non-owning media must be ignored, got %+v", ctxNow)
	}
}

func TestConversationEvidenceConfirmsMatchingStoredResumeIDs(t *testing.T) {
	svc := New(config.Config{SessionTTLSeconds: 3600}, nil, nil)
	id := svc.bindVoiceSession("api_key:1", "vs_evidence", "token", accounts.Account{ID: 1}, UpstreamContext{ConversationID: "conv", ParentMessageID: "parent"})
	svc.learnUpstreamIDs(id, func(*sessionBinding) bool { return true }, []byte(wrapEvent(`{"conversation_id":"conv","parent_message_id":"parent"}`)))
	b, _ := svc.boundVoiceSession("api_key:1", id)
	if b.conversation != "resumed" || b.conversationConfirmed == nil || !*b.conversationConfirmed {
		t.Fatalf("binding=%+v", b)
	}
}

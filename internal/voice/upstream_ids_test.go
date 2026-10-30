package voice

import (
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

func TestRebuildResumesConversationLearnedFromEventsWithoutClientReport(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.m.emit(wrapEvent(`{"type":"conversation_update","conversation_id":"conv-learned","parent_message_id":"msg-learned"}`))

	out, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Swapped || out.ChangedAccount {
		t.Fatalf("outcome: %+v", out)
	}
	reqs := r.requests()
	session := reqs[len(reqs)-1].Fields["session"]
	if !strings.Contains(session, `"conversation_id":"conv-learned"`) || !strings.Contains(session, `"parent_message_id":"msg-learned"`) {
		t.Fatalf("the rebuild must resume the conversation the gateway saw on the wire, session=%q", session)
	}
}

func TestLearnedIDsDoNotLeakToAnotherBindingOrAfterAccountChange(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.learnUpstreamIDs("vs_unknown", func(*sessionBinding) bool { return true }, []byte(wrapEvent(`{"conversation_id":"x"}`)))
	r.svc.learnUpstreamIDs(r.res.VoiceSessionID, func(*sessionBinding) bool { return false }, []byte(wrapEvent(`{"conversation_id":"y"}`)))
	if ctxNow, _ := r.svc.SessionContext("api_key:1", r.res.VoiceSessionID); ctxNow.ConversationID != "" {
		t.Fatalf("ids from a non-owning media must be ignored, got %+v", ctxNow)
	}
}

package voice

import (
	"encoding/json"
	"errors"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"strings"
	"testing"
	"unicode/utf8"
)

type contextSender struct {
	events   [][]byte
	err      error
	callback func()
}

func (s *contextSender) SendUpstreamEvent(b []byte) error {
	if s.callback != nil {
		s.callback()
	}
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, append([]byte(nil), b...))
	return nil
}
func contextPayload(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestAVASContextAppendDelegationAndSession(t *testing.T) {
	for _, tc := range []struct{ id, typ string }{{"d1", "delegation.context.append"}, {"", "session.context.append"}} {
		s := &contextSender{}
		if err := sendAVASContext(s, tc.id, "hello"); err != nil {
			t.Fatal(err)
		}
		v := contextPayload(t, s.events[0])
		if v["type"] != tc.typ || v["channel"] != "speakable" {
			t.Fatalf("%v", v)
		}
		if tc.id != "" && v["delegation_item_id"] != tc.id {
			t.Fatalf("%v", v)
		}
	}
}
func TestAVASContextChunksMultibyteAndPropagatesFailure(t *testing.T) {
	text := strings.Repeat("界", 300)
	s := &contextSender{}
	if err := sendAVASContext(s, "d", text); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, b := range s.events {
		v := contextPayload(t, b)
		part := v["content"].([]any)[0].(map[string]any)["text"].(string)
		if len([]byte(part)) > 500 {
			t.Fatal("chunk exceeds 500 bytes")
		}
		got.WriteString(part)
	}
	if got.String() != text {
		t.Fatal("chunks changed text")
	}
	failed := &contextSender{err: errors.New("write")}
	if err := sendAVASContext(failed, "d", "x"); err == nil || len(failed.events) != 0 {
		t.Fatal("failure not propagated")
	}
}

func TestAVASContextChunkBoundaries(t *testing.T) {
	for _, text := range []string{strings.Repeat("a", 499), strings.Repeat("a", 500), strings.Repeat("a", 501), strings.Repeat("a", 499) + "界"} {
		s := &contextSender{}
		if err := sendAVASContext(s, "d", text); err != nil {
			t.Fatal(err)
		}
		var got strings.Builder
		for _, raw := range s.events {
			v := contextPayload(t, raw)
			part := v["content"].([]any)[0].(map[string]any)["text"].(string)
			if len([]byte(part)) > 500 || !utf8.ValidString(part) {
				t.Fatalf("invalid part: %q", part)
			}
			got.WriteString(part)
		}
		if got.String() != text {
			t.Fatalf("got %q, want %q", got.String(), text)
		}
	}
}
func TestAVASContextSenderDoesNotNeedServiceMutex(t *testing.T) {
	var called bool
	s := &contextSender{callback: func() { called = true }}
	var _ relay.UpstreamEventInjector = s
	if err := sendAVASContext(s, "", "x"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("sender callback not reached")
	}
}

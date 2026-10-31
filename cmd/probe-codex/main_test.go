package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestV3SessionShapeAndRedactedDump(t *testing.T) {
	s := buildSession()
	if s.Model != "gpt-live-1-codex" || s.Delegation.Type != "client" || s.Audio.Output.Voice != "cove" {
		t.Fatalf("unexpected session defaults: %+v", s)
	}
	if len(s.InitialItems) != 2 || s.InitialItems[0].Role != "user" || s.InitialItems[0].Content[0].Type != "input_text" || s.InitialItems[1].Content[0].Type != "output_text" {
		t.Fatalf("unexpected initial_items: %+v", s.InitialItems)
	}
	b, err := json.Marshal(requestBody{SDP: "secret-sdp", Session: s})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "delegation") {
		t.Fatal("session did not encode")
	}
	dump := redactedDump(requestBody{SDP: "secret-sdp", Session: s}, requestHeaders("secret-token"))
	if strings.Contains(dump, "secret-sdp") || strings.Contains(dump, "secret-token") {
		t.Fatalf("dump leaked secret: %s", dump)
	}
	for _, h := range []string{"Authorization", "Content-Type", "User-Agent", "openai-alpha", "originator"} {
		if !strings.Contains(dump, h) {
			t.Fatalf("missing header %s", dump)
		}
	}
}

func TestFramelessOutboundShapes(t *testing.T) {
	// These are the exact V3 wire forms used by Codex's frameless methods.
	if err := sendShape("conversation.item.create", "user", "input_text"); err != nil {
		t.Fatal(err)
	}
}
func sendShape(typ, role, contentType string) error {
	_, err := json.Marshal(map[string]any{"type": typ, "item": map[string]any{"type": "message", "role": role, "content": []any{map[string]any{"type": contentType, "text": "x"}}}})
	return err
}

package main

import (
	"encoding/json"
	"testing"
)

func TestRelayTextMatchesWebMessageShape(t *testing.T) {
	raw := envelope(relayText("hello"))
	var outer map[string]any
	if err := json.Unmarshal(raw, &outer); err != nil { t.Fatal(err) }
	if outer["type"] != "data_message" { t.Fatalf("outer type=%v", outer["type"]) }
	var inner map[string]any
	if err := json.Unmarshal([]byte(outer["data"].(string)), &inner); err != nil { t.Fatal(err) }
	if inner["type"] != "relay_message" { t.Fatalf("inner type=%v", inner["type"]) }
	payload := inner["payload"].(map[string]any)
	if payload["type"] != "relay_message" { t.Fatalf("payload type=%v", payload["type"]) }
	message := payload["message"].(map[string]any)
	for _, key := range []string{"id", "author", "create_time", "content", "metadata", "clientMetadata"} { if _, ok := message[key]; !ok { t.Fatalf("missing message key %q", key) } }
	if message["author"].(map[string]any)["role"] != "user" { t.Fatal("author role") }
	content := message["content"].(map[string]any)
	if content["content_type"] != "text" { t.Fatal("content type") }
	parts := content["parts"].([]any); if len(parts) != 1 || parts[0] != "hello" { t.Fatalf("parts=%v", parts) }
	if message["metadata"].(map[string]any)["serialization_metadata"].(map[string]any)["custom_symbol_offsets"] == nil { t.Fatal("metadata shape") }
	if message["clientMetadata"].(map[string]any)["isOptimistic"] != true { t.Fatal("client metadata shape") }
}

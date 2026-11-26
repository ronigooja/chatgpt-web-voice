package voice

import (
	"encoding/json"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
	"strings"
	"testing"
)

func TestAVASCreationEventPrivacy(t *testing.T) {
	p := buildAVASSession("", UpstreamContext{})
	p.Instructions = "instruction"
	p.InitialItems = []map[string]any{{"type": "message"}}
	p.Model = "SDP authorization secret"
	raw := avasCreationEvent(p)
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != 2 || object["instructions"] == nil || object["initial_items"] == nil {
		t.Fatalf("fields: %v", object)
	}
	if strings.Contains(string(raw), "SDP") || strings.Contains(string(raw), "authorization") {
		t.Fatal("unexpected private creation fields")
	}
}

func TestAVASRejectedContextCapturedOnce(t *testing.T) {
	store := eventlog.New(eventlog.Defaults())
	key := eventlog.Key{SessionID: "s", Generation: 1}
	store.Start(key, "zoe", 4)
	w := &avasSidebandWriter{closed: true, capture: func(data []byte, delivery, sourceKind string) {
		store.Append(key, "gateway_to_upstream", "avas_sideband", delivery, data, false)
	}}
	if err := sendAVASContext(w, "delegation", "hello"); err == nil {
		t.Fatal("expected rejection")
	}
	snap, _ := store.Snapshot(key, 0, 10)
	if len(snap.Events) != 1 || snap.Events[0].Type != "delegation.context.append" || snap.Events[0].Delivery != "rejected" {
		t.Fatalf("events: %+v", snap.Events)
	}
}

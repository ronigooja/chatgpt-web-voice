package voice

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type avasFakeInjector struct {
	mu     sync.Mutex
	events [][]byte
	err    error
}

func (f *avasFakeInjector) InjectEvent(b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, append([]byte(nil), b...))
	return nil
}

type avasFakeLogger struct{}

func (avasFakeLogger) Info(string, ...any) {}

func avasData(v any) []byte {
	b, _ := json.Marshal(v)
	return []byte(fmt.Sprintf(`{"type":"data_message","data":%q}`, b))
}
func delegation(id, target string, parts ...string) []byte {
	content := make([]map[string]string, 0, len(parts))
	for _, p := range parts {
		content = append(content, map[string]string{"type": "input_text", "text": p})
	}
	return avasData(map[string]any{"type": "delegation.created", "item": map[string]any{"id": id, "target": target, "content": content}})
}
func avasMapper() (*avasFinalTurnMapper, *avasFakeInjector, *string) {
	in := &avasFakeInjector{}
	var open string
	return newAVASFinalTurnMapper("s", avasFakeLogger{}, in, func(id string) { open = id }), in, &open
}

func TestAVASFinalTurnDelegationEmitsExpectedEvent(t *testing.T) {
	m, in, open := avasMapper()
	m.Observe(delegation("d1", "client", " hello", " world "))
	if len(in.events) != 1 {
		t.Fatalf("events=%d", len(in.events))
	}
	var got map[string]any
	_ = json.Unmarshal(in.events[0], &got)
	if got["type"] != "user_audio_turn.final" || got["turn_id"] != "d1" || got["revision"] != float64(1) || got["transcript"] != "hello world" || *open != "d1" {
		t.Fatalf("got=%v open=%q", got, *open)
	}
}
func TestAVASFinalTurnFiltersAndDeduplicates(t *testing.T) {
	m, in, open := avasMapper()
	m.Observe(delegation("x", "server", "ignored"))
	m.Observe(delegation("x", "client", "   "))
	if len(in.events) != 0 || *open != "" {
		t.Fatal("ignored input changed state")
	}
	m.Observe(delegation("d", "client", "one"))
	m.Observe(delegation("d", "client", "two"))
	if len(in.events) != 1 || *open != "d" {
		t.Fatalf("events=%d open=%q", len(in.events), *open)
	}
}
func TestAVASFinalTurnIgnoresMalformedUnknownAndOversized(t *testing.T) {
	m, in, open := avasMapper()
	for _, b := range [][]byte{[]byte("bad"), []byte(`{"type":"other"}`), []byte(`{"type":"data_message","data":"bad"}`)} {
		m.Observe(b)
	}
	m.Observe(delegation("huge", "client", strings.Repeat("界", zoeFinalTurnMax+1)))
	if len(in.events) != 0 || *open != "" {
		t.Fatal("invalid event emitted or opened")
	}
}
func TestAVASFinalTurnClearsOnNewUserTurnAndReplacesOnNewDelegation(t *testing.T) {
	m, _, open := avasMapper()
	m.Observe(delegation("d1", "client", "one"))
	m.Observe(avasData(map[string]any{"type": "turn.created", "turn": map[string]string{"role": "user"}}))
	if *open != "" {
		t.Fatalf("open=%q", *open)
	}
	m.Observe(delegation("d2", "client", "two"))
	if *open != "d2" {
		t.Fatalf("open=%q", *open)
	}
}

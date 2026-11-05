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

func (f *avasFakeInjector) setError(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
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

func TestAVASFinalTurnRetriesAfterInjectionFailure(t *testing.T) {
	m, in, open := avasMapper()
	in.setError(fmt.Errorf("temporary"))
	m.Observe(delegation("retry", "client", "one"))
	if *open != "" || len(in.events) != 0 {
		t.Fatalf("failed injection changed state: open=%q events=%d", *open, len(in.events))
	}
	in.setError(nil)
	m.Observe(delegation("retry", "client", "one"))
	if *open != "retry" || len(in.events) != 1 {
		t.Fatalf("retry not emitted: open=%q events=%d", *open, len(in.events))
	}
}

type avasCaptureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *avasCaptureLogger) Info(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprint(append([]any{msg}, args...)...))
}

func (l *avasCaptureLogger) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func TestAVASDebugTextLogsOnlyWhenEnabled(t *testing.T) {
	turn := avasData(map[string]any{"type": "turn.done", "turn": map[string]any{"role": "assistant", "transcript": "ASSISTANT-SAID-THIS"}})
	for _, enabled := range []bool{false, true} {
		logger := &avasCaptureLogger{}
		m := newAVASFinalTurnMapper("s", logger, &avasFakeInjector{}, nil, enabled)
		m.Observe(turn)
		m.Observe(delegation("d", "client", "DELEGATED-REQUEST"))
		got := logger.text()
		has := strings.Contains(got, "ASSISTANT-SAID-THIS") && strings.Contains(got, "DELEGATED-REQUEST")
		if has != enabled {
			t.Fatalf("debug=%v but text logged=%v: %s", enabled, has, got)
		}
		if !enabled && strings.Contains(got, "zoe_debug_message") {
			t.Fatalf("debug line logged with the switch off: %s", got)
		}
	}
}

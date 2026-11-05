package voice

import (
	"encoding/json"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

type avasFinalTurnMapper struct {
	mu          sync.Mutex
	done        map[string]struct{}
	ring        [256]string
	count, next int
	session     string
	logger      interface{ Info(string, ...any) }
	injector    relay.EventInjector
	open        func(string)
}

func newAVASFinalTurnMapper(session string, logger interface{ Info(string, ...any) }, injector relay.EventInjector, open func(string)) *avasFinalTurnMapper {
	return &avasFinalTurnMapper{session: session, logger: logger, injector: injector, done: map[string]struct{}{}, open: open}
}

func (m *avasFinalTurnMapper) Observe(data []byte) {
	var outer struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if len(data) > 1<<20 || json.Unmarshal(data, &outer) != nil || outer.Type != "data_message" {
		return
	}
	in := outer.Data
	var encoded string
	if json.Unmarshal(in, &encoded) == nil {
		in = []byte(encoded)
	}
	var e struct {
		Type string `json:"type"`
		Turn struct {
			Role string `json:"role"`
		} `json:"turn"`
		Item struct {
			ID, Target string
			Content    []struct{ Type, Text string } `json:"content"`
		} `json:"item"`
	}
	if json.Unmarshal(in, &e) != nil {
		return
	}
	if e.Type == "turn.created" && e.Turn.Role == "user" {
		if m.open != nil {
			m.open("")
		}
		return
	}
	if e.Type != "delegation.created" || e.Item.Target != "client" {
		return
	}
	var b strings.Builder
	for _, part := range e.Item.Content {
		if part.Type == "input_text" {
			b.WriteString(part.Text)
		}
	}
	text := strings.TrimSpace(b.String())
	if text == "" {
		return
	}
	m.mu.Lock()
	if utf8.RuneCountInString(text) > zoeFinalTurnMax {
		m.mu.Unlock()
		m.logger.Info("zoe_final_turn_dropped", "voice_session_id", m.session, "reason", "oversized")
		return
	}
	if len(e.Item.ID) < 1 || len(e.Item.ID) > 256 {
		m.mu.Unlock()
		return
	}
	if _, ok := m.done[e.Item.ID]; ok {
		m.mu.Unlock()
		m.logger.Info("zoe_final_turn_dropped", "voice_session_id", m.session, "reason", "duplicate")
		return
	}
	event, _ := json.Marshal(map[string]any{"type": "user_audio_turn.final", "turn_id": e.Item.ID, "revision": 1, "transcript": text})
	// Keep the mapper lock through injection so a duplicate cannot race ahead
	// before the ID is recorded. The open callback remains outside this lock.
	if err := m.injector.InjectEvent(event); err != nil {
		m.mu.Unlock()
		return
	}
	if m.count < len(m.ring) {
		m.ring[m.count] = e.Item.ID
		m.count++
	} else {
		delete(m.done, m.ring[m.next])
		m.ring[m.next] = e.Item.ID
		m.next = (m.next + 1) % len(m.ring)
	}
	m.done[e.Item.ID] = struct{}{}
	m.mu.Unlock()
	if m.open != nil {
		m.open(e.Item.ID)
	}
	m.logger.Info("zoe_final_turn_emitted", "voice_session_id", m.session, "transcript_runes", utf8.RuneCountInString(text), "commit_reason", "delegation_created")
}

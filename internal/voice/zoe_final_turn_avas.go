package voice

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

type avasFinalTurnMapper struct {
	mu                     sync.Mutex
	done                   map[string]struct{}
	ring                   [256]string
	count, next            int
	session                string
	logger                 interface{ Info(string, ...any) }
	injector               relay.EventInjector
	open                   func(string)
	debugText              bool
	probeText              string
	probeSend              func(string, string) error
	probeSeen              map[string]struct{}
	probeAt                map[string]time.Time
	probeReact             map[string]int
	instructionSent        bool
	instructionEnabled     bool
	instructionTurns       int
	instructionDelegations int
}

func newAVASFinalTurnMapper(session string, logger interface{ Info(string, ...any) }, injector relay.EventInjector, open func(string), debugText ...bool) *avasFinalTurnMapper {
	return &avasFinalTurnMapper{session: session, logger: logger, injector: injector, done: map[string]struct{}{}, open: open, debugText: len(debugText) > 0 && debugText[0], probeSeen: map[string]struct{}{}, probeAt: map[string]time.Time{}, probeReact: map[string]int{}}
}

func (m *avasFinalTurnMapper) configureProbe(text string, send func(string, string) error) {
	m.probeText = strings.TrimSpace(text)
	m.probeSend = send
}

func (m *avasFinalTurnMapper) configureInstruction(sent bool) {
	m.instructionSent = sent
	m.instructionEnabled = true
}

func (m *avasFinalTurnMapper) probeEnabled() bool {
	return m.probeSend != nil && m.probeText != ""
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
			Role       string `json:"role"`
			Transcript string `json:"transcript"`
		} `json:"turn"`
		Item struct {
			ID, Target string
			Content    []struct{ Type, Text string } `json:"content"`
		} `json:"item"`
	}
	if json.Unmarshal(in, &e) != nil {
		return
	}
	if m.debugText && (e.Type == "turn.done" || e.Type == "delegation.created") {
		preview := e.Turn.Transcript
		if e.Type == "delegation.created" {
			var b strings.Builder
			for _, part := range e.Item.Content {
				if part.Type == "input_text" {
					b.WriteString(part.Text)
				}
			}
			preview = b.String()
		}
		if utf8.RuneCountInString(preview) > 200 {
			preview = string([]rune(preview)[:200])
		}
		m.logger.Info("zoe_debug_message", "event", e.Type, "role", e.Turn.Role, "target", e.Item.Target, "text", preview)
	}
	if m.instructionEnabled && m.instructionSent {
		m.mu.Lock()
		if e.Type == "turn.done" && e.Turn.Role == "assistant" && m.instructionTurns < 6 {
			args := []any{"event", e.Type}
			if m.debugText {
				args = append(args, "text", e.Turn.Transcript)
			}
			m.logger.Info("probe_instruction_reaction", args...)
			m.instructionTurns++
		}
		if e.Type == "delegation.created" && m.instructionDelegations < 6 {
			args := []any{"event", e.Type, "target", e.Item.Target}
			if m.debugText {
				var b strings.Builder
				for _, part := range e.Item.Content {
					if part.Type == "input_text" {
						b.WriteString(part.Text)
					}
				}
				args = append(args, "text", b.String())
			}
			m.logger.Info("probe_instruction_delegation", args...)
			m.instructionDelegations++
		}
		m.mu.Unlock()
	}
	if m.probeSend != nil && (e.Type == "output_audio.delta" || (e.Type == "turn.done" && e.Turn.Role == "assistant")) {
		m.mu.Lock()
		for id, sentAt := range m.probeAt {
			if m.probeReact[id] >= 2 {
				continue
			}
			m.probeReact[id]++
			args := []any{"delegation_item_id", id, "event", e.Type, "ms_since_probe_sent", time.Since(sentAt).Milliseconds()}
			if e.Type == "turn.done" && m.debugText {
				args = append(args, "text", e.Turn.Transcript)
			}
			m.logger.Info("probe_upstream_reaction", args...)
		}
		m.mu.Unlock()
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
	if m.probeSend != nil && m.probeText != "" {
		createdAt := time.Now()
		m.mu.Lock()
		_, seen := m.probeSeen[e.Item.ID]
		m.mu.Unlock()
		if seen {
			return
		}
		if err := m.probeSend(e.Item.ID, m.probeText); err != nil {
			return
		}
		m.mu.Lock()
		m.probeSeen[e.Item.ID] = struct{}{}
		m.probeAt[e.Item.ID] = time.Now()
		m.mu.Unlock()
		m.logger.Info("probe_speakable_sent", "delegation_item_id", e.Item.ID, "ms_since_delegation_created", time.Since(createdAt).Milliseconds(), "text_runes", utf8.RuneCountInString(m.probeText))
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

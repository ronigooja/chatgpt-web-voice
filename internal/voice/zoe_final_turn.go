package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

const zoeFinalTurnMax = 20000

type zoeTurnMapper interface{ Observe([]byte) }
type wmFinalTurnMapper struct {
	mu                sync.Mutex
	active, oversized bool
	id, text          string
	done              map[string]struct{}
	session           string
	logger            interface{ Info(string, ...any) }
	injector          relay.EventInjector
}

func newWMFinalTurnMapper(session string, logger interface{ Info(string, ...any) }, injector relay.EventInjector) *wmFinalTurnMapper {
	return &wmFinalTurnMapper{session: session, logger: logger, injector: injector, done: map[string]struct{}{}}
}
func (m *wmFinalTurnMapper) Observe(data []byte) {
	if len(data) > 1<<20 {
		return
	}
	var o struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &o) != nil || o.Type != "data_message" {
		return
	}
	in := o.Data
	var s string
	if json.Unmarshal(in, &s) == nil {
		in = []byte(s)
	}
	var e struct {
		Type    string `json:"type"`
		Payload struct {
			Delta json.RawMessage `json:"delta"`
		} `json:"payload"`
	}
	if json.Unmarshal(in, &e) != nil || e.Type != "chat_message_delta" {
		return
	}
	var d struct {
		V json.RawMessage `json:"v"`
	}
	if json.Unmarshal(e.Payload.Delta, &d) != nil || len(d.V) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if d.V[0] == '{' {
		m.add(d.V)
		return
	}
	if d.V[0] != '[' {
		return
	}
	var ops []struct {
		O, P string
		V    json.RawMessage
	}
	if json.Unmarshal(d.V, &ops) != nil {
		return
	}
	for _, op := range ops {
		if op.O == "replace" && op.P == "/message/status" {
			var st string
			if json.Unmarshal(op.V, &st) == nil {
				m.finish(st)
			}
			continue
		}
		if !appendTextPath.MatchString(op.P) || !m.active || (op.O != "append" && op.O != "replace") {
			continue
		}
		var t string
		if json.Unmarshal(op.V, &t) != nil {
			continue
		}
		if op.O == "replace" {
			m.text = t
		} else {
			m.text += t
		}
		if utf8.RuneCountInString(m.text) > zoeFinalTurnMax {
			m.oversized = true
			m.text = ""
		}
	}
}
func (m *wmFinalTurnMapper) add(raw json.RawMessage) {
	var v struct {
		Message *struct {
			ID, Status string
			Author     struct {
				Role string `json:"role"`
			} `json:"author"`
			Content struct {
				Parts []json.RawMessage `json:"parts"`
			} `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(raw, &v) != nil || v.Message == nil {
		return
	}
	if m.active {
		m.active = false
		m.text = ""
		m.oversized = false
		m.logger.Info("zoe_final_turn_dropped", "voice_session_id", m.session, "reason", "ambiguous_interleave")
	}
	if v.Message.Author.Role != "user" || v.Message.Status != "in_progress" {
		return
	}
	m.id, m.active = v.Message.ID, true
	for _, p := range v.Message.Content.Parts {
		var s string
		if json.Unmarshal(p, &s) == nil {
			m.text += s
			continue
		}
		var x struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(p, &x) == nil {
			m.text += x.Text
		}
	}
	if utf8.RuneCountInString(m.text) > zoeFinalTurnMax {
		m.oversized = true
		m.text = ""
	}
}
func (m *wmFinalTurnMapper) finish(status string) {
	if status != "finished_successfully" || !m.active {
		return
	}
	id, text := m.id, strings.TrimSpace(m.text)
	m.active = false
	if m.oversized {
		m.drop("oversized")
		return
	}
	if text == "" {
		m.drop("empty")
		return
	}
	if len(id) < 1 || len(id) > 256 {
		m.drop("empty")
		return
	}
	if _, ok := m.done[id]; ok {
		m.drop("duplicate")
		return
	}
	if len(m.done) >= 256 {
		for k := range m.done {
			delete(m.done, k)
			break
		}
	}
	m.done[id] = struct{}{}
	b, _ := json.Marshal(struct {
		Type       string `json:"type"`
		TurnID     string `json:"turn_id"`
		Revision   int    `json:"revision"`
		Transcript string `json:"transcript"`
	}{"user_audio_turn.final", id, 1, text})
	if m.injector.InjectEvent(b) == nil {
		h := sha256.Sum256([]byte(id))
		m.logger.Info("zoe_final_turn_emitted", "voice_session_id", m.session, "transcript_runes", utf8.RuneCountInString(text), "sha256_prefix", hex.EncodeToString(h[:])[:8])
	}
}
func (m *wmFinalTurnMapper) drop(reason string) {
	m.text = ""
	m.oversized = false
	m.logger.Info("zoe_final_turn_dropped", "voice_session_id", m.session, "reason", reason)
}

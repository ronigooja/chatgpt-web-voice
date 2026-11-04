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
	mu                         sync.Mutex
	active, oversized          bool
	id, text                   string
	lastAddedID, lastAddedRole string
	done                       map[string]struct{}
	doneRing                   [256]string
	doneCount, doneNext        int
	session                    string
	logger                     interface{ Info(string, ...any) }
	injector                   relay.EventInjector
}

func newWMFinalTurnMapper(session string, logger interface{ Info(string, ...any) }, injector relay.EventInjector) *wmFinalTurnMapper {
	return &wmFinalTurnMapper{session: session, logger: logger, injector: injector, done: map[string]struct{}{}}
}

type finalTurnAction struct {
	event          []byte
	id, text, drop string
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
		O, P string
		V    json.RawMessage
	}
	if json.Unmarshal(e.Payload.Delta, &d) != nil {
		return
	}
	m.mu.Lock()
	var actions []finalTurnAction
	if len(d.V) > 0 && d.V[0] == '{' {
		m.add(d.V, &actions)
	} else if len(d.V) > 0 && d.V[0] == '[' {
		var ops []struct {
			O, P string
			V    json.RawMessage
		}
		if json.Unmarshal(d.V, &ops) == nil {
			for _, op := range ops {
				m.apply(op.O, op.P, op.V, &actions)
			}
		}
	} else if d.O == "replace" && d.P == "/message/status" {
		m.finish(d.V, &actions)
	}
	m.mu.Unlock()
	m.deliver(actions)
}
func (m *wmFinalTurnMapper) add(raw json.RawMessage, a *[]finalTurnAction) {
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
		m.resetTurn()
		*a = append(*a, finalTurnAction{drop: "ambiguous_interleave"})
	}
	m.lastAddedID, m.lastAddedRole = v.Message.ID, v.Message.Author.Role
	if v.Message.Author.Role != "user" || v.Message.Status != "in_progress" {
		return
	}
	m.resetTurn()
	m.id, m.active = v.Message.ID, true
	for _, p := range v.Message.Content.Parts {
		var s string
		if json.Unmarshal(p, &s) == nil {
			m.text += s
		} else {
			var x struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(p, &x) == nil {
				m.text += x.Text
			}
		}
	}
	if utf8.RuneCountInString(m.text) > zoeFinalTurnMax {
		m.oversized = true
		m.text = ""
	}
}
func (m *wmFinalTurnMapper) apply(o, p string, v json.RawMessage, a *[]finalTurnAction) {
	if o == "replace" && p == "/message/status" {
		m.finish(v, a)
		return
	}
	if m.active && appendTextPath.MatchString(p) && (o == "append" || o == "replace") {
		var t string
		if json.Unmarshal(v, &t) != nil {
			return
		}
		if o == "replace" {
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
func (m *wmFinalTurnMapper) finish(v json.RawMessage, a *[]finalTurnAction) {
	var st string
	if json.Unmarshal(v, &st) != nil || st != "finished_successfully" || !m.active || m.lastAddedID != m.id || m.lastAddedRole != "user" {
		return
	}
	id, text := m.id, strings.TrimSpace(m.text)
	oversized := m.oversized
	m.resetTurn()
	if oversized {
		*a = append(*a, finalTurnAction{drop: "oversized"})
		return
	}
	if text == "" {
		*a = append(*a, finalTurnAction{drop: "empty"})
		return
	}
	if len(id) < 1 || len(id) > 256 {
		return
	}
	if _, ok := m.done[id]; ok {
		*a = append(*a, finalTurnAction{drop: "duplicate"})
		return
	}
	if m.doneCount < 256 {
		m.doneRing[m.doneCount] = id
		m.doneCount++
	} else {
		old := m.doneRing[m.doneNext]
		delete(m.done, old)
		m.doneRing[m.doneNext] = id
		m.doneNext = (m.doneNext + 1) % 256
	}
	m.done[id] = struct{}{}
	b, _ := json.Marshal(struct {
		Type       string `json:"type"`
		TurnID     string `json:"turn_id"`
		Revision   int    `json:"revision"`
		Transcript string `json:"transcript"`
	}{"user_audio_turn.final", id, 1, text})
	*a = append(*a, finalTurnAction{event: b, id: id, text: text})
}

func (m *wmFinalTurnMapper) resetTurn() {
	m.active = false
	m.oversized = false
	m.id = ""
	m.text = ""
}
func (m *wmFinalTurnMapper) deliver(as []finalTurnAction) {
	for _, a := range as {
		if a.drop != "" {
			m.logger.Info("zoe_final_turn_dropped", "voice_session_id", m.session, "reason", a.drop)
			continue
		}
		if m.injector.InjectEvent(a.event) != nil {
			continue
		}
		h := sha256.Sum256([]byte(a.id))
		m.logger.Info("zoe_final_turn_emitted", "voice_session_id", m.session, "transcript_runes", utf8.RuneCountInString(a.text), "sha256_prefix", hex.EncodeToString(h[:])[:8])
	}
}

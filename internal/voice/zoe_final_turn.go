package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
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
	debug                      bool
	appends, replaces          int
	parts                      map[int]struct{}
	initial                    int
	ops                        []map[string]any
}

func newWMFinalTurnMapper(session string, logger interface{ Info(string, ...any) }, injector relay.EventInjector, debug ...bool) *wmFinalTurnMapper {
	d := len(debug) > 0 && debug[0]
	return &wmFinalTurnMapper{session: session, logger: logger, injector: injector, done: map[string]struct{}{}, debug: d, parts: map[int]struct{}{}}
}

type finalTurnAction struct {
	event          []byte
	id, text, drop string
	commitReason   string
	debug          map[string]any
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
		if v.Message.Author.Role == "assistant" {
			m.commit("assistant_add", a)
		} else {
			m.resetTurn()
			m.resetStats()
			*a = append(*a, finalTurnAction{drop: "ambiguous_interleave"})
		}
	}
	m.lastAddedID, m.lastAddedRole = v.Message.ID, v.Message.Author.Role
	if m.debug {
		parts := make([]string, 0, len(v.Message.Content.Parts))
		var preview string
		for _, p := range v.Message.Content.Parts {
			var s string
			var x struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(p, &s) != nil {
				_ = json.Unmarshal(p, &x)
				s = x.Text
			}
			parts = append(parts, fmt.Sprintf("%T", p))
			preview += s
		}
		if utf8.RuneCountInString(preview) > 200 {
			preview = string([]rune(preview)[:200])
		}
		m.logger.Info("zoe_debug_message", "role", v.Message.Author.Role, "status", v.Message.Status, "content_types", parts, "part_count", len(v.Message.Content.Parts), "text", preview)
	}
	if v.Message.Author.Role != "user" || v.Message.Status != "in_progress" {
		return
	}
	m.resetTurn()
	m.id, m.active = v.Message.ID, true
	m.resetStats()
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
	m.initial = utf8.RuneCountInString(m.text)
	m.ops = append(m.ops, map[string]any{"kind": "init", "part": -1, "runes": m.initial})
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
	if appendTextPath.MatchString(p) && (o == "append" || o == "replace") {
		var t string
		if json.Unmarshal(v, &t) != nil {
			return
		}
		part := textPart(p)
		if m.debug {
			msg := t
			if utf8.RuneCountInString(msg) > 200 {
				msg = string([]rune(msg)[:200])
			}
			state := "not_active"
			if m.active {
				state = "active"
			}
			m.logger.Info("zoe_debug_text_patch", "message_state", state, "part", part, "op", o, "text", msg)
		}
		if !m.active {
			return
		}
		m.parts[part] = struct{}{}
		if o == "replace" {
			m.replaces++
			m.text = t
		} else {
			m.appends++
			m.text += t
		}
		if len(m.ops) < 40 {
			m.ops = append(m.ops, map[string]any{"kind": o, "part": part, "runes": utf8.RuneCountInString(t)})
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
	m.commitData(id, text, oversized, "status_patch", a)
}

func (m *wmFinalTurnMapper) commit(reason string, a *[]finalTurnAction) {
	id, text, oversized := m.id, strings.TrimSpace(m.text), m.oversized
	m.commitData(id, text, oversized, reason, a)
}

func (m *wmFinalTurnMapper) commitData(id, text string, oversized bool, reason string, a *[]finalTurnAction) {
	debug := m.debugFields()
	m.resetTurn()
	m.resetStats()
	if oversized {
		*a = append(*a, finalTurnAction{drop: "oversized", debug: debug})
		return
	}
	if text == "" {
		*a = append(*a, finalTurnAction{drop: "empty", debug: debug})
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
	*a = append(*a, finalTurnAction{event: b, id: id, text: text, commitReason: reason, debug: debug})
}
func (m *wmFinalTurnMapper) resetStats() {
	m.appends = 0
	m.replaces = 0
	m.initial = 0
	m.parts = map[int]struct{}{}
	m.ops = nil
}
func (m *wmFinalTurnMapper) debugFields() map[string]any {
	if !m.debug {
		return nil
	}
	p := []int{}
	for n := range m.parts {
		p = append(p, n)
	}
	return map[string]any{"transcript": strings.TrimSpace(m.text), "turn_id": m.id, "append_ops": m.appends, "replace_ops": m.replaces, "parts": p, "initial_runes": m.initial, "ops": m.ops}
}

var textPathPart = regexp.MustCompile(`/parts/(\d+)/text$`)

func textPart(p string) int {
	x := textPathPart.FindStringSubmatch(p)
	if len(x) < 2 {
		return -1
	}
	n, _ := strconv.Atoi(x[1])
	return n
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
			args := []any{"voice_session_id", m.session, "reason", a.drop}
			for k, v := range a.debug {
				args = append(args, k, v)
			}
			m.logger.Info("zoe_final_turn_dropped", args...)
			continue
		}
		if m.injector.InjectEvent(a.event) != nil {
			continue
		}
		h := sha256.Sum256([]byte(a.id))
		args := []any{"voice_session_id", m.session, "transcript_runes", utf8.RuneCountInString(a.text), "sha256_prefix", hex.EncodeToString(h[:])[:8], "commit_reason", a.commitReason}
		for k, v := range a.debug {
			args = append(args, k, v)
		}
		m.logger.Info("zoe_final_turn_emitted", args...)
	}
}

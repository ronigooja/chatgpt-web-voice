package voice

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"
)

// transcriptTail keeps bounded recent turns and running character metrics.
// It is never written to disk, never logged, and dropped with the media session.
type transcriptTail struct {
	mu           sync.Mutex
	maxChars     int
	turns        []transcriptTurn
	cur          transcriptTurn
	contextChars int
}

type transcriptTurn struct {
	Role string
	ID   string
	Text string
}

var appendTextPath = regexp.MustCompile(`^/message/content/parts/\d+/text$`)

func newTranscriptTail(maxChars int) *transcriptTail {
	return &transcriptTail{maxChars: maxChars}
}

// Observe feeds one raw oai-events text frame. Non-conversation frames are ignored.
func (t *transcriptTail) Observe(data []byte) {
	var outer struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &outer) != nil || outer.Type != "data_message" {
		return
	}
	innerBytes := []byte(outer.Data)
	var asString string
	if json.Unmarshal(outer.Data, &asString) == nil {
		innerBytes = []byte(asString)
	}
	var inner struct {
		Type    string `json:"type"`
		Payload struct {
			Delta json.RawMessage `json:"delta"`
		} `json:"payload"`
	}
	if json.Unmarshal(innerBytes, &inner) != nil || inner.Type != "chat_message_delta" || len(inner.Payload.Delta) == 0 {
		return
	}
	var delta struct {
		V json.RawMessage `json:"v"`
	}
	if json.Unmarshal(inner.Payload.Delta, &delta) != nil || len(delta.V) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch delta.V[0] {
	case '{':
		t.applyMessageLocked(delta.V)
	case '[':
		t.applyOpsLocked(delta.V)
	}
}

func (t *transcriptTail) applyMessageLocked(raw json.RawMessage) {
	var v struct {
		Message *struct {
			ID     string `json:"id"`
			Author struct {
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
	role := v.Message.Author.Role
	var texts []string
	for _, rawPart := range v.Message.Content.Parts {
		var asString string
		if json.Unmarshal(rawPart, &asString) == nil {
			texts = append(texts, asString)
			continue
		}
		var part struct {
			ContentType string `json:"content_type"`
			Direction   string `json:"direction"`
			Text        string `json:"text"`
			Content     string `json:"content"`
			Transcript  string `json:"transcript"`
		}
		if json.Unmarshal(rawPart, &part) != nil {
			continue
		}
		if role != "assistant" && role != "user" && part.ContentType == "audio_transcription" {
			switch part.Direction {
			case "in":
				role = "user"
			case "out":
				role = "assistant"
			}
		}
		for _, candidate := range []string{part.Text, part.Content, part.Transcript} {
			if candidate != "" {
				texts = append(texts, candidate)
				break
			}
		}
	}
	if role != "assistant" && role != "user" {
		return
	}
	if v.Message.ID != t.cur.ID || role != t.cur.Role {
		t.flushLocked()
	}
	t.cur = transcriptTurn{Role: role, ID: v.Message.ID, Text: strings.TrimSpace(strings.Join(texts, "\n"))}
}

func (t *transcriptTail) applyOpsLocked(raw json.RawMessage) {
	if t.cur.Role == "" {
		return
	}
	var ops []struct {
		O string          `json:"o"`
		P string          `json:"p"`
		V json.RawMessage `json:"v"`
	}
	if json.Unmarshal(raw, &ops) != nil {
		return
	}
	for _, op := range ops {
		if op.O != "append" || !appendTextPath.MatchString(op.P) {
			continue
		}
		var text string
		if json.Unmarshal(op.V, &text) == nil {
			t.cur.Text += text
		}
	}
}

func (t *transcriptTail) flushLocked() {
	if t.cur.Role != "" && strings.TrimSpace(t.cur.Text) != "" {
		t.contextChars += len(t.cur.Text)
		t.turns = append(t.turns, t.cur)
		t.trimLocked()
	}
	t.cur = transcriptTurn{}
}

func (t *transcriptTail) ContextChars() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.contextChars
	if t.cur.Role != "" {
		n += len(t.cur.Text)
	}
	return n
}

func (t *transcriptTail) ResetContextChars() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.contextChars = 0
	t.turns = nil
	t.cur = transcriptTurn{}
}

func (t *transcriptTail) sizeLocked() int {
	n := 0
	for _, turn := range t.turns {
		n += len(turn.Text)
	}
	return n
}

func (t *transcriptTail) trimLocked() {
	for len(t.turns) > 1 && t.sizeLocked() > t.maxChars {
		t.turns = t.turns[1:]
	}
}

// Empty reports whether any conversation text was recorded.
func (t *transcriptTail) Empty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if strings.TrimSpace(t.cur.Text) != "" {
		return false
	}
	return len(t.turns) == 0
}

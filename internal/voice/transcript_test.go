package voice

import (
	"strings"
	"testing"
)

func appendFrame(path, text string) string {
	return `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":[{\"o\":\"append\",\"p\":\"` + path + `\",\"v\":\"` + text + `\"}]}}}"}`
}

func deltaFrame(role, id, text string) string {
	return `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"` + id +
		`\",\"author\":{\"role\":\"` + role + `\"},\"content\":{\"parts\":[\"` + text + `\"]}}}}}}"}`
}

func TestTranscriptCallStateIsNotWrittenToDisk(t *testing.T) {
	f := newLeaseFixture(t, okAnswer(t))
	f.create(t)
	f.fr.session(0).emit(deltaFrame("user", "m1", "a very private sentence"))
	for _, table := range []string{"voice_media_sessions", "voice_termination_evidence", "call_sessions"} {
		rows, err := f.pool.DB().Conn().Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for _, v := range vals {
				if s, ok := v.(string); ok && strings.Contains(s, "private sentence") {
					t.Fatalf("conversation text leaked into %s", table)
				}
			}
		}
		rows.Close()
	}
}

func TestTranscriptRecordsBothSidesInOrder(t *testing.T) {
	tr := newTranscriptTail(6000)
	tr.Observe([]byte(deltaFrame("user", "u1", "hello")))
	tr.Observe([]byte(deltaFrame("assistant", "a1", "hi there")))
	tr.Observe([]byte(deltaFrame("user", "u2", "what time is it")))
	if len(tr.turns) != 2 || tr.turns[0].Role != "user" || tr.turns[0].Text != "hello" || tr.turns[1].Role != "assistant" || tr.turns[1].Text != "hi there" || tr.cur.Text != "what time is it" {
		t.Fatalf("turns missing or out of order: %+v %+v", tr.turns, tr.cur)
	}
}

func TestTranscriptAppendsStreamedText(t *testing.T) {
	tr := newTranscriptTail(6000)
	tr.Observe([]byte(deltaFrame("assistant", "a1", "Once")))
	tr.Observe([]byte(appendFrame("/message/content/parts/0/text", " upon")))
	tr.Observe([]byte(appendFrame("/message/content/parts/0/text", " a time")))
	// Unrelated append paths must be ignored.
	tr.Observe([]byte(appendFrame("/message/metadata/foo", " IGNORED")))
	if tr.cur.Role != "assistant" || tr.cur.Text != "Once upon a time" {
		t.Fatalf("streamed text wrong: %+v", tr.cur)
	}
}

func TestTranscriptKeepsAudioTranscriptionDirection(t *testing.T) {
	tr := newTranscriptTail(6000)
	frame := `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"x1\",\"author\":{\"role\":\"tool\"},\"content\":{\"parts\":[{\"content_type\":\"audio_transcription\",\"direction\":\"in\",\"text\":\"spoken words\"}]}}}}}}"}`
	tr.Observe([]byte(frame))
	if tr.cur.Role != "user" || tr.cur.Text != "spoken words" {
		t.Fatalf("audio_transcription direction=in must map to the user: %+v", tr.cur)
	}
}

func TestTranscriptIgnoresNoiseAndBrokenFrames(t *testing.T) {
	tr := newTranscriptTail(6000)
	for _, f := range []string{
		``, `not json`, `{"type":"other"}`, `{"type":"data_message","data":"not json"}`,
		`{"type":"data_message","data":"{\"type\":\"usage_update\",\"payload\":{}}"}`,
		`{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{}}"}`,
		deltaFrame("system", "s1", "must be ignored"),
	} {
		tr.Observe([]byte(f))
	}
	if !tr.Empty() || tr.ContextChars() != 0 {
		t.Fatalf("noise must record nothing, got %+v %+v", tr.turns, tr.cur)
	}
}

func TestTranscriptTailDropsOldestFirst(t *testing.T) {
	tr := newTranscriptTail(500)
	for i := 0; i < 20; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		tr.Observe([]byte(deltaFrame(role, "m"+string(rune('a'+i)), strings.Repeat("x", 60)+string(rune('A'+i)))))
	}
	if tr.sizeLocked() > 500 {
		t.Fatalf("retained text exceeds budget: %d", tr.sizeLocked())
	}
	if last := tr.cur.Text; !strings.HasSuffix(last, string(rune('A'+19))) {
		t.Fatalf("the newest turn must be last, got %q", last)
	}
	if first := tr.turns[0].Text; strings.HasSuffix(first, string(rune('A'+0))) {
		t.Fatalf("the oldest turn must have been dropped, first line %q", first)
	}
}

func TestTranscriptMemoryStaysBounded(t *testing.T) {
	tr := newTranscriptTail(1000)
	for i := 0; i < 2000; i++ {
		tr.Observe([]byte(deltaFrame("user", "id"+string(rune(i%90+33))+string(rune(i/90+33)), strings.Repeat("y", 100))))
	}
	tr.mu.Lock()
	n := tr.sizeLocked()
	tr.mu.Unlock()
	if n > 1100 {
		t.Fatalf("recorded text grew unbounded: %d bytes", n)
	}
}

func TestContextCharsCountsAndResetsOnNewConversation(t *testing.T) {
	tr := newTranscriptTail(1000)
	tr.Observe([]byte(deltaFrame("user", "u1", "hello")))
	tr.Observe([]byte(deltaFrame("assistant", "a1", "world")))
	if got := tr.ContextChars(); got != 10 {
		t.Fatalf("context chars=%d", got)
	}
	tr.ResetContextChars()
	if got := tr.ContextChars(); got != 0 {
		t.Fatalf("after reset=%d", got)
	}
}

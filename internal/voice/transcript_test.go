package voice

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func appendFrame(path, text string) string {
	return `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":[{\"o\":\"append\",\"p\":\"` + path + `\",\"v\":\"` + text + `\"}]}}}"}`
}

func TestTranscriptRecordsBothSidesInOrder(t *testing.T) {
	tr := newTranscriptTail(6000)
	tr.Observe([]byte(deltaFrame("user", "u1", "hello")))
	tr.Observe([]byte(deltaFrame("assistant", "a1", "hi there")))
	tr.Observe([]byte(deltaFrame("user", "u2", "what time is it")))
	h := tr.Handoff()
	iu1, ia1, iu2 := strings.Index(h, "User: hello"), strings.Index(h, "Assistant: hi there"), strings.Index(h, "User: what time is it")
	if iu1 < 0 || ia1 < iu1 || iu2 < ia1 {
		t.Fatalf("turns missing or out of order:\n%s", h)
	}
}

func TestTranscriptAppendsStreamedText(t *testing.T) {
	tr := newTranscriptTail(6000)
	tr.Observe([]byte(deltaFrame("assistant", "a1", "Once")))
	tr.Observe([]byte(appendFrame("/message/content/parts/0/text", " upon")))
	tr.Observe([]byte(appendFrame("/message/content/parts/0/text", " a time")))
	// Unrelated append paths must be ignored.
	tr.Observe([]byte(appendFrame("/message/metadata/foo", " IGNORED")))
	if h := tr.Handoff(); !strings.Contains(h, "Assistant: Once upon a time") || strings.Contains(h, "IGNORED") {
		t.Fatalf("streamed text wrong:\n%s", h)
	}
}

func TestTranscriptKeepsAudioTranscriptionDirection(t *testing.T) {
	tr := newTranscriptTail(6000)
	frame := `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"x1\",\"author\":{\"role\":\"tool\"},\"content\":{\"parts\":[{\"content_type\":\"audio_transcription\",\"direction\":\"in\",\"text\":\"spoken words\"}]}}}}}}"}`
	tr.Observe([]byte(frame))
	if h := tr.Handoff(); !strings.Contains(h, "User: spoken words") {
		t.Fatalf("audio_transcription direction=in must map to the user:\n%s", h)
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
	if !tr.Empty() || tr.Handoff() != "" {
		t.Fatalf("noise must record nothing, got %q", tr.Handoff())
	}
}

func TestTranscriptHandoffRespectsBudgetAndDropsOldestFirst(t *testing.T) {
	tr := newTranscriptTail(500)
	for i := 0; i < 20; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		tr.Observe([]byte(deltaFrame(role, "m"+string(rune('a'+i)), strings.Repeat("x", 60)+string(rune('A'+i)))))
	}
	h := tr.Handoff()
	body := h[strings.Index(h, "\n")+1:]
	if len(body) > 500 {
		t.Fatalf("body %d bytes exceeds the 500 budget", len(body))
	}
	// Turns end with a marker letter; check the body only, since the header
	// contains capital letters of its own.
	lines := strings.Split(body, "\n")
	if last := lines[len(lines)-1]; !strings.HasSuffix(last, string(rune('A'+19))) {
		t.Fatalf("the newest turn must be last, got %q", last)
	}
	if first := lines[0]; strings.HasSuffix(first, string(rune('A'+0))) {
		t.Fatalf("the oldest turn must have been dropped, first line %q", first)
	}
}

func TestTranscriptTrimsOneHugeTurnWithoutBreakingRunes(t *testing.T) {
	tr := newTranscriptTail(500)
	tr.Observe([]byte(deltaFrame("user", "u1", strings.Repeat("语", 400))))
	h := tr.Handoff()
	if h == "" || !utf8.ValidString(h) {
		t.Fatalf("a single oversized turn must still hand over valid UTF-8, got %q", h)
	}
	if len(h[strings.Index(h, "\n")+1:]) > 500 {
		t.Fatal("oversized turn exceeded the budget")
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

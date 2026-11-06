package voice

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

// When debug text changes, verify both mapper paths and the independent Zoe delivery.
func TestDebugTextSwitchPreservesTranscriptForwarding(t *testing.T) {
	for _, path := range []string{"avas", "legacy"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/debug=%v", path, enabled), func(t *testing.T) {
				t.Setenv("VOICE_ZOE_FINAL_TURN_DEBUG_TEXT", fmt.Sprint(enabled))
				cfg := config.Load()
				sink := newForwardSink(t, nil)
				forward := forwardTestWorker(t, sink, &safeForwardLog{}, true)
				logger := &avasCaptureLogger{}
				injected := &zoeFakeInjector{}
				var mapper zoeTurnMapper
				var frames [][]byte
				const request = "PRIVATE-DELEGATED-REQUEST"
				const answer = "PRIVATE-ASSISTANT-TURN"
				if path == "avas" {
					mapper = newAVASFinalTurnMapper("s", logger, injected, nil, cfg.ZoeFinalTurnDebugText)
					frames = [][]byte{
						delegation("u", "client", request),
						avasData(map[string]any{"type": "turn.done", "turn": map[string]any{"id": "a", "role": "assistant", "transcript": answer}}),
					}
				} else {
					mapper = newWMFinalTurnMapper("s", logger, injected, cfg.ZoeFinalTurnDebugText)
					frames = [][]byte{
						zaddWithPart("u", request, "user", "in_progress"), zstatusOp(),
						zaddWithPart("a", answer, "assistant", "in_progress"), zstatusOp(),
					}
				}
				media := &fakeMedia{}
				media.SetEventObserver(func(data []byte) {
					forward.Observe(data)
					mapper.Observe(data)
				})
				for _, frame := range frames {
					media.emit(string(frame))
				}
				forward.Close()
				<-forward.done
				turns := flattenForward(sink.snapshot())
				if len(turns) != 2 || turns[0].Role != "user" || turns[0].Text != request || turns[1].Role != "assistant" || turns[1].Text != answer {
					t.Fatalf("Zoe did not receive both verbatim turns: %+v", turns)
				}
				assertTranscripts(t, injected, request)
				for _, text := range []string{request, answer} {
					if strings.Contains(logger.text(), text) != enabled {
						t.Fatalf("debug=%v: incorrect visibility for %q: %s", enabled, text, logger.text())
					}
				}
				if !enabled && strings.Contains(logger.text(), "zoe_debug_") {
					t.Fatalf("debug diagnostics emitted while disabled: %s", logger.text())
				}
			})
		}
	}
}

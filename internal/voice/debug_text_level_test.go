package voice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

// When content logging changes, exercise real slog filtering on both mapper paths.
func TestDebugTextLevelsAndFullContent(t *testing.T) {
	turn := strings.Repeat("界", 5000)
	delegated := strings.Repeat("delegation-", 500) + "END"
	for _, path := range []string{"wm", "avas"} {
		for _, mode := range []string{"info", "default", "disabled"} {
			t.Run(path+"/"+mode, func(t *testing.T) {
				t.Setenv("VOICE_ZOE_FINAL_TURN_DEBUG_TEXT", "")
				t.Setenv("VOICE_LOG_LEVEL", "")
				if mode == "disabled" {
					t.Setenv("VOICE_ZOE_FINAL_TURN_DEBUG_TEXT", "false")
				}
				cfg := config.Load()
				level := slog.LevelDebug
				if cfg.LogLevel != "debug" {
					t.Fatal("default level does not enable debug text")
				}
				if mode == "info" {
					level = slog.LevelInfo
				}
				var out bytes.Buffer
				logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: level}))
				if path == "wm" {
					m := newWMFinalTurnMapper("s", logger, &zoeFakeInjector{}, cfg.ZoeFinalTurnDebugText)
					runZ(m, zaddWithPart("u", turn, "user", "in_progress"), zops(fmt.Sprintf(`{"o":"append","p":"/message/content/parts/0/text","v":%q}`, delegated)), zstatusOp())
					// A dropped turn also carries gated debug fields.
					runZ(m, zaddWithPart("u", turn, "user", "in_progress"), zops(fmt.Sprintf(`{"o":"append","p":"/message/content/parts/0/text","v":%q}`, strings.Repeat("x", zoeFinalTurnMax))), zstatusOp())
				} else {
					m := newAVASFinalTurnMapper("s", logger, &avasFakeInjector{}, nil, cfg.ZoeFinalTurnDebugText)
					m.Observe(avasData(map[string]any{"type": "turn.done", "turn": map[string]any{"role": "assistant", "transcript": turn}}))
					m.Observe(delegation("u", "client", delegated))
				}
				seen := map[string]bool{}
				for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
					if line == "" {
						continue
					}
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatal(err)
					}
					for _, key := range []string{"text", "transcript"} {
						if value, ok := record[key].(string); ok {
							if mode != "default" || record["level"] != "DEBUG" {
								t.Fatalf("content field %s emitted in %s at %v", key, mode, record["level"])
							}
							seen[value] = true
						}
					}
				}
				if mode == "default" && (!seen[turn] || !seen[delegated]) {
					t.Fatal("full turn/delegation text missing or truncated")
				}
				if mode == "default" && path == "wm" && !seen[turn+delegated] {
					t.Fatal("full accumulated WM transcript missing from DEBUG record")
				}
				if mode != "default" && (strings.Contains(out.String(), turn[:30]) || strings.Contains(out.String(), delegated[:30])) {
					t.Fatal("content leaked through filtering or disabled switch")
				}
			})
		}
	}
}

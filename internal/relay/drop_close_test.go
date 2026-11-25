package relay

import (
	"log/slog"
	"testing"
	"time"
)

func TestCloseDropLevelAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, reason, closingLevel string
		peerFailed                 bool
	}{
		{"sideband_loss", "media_lost", "WARN", false},
		{"peer_failure_already_logged", "media_lost", "INFO", true},
		{"user_hangup", "user_hangup", "INFO", false},
		{"call_end", "upstream_voice_end", "INFO", false},
		{"shutdown", "gateway_shutdown", "INFO", false},
		{"client_channel_loss", "client_data_channel_closed", "WARN", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := make(peerLogSink, 2)
			logger := slog.New(slog.NewJSONHandler(sink, nil))
			s := &pionSession{done: make(chan struct{}), logger: logger, bridge: &dataChannelBridge{}}
			s.diag = newPeerDiagnostics(s.done, time.Now)
			s.peerAFailedLogged.Store(tc.peerFailed)
			if err := s.CloseWithDiagnostics(tc.reason, []slog.Attr{slog.String("drop_cause_class", "read_error_eof"), slog.Uint64("sideband_frames_read", 5)}); err != nil {
				t.Fatal(err)
			}
			closing, closed := decodeDiag(t, receiveDiag(t, sink)), decodeDiag(t, receiveDiag(t, sink))
			assertDiag(t, closing, "msg", "relay session closing")
			assertDiag(t, closing, "level", tc.closingLevel)
			assertDiag(t, closed, "msg", "relay session closed")
			assertDiag(t, closed, "level", "INFO")
			for _, line := range []map[string]any{closing, closed} {
				assertDiag(t, line, "reason", tc.reason)
				switch tc.reason {
				case "media_lost":
					assertDiag(t, line, "drop_cause_class", "read_error_eof")
					assertDiag(t, line, "sideband_frames_read", float64(5))
					if _, ok := line["session_age_ms"].(float64); !ok {
						t.Fatal("missing session age")
					}
				case "client_data_channel_closed":
					assertDiag(t, line, "drop_cause_class", "client_data_channel_closed")
				default:
					assertDiag(t, line, "drop_cause_class", nil)
				}
			}
			if err := s.CloseWithReason("media_lost"); err != nil || len(sink) != 0 {
				t.Fatal("repeated close logged again")
			}
		})
	}
}

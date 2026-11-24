package voice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestAVASTurnDebugLogsMetadataOnly(t *testing.T) {
	var out bytes.Buffer
	svc := &Service{logger: slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	call := &callState{}
	svc.observeAVASCallEvent("", nil, call, []byte(`{"type":"turn.done","turn":{"id":"SECRET-CONTENT-MARKER-1234","role":"SECRET-CONTENT-MARKER-1234","transcript":"SECRET-CONTENT-MARKER-1234"}}`))
	svc.observeAVASCallEvent("", nil, call, []byte(`{"type":"delegation.created","item":{"id":"id","target":"client","content":[{"type":"input_text","text":"SECRET-CONTENT-MARKER-1234"}]}}`))
	if strings.Contains(out.String(), "SECRET-CONTENT-MARKER-1234") || strings.Count(out.String(), "zoe_debug_message") != 2 {
		t.Fatal(out.String())
	}
}

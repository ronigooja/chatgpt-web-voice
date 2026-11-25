package voice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bogdanfinn/websocket"
)

func TestSidebandReadCauseClasses(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ctxErr, err error
		want        sidebandCause
	}{
		{"eof", nil, fmt.Errorf("wrap: %w", io.EOF), sidebandCause{class: "read_error_eof"}},
		{"ws_close", nil, &websocket.CloseError{Code: 1011, Text: "private credential"}, sidebandCause{class: "read_error_ws_close", wsCode: 1011}},
		{"timeout", nil, context.DeadlineExceeded, sidebandCause{class: "read_error_timeout"}},
		{"other", nil, errors.New("private endpoint"), sidebandCause{class: "read_error_other"}},
		{"canceled", context.Canceled, io.EOF, sidebandCause{class: "context_canceled"}},
		{"unknown", nil, nil, sidebandCause{class: "unknown"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySidebandReadError(tc.ctxErr, tc.err); got != tc.want {
				t.Errorf("cause = %+v, want %+v", got, tc.want)
			}
		})
	}
}

type diagnosticSocket struct {
	err    error
	closes int
}

func (*diagnosticSocket) SetWriteDeadline(time.Time) error { return nil }
func (s *diagnosticSocket) WriteMessage(int, []byte) error { return s.err }
func (s *diagnosticSocket) Close() error                   { s.closes++; return nil }

func TestSidebandWriterFailureDoesNotInitiateClose(t *testing.T) {
	conn := &diagnosticSocket{}
	w := &avasSidebandWriter{conn: conn}
	if err := w.send([]byte("private frame")); err != nil {
		t.Fatal(err)
	}
	conn.err = errors.New("private write failure")
	if err := w.send([]byte("private frame")); !errors.Is(err, conn.err) || w.closed || conn.closes != 0 {
		t.Fatalf("write failure changed close semantics: err=%v closed=%v closes=%d", err, w.closed, conn.closes)
	}
	var events []sidebandCloseDiagnostics
	w.setOnClose(func(d sidebandCloseDiagnostics) { events = append(events, d) })
	w.closeWithCause(classifySidebandReadError(nil, &websocket.CloseError{Code: 1006, Text: "private endpoint"}))
	w.close()
	if len(events) != 1 || conn.closes != 1 {
		t.Fatalf("expected one close, got %d events and %d closes", len(events), conn.closes)
	}
	if got := events[0]; got.cause.class != "writer_error" || got.terminalRead.class != "read_error_ws_close" || got.terminalRead.wsCode != 1006 || got.writeCount != 1 {
		t.Errorf("unexpected close diagnostics: %+v", got)
	}
}

func TestSidebandUpstreamErrorPrivacyAndCloseEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, payload, code, length, hidden string
	}{
		{"coded", `{"type":"error","error":{"code":"rate_limit_exceeded","message":"secret text"}}`, "rate_limit_exceeded", "11-100", "secret text"},
		{"free_text", `{"type":"error","error":"secret text"}`, "absent", "11-100", "secret text"},
		{"unexpected_code", `{"type":"error","code":"credential-endpoint.example"}`, "unrecognized", "empty", "credential-endpoint.example"},
		{"numeric", `{"type":"error","code":429}`, "4xx", "empty", "429"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			}}))
			now := time.Unix(1700000000, 0)
			d := &sidebandDiagnostics{}
			if d.readFrame(now, []byte(`{"type":"usage"}`)) != nil {
				t.Fatal("regular frame was treated as an error")
			}
			upstream := d.readFrame(now.Add(time.Millisecond), []byte(tc.payload))
			if upstream == nil || upstream.codeClass != tc.code || upstream.messageLengthClass != tc.length {
				t.Fatalf("wrong event classification: %+v", upstream)
			}
			logSidebandUpstreamError(logger.With("media_session_id", "test-session"), *upstream)
			for _, expected := range []string{`"level":"WARN"`, `"upstream_event_type":"error"`, `"upstream_error_code_class":"` + tc.code + `"`} {
				if !strings.Contains(logs.String(), expected) {
					t.Errorf("event log missing %s", expected)
				}
			}
			if strings.Contains(logs.String(), tc.hidden) {
				t.Fatal("event log leaked payload text")
			}
			diag := d.finish(classifySidebandReadError(nil, io.EOF), now.Add(42*time.Millisecond))
			if diag.cause.class != "upstream_error_event" || diag.terminalRead.class != "read_error_eof" || diag.readCount != 2 || diag.lastReadAge != int64(41) {
				t.Errorf("wrong close diagnostics: %+v", diag)
			}
			d.readFrame(now.Add(43*time.Millisecond), []byte(`{"type":"usage"}`))
			if got := d.finish(classifySidebandReadError(nil, io.EOF), now.Add(44*time.Millisecond)); got.cause.class != "read_error_eof" {
				t.Errorf("earlier event misattributed the drop: %+v", got)
			}
			logs.Reset()
			local := d.finish(classifySidebandReadError(context.Canceled, io.EOF), now.Add(50*time.Millisecond))
			logSidebandClose(logger, "test-session", local)
			if !strings.Contains(logs.String(), `"level":"DEBUG"`) || !strings.Contains(logs.String(), `"drop_cause_class":"context_canceled"`) {
				t.Error("local cancellation should be logged at DEBUG with its cause")
			}
		})
	}
}

type diagnosticReader struct {
	diagnosticSocket
	started chan struct{}
	stopped chan struct{}
}

func (r *diagnosticReader) ReadMessage() (int, []byte, error) {
	close(r.started)
	<-r.stopped
	return 0, nil, io.EOF
}

func (r *diagnosticReader) Close() error {
	close(r.stopped)
	return r.diagnosticSocket.Close()
}

type diagnosticInjector func([]byte) error

func (f diagnosticInjector) InjectSidebandEvent(data []byte) error { return f(data) }

func TestSidebandLocalCancellationStopsReaderAndWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &diagnosticReader{started: make(chan struct{}), stopped: make(chan struct{})}
	ready := make(chan struct{})
	close(ready)
	w := runAVASSideband(ctx, conn, diagnosticInjector(func([]byte) error {
		t.Error("read error unexpectedly forwarded a frame")
		return nil
	}), ready, slog.New(slog.DiscardHandler))
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
	cancel()
	select {
	case <-w.done:
	case <-time.After(time.Second):
		t.Fatal("reader and watcher did not stop")
	}
	if got := w.diag.closed(); got.cause.class != "context_canceled" || conn.closes != 1 {
		t.Errorf("close was not recorded once as canceled: %+v (socket closes %d)", got, conn.closes)
	}
}

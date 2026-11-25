package voice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bogdanfinn/websocket"
)

type sidebandCause struct {
	class  string
	wsCode int
}

func (c sidebandCause) isLocal() bool {
	return c.class == "local_shutdown" || c.class == "context_canceled"
}

func classifySidebandReadError(contextErr, err error) sidebandCause {
	var wsClose *websocket.CloseError
	var networkErr net.Error
	switch {
	case contextErr != nil, errors.Is(err, context.Canceled):
		return sidebandCause{class: "context_canceled"}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return sidebandCause{class: "read_error_eof"}
	case errors.As(err, &wsClose):
		return sidebandCause{class: "read_error_ws_close", wsCode: wsClose.Code}
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &networkErr) && networkErr.Timeout():
		return sidebandCause{class: "read_error_timeout"}
	case err != nil:
		return sidebandCause{class: "read_error_other"}
	default:
		return sidebandCause{class: "unknown"}
	}
}

type sidebandUpstreamError struct {
	codeClass, messageLengthClass string
}

func (e sidebandUpstreamError) attrs() []slog.Attr {
	return []slog.Attr{slog.String("upstream_event_type", "error"), slog.String("upstream_error_code_class", e.codeClass),
		slog.String("upstream_error_message_length_class", e.messageLengthClass)}
}

func parseSidebandUpstreamError(data []byte) *sidebandUpstreamError {
	var event struct {
		Type    string          `json:"type"`
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(data, &event) != nil || event.Type != "error" {
		return nil
	}
	var nested struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(event.Error, &nested) == nil {
		if len(nested.Code) != 0 {
			event.Code = nested.Code
		}
		if nested.Message != "" {
			event.Message = nested.Message
		}
	} else {
		var text string
		if json.Unmarshal(event.Error, &text) == nil && text != "" {
			event.Message = text
		}
	}
	return &sidebandUpstreamError{codeClass: upstreamErrorCodeClass(event.Code), messageLengthClass: errorLengthClass(event.Message)}
}

func upstreamErrorCodeClass(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "absent"
	}
	var code string
	if json.Unmarshal(raw, &code) != nil {
		code = string(raw)
	}
	switch code {
	case "":
		return "absent"
	case "server_error", "internal_error", "invalid_request_error", "rate_limit_exceeded", "session_expired", "authentication_error", "permission_denied", "invalid_api_key":
		return code
	}
	if n, err := strconv.Atoi(code); err == nil && n >= 400 && n <= 599 {
		return strconv.Itoa(n/100) + "xx"
	}
	return "unrecognized"
}

func errorLengthClass(s string) string {
	switch n := utf8.RuneCountInString(s); {
	case n == 0:
		return "empty"
	case n <= 10:
		return "1-10"
	case n <= 100:
		return "11-100"
	case n <= 1000:
		return "101-1000"
	default:
		return "over_1000"
	}
}

type sidebandCloseDiagnostics struct {
	cause, terminalRead          sidebandCause
	lastReadAge                  any
	readCount, writeCount        uint64
	upstream                     *sidebandUpstreamError
}

func (d sidebandCloseDiagnostics) attrs() []slog.Attr {
	class := d.cause.class
	if class == "" {
		class = "unknown"
	}
	age := d.lastReadAge
	if age == nil {
		age = "unavailable"
	}
	attrs := []slog.Attr{slog.String("drop_cause_class", class), slog.Any("sideband_last_frame_read_age_ms", age),
		slog.Uint64("sideband_frames_read", d.readCount), slog.Uint64("sideband_frames_written", d.writeCount)}
	if d.terminalRead.class != "" && d.terminalRead.class != class {
		attrs = append(attrs, slog.String("sideband_terminal_read_class", d.terminalRead.class))
	}
	if code := d.terminalRead.wsCode; code != 0 {
		attrs = append(attrs, slog.Int("sideband_ws_close_code", code))
	}
	if d.upstream != nil {
		attrs = append(attrs, d.upstream.attrs()...)
	}
	return attrs
}

type sidebandDiagnostics struct {
	mu                    sync.Mutex
	lastRead              time.Time
	readCount, writeCount uint64
	writeFailed           bool
	upstream              *sidebandUpstreamError
	result                sidebandCloseDiagnostics
}

func (d *sidebandDiagnostics) readFrame(now time.Time, data []byte) *sidebandUpstreamError {
	upstream := parseSidebandUpstreamError(data)
	d.mu.Lock()
	d.lastRead = now
	d.readCount++
	// A later successful read should not turn an unrelated drop into an upstream error.
	d.upstream = upstream
	d.mu.Unlock()
	return upstream
}

func (d *sidebandDiagnostics) wroteFrame(err error) {
	d.mu.Lock()
	d.writeFailed = err != nil
	if err == nil {
		d.writeCount++
	}
	d.mu.Unlock()
}

func (d *sidebandDiagnostics) finish(terminal sidebandCause, now time.Time) sidebandCloseDiagnostics {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.result = sidebandCloseDiagnostics{cause: terminal, readCount: d.readCount, writeCount: d.writeCount}
	if !d.lastRead.IsZero() {
		d.result.lastReadAge = max(0, now.Sub(d.lastRead).Milliseconds())
	}
	if !terminal.isLocal() {
		d.result.terminalRead = terminal
		d.result.upstream = d.upstream
		if d.upstream != nil {
			d.result.cause = sidebandCause{class: "upstream_error_event"}
		} else if d.writeFailed {
			d.result.cause = sidebandCause{class: "writer_error"}
		}
	}
	return d.result
}

func (d *sidebandDiagnostics) closed() sidebandCloseDiagnostics {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.result
}

func logSidebandClose(logger *slog.Logger, mediaID string, diag sidebandCloseDiagnostics) {
	logger.LogAttrs(context.Background(), slog.LevelDebug, "relay sideband closed", append([]slog.Attr{slog.String("media_session_id", mediaID)}, diag.attrs()...)...)
}

func logSidebandUpstreamError(logger *slog.Logger, mediaID string, upstream sidebandUpstreamError) {
	attrs := []slog.Attr{slog.String("media_session_id", mediaID), slog.String("drop_cause_class", "upstream_error_event")}
	logger.LogAttrs(context.Background(), slog.LevelWarn, "relay sideband upstream error event", append(attrs, upstream.attrs()...)...)
}

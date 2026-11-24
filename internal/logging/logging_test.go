package logging

import (
	"bytes"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type countingWriter struct {
	*httptest.ResponseRecorder
	headers  int
	flushErr error
}

func (w *countingWriter) WriteHeader(status int) {
	w.headers++
	w.ResponseRecorder.WriteHeader(status)
}

func (w *countingWriter) FlushError() error { return w.flushErr }

func TestResponseRecorderFlushBeforeWrite(t *testing.T) {
	underlying := &countingWriter{ResponseRecorder: httptest.NewRecorder()}
	w := &responseRecorder{ResponseWriter: underlying}
	if err := http.NewResponseController(w).Flush(); err != nil {
		t.Fatal(err)
	}
	if w.status != http.StatusOK || underlying.headers != 1 {
		t.Fatalf("status = %d, WriteHeader calls = %d", w.status, underlying.headers)
	}
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if underlying.headers != 1 {
		t.Fatalf("WriteHeader calls after write = %d", underlying.headers)
	}
	want := errors.New("flush failed")
	underlying.flushErr = want
	if err := http.NewResponseController(w).Flush(); !errors.Is(err, want) {
		t.Fatalf("flush error = %v, want %v", err, want)
	}
}

func TestHTTPMiddlewareSSEFlushBeforeWrite(t *testing.T) {
	var serverLog bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := HTTPMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
			return
		}
		if _, err := io.WriteString(w, "data: hello\n\n"); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(&serverLog, "", 0)
	server.Start()
	resp, err := server.Client().Get(server.URL)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	server.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "data: hello\n\n" {
		t.Fatalf("status = %d, body = %q, error = %v", resp.StatusCode, body, err)
	}
	if strings.Contains(serverLog.String(), "superfluous") {
		t.Fatalf("server error log: %s", serverLog.String())
	}
}

func TestHTTPMiddlewareAddsRequestIDAndAccessLog(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := HTTPMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	req.Header.Set("X-Request-ID", "request-123")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.Code)
	}
	if resp.Header().Get("X-Request-ID") != "request-123" {
		t.Fatalf("unexpected request ID: %q", resp.Header().Get("X-Request-ID"))
	}
	logLine := output.String()
	for _, expected := range []string{`"msg":"http_request"`, `"status":201`, `"response_bytes":2`, `"request_id":"request-123"`} {
		if !strings.Contains(logLine, expected) {
			t.Fatalf("missing %s in access log: %s", expected, logLine)
		}
	}
}

func TestHTTPMiddlewareRecoversPanic(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := HTTPMiddleware(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.Code)
	}
	if !strings.Contains(output.String(), `"msg":"http_panic"`) {
		t.Fatalf("panic was not logged: %s", output.String())
	}
}

func TestPanicAndRequestMetadataDoNotLogContent(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	req := httptest.NewRequest("GET", "/safe?message=SECRET-CONTENT-MARKER-1234", nil)
	req.Header.Set("User-Agent", strings.Repeat("a", 120)+"SECRET-CONTENT-MARKER-1234")
	req.Header.Set("X-Request-ID", strings.Repeat("b", 120)+"SECRET-CONTENT-MARKER-1234")
	HTTPMiddleware(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("SECRET-CONTENT-MARKER-1234") })).ServeHTTP(httptest.NewRecorder(), req)
	if strings.Contains(out.String(), "SECRET-CONTENT-MARKER-1234") || !strings.Contains(out.String(), "panic_type") {
		t.Fatal(out.String())
	}
}

package logging

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPMiddlewarePreservesClientRequestIDThroughRealMux(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	got := ""
	mux.HandleFunc("POST /v1/voice/sessions", func(w http.ResponseWriter, r *http.Request) {
		got = RequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/voice/sessions", nil)
	req.Header.Set("X-Request-ID", "zoe-request-123")
	resp := httptest.NewRecorder()
	HTTPMiddleware(logger, mux).ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status=%d", resp.Code)
	}
	if got != "zoe-request-123" {
		t.Fatalf("request id=%q", got)
	}
}

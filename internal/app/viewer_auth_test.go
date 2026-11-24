package app

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/api"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/apikeys"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/auth"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
)

type denyViewerKey struct{}

func (denyViewerKey) Authenticate(string) (apikeys.Key, bool, error) {
	return apikeys.Key{}, false, nil
}

func TestViewerAuthenticationIsolation(t *testing.T) {
	events := eventlog.New(eventlog.Defaults())
	key := eventlog.Key{SessionID: "s", Generation: 1}
	events.Start(key, "")
	events.End(key)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := auth.New("admin", "password", time.Hour, logger)
	server := api.New(api.Dependencies{Events: events})
	staticDir := t.TempDir()
	for _, name := range []string{"sessions", "viewer"} {
		if err := os.WriteFile(filepath.Join(staticDir, name+".html"), []byte(name+" page"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	build := func(secret string) http.Handler {
		return newHandler(config.Config{VoiceViewerReadKey: secret, StaticDir: staticDir}, manager, auth.NewAPIKeyManager(denyViewerKey{}, logger), server, logger)
	}
	h := build("read-secret")
	login := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewBufferString(`{"username":"admin","password":"password"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(login, req)
	var cookie *http.Cookie
	for _, c := range login.Result().Cookies() {
		if c.Name == "voice_gateway_session" {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatal("login failed")
	}
	paths := []string{"/api/call-sessions/s/generations", "/api/call-sessions/s/generations/1/events", "/api/call-sessions/s/generations/1/events/stream", "/api/call-sessions/s/generations/1/frames"}
	for _, path := range paths {
		for _, tc := range []struct {
			bearer string
			cookie bool
			want   int
		}{{"", false, 401}, {"wrong", false, 401}, {"read-secret", false, 200}, {"", true, 200}} {
			r := httptest.NewRequest("GET", path, nil)
			if tc.bearer != "" {
				r.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			if tc.cookie {
				r.AddCookie(cookie)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("%s %+v: %d", path, tc, w.Code)
			}
		}
	}
	for _, path := range []string{"/api/call-sessions", "/api/call-sessions/s", "/api/conversations", "/api/voice/session", "/v1/voice/config", "/voice", "/sessions", "/viewer"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer read-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 && w.Code != 303 {
			t.Fatalf("key escaped scope %s: %d", path, w.Code)
		}
	}
	for _, path := range []string{"/sessions", "/viewer"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Accept", "text/html")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusSeeOther {
			t.Fatalf("unauthenticated %s: %d", path, w.Code)
		}
		r = httptest.NewRequest("GET", path, nil)
		r.AddCookie(cookie)
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK || w.Body.String() != path[1:]+" page" {
			t.Fatalf("authenticated %s: %d %q", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/client/v1/voice/calls", "/client/v1/voice/calls/s/hangup"} {
		r := httptest.NewRequest("POST", path, nil)
		r.Header.Set("Authorization", "Bearer read-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code == 200 || w.Code == 201 || w.Code == 204 {
			t.Fatalf("key escaped scope %s: %d", path, w.Code)
		}
	}
	r := httptest.NewRequest("GET", paths[0], nil)
	r.Header.Set("Authorization", "Bearer read-secret")
	w := httptest.NewRecorder()
	build("").ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}

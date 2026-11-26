package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/auth"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
)

func TestAdminWebVoiceInOwnedModeUsesWM(t *testing.T) {
	pool, err := accounts.NewPool(filepath.Join(t.TempDir(), "web.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	box, err := secretbox.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	pool.WithBox(box)
	if _, err := pool.Create(accounts.Account{AccessToken: "fixture-token"}); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "upstream-fixture")
	if err := os.WriteFile(bin, []byte(`#!/bin/sh
for url; do :; done
[ "$url" = 'https://chatgpt.com/realtime/wm?dcid=0' ] || exit 1
body=$(cat)
case "$body" in *offer*) ;; *) exit 1 ;; esac
printf 'HTTP/1.1 200 OK\r\nContent-Type: application/sdp\r\n\r\nv=0\r\nanswer\r\n'
`), 0700); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := voice.New(config.Config{ZoeGatewayAPIKeyID: 42, VoiceUpstreamRoute: "avas", RelayEnabled: true, MaxAccountAttempts: 1,
		UpstreamTransport: config.TransportCurlImpersonate, CurlImpersonateBin: bin}, pool, logger)
	mux := http.NewServeMux()
	New(Dependencies{Voice: svc}).Register(mux)
	manager := auth.New("admin", "test-password", time.Hour, logger)
	handler := manager.Require(mux)
	const path = "/api/voice/session"
	const body = `{"offer_sdp":"v=0\r\noffer\r\n"}`
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create: %d", w.Code)
	}
	login := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"username":"admin","password":"test-password"}`))
	login.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	manager.HandleLogin(w, login)
	cookies := w.Result().Cookies()
	create := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		for _, c := range cookies {
			req.AddCookie(c)
			if c.Name == "voice_gateway_csrf" {
				req.Header.Set("X-CSRF-Token", c.Value)
			}
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}
	if w := create(`{"offer_sdp":"v=0\r\noffer\r\n","route":"avas"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("wire route override: %d", w.Code)
	}
	w = create(body)
	var result voice.SessionResult
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.AnswerSDP != "v=0\r\nanswer\r\n" {
		t.Fatalf("admin direct WM create: %d", w.Code)
	}
}

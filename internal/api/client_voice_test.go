package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/logging"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
)

type clientVoiceFake struct {
	VoiceService
	dials, hangups int
	panicKey       bool
}

func (f *clientVoiceFake) DialClient(_ context.Context, key string, p voice.ClientDialRequest) (*voice.ClientDialResult, error) {
	f.dials++
	if f.panicKey {
		panic(key)
	}
	return &voice.ClientDialResult{AnswerSDP: p.OfferSDP, GatewaySessionID: "handle", VoiceSessionID: "zoe", Generation: 1}, nil
}

func (f *clientVoiceFake) HangupClient(_ context.Context, key, handle string) error {
	f.hangups++
	return nil
}

func TestClientVoiceInstanceBearerAndAccessLogPrivacy(t *testing.T) {
	const key = "test-bearer-not-for-logs"
	var log bytes.Buffer
	f := &clientVoiceFake{}
	s := &Server{voice: f}
	mux := http.NewServeMux()
	s.RegisterClientVoice(mux)
	handler := logging.HTTPMiddleware(slog.New(slog.NewJSONHandler(&log, nil)), mux)
	do := func(path, auth, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("Authorization", auth)
		r.Header.Set("X-Zoe-Instance-Key", key)
		r.Header.Set("X-Request-ID", key)
		r.Header.Set("User-Agent", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if strings.Contains(w.Body.String(), key) || w.Header().Get("X-Request-ID") == key {
			t.Fatal("credential echoed")
		}
		return w
	}
	const dial = "/client/v1/voice/calls"
	const body = `{"request_id":"one","offer_sdp":"sdp"}`
	for _, malformed := range []string{"", "Basic " + key, "Bearer ", "Bearer embedded space", "Bearer =", "Bearer a=b"} {
		w := do(dial, malformed, body)
		if w.Code != 401 || !strings.Contains(w.Body.String(), `"code":"unauthenticated"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if f.dials != 0 {
		t.Fatal("malformed auth reached dial")
	}
	if w := do(dial, "Bearer "+key, body+` {}`); w.Code != 400 || f.dials != 0 {
		t.Fatal(w.Code)
	}
	w := do(dial, "Bearer "+key, body)
	var answer map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &answer) != nil || len(answer) != 4 || answer["gateway_session_id"] != "handle" {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := do(dial+"/"+key+"/hangup", "Bearer "+key, ""); w.Code != 204 || w.Body.Len() != 0 || f.hangups != 1 {
		t.Fatal(w.Code)
	}
	f.panicKey = true
	if w := do(dial, "Bearer "+key, body); w.Code != 500 {
		t.Fatal(w.Code)
	}
	if strings.Contains(log.String(), key) || !strings.Contains(log.String(), "http_panic") {
		t.Fatal("credential leaked via access or panic logging")
	}
}

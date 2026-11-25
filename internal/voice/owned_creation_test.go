package voice

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

type ownedCreationRoundTrip func(*http.Request) (*http.Response, error)

func (f ownedCreationRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func ownedCreationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func newOfflineOwnedCreation(t *testing.T, count int) (*Service, *store.DB, *fakeRelay, *safeForwardLog) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "voice.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	box, err := secretbox.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	pool := accounts.NewPoolFromDB(db).WithBox(box)
	for i := 0; i < count; i++ {
		if _, err := pool.Create(accounts.Account{AccessToken: testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "jti": i}), Status: "正常"}); err != nil {
			t.Fatal(err)
		}
	}
	logs := &safeForwardLog{}
	r := &fakeRelay{}
	svc := New(config.Config{VoiceUpstreamRoute: "avas", MaxAccountAttempts: 3}, pool,
		slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))).
		WithCreationOutcomes(db).WithMediaEvidence(mediaevidence.NewStore(db)).WithRelay(r)
	svc.cfg.ZoeGatewayAPIKeyID = 42
	svc.cfg.ZoeURL = "https://zoe.invalid"
	svc.zoeToken = "synthetic-runner"
	opening := OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{}}
	instructions := "synthetic-instructions"
	start, err := json.Marshal(callStart{VoiceSessionID: "zoe-call", Generation: 1, RequestID: "gateway-request", Instructions: &instructions,
		Opening: opening, OpeningHash: openingContextContentHash(opening)})
	if err != nil {
		t.Fatal(err)
	}
	starts := 0
	svc.zoeHTTP.Transport = ownedCreationRoundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/runner/v1/voice/calls" {
			t.Errorf("unexpected lifecycle call")
			return ownedCreationResponse(400, ""), nil
		}
		starts++
		status := http.StatusOK
		if starts == 1 {
			status = http.StatusCreated
		}
		return ownedCreationResponse(status, string(start)), nil
	})
	return svc, db, r, logs
}

func TestOwnedCleanRejectionThenAccountExhaustion(t *testing.T) {
	svc, db, r, _ := newOfflineOwnedCreation(t, 1)
	bin := filepath.Join(t.TempDir(), "curl-rejection")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf 'HTTP/1.1 429 Too Many Requests\\r\\n\\r\\nrejected'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	svc.httpOptions.Transport = config.TransportCurlImpersonate
	svc.httpOptions.CurlImpersonateBin = bin
	p := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	for i := 0; i < 2; i++ {
		if _, err := svc.DialClient(context.Background(), ownedTestKey, p); err != unavailableCall {
			t.Fatalf("dial %d: expected unavailable", i)
		}
		row, err := db.GetVoiceCreationOutcome("api_key:42", "gateway-request")
		if err != nil || row.Outcome != "never_established" {
			t.Fatalf("dial %d: stored outcome %q, error %v", i, row.Outcome, err)
		}
		if len(r.sessions) != 1 {
			t.Fatalf("dial %d: expected one relay, got %d", i, len(r.sessions))
		}
	}
}

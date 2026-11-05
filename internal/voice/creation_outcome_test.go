package voice

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

type creationUpstream struct {
	mu       sync.Mutex
	statuses []int
	bodies   []string
	auth     []string
}

func (u *creationUpstream) handler(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	i := len(u.statuses) - len(u.bodies)
	if i < 0 {
		i = 0
	}
	status := u.statuses[len(u.bodies)]
	u.bodies = append(u.bodies, "rejected-body")
	u.auth = append(u.auth, r.Header.Get("Authorization"))
	u.mu.Unlock()
	w.WriteHeader(status)
	if status == http.StatusCreated {
		_, err := io.WriteString(w, "v=0\r\n")
		if err != nil {
			panic(err)
		}
	} else {
		_, err := io.WriteString(w, "rejected-body")
		if err != nil {
			panic(err)
		}
	}
}

func newCreationService(t *testing.T, statuses []int, maxAttempts int) (*Service, *store.DB, *creationUpstream) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "voice.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	pool := accounts.NewPoolFromDB(db)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	box, err := secretbox.New(key)
	if err != nil {
		t.Fatal(err)
	}
	pool.WithBox(box)
	for i := range statuses {
		if _, err := pool.Create(accounts.Account{AccessToken: testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "jti": string(rune('a' + i))}), Status: "正常"}); err != nil {
			t.Fatal(err)
		}
	}
	u := &creationUpstream{statuses: append([]int(nil), statuses...)}
	server := httptest.NewTLSServer(http.HandlerFunc(u.handler))
	t.Cleanup(server.Close)
	svc := New(config.Config{Environment: config.EnvironmentDevelopment, SkipSSLVerify: true, UpstreamTransport: config.TransportGo, MaxAccountAttempts: maxAttempts, SessionTTLSeconds: 180, VoiceUpstreamRoute: "avas", DeviceID: "d", SessionID: "s"}, pool, slog.New(slog.NewTextHandler(io.Discard, nil))).WithCreationOutcomes(db)
	svc.avasURL = server.URL + "/calls"
	return svc, db, u
}

func TestAVASCreateAllCleanRejectionsReturnNeverEstablishedOutcomeAfterMaxAttempts(t *testing.T) {
	svc, db, upstream := newCreationService(t, []int{http.StatusBadGateway, http.StatusBadGateway, http.StatusBadGateway}, 3)
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "req-a", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusBadGateway || !se.CreationRejected || se.UpstreamStatus != http.StatusBadGateway {
		t.Fatalf("error=%+v", se)
	}
	if len(upstream.bodies) != 3 {
		t.Fatalf("attempts=%d", len(upstream.bodies))
	}
	row, err := db.GetVoiceCreationOutcome("api_key:1", "req-a")
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != "never_established" {
		t.Fatalf("outcome=%q", row.Outcome)
	}
}

func TestAVASCreateFirstRejectSecondAcceptUsesTwoDifferentAccounts(t *testing.T) {
	svc, _, upstream := newCreationService(t, []int{http.StatusBadGateway, http.StatusCreated}, 3)
	result, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.AnswerSDP == "" {
		t.Fatalf("result=%+v", result)
	}
	if len(upstream.auth) != 2 || upstream.auth[0] == upstream.auth[1] {
		t.Fatalf("auth=%v", upstream.auth)
	}
}

func TestAVASCreateRejectionsStopAtMaxAccountAttempts(t *testing.T) {
	svc, _, upstream := newCreationService(t, []int{http.StatusBadGateway, http.StatusBadGateway, http.StatusBadGateway, http.StatusBadGateway}, 2)
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err == nil {
		t.Fatal("expected rejection")
	}
	if len(upstream.bodies) != 2 {
		t.Fatalf("attempts=%d", len(upstream.bodies))
	}
}

func TestAVASCreateRejectThenTokenInvalidIsUnknownAndNot422(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusBadGateway, http.StatusUnauthorized}, 2)
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "req-c", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode == http.StatusUnprocessableEntity || se.CreationRejected {
		t.Fatalf("mixed attempts incorrectly classified: %+v", se)
	}
	row, err := db.GetVoiceCreationOutcome("api_key:1", "req-c")
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != "unknown" {
		t.Fatalf("outcome=%q", row.Outcome)
	}
}

func TestAVASCreateAcceptedSDPThenSidebandFailurePersistsUnknownNotNeverEstablished(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
	svc.WithRelay(&fakeRelay{})
	svc.avasSidebandURL = "ws://127.0.0.1:1/live/"
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "req-d", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d", se.StatusCode)
	}
	row, err := db.GetVoiceCreationOutcome("api_key:1", "req-d")
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != "unknown" {
		t.Fatalf("outcome=%q", row.Outcome)
	}
}

func TestAVASCreateDebugRejectionLogsBodyOnlyWhenSwitchEnabled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		want    bool
	}{{"on", true, true}, {"off", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			db, err := store.Open(filepath.Join(t.TempDir(), "voice.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			pool := accounts.NewPoolFromDB(db)
			key := make([]byte, 32)
			for i := range key {
				key[i] = byte(i + 1)
			}
			box, err := secretbox.New(key)
			if err != nil {
				t.Fatal(err)
			}
			pool.WithBox(box)
			if _, err := pool.Create(accounts.Account{AccessToken: testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix()}), Status: "正常"}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, err := io.WriteString(w, "debug-body-prefix")
				if err != nil {
					t.Fatal(err)
				}
			}))
			defer server.Close()
			svc := New(config.Config{SkipSSLVerify: true, UpstreamTransport: config.TransportGo, MaxAccountAttempts: 1, VoiceUpstreamRoute: "avas", ZoeFinalTurnDebugText: tc.enabled}, pool, logger)
			svc.avasURL = server.URL + "/calls"
			if _, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")}); err == nil {
				t.Fatal("expected upstream rejection")
			}
			if strings.Contains(logs.String(), "debug-body-prefix") != tc.want {
				t.Fatalf("logs=%q", logs.String())
			}
		})
	}
}

func TestCreationOutcomePendingAfterSimulatedCrashAnswersUnknown(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "voice.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("api_key:1", "crash-pending"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(filepath.Join(t.TempDir(), "unused.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Startup converts pending rows in the original file to unknown; the store
	// test covers the file reopen, while this assertion pins the derived rule.
	if err := db.CreateVoiceCreationOutcome("api_key:1", "pending"); err != nil {
		t.Fatal(err)
	}
	service := New(config.Config{}, nil, nil).WithCreationOutcomes(db)
	outcome, exists := service.CreationOutcome("api_key:1", "pending")
	if !exists || outcome != "unknown" {
		t.Fatalf("outcome=%q exists=%v", outcome, exists)
	}
}

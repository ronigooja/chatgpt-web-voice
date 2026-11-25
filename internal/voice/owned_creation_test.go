package voice

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/websocket"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
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
	svc, db, r, logs := newOfflineOwnedCreation(t, 1)
	setOwnedCurl(t, svc, ownedRejection)
	p := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	for i := 0; i < 2; i++ {
		if i == 1 {
			logs = &safeForwardLog{}
			svc.logger = slog.New(slog.NewJSONHandler(logs, nil))
		}
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
		if i == 1 {
			log := ownedCreationWarning(t, logs, "synthetic-body")
			if log["creation_stage"] != "creation_claim" || log["error_class"] != "claim_conflict" ||
				log["stored_outcome"] != "never_established" || log["outcome_write_ok"] != false ||
				log["elapsed_ms"] != nil || log["upstream_attempt_count"] != float64(0) {
				t.Fatalf("bad replay diagnostics: %v", log)
			}
		}
	}
}

const ownedRejection = `printf 'HTTP/1.1 429 Too Many Requests\r\n\r\nsynthetic-body\n'`
const ownedAccepted = `printf 'HTTP/1.1 201 Created\r\nLocation: https://sensitive.invalid/call\r\n\r\nv=0\r\n'`

func setOwnedCurl(t *testing.T, svc *Service, script string) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "curl-fixture")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+script+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	svc.httpOptions.Transport = config.TransportCurlImpersonate
	svc.httpOptions.CurlImpersonateBin = bin
}

func fixtureSQL(t *testing.T, db *store.DB, query string) {
	t.Helper()
	if _, err := db.Conn().Exec(query); err != nil {
		t.Fatal(err)
	}
}

func assertOwnedClaim(t *testing.T, db *store.DB, want string) {
	t.Helper()
	row, err := db.GetVoiceCreationOutcome("api_key:42", "gateway-request")
	if want == "" {
		if !errors.Is(err, store.ErrVoiceCreationOutcomeNotFound) {
			t.Fatalf("wanted no claim, got %q (%v)", row.Outcome, err)
		}
	} else if err != nil || row.Outcome != want {
		t.Fatalf("stored outcome=%q error=%v, want %q", row.Outcome, err, want)
	}
}

func TestOwnedCreationFailureDiagnostics(t *testing.T) {
	const leak = "synthetic-credential synthetic-body https://sensitive.invalid 203.0.113.94:54321"
	setZoe := func(s *Service, status int, body string) {
		s.zoeHTTP.Transport = ownedCreationRoundTrip(func(*http.Request) (*http.Response, error) {
			return ownedCreationResponse(status, body), nil
		})
	}
	setZoeError := func(s *Service, err error) {
		s.zoeHTTP.Transport = ownedCreationRoundTrip(func(*http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: "Post", URL: leak, Err: err}
		})
	}
	const (
		none    = 0
		clean   = 1
		unclean = 2
	)
	for _, tc := range []struct {
		name, script  string
		accounts      int
		change        func(*testing.T, *Service, *store.DB, *fakeRelay, *ClientDialRequest)
		stage, class  string
		outcome       string
		posts, status int
		started       bool
		closure       int
		badWrite      bool
	}{
		{name: "unready", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { s.zoeToken = "" }, stage: "admission", class: "internal"},
		{name: "stopping", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			s.ownedStopping = true
		}, stage: "admission", class: "internal"},
		{name: "Zoe rejects", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { setZoe(s, 401, leak) }, stage: "zoe_start", class: "http_4xx"},
		{name: "Zoe unavailable", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { setZoe(s, 503, leak) }, stage: "zoe_start", class: "http_5xx"},
		{name: "Zoe quarantine", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			setZoe(s, 503, `{"error":{"code":"voice_pool_quarantined"}}`)
		}, stage: "zoe_start", class: "pool_quarantined"},
		{name: "Zoe unexpected status", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { setZoe(s, 302, leak) }, stage: "zoe_start", class: "unexpected_status"},
		{name: "Zoe malformed JSON", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { setZoe(s, 201, leak) }, stage: "zoe_start", class: "malformed_response"},
		{name: "Zoe missing identity", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) { setZoe(s, 201, `{}`) }, stage: "zoe_start", class: "malformed_response"},
		{name: "Zoe canceled", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			setZoeError(s, context.Canceled)
		}, stage: "zoe_start", class: "context_canceled"},
		{name: "Zoe TLS", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			setZoeError(s, &tls.CertificateVerificationError{Err: errors.New(leak)})
		}, stage: "zoe_start", class: "tls"},
		{name: "Zoe refused", change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			setZoeError(s, syscall.ECONNREFUSED)
		}, stage: "zoe_start", class: "transport_refused"},
		{name: "claim write", change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_claim BEFORE INSERT ON voice_creation_outcomes BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "creation_claim", class: "db_write_failed"},
		{name: "validation", change: func(_ *testing.T, _ *Service, _ *store.DB, _ *fakeRelay, p *ClientDialRequest) { p.OfferSDP = leak }, stage: "request_validation", class: "validation", outcome: "never_established"},
		{name: "no accounts", stage: "account_reservation", class: "pool_no_account", outcome: "never_established"},
		{name: "account database", accounts: 1, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_account BEFORE UPDATE ON accounts BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "account_reservation", class: "db_write_failed", outcome: "unknown"},
		{name: "account occupied", accounts: 1, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			if err := mediaevidence.NewStore(db).ReserveOwned("api_key:42", mediaevidence.Identity{VoiceSessionID: "occupied", MediaGeneration: 1, AccountID: 1, LeaseID: "fixture", LeaseGeneration: 1}, "other", 1); err != nil {
				t.Fatal(err)
			}
		}, stage: "account_reservation", class: "pool_quarantined", outcome: "never_established"},
		{name: "reservation evidence", accounts: 1, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_reserve BEFORE INSERT ON voice_media_sessions BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "account_reservation", class: "evidence_failed", outcome: "unknown"},
		{name: "capacity", accounts: 1, change: func(_ *testing.T, _ *Service, _ *store.DB, r *fakeRelay, _ *ClientDialRequest) {
			r.newErr = relay.ErrCapacity
		}, stage: "relay_construct", class: "capacity", outcome: "never_established"},
		{name: "relay construction", accounts: 1, change: func(_ *testing.T, _ *Service, _ *store.DB, r *fakeRelay, _ *ClientDialRequest) {
			r.newErr = errors.Join(relay.ErrCreateAfterStart, errors.New(leak))
		}, stage: "relay_construct", class: "internal", outcome: "unknown", started: true, closure: unclean},
		{name: "relay offer", accounts: 1, change: func(_ *testing.T, _ *Service, _ *store.DB, r *fakeRelay, _ *ClientDialRequest) {
			r.newErr = errors.Join(relay.ErrApplyOffer, relay.ErrCreateClean, errors.New(leak))
		}, stage: "relay_apply_offer", class: "validation", outcome: "never_established", started: true, closure: unclean},
		{name: "upstream timeout", accounts: 1, script: `printf '%s' '` + leak + `' >&2; exit 28`, stage: "upstream_post", class: "transport_timeout", outcome: "unknown", posts: 1, started: true, closure: unclean},
		{name: "upstream empty response", accounts: 1, script: `printf '%s' '` + leak + `' >&2; exit 52`, stage: "upstream_post", class: "transport_eof", outcome: "unknown", posts: 1, started: true, closure: unclean},
		{name: "upstream truncated rejection", accounts: 1, script: ownedRejection + `; exit 18`, stage: "upstream_response", class: "transport_eof", outcome: "unknown", posts: 1, status: 429, started: true, closure: unclean},
		{name: "upstream oversized rejection", accounts: 1, script: ownedRejection + `; head -c 2097153 /dev/zero`, stage: "upstream_response", class: "malformed_response", outcome: "unknown", posts: 1, status: 429, started: true, closure: unclean},
		{name: "upstream unexpected status", accounts: 1, script: `printf 'HTTP/1.1 200 OK\r\n\r\nv=0\r\n'`, stage: "upstream_response", class: "unexpected_status", outcome: "unknown", posts: 1, status: 200, started: true, closure: unclean},
		{name: "upstream malformed SDP", accounts: 1, script: `printf 'HTTP/1.1 201 Created\r\n\r\n%s\n' '` + leak + `'`, stage: "upstream_response", class: "malformed_response", outcome: "unknown", posts: 1, status: 201, started: true, closure: unclean},
		{name: "explicit 5xx at limit", accounts: 1, script: `printf 'HTTP/1.1 503 Service Unavailable\r\n\r\n%s\n' '` + leak + `'`, change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			s.cfg.MaxAccountAttempts = 1
		}, stage: "upstream_response", class: "http_5xx", outcome: "never_established", posts: 1, status: 503, started: true, closure: clean},
		{name: "explicit 429 then exhaustion", accounts: 1, script: ownedRejection, stage: "account_reservation", class: "pool_no_account", outcome: "never_established", posts: 1, status: 429, started: true, closure: clean},
		{name: "explicit 429 without durable evidence", accounts: 1, script: ownedRejection, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_evidence BEFORE INSERT ON voice_termination_evidence BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "account_reservation", class: "pool_no_account", outcome: "unknown", posts: 1, status: 429, started: true, closure: unclean},
		{name: "explicit 429 with health write failure", accounts: 1, script: ownedRejection, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_health BEFORE UPDATE OF cooldown_until ON accounts BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "account_reservation", class: "pool_no_account", outcome: "unknown", posts: 1, status: 429, started: true, closure: clean},
		{name: "explicit 429 with incomplete teardown", accounts: 1, script: ownedRejection, change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			s.WithRelay(&creationFailureRelay{termination: &relay.Termination{PeerA: relay.ComponentResult{Closed: true}}})
		}, stage: "account_reservation", class: "pool_no_account", outcome: "unknown", posts: 1, status: 429, started: true, closure: unclean},
		{name: "non-HTML 401 then exhaustion", accounts: 1, script: `printf 'HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n\r\n{}\n'`, stage: "account_reservation", class: "pool_no_account", outcome: "unknown", posts: 1, status: 401, started: true, closure: clean},
		{name: "explicit rejection then ambiguous second attempt", accounts: 2, stage: "upstream_post", class: "transport_timeout", outcome: "unknown", posts: 2, status: 429, started: true, closure: unclean},
		{name: "outcome write rejected", accounts: 1, script: ownedRejection, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_outcome BEFORE UPDATE ON voice_creation_outcomes WHEN NEW.outcome = 'never_established' BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "outcome_write", class: "db_write_failed", outcome: "unknown", posts: 1, status: 429, started: true, closure: clean, badWrite: true},
		{name: "all outcome updates rejected", accounts: 1, script: ownedRejection, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_outcome BEFORE UPDATE ON voice_creation_outcomes BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "outcome_write", class: "db_write_failed", outcome: "pending", posts: 1, status: 429, started: true, closure: clean, badWrite: true},
		{name: "sideband missing call ID", accounts: 1, script: `printf 'HTTP/1.1 201 Created\r\n\r\nv=0\r\n'`, stage: "upstream_sideband", class: "malformed_response", outcome: "unknown", posts: 1, status: 201, started: true, closure: unclean},
		{name: "sideband rejected", accounts: 1, script: ownedAccepted, change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			s.avasDial = func(websocket.Dialer, string, fhttp.Header) (avasSidebandConnection, *fhttp.Response, error) {
				return nil, &fhttp.Response{StatusCode: 403}, errors.New(leak)
			}
		}, stage: "upstream_sideband", class: "http_4xx", outcome: "unknown", posts: 1, status: 201, started: true, closure: unclean},
		{name: "answer negotiation", accounts: 1, script: ownedAccepted, change: func(_ *testing.T, _ *Service, _ *store.DB, r *fakeRelay, _ *ClientDialRequest) {
			r.acceptFn = func(*fakeMedia) error { return &url.Error{URL: leak, Err: context.DeadlineExceeded} }
		}, stage: "answer_negotiation", class: "transport_timeout", outcome: "unknown", posts: 1, status: 201, started: true, closure: unclean},
		{name: "attach relay", accounts: 1, script: ownedAccepted, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_attach BEFORE UPDATE OF relay_session_id ON voice_media_sessions BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "relay_attach", class: "evidence_failed", outcome: "unknown", posts: 1, status: 201, started: true, closure: unclean},
		{name: "established write failed", accounts: 1, script: ownedAccepted, change: func(t *testing.T, _ *Service, db *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			fixtureSQL(t, db, `CREATE TRIGGER fail_established BEFORE UPDATE ON voice_creation_outcomes WHEN NEW.outcome = 'established' BEGIN SELECT RAISE(FAIL, '`+leak+`'); END`)
		}, stage: "outcome_write", class: "db_write_failed", outcome: "unknown", posts: 1, status: 201, started: true, closure: clean, badWrite: true},
		{name: "binding ack refused", accounts: 1, script: ownedAccepted, change: func(_ *testing.T, s *Service, _ *store.DB, _ *fakeRelay, _ *ClientDialRequest) {
			start := s.zoeHTTP.Transport
			s.zoeHTTP.Transport = ownedCreationRoundTrip(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/established") {
					return ownedCreationResponse(403, leak), nil
				}
				return start.RoundTrip(r)
			})
		}, stage: "binding_ack", class: "http_4xx", outcome: "established", posts: 1, status: 201, started: true, closure: clean},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, db, r, logs := newOfflineOwnedCreation(t, tc.accounts)
			svc.avasDial = func(websocket.Dialer, string, fhttp.Header) (avasSidebandConnection, *fhttp.Response, error) {
				return &diagnosticReader{started: make(chan struct{}), stopped: make(chan struct{})}, nil, nil
			}
			script := tc.script
			if tc.posts == 2 {
				script = "if mkdir '" + filepath.Join(t.TempDir(), "first") + "' 2>/dev/null; then " + ownedRejection + "; else exit 28; fi"
			}
			if script == "" {
				script = "exit 99"
			}
			setOwnedCurl(t, svc, script)
			p := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
			if tc.change != nil {
				tc.change(t, svc, db, r, &p)
			}
			_, err := svc.DialClient(context.Background(), "synthetic-credential", p)
			var public *CallError
			if !errors.As(err, &public) || public.Status != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %v", err)
			}
			assertOwnedClaim(t, db, tc.outcome)
			if tc.name == "established write failed" {
				var sessionID string
				var generation int64
				if err := db.Conn().QueryRow(`SELECT voice_session_id, media_generation FROM voice_media_sessions`).Scan(&sessionID, &generation); err != nil {
					t.Fatal(err)
				}
				row, err := svc.evidence.Get(sessionID, generation)
				if err != nil || !svc.hasCleanOwnedClosure(row.Owner, row.Identity) {
					t.Fatalf("expected durable bound closure, state=%q, error=%v", row.State, err)
				}
				if reasons := r.session(0).closedReasons(); len(reasons) != 1 || reasons[0] != "failure" {
					t.Fatalf("expected one bound close, got %v", reasons)
				}
			}
			log := ownedCreationWarning(t, logs, leak)
			var status, closure, outcome any
			if tc.status != 0 {
				status = float64(tc.status)
			}
			if tc.closure != none {
				closure = tc.closure == clean
			}
			if tc.outcome != "" {
				outcome = tc.outcome
				if value, ok := log["elapsed_ms"].(float64); !ok || value < 0 {
					t.Fatalf("invalid elapsed_ms: %v", log["elapsed_ms"])
				}
			} else if log["elapsed_ms"] != nil {
				t.Fatal("elapsed_ms is available without a claim")
			}
			for field, want := range map[string]any{"level": "WARN", "creation_stage": tc.stage, "error_class": tc.class,
				"upstream_attempt_count": float64(tc.posts), "upstream_last_status": status, "relay_started": tc.started,
				"relay_close_clean": closure, "stored_outcome": outcome, "outcome_write_ok": tc.outcome != "" && !tc.badWrite} {
				if got, exists := log[field]; !exists || got != want {
					t.Errorf("%s=%v (present=%v), want %v", field, got, exists, want)
				}
			}
			rejectionStatus := map[string]float64{"Zoe rejects": 401, "Zoe unavailable": 503, "Zoe quarantine": 503,
				"sideband rejected": 403, "binding ack refused": 403}
			if want, expected := rejectionStatus[tc.name]; expected {
				if log["rejection_status"] != want {
					t.Errorf("rejection_status=%v, want %v", log["rejection_status"], want)
				}
			} else if _, present := log["rejection_status"]; present {
				t.Errorf("unexpected rejection_status=%v", log["rejection_status"])
			}
		})
	}
}

func ownedCreationWarning(t *testing.T, logs *safeForwardLog, sensitive string) map[string]any {
	t.Helper()
	text := logs.String()
	for _, value := range append(strings.Fields(sensitive), "54321", "203.0.113.94", "sensitive.invalid", "synthetic-instructions", "synthetic-runner") {
		if strings.Contains(text, value) {
			t.Fatalf("creation logs leaked fixture data: %s", value)
		}
	}
	var warning map[string]any
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["msg"] == "owned_call_creation_failed" {
			if warning != nil {
				t.Fatal("dial emitted two creation failure warnings")
			}
			warning = entry
		}
	}
	if warning == nil {
		t.Fatal("missing creation failure warning")
	}
	return warning
}

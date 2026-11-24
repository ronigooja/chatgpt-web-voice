package voice

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func TestCreationTransportErrorClasses(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"EOF", io.EOF, false}, {"UnexpectedEOF", io.ErrUnexpectedEOF, false}, {"reset", syscall.ECONNRESET, false},
		{"DNS", &net.DNSError{Err: "not found"}, true},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"unknown phase", &net.OpError{Op: "write", Err: errors.New("unknown")}, false},
		{"timeout", &net.DNSError{IsTimeout: true}, false},
		{"cancel", context.Canceled, false}, {"deadline", context.DeadlineExceeded, false},
		{"TLS certificate", &tls.CertificateVerificationError{Err: errors.New("invalid")}, true},
		{"generic", errors.New("transport failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, wrapped := range []error{tc.err, fmt.Errorf("outer: %w", tc.err), &url.Error{Op: "Post", URL: "https://example.invalid", Err: tc.err}, fmt.Errorf("outer: %w", &url.Error{Op: "Post", Err: &net.OpError{Op: "read", Err: tc.err}})} {
				var marker creationTransportError
				if got := errors.As(noResponseTransportError(nil, wrapped), &marker); got != tc.want {
					t.Fatalf("eligible=%v want=%v", got, tc.want)
				}
			}
		})
	}
	for _, code := range []int{-1, 0, 5, 6, 7, 28, 35, 52, 55, 56, 60, 97} {
		for _, began := range []bool{false, true} {
			err := &httpclient.CurlRequestError{Err: io.EOF, ExitCode: code, ResponseBegan: began}
			want := !began && (code == 6 || code == 7 || code == 35 || code == 52 || code == 56 || code == 97)
			var marker creationTransportError
			if got := errors.As(noResponseTransportError(nil, &url.Error{Op: "Post", Err: fmt.Errorf("wrapped: %w", err)}), &marker); got != want {
				t.Fatalf("curl %d began=%v eligible=%v", code, began, got)
			}
		}
	}
}

func TestCreateDuplicateResetRaceKeepsCurrentPending(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusCreated, http.StatusCreated}, 1)
	svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
	entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(calls.Add(1)) - 1
		close(entered[i])
		<-release[i]
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()
	svc.avasURL = server.URL
	result := []chan error{make(chan error, 1), make(chan error, 1)}
	req := CreateSessionRequest{Owner: "api_key:1", RequestID: "duplicate", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	for i := 0; i < 2; i++ {
		go func(i int) { _, err := svc.CreateSession(req); result[i] <- err }(i)
		<-entered[i]
	}
	close(release[0])
	if err := <-result[0]; err == nil {
		t.Error("expected old request failure")
	}
	row, err := db.GetVoiceCreationOutcome(req.Owner, req.RequestID)
	if err != nil || row.Outcome != "pending" {
		t.Errorf("current attempt overwritten: row=%+v err=%v", row, err)
	}
	outcome, evidence, exists := svc.CreationOutcomeSnapshot(req.Owner, req.RequestID)
	if !exists || outcome != "unknown" || evidence != "" {
		t.Errorf("active duplicate snapshot=%s,%s,%v", outcome, evidence, exists)
	}
	close(release[1])
	if err := <-result[1]; err == nil {
		t.Error("expected current request failure")
	}
	outcome, evidence, exists = svc.CreationOutcomeSnapshot(req.Owner, req.RequestID)
	if !exists || outcome != "unknown" || evidence != "" {
		t.Fatalf("finished duplicate snapshot=%s,%s,%v", outcome, evidence, exists)
	}
}

type creationFailureRelay struct {
	fakeRelay
	termination *relay.Termination
}

func (r *creationFailureRelay) NewSession(ctx context.Context, offer string, route relay.Route) (relay.Session, string, error) {
	m, sdp, err := r.fakeRelay.NewSession(ctx, offer, route)
	if err == nil {
		m.(*fakeMedia).termination = r.termination
	}
	return m, sdp, err
}

func TestCreateNetworkResetRelayOutcome(t *testing.T) {
	clean := relay.ComponentResult{Closed: true}
	for _, mode := range []string{"clean", "peer_a", "peer_b", "data_channels", "tunnels", "uncertain", "evidence_write", "no_evidence"} {
		t.Run(mode, func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusCreated, http.StatusCreated}, 1)
			ev := mediaevidence.NewStore(db)
			if mode != "no_evidence" {
				svc.WithMediaEvidence(ev)
			}
			term := &relay.Termination{PeerA: clean, PeerB: clean, DataChannels: clean, Tunnels: clean}
			switch mode {
			case "peer_a":
				term.PeerA.Err = errors.New("close failed")
			case "peer_b":
				term.PeerB.Err = errors.New("close failed")
			case "data_channels":
				term.DataChannels.Err = errors.New("close failed")
			case "tunnels":
				term.Tunnels.Err = errors.New("close failed")
			case "uncertain":
				term = &relay.Termination{}
			case "evidence_write":
				if _, err := db.Conn().Exec(`CREATE TRIGGER fail_creation_evidence BEFORE INSERT ON voice_termination_evidence BEGIN SELECT RAISE(FAIL, 'synthetic evidence failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			r := &creationFailureRelay{termination: term}
			svc.WithRelay(r)
			// Closing the accepted TLS connection without an HTTP response produces EOF.
			reset := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}))
			defer reset.Close()
			svc.avasURL = reset.URL
			// Production uses curl: only its explicit no-response metadata may
			// make EOF/reset eligible. A tiny local executable reproduces exit 97.
			bin := filepath.Join(t.TempDir(), "curl-reset")
			if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
				t.Fatal(err)
			}
			svc.httpOptions.Transport = config.TransportCurlImpersonate
			svc.httpOptions.CurlImpersonateBin = bin
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "network-reset", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			want := "unknown"
			if mode == "clean" {
				want = "released"
			}
			if outcome, exists := svc.CreationOutcome("api_key:1", "network-reset"); !exists || outcome != want {
				t.Fatalf("outcome=%q exists=%v want=%q", outcome, exists, want)
			}
			evidenceID := svc.CreationOutcomeEvidence("api_key:1", "network-reset")
			if (evidenceID != "") != (mode == "clean") {
				t.Fatalf("evidence=%q mode=%s", evidenceID, mode)
			}
			if svc.CreationOutcomeEvidence("api_key:2", "network-reset") != "" {
				t.Fatal("evidence leaked across owners")
			}
			if reasons := r.session(0).closedReasons(); len(reasons) != 1 || reasons[0] != "upstream_request_failed" {
				t.Fatalf("close reasons=%v", reasons)
			}
			// The existing transient cooldown must exclude the failed account
			// even when the next independent request supplies no exclusions.
			var failures int
			var cooldown int64
			if err := db.Conn().QueryRow(`SELECT failure_count, cooldown_until FROM accounts WHERE failure_count > 0`).Scan(&failures, &cooldown); err != nil {
				t.Fatal(err)
			}
			if failures != 1 || cooldown <= time.Now().UnixMilli() {
				t.Fatalf("failures=%d cooldown=%d", failures, cooldown)
			}
			_, account, err := svc.pool.Pick("", nil)
			if err != nil {
				t.Fatal(err)
			}
			if account.CooldownUntil > time.Now().UnixMilli() || account.FailureCount != 0 {
				t.Fatal("immediate retry selected the failed account")
			}
		})
	}
}

func TestCreateNativePartialHeadersAndResetStayUnknown(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				buf := make([]byte, 4096)
				_, _ = conn.Read(buf)
				if partial {
					_, _ = io.WriteString(conn, "HTTP/1.1 201 Created\r\nContent-Type: application/sdp\r\n")
				} else {
					_ = conn.(*net.TCPConn).SetLinger(0)
				}
			}()
			svc.avasURL = "http://" + listener.Addr().String()
			svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
			_, err = svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if err == nil {
				t.Fatal("expected transport error")
			}
			<-done
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

func TestUpstreamPOSTRetainsCurlBodyFailure(t *testing.T) {
	for _, route := range []string{"avas", "wm"} {
		t.Run(route, func(t *testing.T) {
			svc, _, _ := newCreationService(t, []int{http.StatusCreated}, 1)
			bin := filepath.Join(t.TempDir(), "curl-body-fail")
			if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf 'HTTP/1.1 201 Created\\r\\n\\r\\nv=0\\r\\n'\nexit 56\n"), 0700); err != nil {
				t.Fatal(err)
			}
			svc.httpOptions.Transport = config.TransportCurlImpersonate
			svc.httpOptions.CurlImpersonateBin = bin
			var status int
			var err error
			if route == "avas" {
				status, _, _, _, err = svc.postAVASResponse("test", "offer", avasSessionPayload{}, "direct", "")
			} else {
				status, _, _, _, err = svc.postWMResponse("test", "offer", "{}", "direct", "")
			}
			var failure *httpclient.CurlRequestError
			if status != 201 || !errors.As(err, &failure) || !failure.ResponseBegan || failure.ExitCode != 56 {
				t.Fatalf("status=%d err=%v", status, err)
			}
		})
	}
}

func TestCreateCleanRejectionWithEvidenceStaysNeverEstablished(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusBadGateway}, 1)
	svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || !se.CreationRejected {
		t.Fatalf("error=%+v", se)
	}
	assertCreationOutcome(t, svc, db, "never_established")
	if svc.CreationOutcomeEvidence("api_key:1", "no-account") != "" {
		t.Fatal("clean rejection should not expose release evidence")
	}
}

func TestCreateUncleanRejectionOutcomeUnknown(t *testing.T) {
	for _, mode := range []string{"unclean", "evidence_write"} {
		t.Run(mode, func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusBadGateway}, 1)
			term := &relay.Termination{}
			if mode == "evidence_write" {
				term = nil
				if _, err := db.Conn().Exec(`CREATE TRIGGER fail_rejection_evidence BEFORE INSERT ON voice_termination_evidence BEGIN SELECT RAISE(FAIL,'test'); END`); err != nil {
					t.Fatal(err)
				}
			}
			svc.WithRelay(&creationFailureRelay{termination: term}).WithMediaEvidence(mediaevidence.NewStore(db))
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

type failingCreationBeginStore struct{ *store.DB }

func (s failingCreationBeginStore) BeginVoiceCreationOutcome(owner, request string) (string, error) {
	return "", errors.New("synthetic outcome reset failure")
}

func TestCreateOutcomeBeginFailureAbortsBeforeResources(t *testing.T) {
	for _, oldOutcome := range []string{"never_established", "released"} {
		t.Run(oldOutcome, func(t *testing.T) {
			svc, db, upstream := newCreationService(t, []int{http.StatusCreated}, 1)
			attempt, err := db.BeginVoiceCreationOutcome("api_key:1", "duplicate")
			if err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateVoiceCreationOutcome("api_key:1", "duplicate", oldOutcome, "", 0, attempt); err != nil {
				t.Fatal(err)
			}
			r := &fakeRelay{}
			svc.WithRelay(r).WithCreationOutcomes(failingCreationBeginStore{db})
			_, err = svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "duplicate", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			if r.count() != 0 || len(upstream.bodies) != 0 {
				t.Fatal("resources started after outcome reset failure")
			}
			row, err := db.GetVoiceCreationOutcome("api_key:1", "duplicate")
			if err != nil || row.Outcome != oldOutcome || row.AttemptID != attempt {
				t.Fatalf("row=%+v err=%v", row, err)
			}
		})
	}
}

func TestCreateResumedAVASRejectionStaysUnknown(t *testing.T) {
	for _, relayed := range []bool{false, true} {
		t.Run(fmt.Sprint(relayed), func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusBadGateway}, 1)
			if relayed {
				svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
			}
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", UpstreamVoiceSessionID: "existing-upstream", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

func TestCreateRejectionOutcomeWriteFailureIsNonCommittal(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusBadGateway}, 1)
	if _, err := db.Conn().Exec(`CREATE TRIGGER fail_rejection_outcome BEFORE UPDATE ON voice_creation_outcomes WHEN NEW.outcome='never_established' BEGIN SELECT RAISE(FAIL,'test'); END`); err != nil {
		t.Fatal(err)
	}
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.CreationRejected {
		t.Fatalf("error=%+v", se)
	}
	assertCreationOutcome(t, svc, db, "unknown")
}

func TestCreateAVASInvalidSuccessOutcome(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusOK, http.StatusNoContent, http.StatusFound} {
		for _, body := range []string{"", "not SDP"} {
			for _, mode := range []string{"direct", "clean", "unclean"} {
				t.Run(fmt.Sprintf("%d/%q/%s", status, body, mode), func(t *testing.T) {
					svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status); _, _ = io.WriteString(w, body) }))
					defer server.Close()
					svc.avasURL = server.URL
					if mode != "direct" {
						var term *relay.Termination
						if mode == "unclean" {
							term = &relay.Termination{}
						}
						svc.WithRelay(&creationFailureRelay{termination: term}).WithMediaEvidence(mediaevidence.NewStore(db))
					}
					_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "invalid-success", OfferSDP: readTestdata(t, "wm_offer.sdp")})
					if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.CreationRejected {
						t.Fatalf("error=%+v", se)
					}
					want := "unknown"
					outcome, evidence, exists := svc.CreationOutcomeSnapshot("api_key:1", "invalid-success")
					if !exists || outcome != want || (evidence != "") != (want == "released") {
						t.Fatalf("snapshot=%q,%q,%v want=%s", outcome, evidence, exists, want)
					}
				})
			}
		}
	}
}

func TestAVASSidebandEarlyCloseAttachmentDoesNotDeadlock(t *testing.T) {
	svc := New(config.Config{}, nil, nil)
	media := &fakeMedia{id: "early-close"}
	sideband := &avasSidebandWriter{closed: true}
	svc.bindings["vs-early"] = &sessionBinding{media: media, openDelegation: "pending"}
	done := make(chan struct{})
	go func() { svc.attachAVASSideband("vs-early", media, sideband, func() {}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("already closed sideband deadlocked during attachment")
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.bindings["vs-early"].openDelegation != "" {
		t.Fatal("early-close callback did not clear delegation")
	}
}

type creationHandoffMedia struct {
	relay.Session
	onHandoff func()
}

func (m *creationHandoffMedia) SetEventShapeMetadata(string, string) { m.onHandoff() }
func (m *creationHandoffMedia) InjectSidebandEvent(data []byte) error {
	return m.Session.(relay.SidebandEventInjector).InjectSidebandEvent(data)
}

type creationHandoffRelay struct {
	fakeRelay
	onHandoff func()
}

func (r *creationHandoffRelay) NewSession(ctx context.Context, offer string, route relay.Route) (relay.Session, string, error) {
	m, sdp, err := r.fakeRelay.NewSession(ctx, offer, route)
	if err != nil {
		return m, sdp, err
	}
	return &creationHandoffMedia{Session: m, onHandoff: r.onHandoff}, sdp, nil
}

func TestCreateAcknowledgedSuccessLaterFailureStaysUnknown(t *testing.T) {
	for _, mode := range []string{"answer", "register", "post_handoff", "panic_before_handoff", "panic_after_handoff"} {
		t.Run(mode, func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
			svc.cfg.VoiceUpstreamRoute = "wm"
			svc.realtimeURL = svc.avasURL
			svc.WithMediaEvidence(mediaevidence.NewStore(db))
			req := CreateSessionRequest{Owner: "api_key:1", RequestID: "acknowledged", OfferSDP: readTestdata(t, "wm_offer.sdp")}
			r := &fakeRelay{}
			if mode == "answer" {
				r.acceptFn = func(*fakeMedia) error { return errors.New("synthetic answer failure") }
			}
			if mode == "panic_before_handoff" {
				r.acceptFn = func(*fakeMedia) error { panic("synthetic before handoff") }
			}
			svc.WithRelay(r)
			if mode == "register" {
				if _, err := db.Conn().Exec(`CREATE TRIGGER fail_ack_register BEFORE INSERT ON voice_media_sessions BEGIN SELECT RAISE(FAIL,'test'); END`); err != nil {
					t.Fatal(err)
				}
			}
			var handedOff *creationHandoffRelay
			if mode == "post_handoff" || mode == "panic_after_handoff" {
				req.OpeningContext = OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "valid"}}}
				handedOff = &creationHandoffRelay{onHandoff: func() { req.OpeningContext.Items[0].Role = "invalid" }}
				if mode == "panic_after_handoff" {
					handedOff.onHandoff = func() { panic("synthetic after handoff") }
				}
				svc.WithRelay(handedOff)
			}
			var err error
			var recovered any
			func() { defer func() { recovered = recover() }(); _, err = svc.CreateSession(req) }()
			if strings.HasPrefix(mode, "panic_") && recovered == nil {
				t.Fatal("panic was swallowed")
			}
			if err == nil && recovered == nil {
				t.Fatal("expected failed create")
			}
			outcome, evidence, exists := svc.CreationOutcomeSnapshot(req.Owner, req.RequestID)
			if !exists || outcome != "unknown" || evidence != "" {
				t.Fatalf("snapshot=%q,%q,%v", outcome, evidence, exists)
			}
			if handedOff != nil {
				r = &handedOff.fakeRelay
			}
			if len(r.session(0).closedReasons()) == 0 {
				t.Fatal("failed create leaked relay")
			}
			if len(svc.bindings) != 0 {
				t.Fatal("failed create left binding alive")
			}
		})
	}
}

func TestCreateHTTPFailuresNeverRelease(t *testing.T) {
	for _, route := range []string{"wm", "avas"} {
		t.Run(route, func(t *testing.T) {
			status := http.StatusBadGateway
			if route == "avas" {
				status = http.StatusUnauthorized
			}
			svc, db, _ := newCreationService(t, []int{status}, 1)
			svc.cfg.VoiceUpstreamRoute = route
			svc.realtimeURL = svc.avasURL
			svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if err == nil {
				t.Fatal("expected error")
			}
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

func TestCreationTransportAllowListRejectsContextAndResponse(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		var transport creationTransportError
		if errors.As(noResponseTransportError(nil, err), &transport) {
			t.Fatal("context error eligible for release")
		}
	}
	var transport creationTransportError
	if errors.As(noResponseTransportError(&http.Response{StatusCode: 201}, io.EOF), &transport) {
		t.Fatal("received response eligible for release")
	}
}

func TestCreateResumedTransportFailureStaysUnknown(t *testing.T) {
	svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
	closed := httptest.NewTLSServer(http.NotFoundHandler())
	closed.Close()
	svc.avasURL = closed.URL
	svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
	_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", UpstreamVoiceSessionID: "existing", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err == nil {
		t.Fatal("expected transport failure")
	}
	assertCreationOutcome(t, svc, db, "unknown")
}

func assertCreationOutcome(t *testing.T, svc *Service, db *store.DB, want string) {
	t.Helper()
	row, err := db.GetVoiceCreationOutcome("api_key:1", "no-account")
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != want {
		t.Fatalf("stored outcome=%q want %q", row.Outcome, want)
	}
	if outcome, exists := svc.CreationOutcome("api_key:1", "no-account"); !exists || outcome != want {
		t.Fatalf("outcome=%q exists=%v want %q", outcome, exists, want)
	}
}

func TestCreateNoUsableAccountBeforeResourcesNeverEstablished(t *testing.T) {
	for _, route := range []string{"avas", "wm"} {
		for _, state := range []string{"empty", "expired", "cooling", "disabled"} {
			t.Run(route+"/"+state, func(t *testing.T) {
				svc, db, upstream := newCreationService(t, nil, 3)
				svc.cfg.VoiceUpstreamRoute = route
				r := &fakeRelay{}
				svc.WithRelay(r)
				if state != "empty" {
					for i := 0; i < 2; i++ {
						expiry := time.Now().Add(time.Hour)
						if state == "expired" {
							expiry = time.Now().Add(-time.Hour)
						}
						account, err := svc.pool.(*accounts.Pool).Create(accounts.Account{AccessToken: testJWT(map[string]any{"exp": expiry.Unix(), "jti": string(rune('a' + i))}), Disabled: state == "disabled"})
						if err != nil {
							t.Fatal(err)
						}
						if state == "cooling" {
							if _, err := db.Conn().Exec("UPDATE accounts SET cooldown_until = ? WHERE id = ?", time.Now().Add(time.Hour).UnixMilli(), account.ID); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
				if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable || se.CreationRejected {
					t.Fatalf("error=%+v", se)
				}
				assertCreationOutcome(t, svc, db, "never_established")
				if r.count() != 0 || len(upstream.bodies) != 0 {
					t.Fatal("resources started without a usable account")
				}
			})
		}
	}
}

func TestCreateLaterSelectionFailureAfterResourcesStaysUnknown(t *testing.T) {
	for _, relayed := range []bool{false, true} {
		name := "direct"
		if relayed {
			name = "relay"
		}
		t.Run(name, func(t *testing.T) {
			svc, db, upstream := newCreationService(t, []int{http.StatusBadGateway}, 2)
			if relayed {
				svc.WithRelay(&fakeRelay{})
			}
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			assertCreationOutcome(t, svc, db, "unknown")
			if len(upstream.bodies) != 1 {
				t.Fatalf("attempts=%d", len(upstream.bodies))
			}
		})
	}
}

func TestCreateResourceStartFailuresStayUnknown(t *testing.T) {
	for _, mode := range []string{"relay", "capacity", "avas", "wm"} {
		t.Run(mode, func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 2)
			wantStatus := http.StatusBadGateway
			switch mode {
			case "relay":
				svc.WithRelay(&fakeRelay{newErr: errors.New("synthetic start failure")})
			case "capacity":
				svc.WithRelay(&fakeRelay{newErr: relay.ErrCapacity})
				wantStatus = http.StatusServiceUnavailable
			default:
				// A closed local TLS server makes the real upstream POST fail.
				closed := httptest.NewTLSServer(http.NotFoundHandler())
				closed.Close()
				svc.cfg.VoiceUpstreamRoute = mode
				svc.avasURL = closed.URL
				svc.realtimeURL = closed.URL
			}
			_, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			if se := serviceError(t, err); se.StatusCode != wantStatus || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

func TestCreateResumedIdentitySelectionFailureStaysUnknown(t *testing.T) {
	for _, identity := range []string{"binding", "durable", "upstream_voice", "conversation", "parent"} {
		t.Run(identity, func(t *testing.T) {
			svc, db, _ := newCreationService(t, nil, 2)
			req := CreateSessionRequest{Owner: "api_key:1", RequestID: "no-account", OfferSDP: readTestdata(t, "wm_offer.sdp")}
			switch identity {
			case "binding":
				req.VoiceSessionID = svc.bindVoiceSession(req.Owner, "", "old-token", accounts.Account{ID: 99}, UpstreamContext{})
			case "durable":
				req.VoiceSessionID = "vs_durable"
				svc.WithCallSessions(&testCallSessionStore{rows: map[string]callsessions.Session{
					req.VoiceSessionID: {VoiceSessionID: req.VoiceSessionID, AccountID: 99},
				}})
			case "upstream_voice":
				req.UpstreamVoiceSessionID = "existing-upstream"
			case "conversation":
				req.UpstreamConversationID = "existing-conversation"
			case "parent":
				req.UpstreamParentMessageID = "existing-parent"
			}
			_, err := svc.CreateSession(req)
			if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable || se.CreationRejected {
				t.Fatalf("error=%+v", se)
			}
			assertCreationOutcome(t, svc, db, "unknown")
		})
	}
}

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
	svc.WithRelay(&fakeRelay{}).WithMediaEvidence(mediaevidence.NewStore(db))
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
	if outcome, evidence, exists := svc.CreationOutcomeSnapshot("api_key:1", "req-d"); !exists || outcome != "unknown" || evidence != "" {
		t.Fatalf("snapshot=%q,%q,%v", outcome, evidence, exists)
	}
}

func TestCreateDebugRejectionLogsMetadataOnly(t *testing.T) {
	for _, route := range []string{"avas", "wm"} {
		for _, tc := range []struct {
			name    string
			enabled bool
			want    bool
			level   slog.Level
		}{{"debug", true, true, slog.LevelDebug}, {"info", false, false, slog.LevelInfo}} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				var logs strings.Builder
				logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: tc.level}))
				body := "SECRET-CONTENT-MARKER-1234" + strings.Repeat("界", 5000) + "END"
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
					_, err := io.WriteString(w, body)
					if err != nil {
						t.Fatal(err)
					}
				}))
				defer server.Close()
				svc := New(config.Config{SkipSSLVerify: true, UpstreamTransport: config.TransportGo, MaxAccountAttempts: 1, VoiceUpstreamRoute: route}, pool, logger)
				svc.avasURL = server.URL + "/calls"
				svc.realtimeURL = server.URL + "/realtime/wm"
				if _, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")}); err == nil {
					t.Fatal("expected upstream rejection")
				}
				if strings.Contains(logs.String(), "SECRET-CONTENT-MARKER-1234") || strings.Contains(logs.String(), body) {
					t.Fatalf("body leaked: %q", logs.String())
				}
				if strings.Contains(logs.String(), "upstream_body_bytes") != tc.want {
					t.Fatalf("logs=%q", logs.String())
				}
			})
		}
	}
}

func TestCreationOutcomePendingAfterSimulatedCrashAnswersUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "voice.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("api_key:1", "crash-pending"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := New(config.Config{}, nil, nil).WithCreationOutcomes(db)
	outcome, exists := service.CreationOutcome("api_key:1", "crash-pending")
	if !exists || outcome != "unknown" {
		t.Fatalf("outcome=%q exists=%v", outcome, exists)
	}
}

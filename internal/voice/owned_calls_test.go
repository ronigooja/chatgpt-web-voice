package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	fhttptest "github.com/bogdanfinn/fhttp/httptest"
	"github.com/bogdanfinn/websocket"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
)

const ownedTestKey = "synthetic-instance-bearer"

type ownedTestRig struct {
	svc                 *Service
	relay               *fakeRelay
	upstream            *avasUpstream
	payload             ClientDialRequest
	started             chan struct{}
	established         chan lifecycleIdentity
	receipt             chan map[string]any
	ended               chan map[string]any
	turns               chan forwardedTurn
	release             chan struct{}
	startStatus         int
	startError          string
	badAuth, badBinding atomic.Bool
	establishStatus     int
	forwardStatus       int
	endRetried          bool
	establishRetried    bool
	logs                *safeForwardLog
}

func newOwnedTestRig(t *testing.T, upstreamStatus int) *ownedTestRig {
	t.Helper()
	svc, db, _ := newCreationService(t, []int{upstreamStatus}, 1)
	avas := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://example.invalid/live/call-test")
		w.WriteHeader(upstreamStatus)
		if upstreamStatus == 201 {
			_, _ = io.WriteString(w, "v=0\r\n")
		}
	}))
	t.Cleanup(avas.Close)
	svc.avasURL = avas.URL
	up := &avasUpstream{ready: make(chan struct{})}
	upgrade := websocket.Upgrader{CheckOrigin: func(*fhttp.Request) bool { return true }}
	ws := fhttptest.NewServer(fhttp.HandlerFunc(func(w fhttp.ResponseWriter, r *fhttp.Request) {
		conn, err := upgrade.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		up.mu.Lock()
		up.conn = conn
		up.mu.Unlock()
		close(up.ready)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var contents map[string]any
			if json.Unmarshal(data, &contents) == nil {
				up.mu.Lock()
				up.frames = append(up.frames, contents)
				up.mu.Unlock()
			}
		}
	}))
	t.Cleanup(ws.Close)
	logs := &safeForwardLog{}
	svc.logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc.avasSidebandURL = "ws" + strings.TrimPrefix(ws.URL, "http") + "/"
	svc.cfg.ZoeGatewayAPIKeyID = 42
	svc.cfg.ZoeFinalTurnDebugText = true
	svc.zoeToken = "synthetic-runner"
	rig := &ownedTestRig{svc: svc, logs: logs, relay: &fakeRelay{}, upstream: up,
		payload: ClientDialRequest{RequestID: "apple-id", OfferSDP: readTestdata(t, "wm_offer.sdp"), Voice: "cove"},
		started: make(chan struct{}, 16), established: make(chan lifecycleIdentity, 16), receipt: make(chan map[string]any, 16),
		ended: make(chan map[string]any, 16), turns: make(chan forwardedTurn, 16), release: make(chan struct{})}
	svc.WithRelay(rig.relay).WithMediaEvidence(mediaevidence.NewStore(db)).WithCallSessions(callsessions.NewStore(db))
	var starts atomic.Int32
	var establishes, ends atomic.Int32
	zoe := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/runner/v1/voice/calls":
			if r.Header.Get("Authorization") != "Bearer synthetic-runner" || r.Header.Get("X-Zoe-Instance-Key") != ownedTestKey {
				t.Error("wrong start auth")
			}
			var body ClientDialRequest
			if json.NewDecoder(r.Body).Decode(&body) != nil || body != rig.payload {
				t.Error("dial body altered")
			}
			n := starts.Add(1)
			if n == 1 {
				rig.relay.mu.Lock()
				if len(rig.relay.sessions) != 0 {
					t.Error("relay allocated before authorization")
				}
				rig.relay.mu.Unlock()
			}
			rig.started <- struct{}{}
			if rig.startStatus != 0 {
				w.WriteHeader(rig.startStatus)
				_, _ = io.WriteString(w, rig.startError)
				return
			}
			if n == 1 {
				w.WriteHeader(201)
			}
			manifest := OpeningContext{Kind: "full", Version: 7, Items: []OpeningContextItem{{Role: "user", Text: "opening"}}}
			_ = json.NewEncoder(w).Encode(map[string]any{"voice_session_id": "z-1", "generation": 3, "gateway_request_id": "gw-request", "instructions": "instruction",
				"opening_context": manifest, "opening_context_hash": openingContextContentHash(manifest)})
		case "/runner/v1/voice/calls/z-1/established":
			if r.Header.Get("X-Zoe-Instance-Key") != "" {
				t.Error("instance key retained")
			}
			var body struct {
				lifecycleIdentity
				Receipt map[string]any `json:"opening_context"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("invalid established body")
			}
			rig.established <- body.lifecycleIdentity
			rig.receipt <- body.Receipt
			select {
			case <-rig.release:
			case <-r.Context().Done():
				return
			}
			if rig.establishStatus != 0 {
				w.WriteHeader(rig.establishStatus)
				return
			}
			if rig.establishRetried && establishes.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		case "/runner/v1/voice/calls/z-1/ended":
			var body map[string]any
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("invalid ended body")
			}
			rig.ended <- body
			if rig.endRetried && ends.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true,"state":"ended","upstream_termination":"verified","pool_blocked":false}`)
		case "/v1/voice/sessions/z-1", "/v1/voice/sessions/-":
			if r.Header.Get("Authorization") != "Bearer "+ownedTestKey || r.Header.Get("X-Zoe-Instance-Key") != "" {
				t.Error("wrong hangup auth")
			}
			if rig.badAuth.Load() || strings.HasSuffix(r.URL.Path, "/-") {
				w.WriteHeader(404)
				return
			}
			gen := 3
			if rig.badBinding.Load() {
				gen = 4
			}
			_, _ = fmt.Fprintf(w, `{"voice_session_id":"z-1","generation":%d,"state":"ended"}`, gen)
		case "/runner/v1/voice/turns":
			var batch transcriptBatch
			if json.NewDecoder(r.Body).Decode(&batch) != nil {
				t.Error("invalid turn batch")
			}
			for _, tr := range batch.Turns {
				rig.turns <- tr
				if tr.DelegationID != "" && rig.forwardStatus > 0 {
					w.WriteHeader(rig.forwardStatus)
					return
				}
			}
			_, _ = io.WriteString(w, `{"accepted":1,"duplicates":0}`)
		default:
			t.Errorf("unexpected Zoe path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(zoe.Close)
	svc.cfg.ZoeURL = zoe.URL
	svc.zoeHTTP.Transport = zoe.Client().Transport
	t.Cleanup(func() { svc.zoeHTTP.CloseIdleConnections() })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		svc.ShutdownOwned(ctx)
	})
	return rig
}

func takeOwned[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for owned call event")
	}
	var zero T
	return zero
}

func TestOwnedDialAuthorizationAndCreationFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body, want string
	}{
		{"instance", 401, `{"error":{"code":"unauthenticated","message":"synthetic-instance-bearer","details":{"credential":"instance_key"}}}`, "unauthenticated"},
		{"runner", 401, `{"error":{"code":"unauthenticated"}}`, "voice_unavailable"},
		{"scope", 403, `{"error":{"code":"forbidden"}}`, "voice_unavailable"},
		{"busy", 409, `{"error":{"code":"voice_lease_busy"}}`, "voice_lease_busy"},
		{"conflict", 409, `{"error":{"code":"idempotency_conflict"}}`, "idempotency_conflict"},
		{"quarantine", 503, `{"error":{"code":"voice_pool_quarantined"}}`, "voice_pool_quarantined"},
		{"redirect", 307, ``, "voice_unavailable"},
		{"upstream rejection", 0, ``, "voice_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newOwnedTestRig(t, 400)
			r.startStatus, r.startError = tc.status, tc.body
			close(r.release)
			ctx := context.Background()
			if _, err := r.svc.DialClient(ctx, "", r.payload); err == nil || err.Error() != "unauthenticated" || len(r.started) != 0 {
				t.Fatal("missing bearer did work")
			}
			if _, err := r.svc.DialClient(ctx, ownedTestKey, r.payload); err == nil || err.Error() != tc.want {
				t.Fatalf("dial error: %v", err)
			}
			if tc.status != 0 && len(r.relay.sessions) != 0 {
				t.Fatal("refused start created media")
			}
			if tc.status == 0 {
				if got, _, ok := r.svc.CreationOutcomeSnapshot("api_key:42", "gw-request"); !ok || got != "never_established" {
					t.Fatalf("outcome=%s", got)
				}
				_, _ = r.svc.DialClient(ctx, ownedTestKey, r.payload)
				if len(r.relay.sessions) != 1 {
					t.Fatal("replay allocated again")
				}
			}
			if len(r.established) != 0 || len(r.ended) != 0 || strings.Contains(r.logs.String(), ownedTestKey) {
				t.Fatal("unbound report or unredacted key")
			}
		})
	}
}

func TestOwnedEstablishedBeforeAnswerAndForwarding(t *testing.T) {
	r := newOwnedTestRig(t, 201)
	type result struct {
		answer *ClientDialResult
		err    error
	}
	dial := make(chan result, 1)
	go func() {
		a, err := r.svc.DialClient(context.Background(), ownedTestKey, r.payload)
		dial <- result{a, err}
	}()
	binding := takeOwned(t, r.established)
	receipt := takeOwned(t, r.receipt)
	if len(receipt) != 4 || receipt["applied"] != true || receipt["silent"] != true || receipt["version"] != float64(7) || receipt["content_hash"] == "" {
		t.Fatalf("receipt: %v", receipt)
	}
	media := r.relay.session(0)
	media.emit(string(avasData(map[string]any{"type": "turn.done", "turn": map[string]string{"id": "u1", "role": "user", "transcript": "test"}})))
	select {
	case <-dial:
		t.Fatal("answer before ack")
	case <-r.turns:
		t.Fatal("turn before ack")
	case <-time.After(550 * time.Millisecond):
	}
	close(r.release)
	got := takeOwned(t, dial)
	if got.err != nil || got.answer.AnswerSDP != fakeClientAnswer || got.answer.GatewaySessionID != binding.GatewaySessionID {
		t.Fatalf("dial=%+v", got)
	}
	if tr := takeOwned(t, r.turns); tr.Seq != 0 || tr.Text != "test" {
		t.Fatalf("turn=%+v", tr)
	}
	if replay, err := r.svc.DialClient(context.Background(), ownedTestKey, r.payload); err != nil || *replay != *got.answer || len(r.relay.sessions) != 1 {
		t.Fatal("live replay failed")
	}
	if r.svc.ReleaseSession("api_key:42", binding.GatewaySessionID) {
		t.Fatal("legacy release closed owned media")
	}
	if got, _, _ := r.svc.CreationOutcomeSnapshot("api_key:42", "gw-request"); got != "open" {
		t.Fatal(got)
	}
	if _, _, ok := r.svc.CreationOutcomeSnapshot("api_key:2", "gw-request"); ok {
		t.Fatal("attempt crossed owner namespaces")
	}
}

func TestOwnedEndTriggersAndEvidence(t *testing.T) {
	for _, reason := range []string{"user_hangup", "upstream_voice_end", "media_lost", "gateway_shutdown", "failure"} {
		t.Run(reason, func(t *testing.T) {
			r := newOwnedTestRig(t, 201)
			close(r.release)
			ctx := context.Background()
			a, err := r.svc.DialClient(ctx, ownedTestKey, r.payload)
			if err != nil {
				t.Fatal(err)
			}
			b := takeOwned(t, r.established)
			id := mediaevidence.Identity{VoiceSessionID: b.GatewaySessionID, MediaGeneration: b.MediaGeneration, LeaseID: b.LeaseID, LeaseGeneration: b.LeaseGeneration}
			if lookup, err := r.svc.LookupTermination("api_key:42", id); err != nil || lookup.Outcome != "not_yet" {
				t.Fatalf("lookup=%+v err=%v", lookup, err)
			}
			media := r.relay.session(0)
			switch reason {
			case "user_hangup":
				r.badAuth.Store(true)
				if err := r.svc.HangupClient(ctx, ownedTestKey, a.GatewaySessionID); err == nil || err.Error() != "not_found" {
					t.Fatal(err)
				}
				r.badAuth.Store(false)
				r.badBinding.Store(true)
				if err := r.svc.HangupClient(ctx, ownedTestKey, a.GatewaySessionID); err == nil || err.Error() != "voice_unavailable" {
					t.Fatal(err)
				}
				if _, closed := media.Termination(); closed {
					t.Fatal("unowned hangup closed media")
				}
				r.badBinding.Store(false)
				for range 2 {
					if err := r.svc.HangupClient(ctx, ownedTestKey, a.GatewaySessionID); err != nil {
						t.Fatal(err)
					}
				}
			case "upstream_voice_end":
				media.emit(`{"type":"voice.end"}`)
			case "media_lost":
				_ = media.CloseWithReason("peer_a_failed")
			case "gateway_shutdown":
				r.svc.ShutdownOwned(ctx)
			case "failure":
				_ = media.CloseWithReason("failure")
			}
			report := takeOwned(t, r.ended)
			evidence, _ := report["termination_evidence"].(map[string]any)
			if report["reason"] != reason || report["closed_at"] == "" || evidence["terminated"] != true || evidence["voice_session_id"] != a.GatewaySessionID {
				t.Fatalf("report=%v", report)
			}
			if lookup, err := r.svc.LookupTermination("api_key:42", id); err != nil || lookup.Outcome != "released" || lookup.Evidence.EvidenceID != evidence["evidence_id"] {
				t.Fatalf("lookup=%+v err=%v", lookup, err)
			}
			if lookup, _ := r.svc.LookupTermination("api_key:2", id); lookup.Outcome != "unknown" || lookup.Evidence != nil {
				t.Fatal("evidence crossed owners")
			}
			id.LeaseGeneration++
			if lookup, _ := r.svc.LookupTermination("api_key:42", id); lookup.Outcome != "unknown" {
				t.Fatal("mismatched lookup")
			}
		})
	}
}

func TestOwnedEstablishmentFailureClosesWithoutEndOrTranscript(t *testing.T) {
	r := newOwnedTestRig(t, 201)
	r.establishStatus = 409
	close(r.release)
	if _, err := r.svc.DialClient(context.Background(), ownedTestKey, r.payload); !errors.Is(err, unavailableCall) {
		t.Fatal(err)
	}
	if _, closed := r.relay.session(0).Termination(); !closed {
		t.Fatal("media left open")
	}
	if len(r.ended) != 0 || len(r.turns) != 0 {
		t.Fatal("unacknowledged call forwarded")
	}
	if got, _, _ := r.svc.CreationOutcomeSnapshot("api_key:42", "gw-request"); got != "released" {
		t.Fatal(got)
	}
}

func TestOwnedDelegationFailureAppendsSidebandNotice(t *testing.T) {
	r := newOwnedTestRig(t, 201)
	r.forwardStatus = 409
	close(r.release)
	if _, err := r.svc.DialClient(context.Background(), ownedTestKey, r.payload); err != nil {
		t.Fatal(err)
	}
	r.upstream.push(t, delegationEvent("deleg-failed", "please help"))
	if turn := takeOwned(t, r.turns); turn.DelegationID != "deleg-failed" {
		t.Fatalf("turn=%+v", turn)
	}
	f := r.upstream.waitFrames(t, 1)[0]
	if f["type"] != "delegation.context.append" || f["delegation_item_id"] != "deleg-failed" || contentText(t, f) != failedHandoffNote {
		t.Fatalf("frame=%+v", f)
	}
	if turn := takeOwned(t, r.turns); turn.DelegationID != "" || turn.Seq != 0 || turn.Text != "please help" {
		t.Fatalf("degraded=%+v", turn)
	}
}

func TestOwnedLifecycleRetriesIdenticalReports(t *testing.T) {
	r := newOwnedTestRig(t, 201)
	r.establishRetried, r.endRetried = true, true
	close(r.release)
	a, err := r.svc.DialClient(context.Background(), ownedTestKey, r.payload)
	if err != nil {
		t.Fatal(err)
	}
	if first, second := takeOwned(t, r.established), takeOwned(t, r.established); first != second {
		t.Fatal("established binding changed on retry")
	}
	if err := r.svc.HangupClient(context.Background(), ownedTestKey, a.GatewaySessionID); err != nil {
		t.Fatal(err)
	}
	first, _ := json.Marshal(takeOwned(t, r.ended))
	second, _ := json.Marshal(takeOwned(t, r.ended))
	if string(first) != string(second) {
		t.Fatal("end report changed on retry")
	}
}

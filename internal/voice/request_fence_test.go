package voice

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/websocket"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func assertRequestFence(t *testing.T, s *Service, request, want string) {
	t.Helper()
	got, evidence, err := s.FenceCreationRequest(s.zoeCallOwner(), request)
	if got != want || err != nil || (evidence != "") != (want == "released") {
		t.Fatalf("fence result=%q evidence present=%v err=%v, want %q", got, evidence != "", err, want)
	}
}

func TestRequestFenceWinsAgainstLateClaimOrReservation(t *testing.T) {
	s, db, r, _ := newOfflineOwnedCreation(t, 1)
	startEntered, returnStart := make(chan struct{}), make(chan struct{})
	start := s.zoeHTTP.Transport
	s.zoeHTTP.Transport = ownedCreationRoundTrip(func(req *http.Request) (*http.Response, error) {
		close(startEntered)
		<-returnStart
		return start.RoundTrip(req)
	})
	dial := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	done := make(chan error, 1)
	go func() {
		answer, err := s.DialClient(context.Background(), ownedTestKey, dial)
		if answer != nil {
			err = errors.New("unexpected answer")
		}
		done <- err
	}()
	t.Cleanup(func() {
		select {
		case <-returnStart:
		default:
			close(returnStart)
		}
	})
	select {
	case <-startEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("late claim fixture did not enter start")
	}
	if _, err := db.GetVoiceCreationOutcome(s.zoeCallOwner(), "gateway-request"); !errors.Is(err, store.ErrVoiceCreationOutcomeNotFound) {
		t.Fatalf("claim became visible before the start reply: %v", err)
	}
	assertRequestFence(t, s, "gateway-request", "fenced_no_live_media")
	assertRequestFence(t, s, "gateway-request", "fenced_no_live_media")
	close(returnStart)
	select {
	case err := <-done:
		var call *CallError
		if !errors.As(err, &call) || call.Status != 409 || call.Code != "voice_request_fenced" {
			t.Fatalf("late dial: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late dial did not finish")
	}
	if r.count() != 0 {
		t.Fatal("late dial constructed media")
	}
	if _, err := db.ClaimOwnedVoiceCreation(s.zoeCallOwner(), "gateway-request"); !errors.Is(err, store.ErrVoiceCreationFenced) {
		t.Fatalf("late claim: %v", err)
	}
	attempt, err := db.ClaimOwnedVoiceCreation(s.zoeCallOwner(), "unstarted-claim")
	if err != nil {
		t.Fatal(err)
	}
	assertRequestFence(t, s, "unstarted-claim", "fenced_no_live_media")
	id := mediaevidence.Identity{VoiceSessionID: "late-media", MediaGeneration: 1, AccountID: 1, LeaseID: "late-lease", LeaseGeneration: 1}
	if err := s.evidence.(ownedEvidenceStore).ReserveOwned(s.zoeCallOwner(), "unstarted-claim", attempt, id, "late-zoe", 1); !errors.Is(err, store.ErrVoiceCreationFenced) {
		t.Fatalf("late reservation: %v", err)
	}
	var count int
	if err := db.Conn().QueryRow(`SELECT count(*) FROM voice_media_sessions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("late create wrote media: count=%d, err=%v", count, err)
	}
}

type blockedFenceRelay struct {
	entered, finish chan struct{}
	err             error
}

func (r *blockedFenceRelay) NewSession(context.Context, string, relay.Route) (relay.Session, string, error) {
	close(r.entered)
	<-r.finish
	return nil, "", r.err
}

func TestRequestFenceRejectsInFlightCreateWithoutRelayHandle(t *testing.T) {
	s, _, _, _ := newOfflineOwnedCreation(t, 1)
	r := &blockedFenceRelay{entered: make(chan struct{}), finish: make(chan struct{}), err: errors.Join(relay.ErrCreateAfterStart, relay.ErrCreateLocalClosed)}
	s.WithRelay(r)
	dial := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	done := make(chan error, 1)
	go func() {
		_, err := s.DialClient(context.Background(), ownedTestKey, dial)
		done <- err
	}()
	t.Cleanup(func() {
		select {
		case <-r.finish:
		default:
			close(r.finish)
		}
	})
	select {
	case <-r.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("constructor did not start")
	}
	assertRequestFence(t, s, "gateway-request", "busy")
	close(r.finish)
	select {
	case err := <-done:
		if err != unavailableCall {
			t.Fatalf("unexpected dial result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not finish cleanup")
	}
	assertRequestFence(t, s, "gateway-request", "busy") // Local cleanup did not resolve the reservation.
}

func TestRequestFenceCanceledCreateTracksCleanup(t *testing.T) {
	s, db, _, _ := newOfflineOwnedCreation(t, 1)
	r := &blockedFenceRelay{entered: make(chan struct{}), finish: make(chan struct{}), err: relay.ErrCreateClean}
	s.WithRelay(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dial := ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")}
	done := make(chan error, 1)
	go func() {
		_, err := s.DialClient(ctx, ownedTestKey, dial)
		done <- err
	}()
	t.Cleanup(func() {
		select {
		case <-r.finish:
		default:
			close(r.finish)
		}
	})
	select {
	case <-r.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("constructor did not start")
	}
	cancel()
	assertRequestFence(t, s, "gateway-request", "busy")
	request := ownedRequest{s.zoeCallOwner(), "gateway-request"}
	s.requestMu.Lock()
	inFlight := s.requestActivity[request] != nil && s.requestActivity[request].creating
	s.requestMu.Unlock()
	if !inFlight {
		t.Fatal("cancellation dropped in-flight tracking before cleanup")
	}
	// Keep the outcome unresolved even after clean abandonment, so the final
	// result must come from the fence and not from a previously decided claim.
	fixtureSQL(t, db, `CREATE TRIGGER fail_outcome BEFORE UPDATE OF outcome ON voice_creation_outcomes BEGIN SELECT RAISE(FAIL, 'fixture'); END`)
	close(r.finish)
	select {
	case err := <-done:
		if err != unavailableCall {
			t.Fatalf("unexpected dial result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not finish cleanup")
	}
	assertRequestFence(t, s, "gateway-request", "fenced_no_live_media")
}

func TestRequestFenceWaitsForSidebandCleanupAfterDialReturns(t *testing.T) {
	s, db, _, _ := newOfflineOwnedCreation(t, 0)
	request := ownedRequest{s.zoeCallOwner(), "cleanup"}
	if _, err := db.ClaimOwnedVoiceCreation(request.owner, request.id); err != nil {
		t.Fatal(err)
	}
	s.requestActivity = map[ownedRequest]*ownedRequestActivity{request: {creating: true}}
	worker := make(chan struct{})
	done := s.trackRequestSidebandCleanup(&ownedCall{owner: request.owner, requestID: request.id}, &avasSidebandWriter{closed: true, done: worker})
	s.finishRequestCreate(request, false)
	assertRequestFence(t, s, request.id, "busy")
	close(worker)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup watcher did not finish")
	}
	assertRequestFence(t, s, request.id, "fenced_no_live_media")
}

func TestLateEstablishmentRejectionClosesRelayWithoutAnswer(t *testing.T) {
	s, _, r, _ := newOfflineOwnedCreation(t, 1)
	setOwnedCurl(t, s, ownedAccepted)
	s.avasDial = func(websocket.Dialer, string, fhttp.Header) (avasSidebandConnection, *fhttp.Response, error) {
		return &diagnosticReader{started: make(chan struct{}), stopped: make(chan struct{})}, nil, nil
	}
	start := s.zoeHTTP.Transport
	s.zoeHTTP.Transport = ownedCreationRoundTrip(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/runner/v1/voice/calls/zoe-call/established" {
			assertRequestFence(t, s, "gateway-request", "busy")
			return ownedCreationResponse(http.StatusConflict, `{"error":{"code":"voice_generation_stale"}}`), nil
		}
		return start.RoundTrip(req)
	})
	answer, err := s.DialClient(context.Background(), ownedTestKey, ClientDialRequest{RequestID: "client-request", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if answer != nil || err != unavailableCall || r.count() != 1 {
		t.Fatalf("stale establishment returned an answer or missed media cleanup: err=%v", err)
	}
	if term, closed := r.session(0).Termination(); !closed || !term.Clean() {
		t.Fatal("stale establishment left the relay live")
	}
	assertRequestFence(t, s, "gateway-request", "released")
}

func TestRequestFenceSurvivesRestartWithoutReleasingAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restore.db")
	open := func() (*Service, *store.DB, *mediaevidence.Store) {
		t.Helper()
		db, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		ev := mediaevidence.NewStore(db)
		if _, err := ev.RecoverUnfinished(); err != nil {
			t.Fatal(err)
		}
		return New(config.Config{ZoeGatewayAPIKeyID: 42}, nil, nil).WithCreationOutcomes(db), db, ev
	}
	s, db, ev := open()
	owner := s.zoeCallOwner()
	attempt, err := db.ClaimOwnedVoiceCreation(owner, "interrupted")
	if err != nil {
		t.Fatal(err)
	}
	id := mediaevidence.Identity{VoiceSessionID: "interrupted-media", MediaGeneration: 1, AccountID: 1, LeaseID: "interrupted-lease", LeaseGeneration: 1}
	if err := ev.ReserveOwned(owner, "interrupted", attempt, id, "interrupted-zoe", 1); err != nil {
		t.Fatal(err)
	}
	if claim, err := db.GetVoiceCreationOutcome(owner, "interrupted"); err != nil || claim.VoiceSessionID.Valid {
		t.Fatal("reservation fixture must not be linked from the claim")
	}
	assertRequestFence(t, s, "interrupted", "busy")
	assertRequestFence(t, s, "unrelated", "fenced_no_live_media")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, db, _ = open()
	assertRequestFence(t, s, "interrupted", "busy")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, db, ev = open()
	assertRequestFence(t, s, "interrupted", "busy")
	assertRequestFence(t, s, "unrelated", "fenced_no_live_media")
	if row, err := ev.Get(id.VoiceSessionID, id.MediaGeneration); err != nil || row.State != mediaevidence.StateUnknown || row.EvidenceID != "" {
		t.Fatalf("fence changed account reservation: state=%q, err=%v", row.State, err)
	}
	second, err := db.ClaimOwnedVoiceCreation(owner, "second")
	if err != nil {
		t.Fatal(err)
	}
	id.VoiceSessionID, id.LeaseID = "second-media", "second-lease"
	if err := ev.ReserveOwned(owner, "second", second, id, "second-zoe", 2); !errors.Is(err, mediaevidence.ErrAccountOccupied) {
		t.Fatalf("second reservation did not preserve account fence: %v", err)
	}
}

func TestRequestFenceAfterUnlinkedReservationTerminates(t *testing.T) {
	s, db, _, _ := newOfflineOwnedCreation(t, 1)
	owner, request := s.zoeCallOwner(), "not-established"
	attempt, err := db.ClaimOwnedVoiceCreation(owner, request)
	if err != nil {
		t.Fatal(err)
	}
	ev := mediaevidence.NewStore(db)
	id := mediaevidence.Identity{VoiceSessionID: "media", MediaGeneration: 1, AccountID: 1, LeaseID: "test-lease", LeaseGeneration: 1}
	if err := ev.ReserveOwned(owner, request, attempt, id, "zoe", 1); err != nil {
		t.Fatal(err)
	}
	assertRequestFence(t, s, request, "busy")
	if err := ev.MarkClosing(id, "failure"); err != nil {
		t.Fatal(err)
	}
	assertRequestFence(t, s, request, "busy")
	if _, err := ev.Close(id, mediaevidence.Components{PeerAClosed: true, PeerBClosed: true, DataChannelsClosed: true, TunnelsClosed: true}, "failure"); err != nil {
		t.Fatal(err)
	}
	assertRequestFence(t, s, request, "fenced_no_live_media")
	assertRequestFence(t, s, request, "fenced_no_live_media")
	id.VoiceSessionID, id.LeaseID = "late-media", "late-lease"
	if err := ev.ReserveOwned(owner, request, attempt, id, "zoe", 1); !errors.Is(err, store.ErrVoiceCreationFenced) {
		t.Fatalf("fenced request reserved after termination: %v", err)
	}
}

package voice

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

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
}

func (r *blockedFenceRelay) NewSession(context.Context, string, relay.Route) (relay.Session, string, error) {
	close(r.entered)
	<-r.finish
	return nil, "", errors.Join(relay.ErrCreateAfterStart, relay.ErrCreateLocalClosed)
}

func TestRequestFenceRejectsInFlightCreateWithoutRelayHandle(t *testing.T) {
	s, _, _, _ := newOfflineOwnedCreation(t, 1)
	r := &blockedFenceRelay{entered: make(chan struct{}), finish: make(chan struct{})}
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, db, _ = open()
	assertRequestFence(t, s, "interrupted", "fenced_no_live_media")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, db, ev = open()
	assertRequestFence(t, s, "interrupted", "fenced_no_live_media")
	if _, err := db.ClaimOwnedVoiceCreation(owner, "interrupted"); !errors.Is(err, store.ErrVoiceCreationFenced) {
		t.Fatal(err)
	}
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

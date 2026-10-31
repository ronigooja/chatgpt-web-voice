package voice

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

type leaseFixture struct {
	*wmFixture
	fr  *fakeRelay
	ev  *mediaevidence.Store
	rec *callsessions.Store
}

func newLeaseFixture(t *testing.T, respond func(w http.ResponseWriter, r *http.Request, c wmCapture)) *leaseFixture {
	t.Helper()
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, respond)
	db := f.pool.DB()
	ev := mediaevidence.NewStore(db)
	rec := callsessions.NewStore(db)
	f.svc.WithMediaEvidence(ev).WithCallSessions(rec)
	return &leaseFixture{wmFixture: f, fr: fr, ev: ev, rec: rec}
}

func (l *leaseFixture) create(t *testing.T) *SessionResult {
	t.Helper()
	res, err := l.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func okAnswer(t *testing.T) func(http.ResponseWriter, *http.Request, wmCapture) {
	return answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated)
}

func (l *leaseFixture) release(res *SessionResult) (*AccountLeaseRelease, error) {
	return l.svc.ReleaseAccountLease("api_key:1", res.AccountLeaseID, res.AccountLeaseGeneration, res.VoiceSessionID, res.VoiceSessionGeneration)
}

func TestLeaseCreateRegistersGeneration(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	if res.VoiceSessionGeneration != 1 || res.AccountLeaseGeneration != 1 {
		t.Fatalf("generations: media=%d lease=%d", res.VoiceSessionGeneration, res.AccountLeaseGeneration)
	}
	row, err := l.ev.Get(res.VoiceSessionID, 1)
	if err != nil || row.State != mediaevidence.StateActive || row.LeaseID != res.AccountLeaseID || row.Owner != "api_key:1" {
		t.Fatalf("media row: %+v %v", row, err)
	}
}

func TestLeaseReleaseReturnsEvidenceForCleanClose(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	out, err := l.release(res)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Terminated || out.EvidenceID == "" || out.ObservedAt == "" ||
		out.LeaseID != res.AccountLeaseID || out.Generation != 1 || out.SessionID != res.VoiceSessionID || out.SessionGeneration != 1 {
		t.Fatalf("unexpected release: %+v", out)
	}
	if out.AccountID != "account-1" {
		t.Fatalf("account id %q", out.AccountID)
	}
	ev, err := l.ev.Evidence(mediaevidence.Identity{VoiceSessionID: res.VoiceSessionID, MediaGeneration: 1, AccountID: 1, LeaseID: res.AccountLeaseID, LeaseGeneration: 1})
	if err != nil || ev.EvidenceID != out.EvidenceID {
		t.Fatalf("evidence not durable: %+v %v", ev, err)
	}
	if got := l.fr.session(0).closedReasons(); len(got) != 1 || got[0] != "released" {
		t.Fatalf("media close reasons %v", got)
	}
}

func TestLeaseRepeatedReleaseIsIdempotent(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	first, err := l.release(res)
	if err != nil {
		t.Fatal(err)
	}
	second, err := l.release(res)
	if err != nil {
		t.Fatalf("repeat release must succeed: %v", err)
	}
	if *first != *second {
		t.Fatalf("repeat release changed the answer:\n%+v\n%+v", first, second)
	}
	if got := l.fr.session(0).closedReasons(); len(got) != 1 {
		t.Fatalf("media closed %d times", len(got))
	}
}

func TestLeaseReleaseAfterPlainSessionReleaseStillHasEvidence(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	if !l.svc.ReleaseSession("api_key:1", res.VoiceSessionID) {
		t.Fatal("session release failed")
	}
	out, err := l.release(res)
	if err != nil || !out.Terminated {
		t.Fatalf("evidence must survive the binding being gone: %+v %v", out, err)
	}
}

func TestLeaseReleaseAfterPeerAClosesStillReturnsEvidence(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	if err := l.fr.session(0).CloseWithReason("peer_a_closed"); err != nil {
		t.Fatal(err)
	}
	if released, already := l.svc.ReleaseSessionStatus("api_key:1", res.VoiceSessionID); !released || !already {
		t.Fatalf("DELETE after relay self-termination: released=%v already=%v", released, already)
	}
	first, err := l.release(res)
	if err != nil || !first.Terminated || first.EvidenceID == "" {
		t.Fatalf("self-terminated release: %+v %v", first, err)
	}
	second, err := l.release(res)
	if err != nil || *first != *second {
		t.Fatalf("repeat self-terminated release: first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestLeaseStaleOrWrongIdentityIsRejected(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	cases := []struct {
		name       string
		lease      string
		leaseGen   int64
		session    string
		sessionGen int64
		status     int
	}{
		{"wrong lease id", "lease-other", 1, res.VoiceSessionID, 1, http.StatusConflict},
		{"stale lease generation", res.AccountLeaseID, 2, res.VoiceSessionID, 1, http.StatusConflict},
		{"stale media generation", res.AccountLeaseID, 1, res.VoiceSessionID, 2, http.StatusServiceUnavailable},
		{"unknown session", res.AccountLeaseID, 1, "vs_unknown", 1, http.StatusServiceUnavailable},
		{"zero generation", res.AccountLeaseID, 0, res.VoiceSessionID, 1, http.StatusBadRequest},
		{"empty lease", "", 1, res.VoiceSessionID, 1, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := l.svc.ReleaseAccountLease("api_key:1", tc.lease, tc.leaseGen, tc.session, tc.sessionGen)
			se := serviceError(t, err)
			if se.StatusCode != tc.status {
				t.Fatalf("status %d want %d (%+v)", se.StatusCode, tc.status, se)
			}
		})
	}
	row, _ := l.ev.Get(res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateActive {
		t.Fatalf("rejected releases must not touch the session: %+v", row)
	}
	if got := l.fr.session(0).closedReasons(); len(got) != 0 {
		t.Fatalf("rejected release closed media: %v", got)
	}
}

func TestLeaseOtherOwnerCannotReleaseOrLearnOfSession(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	_, err := l.svc.ReleaseAccountLease("api_key:2", res.AccountLeaseID, 1, res.VoiceSessionID, 1)
	se := serviceError(t, err)
	if se.StatusCode != http.StatusServiceUnavailable || !strings.Contains(se.Message, "termination evidence") {
		t.Fatalf("other owner must get the generic fail-closed answer, got %+v", se)
	}
	if got := l.fr.session(0).closedReasons(); len(got) != 0 {
		t.Fatalf("other owner closed the media: %v", got)
	}
}

func TestLeaseCloseErrorStaysBlocked(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	l.fr.session(0).termination = &relay.Termination{
		PeerA:        relay.ComponentResult{Closed: true},
		PeerB:        relay.ComponentResult{Closed: false, Err: errors.New("synthetic close failure")},
		DataChannels: relay.ComponentResult{Closed: true},
		Tunnels:      relay.ComponentResult{Closed: true},
	}
	_, err := l.release(res)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("close error must stay blocked: %+v", se)
	}
	row, _ := l.ev.Get(res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateUnknown || row.EvidenceID != "" {
		t.Fatalf("row must be unknown without evidence: %+v", row)
	}
	// Retrying must not turn unknown into terminated.
	if _, err := l.release(res); err == nil {
		t.Fatal("retry after an unclean close must stay blocked")
	}
}

func TestLeaseEvidenceWriteFailureStaysBlocked(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	if _, err := l.pool.DB().Conn().Exec(`DROP TABLE voice_termination_evidence`); err != nil {
		t.Fatal(err)
	}
	_, err := l.release(res)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("evidence write failure must stay blocked: %+v", se)
	}
	row, _ := l.ev.Get(res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateUnknown {
		t.Fatalf("row must be unknown: %+v", row)
	}
}

func TestLeaseClientDirectSessionHasNoEvidence(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, okAnswer(t))
	db := f.pool.DB()
	f.svc.WithMediaEvidence(mediaevidence.NewStore(db)).WithCallSessions(callsessions.NewStore(db))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	if res.VoiceSessionGeneration != 0 {
		t.Fatalf("client-direct session must not advertise a media generation, got %d", res.VoiceSessionGeneration)
	}
	_, err = f.svc.ReleaseAccountLease("api_key:1", res.AccountLeaseID, 1, res.VoiceSessionID, 1)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("client-direct media must stay fail-closed: %+v", se)
	}
}

func TestLeaseNoEvidenceStoreStaysFailClosed(t *testing.T) {
	f, _ := newRelayFixture(t, []string{fixtureTokenA}, 1, okAnswer(t))
	_, err := f.svc.ReleaseAccountLease("api_key:1", "lease", 1, "vs_x", 1)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %+v", se)
	}
}

func TestLeaseMediaEndedByPeerFailureIsNotTerminatedUnlessClean(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	// The client vanished: relay closes itself with all components clean.
	_ = l.fr.session(0).CloseWithReason("peer_a_failed")
	row, _ := l.ev.Get(res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateTerminated || row.CloseReason != "peer_a_failed" {
		t.Fatalf("self-close must be recorded: %+v", row)
	}
	out, err := l.release(res)
	if err != nil || !out.Terminated {
		t.Fatalf("release after self-close: %+v %v", out, err)
	}
}

func TestLeaseRestartLeavesUnknownAndBlocked(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	res := l.create(t)
	// Simulate a restart: the new process has only the session row.
	if n, err := l.ev.RecoverUnfinished(); err != nil || n != 1 {
		t.Fatalf("recover n=%d err=%v", n, err)
	}
	restarted := New(l.svc.cfg, l.pool, nil).WithMediaEvidence(l.ev).WithCallSessions(l.rec)
	_, err := restarted.ReleaseAccountLease("api_key:1", res.AccountLeaseID, 1, res.VoiceSessionID, 1)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("restart must stay blocked: %+v", se)
	}
}

func TestLeaseSupersedeKeepsPerGenerationEvidence(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	offer := readTestdata(t, "wm_offer.sdp")
	first, err := l.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer})
	if err != nil {
		t.Fatal(err)
	}
	second, err := l.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer, VoiceSessionID: first.VoiceSessionID})
	if err != nil {
		t.Fatal(err)
	}
	if first.VoiceSessionGeneration != 1 || second.VoiceSessionGeneration != 2 {
		t.Fatalf("generations %d, %d", first.VoiceSessionGeneration, second.VoiceSessionGeneration)
	}
	waitClosed(t, l.fr.session(0))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if row, _ := l.ev.Get(first.VoiceSessionID, 1); row.State == mediaevidence.StateTerminated {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if row, _ := l.ev.Get(first.VoiceSessionID, 1); row.State != mediaevidence.StateTerminated || row.CloseReason != "superseded" {
		t.Fatalf("generation 1: %+v", row)
	}
	if row, _ := l.ev.Get(first.VoiceSessionID, 2); row.State != mediaevidence.StateActive {
		t.Fatalf("generation 2 must stay active: %+v", row)
	}
	// Releasing generation 1 returns its own evidence; generation 2 keeps running.
	out, err := l.svc.ReleaseAccountLease("api_key:1", first.AccountLeaseID, 1, first.VoiceSessionID, 1)
	if err != nil || !out.Terminated || out.SessionGeneration != 1 {
		t.Fatalf("generation 1 release: %+v %v", out, err)
	}
	if got := l.fr.session(1).closedReasons(); len(got) != 0 {
		t.Fatalf("releasing generation 1 closed generation 2: %v", got)
	}
}

func TestLeaseRegisterFailureRejectsSession(t *testing.T) {
	l := newLeaseFixture(t, okAnswer(t))
	if _, err := l.pool.DB().Conn().Exec(`DROP TABLE voice_media_sessions`); err != nil {
		t.Fatal(err)
	}
	_, err := l.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unrecordable session must be refused: %+v", se)
	}
	if got := waitClosed(t, l.fr.session(0)); got[0] != "evidence_register_failed" {
		t.Fatalf("media must be closed: %v", got)
	}
}

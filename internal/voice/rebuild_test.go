package voice

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

type rebuildFixture struct {
	*leaseFixture
	res *SessionResult
	m   *fakeMedia
}

// newRebuildFixture creates one relayed call on a two-account pool.
func newRebuildFixture(t *testing.T, respond func(w http.ResponseWriter, r *http.Request, c wmCapture)) *rebuildFixture {
	t.Helper()
	f, fr := newRelayFixture(t, []string{fixtureTokenA, fixtureTokenB}, 2, respond)
	db := f.pool.DB()
	ev := mediaevidence.NewStore(db)
	rec := callsessions.NewStore(db)
	f.svc.WithMediaEvidence(ev).WithCallSessions(rec)
	f.svc.cfg.SessionTTLSeconds = 180
	f.svc.cfg.RelayRebuildMinutes = 0 // tests trigger rebuilds directly
	f.svc.cfg.RelayRebuildReadySeconds = 5
	f.svc.cfg.RelayHandoffEnabled = true
	f.svc.cfg.RelayHandoffMaxChars = 6000
	lf := &leaseFixture{wmFixture: f, fr: fr, ev: ev, rec: rec}
	res := lf.create(t)
	return &rebuildFixture{leaseFixture: lf, res: res, m: fr.session(0)}
}

func deltaFrame(role, id, text string) string {
	return `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"` + id +
		`\",\"author\":{\"role\":\"` + role + `\"},\"content\":{\"parts\":[\"` + text + `\"]}}}}}}"}`
}

func (r *rebuildFixture) accountOf(t *testing.T) int64 {
	t.Helper()
	r.svc.mu.Lock()
	defer r.svc.mu.Unlock()
	b := r.svc.bindings[r.res.VoiceSessionID]
	if b == nil {
		t.Fatal("binding gone")
	}
	return b.AccountID
}

func TestRebuildSameAccountKeepsConversationAndSkipsHandoff(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.m.emit(deltaFrame("user", "m1", "remember pineapple"))
	r.svc.UpdateSessionContext("api_key:1", r.res.VoiceSessionID, UpstreamContext{ConversationID: "conv-1", ParentMessageID: "msg-1"})
	before := r.accountOf(t)

	out, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Swapped || out.ChangedAccount || out.HandedOff {
		t.Fatalf("same-account rebuild: %+v", out)
	}
	if r.accountOf(t) != before {
		t.Fatal("account must not change")
	}
	if len(r.m.pendingTexts) != 0 {
		t.Fatalf("same account resumes natively, no handoff expected: %v", r.m.pendingTexts)
	}
	reqs := r.requests()
	last := reqs[len(reqs)-1]
	if !strings.Contains(last.Fields["session"], `"conversation_id":"conv-1"`) || !strings.Contains(last.Fields["session"], `"parent_message_id":"msg-1"`) {
		t.Fatalf("same-account rebuild must resume the conversation, session=%q", last.Fields["session"])
	}
	if r.m.committed != 1 {
		t.Fatalf("committed=%d", r.m.committed)
	}
	row, _ := r.ev.Get(r.res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateActive || row.RetiredLegs != 1 || row.RetiredLegsUnclean != 0 {
		t.Fatalf("evidence row after rebuild: %+v", row)
	}
	if got := r.m.closedReasons(); len(got) != 0 {
		t.Fatalf("a rebuild must not close the session: %v", got)
	}
}

func TestRebuildKeepsLeaseIdentityAndMediaGeneration(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err != nil {
		t.Fatal(err)
	}
	out, err := r.release(r.res)
	if err != nil {
		t.Fatalf("lease released with the identity handed out at creation: %v", err)
	}
	if !out.Terminated || out.LeaseID != r.res.AccountLeaseID || out.SessionGeneration != r.res.VoiceSessionGeneration {
		t.Fatalf("release after rebuild: %+v", out)
	}
}

func TestRebuildAccountChangeDropsConversationAndHandsOffText(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	first := r.accountOf(t)
	r.m.emit(deltaFrame("user", "m1", "remember the code word pineapple"))
	r.m.emit(deltaFrame("assistant", "m2", "I will remember pineapple"))
	r.svc.UpdateSessionContext("api_key:1", r.res.VoiceSessionID, UpstreamContext{ConversationID: "conv-1", ParentMessageID: "msg-1"})
	// Take the current account out of rotation (cooldown), forcing a change.
	if _, err := r.pool.RecordOutcome(r.currentToken(t), accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	out, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "account_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Swapped || !out.ChangedAccount || !out.HandedOff {
		t.Fatalf("account-change rebuild: %+v", out)
	}
	if r.accountOf(t) == first {
		t.Fatal("account must change")
	}
	reqs := r.requests()
	last := reqs[len(reqs)-1]
	if strings.Contains(last.Fields["session"], "conversation_id") || strings.Contains(last.Fields["session"], "parent_message_id") {
		t.Fatalf("another account must never be sent the old conversation ids: %q", last.Fields["session"])
	}
	if len(r.m.pendingTexts) != 1 {
		t.Fatalf("expected one handoff text, got %d", len(r.m.pendingTexts))
	}
	h := r.m.pendingTexts[0]
	for _, want := range []string{"User: remember the code word pineapple", "Assistant: I will remember pineapple", "Do not reply"} {
		if !strings.Contains(h, want) {
			t.Fatalf("handoff missing %q:\n%s", want, h)
		}
	}
	if ctxNow, _ := r.svc.SessionContext("api_key:1", r.res.VoiceSessionID); ctxNow.ConversationID != "" {
		t.Fatalf("stored conversation must be cleared after an account change, got %+v", ctxNow)
	}
	row, _ := r.ev.Get(r.res.VoiceSessionID, 1)
	if row.CurrentAccountID == 0 || row.CurrentAccountID == first || row.RetiredLegs != 1 {
		t.Fatalf("evidence must follow the account: %+v", row)
	}
}

func (r *rebuildFixture) currentToken(t *testing.T) string {
	t.Helper()
	r.svc.mu.Lock()
	defer r.svc.mu.Unlock()
	return r.svc.bindings[r.res.VoiceSessionID].AccessToken
}

func TestRebuildHandoffDisabledStillChangesAccountWithoutText(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.cfg.RelayHandoffEnabled = false
	r.m.emit(deltaFrame("user", "m1", "secret text that must not be kept"))
	if _, err := r.pool.RecordOutcome(r.currentToken(t), accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	out, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "account_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Swapped || !out.ChangedAccount || out.HandedOff || len(r.m.pendingTexts) != 0 {
		t.Fatalf("handoff off: %+v texts=%v", out, r.m.pendingTexts)
	}
}

func TestRebuildNewLegRejectedKeepsOldLegAndSession(t *testing.T) {
	calls := 0
	ok := okAnswer(t)
	r := newRebuildFixture(t, func(w http.ResponseWriter, req *http.Request, c wmCapture) {
		calls++
		if calls == 1 {
			ok(w, req, c)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"detail":"synthetic"}`))
	})
	_, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled")
	if err == nil {
		t.Fatal("rejected new leg must be an error")
	}
	if r.m.committed != 0 || len(r.m.aborted) != 1 || r.m.aborted[0] != "upstream_rejected" {
		t.Fatalf("old leg must be kept and the new one aborted: committed=%d aborted=%v", r.m.committed, r.m.aborted)
	}
	if got := r.m.closedReasons(); len(got) != 0 {
		t.Fatalf("session must stay open: %v", got)
	}
	row, _ := r.ev.Get(r.res.VoiceSessionID, 1)
	if row.State != mediaevidence.StateActive || row.RetiredLegs != 0 {
		t.Fatalf("a failed rebuild must not touch evidence: %+v", row)
	}
}

func TestRebuildFailureModesKeepOldLeg(t *testing.T) {
	cases := []struct {
		name  string
		setup func(m *fakeMedia)
		abort string
	}{
		{"begin swap fails", func(m *fakeMedia) { m.beginErr = errors.New("capacity") }, ""},
		{"accept answer fails", func(m *fakeMedia) { m.acceptErr = errors.New("bad answer") }, "accept_answer_failed"},
		{"handoff cannot be delivered", func(m *fakeMedia) { m.textErr = errors.New("channel closed") }, "handoff_failed"},
		{"commit fails", func(m *fakeMedia) { m.commitErr = errors.New("not ready") }, "commit_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRebuildFixture(t, okAnswer(t))
			r.m.emit(deltaFrame("user", "m1", "hello there"))
			tc.setup(r.m)
			if tc.name == "handoff cannot be delivered" || tc.name == "commit fails" {
				if _, err := r.pool.RecordOutcome(r.currentToken(t), accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err == nil {
				t.Fatal("expected an error")
			}
			if r.m.committed != 0 {
				t.Fatal("nothing may be committed on failure")
			}
			if tc.abort != "" && (len(r.m.aborted) == 0 || r.m.aborted[0] != tc.abort) {
				t.Fatalf("aborted=%v want %q", r.m.aborted, tc.abort)
			}
			if got := r.m.closedReasons(); len(got) != 0 {
				t.Fatalf("old leg and session must survive: %v", got)
			}
			if b, _ := r.svc.boundVoiceSession("api_key:1", r.res.VoiceSessionID); b == nil {
				t.Fatal("binding must survive")
			}
		})
	}
}

func TestRebuildNotReadyInTimeKeepsOldLeg(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.cfg.RelayRebuildReadySeconds = 1
	r.m.mu.Lock()
	r.m.swapReady = nil // handled below: SwapReady returns a channel that never closes
	r.m.mu.Unlock()
	blocked := &neverReadyMedia{fakeMedia: r.m}
	r.svc.mu.Lock()
	r.svc.bindings[r.res.VoiceSessionID].media = blocked
	r.svc.mu.Unlock()
	start := time.Now()
	_, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled")
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("expected a not-ready error, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("rebuild must give up after the ready timeout")
	}
	if len(r.m.aborted) != 1 || r.m.aborted[0] != "not_ready" || r.m.committed != 0 {
		t.Fatalf("aborted=%v committed=%d", r.m.aborted, r.m.committed)
	}
}

// neverReadyMedia reports a pending swap that never becomes ready.
type neverReadyMedia struct{ *fakeMedia }

func (n *neverReadyMedia) SwapReady() <-chan struct{} { return make(chan struct{}) }

func TestRebuildUncleanRetiredLegBlocksTerminationEvidence(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.m.legClose = relay.LegTermination{
		Reason:       "swapped",
		PeerB:        relay.ComponentResult{Closed: false, Err: errors.New("synthetic leg close failure")},
		DataChannels: relay.ComponentResult{Closed: true},
		Tunnels:      relay.ComponentResult{Closed: true},
	}
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err != nil {
		t.Fatal(err)
	}
	row, _ := r.ev.Get(r.res.VoiceSessionID, 1)
	if row.RetiredLegs != 1 || row.RetiredLegsUnclean != 1 {
		t.Fatalf("unclean retired leg must be counted: %+v", row)
	}
	_, err := r.release(r.res)
	if se := serviceError(t, err); se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("an unclean retired leg must keep the lease blocked: %+v", se)
	}
	if row, _ := r.ev.Get(r.res.VoiceSessionID, 1); row.State != mediaevidence.StateUnknown || row.EvidenceID != "" {
		t.Fatalf("row must be unknown with no evidence: %+v", row)
	}
}

func TestRebuildAfterReleaseOrUnknownSessionFails(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	if _, err := r.svc.RebuildMedia("api_key:2", r.res.VoiceSessionID, "x"); err == nil {
		t.Fatal("another owner must not rebuild")
	}
	if _, err := r.svc.RebuildMedia("api_key:1", "vs_unknown", "x"); err == nil {
		t.Fatal("unknown session must fail")
	}
	r.svc.ReleaseSession("api_key:1", r.res.VoiceSessionID)
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "x"); err == nil {
		t.Fatal("released session must fail")
	}
	if r.m.committed != 0 {
		t.Fatal("no swap after release")
	}
}

func TestRebuildNoUsableAccountKeepsOldLeg(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	list, _ := r.pool.List()
	for _, a := range list {
		_, tok, _ := r.pool.PickByID(a.ID, nil)
		_ = tok
	}
	// Cool down both accounts.
	for _, a := range list {
		token, _, err := r.pool.PickByID(a.ID, nil)
		if err != nil {
			continue
		}
		if _, err := r.pool.RecordOutcome(token, accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err == nil {
		t.Fatal("no usable account must be an error")
	}
	if r.m.committed != 0 || len(r.m.swapRoutes) != 0 {
		t.Fatal("no leg may be started without an account")
	}
	if got := r.m.closedReasons(); len(got) != 0 {
		t.Fatalf("call must continue on its current leg: %v", got)
	}
}

func TestRebuildRotatesAcrossAccountsWhenCurrentIsUnavailable(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	seen := map[int64]bool{r.accountOf(t): true}
	for i := 0; i < 2; i++ {
		if _, err := r.pool.RecordOutcome(r.currentToken(t), accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		// Free the other account again so rotation can come back.
		list, _ := r.pool.List()
		for _, a := range list {
			if a.ID != r.accountOf(t) {
				if _, err := r.pool.Update(a.ID, accounts.AccountUpdate{Email: a.Email, Status: "正常"}); err != nil {
					t.Fatal(err)
				}
			}
		}
		out, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "account_unavailable")
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if !out.ChangedAccount {
			t.Fatalf("round %d: account must change", i)
		}
		seen[r.accountOf(t)] = true
	}
	if len(seen) != 2 {
		t.Fatalf("rotation must use both accounts, saw %v", seen)
	}
}

func TestRebuildRouteFollowsMediaRouteSwitch(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.cfg.RelayMediaRoute = "direct"
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err != nil {
		t.Fatal(err)
	}
	if len(r.m.swapRoutes) != 1 || r.m.swapRoutes[0].ProxyURL != "" {
		t.Fatalf("direct media route must pass no proxy to the new leg: %+v", r.m.swapRoutes)
	}
}

func TestRebuildLoopRunsOnScheduleAndStopsWithTheCall(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.cfg.RelayRebuildMinutes = 1
	r.svc.mu.Lock()
	call := r.svc.bindings[r.res.VoiceSessionID].call
	r.svc.mu.Unlock()
	if call == nil {
		t.Fatal("relayed call must carry rebuild state")
	}
	// Run one iteration synchronously instead of waiting a minute.
	if _, err := r.svc.RebuildMedia("api_key:1", r.res.VoiceSessionID, "scheduled"); err != nil {
		t.Fatal(err)
	}
	r.svc.ReleaseSession("api_key:1", r.res.VoiceSessionID)
	select {
	case <-call.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("releasing the call must stop its rebuild loop")
	}
}

func TestRebuildLoopDisabledWhenIntervalIsZero(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.mu.Lock()
	call := r.svc.bindings[r.res.VoiceSessionID].call
	r.svc.mu.Unlock()
	r.svc.startRebuildLoop("api_key:1", r.res.VoiceSessionID, r.m, call)
	time.Sleep(100 * time.Millisecond)
	if r.m.committed != 0 || len(r.m.swapRoutes) != 0 {
		t.Fatal("interval 0 must never rebuild")
	}
}

func TestRebuildSupersededCallHaltsOldLoop(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.svc.mu.Lock()
	oldCall := r.svc.bindings[r.res.VoiceSessionID].call
	r.svc.mu.Unlock()
	if _, err := r.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"), VoiceSessionID: r.res.VoiceSessionID}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldCall.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("a superseded generation must stop its rebuild loop")
	}
}

func TestRebuildCallStateIsNotWrittenToDisk(t *testing.T) {
	r := newRebuildFixture(t, okAnswer(t))
	r.m.emit(deltaFrame("user", "m1", "a very private sentence"))
	// Nothing in the evidence or call-session rows may hold conversation text.
	for _, table := range []string{"voice_media_sessions", "voice_termination_evidence", "call_sessions"} {
		rows, err := r.pool.DB().Conn().Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for _, v := range vals {
				if s, ok := v.(string); ok && strings.Contains(s, "private sentence") {
					t.Fatalf("conversation text leaked into %s", table)
				}
			}
		}
		rows.Close()
	}
}

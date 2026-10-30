package voice

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

// fakeUpstream is one loopback Pion peer standing in for /realtime/wm.
type fakeUpstream struct {
	pc     *webrtc.PeerConnection
	dc     *webrtc.DataChannel
	got    chan string
	opened chan struct{}
	once   sync.Once
}

func newFakeUpstream(t *testing.T, api *webrtc.API) *fakeUpstream {
	t.Helper()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	neg, id0 := true, uint16(0)
	dc, err := pc.CreateDataChannel("oai-events", &webrtc.DataChannelInit{Negotiated: &neg, ID: &id0})
	if err != nil {
		t.Fatal(err)
	}
	u := &fakeUpstream{pc: pc, dc: dc, got: make(chan string, 16), opened: make(chan struct{})}
	dc.OnMessage(func(m webrtc.DataChannelMessage) { u.got <- string(m.Data) })
	dc.OnOpen(func() { close(u.opened) })
	t.Cleanup(func() { _ = pc.Close() })
	return u
}

func (u *fakeUpstream) answer(offer string) (string, error) {
	var out string
	var err error
	u.once.Do(func() {
		if err = u.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
			return
		}
		var ans webrtc.SessionDescription
		if ans, err = u.pc.CreateAnswer(nil); err != nil {
			return
		}
		gather := webrtc.GatheringCompletePromise(u.pc)
		if err = u.pc.SetLocalDescription(ans); err != nil {
			return
		}
		<-gather
		out = u.pc.LocalDescription().SDP
	})
	return out, err
}

// TestRebuildEndToEndWithRealPeersChangesAccountAndHandsOff drives the real relay:
// the call starts on one fake upstream, the account is taken out of rotation, a
// rebuild moves the call to a second fake upstream on the other account, and the
// client's channel never closes.
func TestRebuildEndToEndWithRealPeersChangesAccountAndHandsOff(t *testing.T) {
	mkAPI := func() *webrtc.API {
		me := &webrtc.MediaEngine{}
		if err := me.RegisterDefaultCodecs(); err != nil {
			t.Fatal(err)
		}
		se := webrtc.SettingEngine{}
		se.SetIncludeLoopbackCandidate(true)
		return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))
	}
	api := mkAPI()
	ups := []*fakeUpstream{newFakeUpstream(t, api), newFakeUpstream(t, api)}
	var mu sync.Mutex
	call := 0
	respond := func(w http.ResponseWriter, _ *http.Request, c wmCapture) {
		mu.Lock()
		u := ups[call%len(ups)]
		call++
		mu.Unlock()
		ans, err := u.answer(c.Fields["sdp"])
		if err != nil {
			t.Errorf("fake upstream: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, ans)
	}

	f := newWMFixture(t, []string{fixtureTokenA, fixtureTokenB}, 2, respond)
	rl, err := relay.NewForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ev := mediaevidence.NewStore(f.pool.DB())
	f.svc.WithRelay(rl).WithMediaEvidence(ev).WithCallSessions(callsessions.NewStore(f.pool.DB()))
	f.svc.cfg.SessionTTLSeconds = 180
	f.svc.cfg.RelayRebuildMinutes = 0
	f.svc.cfg.RelayRebuildReadySeconds = 20
	f.svc.cfg.RelayHandoffEnabled = true
	f.svc.cfg.RelayHandoffMaxChars = 6000

	client, err := mkAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	neg, id0 := true, uint16(0)
	clientDC, err := client.CreateDataChannel("oai-events", &webrtc.DataChannelInit{Negotiated: &neg, ID: &id0})
	if err != nil {
		t.Fatal(err)
	}
	gotClient := make(chan string, 16)
	clientDC.OnMessage(func(m webrtc.DataChannelMessage) { gotClient <- string(m.Data) })
	clientOpen := make(chan struct{})
	clientDC.OnOpen(func() { close(clientOpen) })
	var clientClosed bool
	var cmu sync.Mutex
	clientDC.OnClose(func() { cmu.Lock(); clientClosed = true; cmu.Unlock() })

	offer, err := client.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(client)
	if err := client.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather

	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: client.LocalDescription().SDP})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: res.AnswerSDP}); err != nil {
		t.Fatal(err)
	}
	wait := func(ch chan struct{}, what string) {
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			t.Fatalf("%s never opened", what)
		}
	}
	wait(clientOpen, "client channel")
	wait(ups[0].opened, "first upstream channel")

	// Conversation text flows upstream -> client; the service records it.
	frame := func(role, id, text string) string {
		return `{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"` + id +
			`\",\"author\":{\"role\":\"` + role + `\"},\"content\":{\"parts\":[\"` + text + `\"]}}}}}}"}`
	}
	if err := ups[0].dc.SendText(frame("user", "m1", "the code word is pineapple")); err != nil {
		t.Fatal(err)
	}
	if err := ups[0].dc.SendText(frame("assistant", "m2", "got it pineapple")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-gotClient:
		case <-time.After(10 * time.Second):
			t.Fatal("conversation events did not reach the client")
		}
	}

	f.svc.mu.Lock()
	token := f.svc.bindings[res.VoiceSessionID].AccessToken
	first := f.svc.bindings[res.VoiceSessionID].AccountID
	f.svc.mu.Unlock()
	if _, err := f.pool.RecordOutcome(token, accounts.Outcome{Kind: accounts.OutcomeRateLimited, RetryAfter: time.Hour, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	out, err := f.svc.RebuildMedia("api_key:1", res.VoiceSessionID, "account_unavailable")
	if err != nil {
		t.Fatalf("rebuild with real peers: %v", err)
	}
	if !out.Swapped || !out.ChangedAccount || !out.HandedOff {
		t.Fatalf("outcome: %+v", out)
	}

	// The handoff text reached the NEW upstream, as a user text message, and
	// carried the recorded conversation.
	var got string
	select {
	case got = <-ups[1].got:
	case <-time.After(10 * time.Second):
		t.Fatal("new upstream did not receive the handoff text")
	}
	var outer struct{ Type, Data string }
	if err := json.Unmarshal([]byte(got), &outer); err != nil || outer.Type != "data_message" {
		t.Fatalf("handoff envelope: %q %v", got, err)
	}
	if !strings.Contains(outer.Data, "relay_message") || !strings.Contains(outer.Data, "pineapple") || !strings.Contains(outer.Data, "Do not reply") {
		t.Fatalf("handoff content: %s", outer.Data)
	}

	// Events from the NEW upstream reach the client; the old upstream is retired.
	if err := ups[1].dc.SendText("from-new-upstream"); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotClient:
		if m != "from-new-upstream" {
			t.Fatalf("client got %q", m)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("client did not receive events from the new upstream")
	}
	// The client's own events now go to the new upstream.
	if err := clientDC.SendText("from-client-after"); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ups[1].got:
		if m != "from-client-after" {
			t.Fatalf("new upstream got %q", m)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("client events did not reach the new upstream")
	}

	cmu.Lock()
	closed := clientClosed
	cmu.Unlock()
	if closed || clientDC.ReadyState() != webrtc.DataChannelStateOpen {
		t.Fatal("the client's channel must stay open across the rebuild")
	}
	f.svc.mu.Lock()
	now := f.svc.bindings[res.VoiceSessionID].AccountID
	f.svc.mu.Unlock()
	if now == first {
		t.Fatal("the call must now be on the other account")
	}

	// The lease handed out at creation still releases, with evidence, after the swap.
	rel, err := f.svc.ReleaseAccountLease("api_key:1", res.AccountLeaseID, res.AccountLeaseGeneration, res.VoiceSessionID, res.VoiceSessionGeneration)
	if err != nil || !rel.Terminated {
		t.Fatalf("release after a real rebuild: %+v %v", rel, err)
	}
	row, _ := ev.Get(res.VoiceSessionID, res.VoiceSessionGeneration)
	if row.State != mediaevidence.StateTerminated || row.RetiredLegs != 1 || row.RetiredLegsUnclean != 0 || !row.Components.Clean() {
		t.Fatalf("evidence after a real rebuild: %+v", row)
	}
}

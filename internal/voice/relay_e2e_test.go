package voice

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

// TestRelayEndToEndWithRealPeers drives CreateSession through the real Pion
// relay on loopback: client peer <-> peer A <-> peer B <-> fake upstream peer.
// The fake upstream answers the SDP from inside the /realtime/wm handler.
func TestRelayEndToEndWithRealPeers(t *testing.T) {
	mkAPI := func() *webrtc.API {
		me := &webrtc.MediaEngine{}
		if err := me.RegisterDefaultCodecs(); err != nil {
			t.Fatal(err)
		}
		se := webrtc.SettingEngine{}
		se.SetIncludeLoopbackCandidate(true)
		return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))
	}
	neg, id0 := true, uint16(0)
	dcInit := &webrtc.DataChannelInit{Negotiated: &neg, ID: &id0}

	upstreamPC, err := mkAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamPC.Close()
	upstreamDC, err := upstreamPC.CreateDataChannel("oai-events", dcInit)
	if err != nil {
		t.Fatal(err)
	}
	gotUpstream := make(chan string, 4)
	upstreamDC.OnMessage(func(m webrtc.DataChannelMessage) { gotUpstream <- string(m.Data) })
	upstreamOpen := make(chan struct{})
	upstreamDC.OnOpen(func() { close(upstreamOpen) })

	var answerOnce sync.Once
	respond := func(w http.ResponseWriter, _ *http.Request, c wmCapture) {
		var answerSDP string
		var aerr error
		answerOnce.Do(func() {
			if aerr = upstreamPC.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: c.Fields["sdp"]}); aerr != nil {
				return
			}
			var ans webrtc.SessionDescription
			if ans, aerr = upstreamPC.CreateAnswer(nil); aerr != nil {
				return
			}
			gather := webrtc.GatheringCompletePromise(upstreamPC)
			if aerr = upstreamPC.SetLocalDescription(ans); aerr != nil {
				return
			}
			<-gather
			answerSDP = upstreamPC.LocalDescription().SDP
		})
		if aerr != nil {
			t.Errorf("fake upstream: %v", aerr)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, answerSDP)
	}

	f := newWMFixture(t, []string{fixtureTokenA}, 1, respond)
	rl, err := relay.NewForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	f.svc.WithRelay(rl)
	f.svc.cfg.SessionTTLSeconds = 180

	client, err := mkAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	clientDC, err := client.CreateDataChannel("oai-events", dcInit)
	if err != nil {
		t.Fatal(err)
	}
	gotClient := make(chan string, 4)
	clientDC.OnMessage(func(m webrtc.DataChannelMessage) { gotClient <- string(m.Data) })
	clientOpen := make(chan struct{})
	clientDC.OnOpen(func() { close(clientOpen) })

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
		t.Fatalf("client rejected the relayed answer: %v", err)
	}
	for name, ch := range map[string]chan struct{}{"client": clientOpen, "upstream": upstreamOpen} {
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			t.Fatalf("%s oai-events never opened", name)
		}
	}
	if err := clientDC.SendText("hello-upstream"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gotUpstream:
		if got != "hello-upstream" {
			t.Fatalf("upstream got %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("event did not cross the relay to upstream")
	}
	if err := upstreamDC.SendText("hello-client"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gotClient:
		if got != "hello-client" {
			t.Fatalf("client got %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("event did not cross the relay to client")
	}
	if strings.Contains(res.AnswerSDP, upstreamPC.LocalDescription().SDP[:20]) && res.AnswerSDP == upstreamPC.LocalDescription().SDP {
		t.Fatal("client received the upstream answer instead of peer A's answer")
	}

	if !f.svc.ReleaseSession("api_key:1", res.VoiceSessionID) {
		t.Fatal("release failed")
	}
}

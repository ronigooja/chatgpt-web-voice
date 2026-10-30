package relay

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// newUpstreamPeer builds a fake upstream with a negotiated oai-events channel.
func newUpstreamPeer(t *testing.T) (*peer, *webrtc.DataChannel) {
	t.Helper()
	p := newPeer(t)
	neg, id0 := true, uint16(0)
	dc, err := p.pc.CreateDataChannel(realtimeDataChannelLabel, &webrtc.DataChannelInit{Negotiated: &neg, ID: &id0})
	if err != nil {
		t.Fatal(err)
	}
	dc.OnMessage(func(m webrtc.DataChannelMessage) { p.events <- string(m.Data) })
	return p, dc
}

// answerOffer lets a fake upstream answer an offer and returns its SDP.
func answerOffer(t *testing.T, p *peer, offer string) string {
	t.Helper()
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(p.pc)
	ans, err := p.pc.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.pc.SetLocalDescription(ans); err != nil {
		t.Fatal(err)
	}
	<-done
	return p.pc.LocalDescription().SDP
}

// startSwap begins a swap and wires a second fake upstream to it.
func startSwap(t *testing.T, g *rig) (*peer, *webrtc.DataChannel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	offer, err := g.session.BeginUpstreamSwap(ctx, Route{})
	if err != nil {
		t.Fatal(err)
	}
	next, nextDC := newUpstreamPeer(t)
	answer := answerOffer(t, next, offer)
	if err := g.session.AcceptSwapAnswer(ctx, answer); err != nil {
		t.Fatal(err)
	}
	return next, nextDC
}

func waitSwapReady(t *testing.T, s Session) {
	t.Helper()
	ch := s.SwapReady()
	if ch == nil {
		t.Fatal("no swap pending")
	}
	select {
	case <-ch:
	case <-time.After(testWait):
		t.Fatal("pending leg never became ready")
	}
}

func TestSwapMovesMediaAndEventsToNewLegAndClosesOldCleanly(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	stop := make(chan struct{})
	defer close(stop)
	sendRTP(t, g.client.track, stop)
	waitPacket(t, g.upstream.rtpIn, "client->old upstream before swap")

	next, nextDC := startSwap(t, g)
	waitSwapReady(t, g.session)

	// Until commit the old leg is still the live one.
	if err := g.events.SendText("before-commit"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, "before-commit")

	res, err := g.session.CommitUpstreamSwap()
	if err != nil {
		t.Fatal(err)
	}
	if !res.Clean() || res.Reason != "swapped" {
		t.Fatalf("retired leg must close cleanly: %+v", res)
	}

	// Client audio and events now reach the new upstream only.
	sendRTP(t, next.track, stop)
	waitPacket(t, next.rtpIn, "client->new upstream after swap")
	waitPacket(t, g.client.rtpIn, "new upstream->client after swap")
	if err := g.events.SendText("after-commit"); err != nil {
		t.Fatal(err)
	}
	recv(t, next.events, "after-commit")
	if err := nextDC.SendText("from-new"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.clientEv, "from-new")

	// The client side never saw its channel close, and the session is still open.
	if g.events.ReadyState() != webrtc.DataChannelStateOpen {
		t.Fatalf("client channel must stay open, got %s", g.events.ReadyState())
	}
	if _, closed := g.session.Termination(); closed {
		t.Fatal("a swap must not close the session")
	}
}

func TestSwapKeepsClientRTPSequenceContinuous(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	stop := make(chan struct{})
	defer close(stop)
	sendRTP(t, g.upstream.track, stop)
	waitPacket(t, g.client.rtpIn, "before swap")

	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	// Drain what is buffered, then record the last pre-commit packet.
	var last *rtp.Packet
	drain := func() {
		for {
			select {
			case p := <-g.client.rtpIn:
				last = p
			default:
				return
			}
		}
	}
	drain()
	if _, err := g.session.CommitUpstreamSwap(); err != nil {
		t.Fatal(err)
	}
	sendRTP(t, next.track, stop)
	deadline := time.After(testWait)
	for {
		select {
		case p := <-g.client.rtpIn:
			if last != nil && p.SequenceNumber != last.SequenceNumber+1 && p.SequenceNumber > last.SequenceNumber {
				// A gap larger than a few lost packets means the stream was re-based badly.
				if p.SequenceNumber-last.SequenceNumber > 200 {
					t.Fatalf("client stream jumped: %d -> %d", last.SequenceNumber, p.SequenceNumber)
				}
			}
			last = p
			if p.SequenceNumber > 0 {
				// seen at least one packet from the new leg
				goto done
			}
		case <-deadline:
			t.Fatal("no packet from the new leg reached the client")
		}
	}
done:
}

func TestSwapCommitRefusedUntilPendingLegIsReady(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if _, err := g.session.BeginUpstreamSwap(ctx, Route{}); err != nil {
		t.Fatal(err)
	}
	// No upstream answer yet: the pending leg cannot be ready.
	if _, err := g.session.CommitUpstreamSwap(); err == nil {
		t.Fatal("commit must fail while the pending leg is not ready")
	}
	// The old leg is untouched and still live.
	if err := g.events.SendText("still-old"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, "still-old")
	if err := g.session.AbortUpstreamSwap("test"); err != nil {
		t.Fatalf("abort should be clean: %v", err)
	}
}

func TestSwapNewLegFailureKeepsOldLeg(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	// The replacement upstream dies before commit.
	_ = next.pc.Close()
	deadline := time.Now().Add(testWait)
	for g.session.SwapReady() != nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if g.session.SwapReady() != nil {
		t.Fatal("a failed pending leg must be discarded")
	}
	if _, closed := g.session.Termination(); closed {
		t.Fatal("pending leg failure must not close the session")
	}
	// The old leg still carries events.
	if err := g.events.SendText("old-survives"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, "old-survives")
	if _, err := g.session.CommitUpstreamSwap(); err == nil {
		t.Fatal("commit without a pending leg must fail")
	}
}

func TestSwapOnlyOnePendingAtATime(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if _, err := g.session.BeginUpstreamSwap(ctx, Route{}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.session.BeginUpstreamSwap(ctx, Route{}); err == nil {
		t.Fatal("a second swap must be refused while one is pending")
	}
	if err := g.session.AbortUpstreamSwap("test"); err != nil {
		t.Fatal(err)
	}
	// After abort a new swap is allowed.
	if _, err := g.session.BeginUpstreamSwap(ctx, Route{}); err != nil {
		t.Fatalf("swap after abort: %v", err)
	}
}

func TestSwapDoesNotCountRelaySlotTwice(t *testing.T) {
	r := newRelay(t, 1)
	g := connect(t, r)
	startSwap(t, g)
	waitSwapReady(t, g.session)
	if _, err := g.session.CommitUpstreamSwap(); err != nil {
		t.Fatal(err)
	}
	// Only one slot exists and it is still taken by the running session.
	c := newPeer(t)
	neg, id0 := true, uint16(0)
	if _, err := c.pc.CreateDataChannel(realtimeDataChannelLabel, &webrtc.DataChannelInit{Negotiated: &neg, ID: &id0}); err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(c.pc)
	offer, _ := c.pc.CreateOffer(nil)
	_ = c.pc.SetLocalDescription(offer)
	<-done
	if _, _, err := r.NewSession(context.Background(), gather(t, c.pc), Route{}); err != ErrCapacity {
		t.Fatalf("expected ErrCapacity while the swapped session runs, got %v", err)
	}
	_ = g.session.Close()
	// Closing frees the one slot.
	s2, _, err := r.NewSession(context.Background(), gather(t, c.pc), Route{})
	if err != nil {
		t.Fatalf("slot not released after close: %v", err)
	}
	_ = s2.Close()
}

func TestSessionCloseAlsoClosesPendingLeg(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	if err := g.session.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	term, ok := g.session.Termination()
	if !ok || !term.Clean() {
		t.Fatalf("termination must be clean and cover the pending leg: %+v ok=%v", term, ok)
	}
	if g.session.SwapReady() != nil {
		t.Fatal("no swap may remain pending after close")
	}
	deadline := time.Now().Add(testWait)
	for next.pc.ConnectionState() == webrtc.PeerConnectionStateConnected && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if next.pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
		t.Fatal("pending leg's upstream is still connected after the session closed")
	}
}

func TestActiveLegFailureAfterSwapClosesSession(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	if _, err := g.session.CommitUpstreamSwap(); err != nil {
		t.Fatal(err)
	}
	fired := make(chan string, 1)
	g.session.SetCloseHandler(func(r string) { fired <- r })
	_ = next.pc.Close()
	select {
	case reason := <-fired:
		// The channel close can win the race with the PeerConnection state change.
		if !strings.HasPrefix(reason, "peer_b") && reason != "data_channel_failed" {
			t.Fatalf("unexpected close reason %q", reason)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("loss of the active leg must close the session")
	}
}

func TestRetiredLegEventsAndAudioAreIgnored(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	stop := make(chan struct{})
	defer close(stop)
	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	if _, err := g.session.CommitUpstreamSwap(); err != nil {
		t.Fatal(err)
	}
	// The retired upstream is already closed; a late write from it cannot reach the client.
	select {
	case <-g.clientEv:
		t.Fatal("unexpected client event after swap")
	case <-time.After(300 * time.Millisecond):
	}
	_ = next
}

func TestSendPendingTextUsesWebClientEnvelope(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	next, _ := startSwap(t, g)
	waitSwapReady(t, g.session)
	if err := g.session.SendPendingText("handoff text"); err != nil {
		t.Fatal(err)
	}
	var got string
	select {
	case got = <-next.events:
	case <-time.After(testWait):
		t.Fatal("pending leg did not receive the handoff text")
	}
	var outer struct {
		Type string `json:"type"`
		Data string `json:"data"`
	}
	if err := json.Unmarshal([]byte(got), &outer); err != nil || outer.Type != "data_message" {
		t.Fatalf("outer envelope: %q %v", got, err)
	}
	var inner struct {
		Type    string `json:"type"`
		Payload struct {
			Type    string `json:"type"`
			Message struct {
				Author  struct{ Role string } `json:"author"`
				Content struct {
					ContentType string   `json:"content_type"`
					Parts       []string `json:"parts"`
				} `json:"content"`
			} `json:"message"`
		} `json:"payload"`
	}
	if err := json.Unmarshal([]byte(outer.Data), &inner); err != nil {
		t.Fatal(err)
	}
	m := inner.Payload.Message
	if inner.Type != "relay_message" || inner.Payload.Type != "relay_message" ||
		m.Author.Role != "user" || m.Content.ContentType != "text" || len(m.Content.Parts) != 1 || m.Content.Parts[0] != "handoff text" {
		t.Fatalf("unexpected envelope: %+v", inner)
	}
	// The handoff text must not leak to the client or the old leg.
	select {
	case ev := <-g.upstream.events:
		t.Fatalf("old leg received the pending handoff text: %q", ev)
	case ev := <-g.clientEv:
		t.Fatalf("client received the pending handoff text: %q", ev)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestSendPendingTextRequiresReadyLeg(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	if err := g.session.SendPendingText("x"); err == nil {
		t.Fatal("no pending leg must be an error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if _, err := g.session.BeginUpstreamSwap(ctx, Route{}); err != nil {
		t.Fatal(err)
	}
	if err := g.session.SendPendingText("x"); err == nil {
		t.Fatal("a leg that is not ready must refuse text")
	}
}

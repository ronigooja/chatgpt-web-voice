package relay

import (
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestLargeMessagesPassBothIngressDirections(t *testing.T) {
	g := connect(t, newRelay(t, 1))
	payload := strings.Repeat("x", 300<<10)
	// The loopback peers advertise a sufficiently large SCTP maximum. Other
	// peers may negotiate a smaller limit or fail during transport or send.
	if err := g.events.SendText(payload); err != nil {
		t.Fatal(err)
	}
	recvLarge(t, g.upstream.events, payload)
	if err := g.upstreamEvents.SendText(payload); err != nil {
		t.Fatal(err)
	}
	recvLarge(t, g.clientEv, payload)
	if tr, ok := g.session.Termination(); ok {
		t.Fatalf("session terminated: %+v", tr)
	}
}

func TestLargeSidebandTransportLimit(t *testing.T) {
	g := connectWithOffer(t, newRelay(t, 1), func(offer string) string {
		const original = "a=max-message-size:1073741823"
		if !strings.Contains(offer, original) {
			t.Fatal("unexpected client SDP limit")
		}
		return strings.Replace(offer, original, "a=max-message-size:262144", 1)
	})
	session := g.session.(*pionSession)
	select {
	case <-session.bridge.upToDown.ready:
	case <-time.After(testWait):
		t.Fatal("relay downstream channel not open")
	}
	if got := session.downstream.SCTP().GetCapabilities().MaxMessageSize; got != mediaDataMessageMaxSize {
		t.Fatalf("relay SCTP send limit = %d", got)
	}
	reason := make(chan string, 1)
	g.session.SetCloseHandler(func(s string) { reason <- s })
	if err := g.session.(SidebandEventInjector).InjectSidebandEvent([]byte(strings.Repeat("x", 300<<10))); err != nil {
		t.Fatal(err)
	}
	// Injection queues the frame; this negotiated limit causes Pion's later
	// send to fail, which still closes the call through data_channel_failed.
	select {
	case got := <-reason:
		if got != "data_channel_failed" {
			t.Fatalf("reason %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("expected transport send failure")
	}
}

func TestLargeSidebandDefaultLoopback(t *testing.T) {
	g := connect(t, newRelay(t, 1))
	session := g.session.(*pionSession)
	select {
	case <-session.bridge.upToDown.ready:
	case <-time.After(testWait):
		t.Fatal("relay downstream channel not open")
	}
	if g.events.ReadyState() != webrtc.DataChannelStateOpen {
		t.Fatal("client channel not open")
	}
	payload := strings.Repeat("x", 300<<10)
	if err := g.session.(SidebandEventInjector).InjectSidebandEvent([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	recvLarge(t, g.clientEv, payload)
	if tr, ok := g.session.Termination(); ok {
		t.Fatalf("session terminated: %+v", tr)
	}
}

func recvLarge(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("received message differs: got %d bytes, want %d", len(got), len(want))
		}
	case <-time.After(testWait):
		t.Fatal("timed out waiting for large message")
	}
}

package relay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const testWait = 15 * time.Second

// peer is a loopback Pion peer standing in for the client or the upstream.
type peer struct {
	pc      *webrtc.PeerConnection
	track   *webrtc.TrackLocalStaticRTP
	rtpIn   chan *rtp.Packet
	events  chan string
	chanIn  chan *webrtc.DataChannel
	closedC chan string
}

func newTestAPI(t *testing.T) *webrtc.API {
	t.Helper()
	api, err := newPionAPI(Config{includeLoopback: true}, false, false)
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func newPeer(t *testing.T) *peer {
	t.Helper()
	pc, err := newTestAPI(t).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	track, err := webrtc.NewTrackLocalStaticRTP(opusCodec, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if _, _, err := sender.ReadRTCP(); err != nil {
				return
			}
		}
	}()
	p := &peer{pc: pc, track: track, rtpIn: make(chan *rtp.Packet, 256), events: make(chan string, 64), chanIn: make(chan *webrtc.DataChannel, 8), closedC: make(chan string, 8)}
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			pkt, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			select {
			case p.rtpIn <- pkt:
			default:
			}
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnClose(func() { p.closedC <- dc.Label() })
		if dc.Label() == realtimeDataChannelLabel {
			dc.OnMessage(func(m webrtc.DataChannelMessage) { p.events <- string(m.Data) })
		}
		p.chanIn <- dc
	})
	t.Cleanup(func() { _ = pc.Close() })
	return p
}

func gather(t *testing.T, pc *webrtc.PeerConnection) string {
	t.Helper()
	return pc.LocalDescription().SDP
}

type rig struct {
	relay      *PionRelay
	client     *peer
	upstream   *peer
	session    Session
	events     *webrtc.DataChannel // client's oai-events
	other      *webrtc.DataChannel // client's extra channel
	clientEv   chan string
	eventsOpen chan struct{}
}

func newRelay(t *testing.T, max int) *PionRelay {
	t.Helper()
	r, err := New(Config{MaxSessions: max, includeLoopback: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// connect wires client -> relay (A/B) -> fake upstream, all on loopback.
func connect(t *testing.T, r *PionRelay) *rig {
	t.Helper()
	g := &rig{relay: r, client: newPeer(t), upstream: newPeer(t), clientEv: make(chan string, 64)}
	var err error
	g.events, err = g.client.pc.CreateDataChannel(realtimeDataChannelLabel, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.events.OnMessage(func(m webrtc.DataChannelMessage) { g.clientEv <- string(m.Data) })
	g.eventsOpen = make(chan struct{})
	g.events.OnOpen(func() { close(g.eventsOpen) })
	g.other, err = g.client.pc.CreateDataChannel("other", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(g.client.pc)
	offer, err := g.client.pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.client.pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-done

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	sess, upstreamOffer, err := r.NewSession(ctx, gather(t, g.client.pc), Route{})
	if err != nil {
		t.Fatal(err)
	}
	g.session = sess
	t.Cleanup(func() { _ = sess.Close() })

	if err := g.upstream.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: upstreamOffer}); err != nil {
		t.Fatal(err)
	}
	done = webrtc.GatheringCompletePromise(g.upstream.pc)
	answer, err := g.upstream.pc.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.upstream.pc.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	<-done
	clientAnswer, err := sess.AcceptUpstreamAnswer(ctx, gather(t, g.upstream.pc))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.client.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: clientAnswer}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sess.Ready():
	case <-time.After(testWait):
		t.Fatal("session never became ready")
	}
	select {
	case <-g.eventsOpen:
	case <-time.After(testWait):
		t.Fatal("client oai-events never opened")
	}
	return g
}

func sendRTP(t *testing.T, track *webrtc.TrackLocalStaticRTP, stop <-chan struct{}) {
	go func() {
		var seq uint16
		ts := uint32(0)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				seq++
				ts += 960
				_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: ts, SSRC: 1234}, Payload: []byte{0xf8, 0xff, 0xfe}})
			}
		}
	}()
}

func waitPacket(t *testing.T, ch <-chan *rtp.Packet, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWait):
		t.Fatalf("no RTP received %s", what)
	}
}

func TestBidirectionalRTP(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	stop := make(chan struct{})
	defer close(stop)
	sendRTP(t, g.client.track, stop)
	sendRTP(t, g.upstream.track, stop)
	waitPacket(t, g.upstream.rtpIn, "client->upstream")
	waitPacket(t, g.client.rtpIn, "upstream->client")
}

func recv(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	case <-time.After(testWait):
		t.Fatalf("timeout waiting for %q", want)
	}
}

func TestBidirectionalEvents(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	var upstreamDC *webrtc.DataChannel
	for upstreamDC == nil {
		select {
		case dc := <-g.upstream.chanIn:
			if dc.Label() == realtimeDataChannelLabel {
				upstreamDC = dc
			}
		case <-time.After(testWait):
			t.Fatal("upstream never saw oai-events")
		}
	}
	// the upstream channel is open once the relay is ready
	if err := g.events.SendText("from-client"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, "from-client")
	if err := upstreamDC.SendText("from-upstream"); err != nil {
		t.Fatal(err)
	}
	recv(t, g.clientEv, "from-upstream")
}

func TestOtherChannelLabelsClosed(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	closed := make(chan struct{})
	g.other.OnClose(func() { close(closed) })
	select {
	case <-closed:
	case <-time.After(testWait):
		t.Fatal("unsupported channel was not closed")
	}
	// the supported channel stays usable
	if g.events.ReadyState() == webrtc.DataChannelStateClosed {
		t.Fatal("oai-events closed")
	}
}

func TestOneSidedCloseClosesSession(t *testing.T) {
	for _, side := range []string{"client", "upstream"} {
		t.Run(side, func(t *testing.T) {
			g := connect(t, newRelay(t, 2))
			var reason atomic.Value
			fired := make(chan struct{}, 4)
			g.session.SetCloseHandler(func(r string) { reason.Store(r); fired <- struct{}{} })
			if side == "client" {
				_ = g.client.pc.Close()
			} else {
				_ = g.upstream.pc.Close()
			}
			select {
			case <-fired:
			case <-time.After(30 * time.Second):
				t.Fatal("close handler did not fire after one-sided close")
			}
			tr, ok := g.session.Termination()
			if !ok {
				t.Fatal("no termination recorded")
			}
			if !tr.PeerA.Closed || !tr.PeerB.Closed || !tr.DataChannels.Closed || !tr.Tunnels.Closed {
				t.Fatalf("components not all closed: %+v", tr)
			}
			if tr.Reason != reason.Load().(string) || tr.Reason == "" || tr.Reason == "closed" {
				t.Fatalf("reason %q handler %v", tr.Reason, reason.Load())
			}
			select {
			case <-fired:
				t.Fatal("handler fired twice")
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
}

func TestRepeatedCloseFiresHandlerOnce(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	var n atomic.Int32
	g.session.SetCloseHandler(func(string) { n.Add(1) })
	if _, ok := g.session.Termination(); ok {
		t.Fatal("termination reported before close")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = g.session.CloseWithReason("test") }()
	}
	wg.Wait()
	_ = g.session.Close()
	time.Sleep(300 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("handler fired %d times", n.Load())
	}
	tr, ok := g.session.Termination()
	if !ok || tr.Reason != "test" || !tr.Clean() {
		t.Fatalf("termination %+v ok=%v", tr, ok)
	}
	// handler registered after close fires once, immediately
	var late atomic.Int32
	g.session.SetCloseHandler(func(string) { late.Add(1) })
	g.session.SetCloseHandler(func(string) { late.Add(1) })
	if late.Load() != 0 {
		t.Fatalf("handler already consumed must not fire again, got %d", late.Load())
	}
}

func TestConcurrencyLimit(t *testing.T) {
	r := newRelay(t, 1)
	c := newPeer(t)
	if _, err := c.pc.CreateDataChannel(realtimeDataChannelLabel, nil); err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(c.pc)
	offer, _ := c.pc.CreateOffer(nil)
	_ = c.pc.SetLocalDescription(offer)
	<-done
	sdp := gather(t, c.pc)

	first, _, err := r.NewSession(context.Background(), sdp, Route{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.NewSession(context.Background(), sdp, Route{}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("want ErrCapacity, got %v", err)
	}
	_ = first.Close()
	second, _, err := r.NewSession(context.Background(), sdp, Route{})
	if err != nil {
		t.Fatalf("slot not released: %v", err)
	}
	_ = second.Close()
}

func TestConfigValidate(t *testing.T) {
	bad := []Config{
		{MaxSessions: -1},
		{UDPPortMin: 100},
		{UDPPortMin: 200, UDPPortMax: 100},
		{PublicIP: "nope"},
		{ICEServers: []ICEServer{{}}},
	}
	for i, c := range bad {
		if c.Validate() == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	if err := (Config{}).Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{MaxSessions: -1}, nil); err == nil {
		t.Fatal("New accepted bad config")
	}
}

package relay

import (
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
	"github.com/pion/webrtc/v4"
	"testing"
	"time"
)

func TestEventCaptureRelayPaths(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	store := eventlog.New(eventlog.Defaults())
	key := eventlog.Key{SessionID: "gateway", Generation: 1}
	store.Start(key, "zoe", 3)
	s := g.session.(*pionSession)
	deadlineOpen := time.Now().Add(testWait)
	for g.upstreamEvents.ReadyState() != webrtc.DataChannelStateOpen {
		if time.Now().After(deadlineOpen) {
			t.Fatal("upstream channel did not open")
		}
		time.Sleep(time.Millisecond)
	}
	s.SetEventLog(store, key)
	observed := make(chan struct{}, 1)
	s.SetEventObserver(func([]byte) { observed <- struct{}{} })
	if err := s.InjectSidebandEvent([]byte(`{"type":"sideband"}`)); err != nil {
		t.Fatal(err)
	}
	recv(t, g.clientEv, `{"type":"sideband"}`)
	<-observed
	if err := g.upstreamEvents.SendText(`{"type":"upstream"}`); err != nil {
		t.Fatal(err)
	}
	recv(t, g.clientEv, `{"type":"upstream"}`)
	<-observed
	if err := g.events.SendText(`{"type":"client"}`); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, `{"type":"client"}`)
	if err := s.SendUpstreamEvent([]byte(`{"type":"gateway"}`)); err != nil {
		t.Fatal(err)
	}
	recv(t, g.upstream.events, `{"type":"gateway"}`)
	deadline := time.Now().Add(time.Second)
	for {
		snap, _ := store.Snapshot(key, 0, 20)
		if len(snap.Events) == 4 {
			want := []string{"upstream_to_client", "upstream_to_client", "client_to_upstream", "gateway_to_upstream"}
			for i, e := range snap.Events {
				if e.Direction != want[i] {
					t.Fatalf("events: %+v", snap.Events)
				}
			}
			if snap.ZoeGeneration != 3 {
				t.Fatal(snap.ZoeGeneration)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events: %+v", snap.Events)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEventCaptureStalledSubscriber(t *testing.T) {
	store := eventlog.New(eventlog.Config{MaxEvents: 1, MaxBytes: 512, GlobalBytes: 2048, EventBytes: 128})
	key := eventlog.Key{SessionID: "gateway", Generation: 1}
	store.Start(key, "")
	_, _, cancel, _ := store.Subscribe(key, 0, 1)
	defer cancel()
	s := &pionSession{}
	s.SetEventLog(store, key)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			s.capture("client_to_upstream", "datachannel", "queued", []byte(`{"type":"test"}`), false)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("capture blocked")
	}
	snap, _ := store.Snapshot(key, 0, 1)
	if snap.Dropped == 0 {
		t.Fatal("cap not reached")
	}
	var disabled *eventlog.Store
	s.SetEventLog(disabled, key)
	s.capture("x", "x", "x", []byte("x"), false)
}

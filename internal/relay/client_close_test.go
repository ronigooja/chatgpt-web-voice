package relay

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestClientDataChannelCloseIsNormal(t *testing.T) {
	logs := &lockedLog{}
	r := newRelay(t, 1)
	r.logger = slog.New(slog.NewTextHandler(logs, nil))
	g := connect(t, r)
	reason := make(chan string, 1)
	g.session.SetCloseHandler(func(s string) { reason <- s })
	if err := g.events.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reason:
		if got != "client_data_channel_closed" {
			t.Fatalf("reason = %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("client channel close did not terminate session")
	}
	term, ok := g.session.Termination()
	if !ok || term.Reason != "client_data_channel_closed" {
		t.Fatalf("termination = %+v, ok = %v", term, ok)
	}
	log := logs.String()
	for _, want := range []string{`level=INFO msg="relay session closing"`, `level=INFO msg="relay session closed"`, "reason=client_data_channel_closed"} {
		if !strings.Contains(log, want) {
			t.Errorf("log missing %q: %s", want, log)
		}
	}
	if strings.Contains(log, `msg="relay session failed"`) {
		t.Errorf("unexpected failure log: %s", log)
	}
}

func TestUpstreamDataChannelCloseIsFailure(t *testing.T) {
	g := connect(t, newRelay(t, 1))
	if err := g.events.SendText("ready"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.upstream.events:
	case <-time.After(testWait):
		t.Fatal("upstream channel did not become ready")
	}
	reason := make(chan string, 1)
	g.session.SetCloseHandler(func(s string) { reason <- s })
	if err := g.upstreamEvents.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reason:
		if got != "data_channel_failed" {
			t.Fatalf("reason = %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("upstream channel close did not terminate session")
	}
}

func TestClientDataChannelCloseWithoutHookIsFailure(t *testing.T) {
	g := connect(t, newRelay(t, 1))
	g.session.(*pionSession).bridge.onClientClose = nil
	reason := make(chan string, 1)
	g.session.SetCloseHandler(func(s string) { reason <- s })
	if err := g.events.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reason:
		if got != "data_channel_failed" {
			t.Fatalf("reason = %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("client channel close did not terminate session")
	}
}

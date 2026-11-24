package eventlog

import (
	"strings"
	"testing"
	"time"
)

func TestBoundsAndTruncation(t *testing.T) {
	s := New(Config{MaxEvents: 2, MaxBytes: 4096, GlobalBytes: 8192, EventBytes: 4})
	k := Key{"vs_1", 1}
	s.Start(k, "zoe")
	for _, p := range []string{"abcdef", "12", "34"} {
		s.Append(k, "client_to_upstream", "test", "queued", []byte(p), false)
	}
	x, ok := s.Snapshot(k, 0, 10)
	if !ok || !x.Gap || x.Dropped != 1 || len(x.Events) != 2 || x.Events[0].Seq != 2 {
		t.Fatalf("snapshot: %+v", x)
	}
	s.Append(k, "client_to_upstream", "test", "queued", []byte("abcdef"), false)
	x, _ = s.Snapshot(k, 2, 10)
	e := x.Events[1]
	if !e.Truncated || e.Size != 6 || e.Payload != "abcd" {
		t.Fatalf("event: %+v", e)
	}
}

func TestGlobalEvictionAndIsolation(t *testing.T) {
	s := New(Config{MaxEvents: 10, MaxBytes: 4096, GlobalBytes: 2400, EventBytes: 4})
	a, b := Key{"a", 1}, Key{"b", 1}
	s.Start(a, "")
	s.Append(a, "", "", "", []byte("1234"), false)
	s.End(a)
	s.Start(b, "")
	s.Append(b, "", "", "", []byte("5678"), false)
	if _, ok := s.Snapshot(a, 0, 10); ok {
		t.Fatal("completed generation not evicted")
	}
	x, ok := s.Snapshot(b, 0, 10)
	if !ok || len(x.Events) != 1 || x.Events[0].Payload != "5678" {
		t.Fatalf("snapshot: %+v", x)
	}
}

func TestEstimatedAccounting(t *testing.T) {
	s := New(Config{MaxEvents: 10, MaxBytes: 300, GlobalBytes: 2048})
	k := Key{"a", 1}
	s.Start(k, "")
	s.Append(k, "direction", "source", "delivery", []byte("payload"), false)
	if got, want := s.bytes, generationOverhead+eventOverhead+len("direction")+len("source")+len("delivery")+len("payload")+len("text"); got != want {
		t.Fatalf("bytes: %d, want %d", got, want)
	}
	s.Append(k, "direction", "source", "delivery", []byte("payload"), false)
	x, _ := s.Snapshot(k, 0, 10)
	if x.Dropped != 1 || len(x.Events) != 1 {
		t.Fatalf("snapshot: %+v", x)
	}
}

func TestExpiry(t *testing.T) {
	s := New(Config{TTL: time.Nanosecond})
	k := Key{"a", 1}
	s.Start(k, "")
	s.End(k)
	time.Sleep(time.Millisecond)
	if _, ok := s.Snapshot(k, 0, 10); ok {
		t.Fatal("expired generation retained")
	}
}

func TestStalledSubscriber(t *testing.T) {
	s := New(Config{MaxEvents: 1})
	k := Key{"a", 1}
	s.Start(k, "")
	_, _, cancel, ok := s.Subscribe(k, 0, 10)
	if !ok {
		t.Fatal("subscribe")
	}
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			s.Append(k, "", "", "", []byte(strings.Repeat("x", 10)), false)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("append blocked on subscriber")
	}
	x, _ := s.Snapshot(k, 0, 10)
	if !x.Gap || x.Dropped != 9999 {
		t.Fatalf("snapshot: %+v", x)
	}
}

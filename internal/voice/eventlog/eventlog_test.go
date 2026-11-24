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

func TestAudioNotStored(t *testing.T) {
	s := New(Config{MaxEvents: 1})
	k := Key{"a", 1}
	s.Start(k, "")
	frames := []string{`{"type":"session.input_audio.append","data":"abc"}`, `{"type":"data_message","data":"{\"type\":\"session.output_audio.delta\"}"}`, `{"type":"data_message","data":{"type":"session.input_audio.append"}}`}
	var size int
	for _, frame := range frames {
		size += len(frame)
		s.Append(k, "client_to_upstream", "test", "queued", []byte(frame), false)
	}
	x, _ := s.Snapshot(k, 0, 10)
	if x.AudioEvents != 3 || x.AudioBytes != uint64(size) || x.NewestSeq != 0 || len(x.Events) != 0 || x.Dropped != 0 {
		t.Fatalf("snapshot: %+v", x)
	}
	if s.bytes != generationOverhead {
		t.Fatalf("audio consumed capacity: %d", s.bytes)
	}
	if g := s.List("a"); len(g) != 1 || g[0].AudioEvents != 3 || g[0].AudioBytes != uint64(size) {
		t.Fatalf("generations: %+v", g)
	}
}

func TestEventTypes(t *testing.T) {
	cases := []struct {
		name, payload, typ, wrapper string
		binary                      bool
	}{
		{"wrapped object", `{"type":"data_message","data":{"type":"turn.delta"}}`, "turn.delta", "data_message", false},
		{"wrapped string", `{"type":"data_message","data":"{\"type\":\"output_transcript.added\"}"}`, "output_transcript.added", "data_message", false},
		{"delegation", `{"type":"data_message","data":{"type":"delegation.created"}}`, "delegation.created", "data_message", false},
		{"direct", `{"type":"session.context.append","data":{"type":"other"}}`, "session.context.append", "", false},
		{"text", `not json`, "text", "", false},
		{"binary", `{"type":"data_message"}`, "binary", "", true},
		{"missing inner type", `{"type":"data_message","data":{}}`, "data_message", "", false},
		{"same inner type", `{"type":"data_message","data":{"type":"data_message"}}`, "data_message", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(Config{})
			k := Key{"a", 1}
			s.Start(k, "")
			s.Append(k, "d", "s", "queued", []byte(tc.payload), tc.binary)
			snap, _ := s.Snapshot(k, 0, 10)
			if len(snap.Events) != 1 || snap.Events[0].Type != tc.typ || snap.Events[0].Wrapper != tc.wrapper || snap.Events[0].Payload != tc.payload {
				t.Fatalf("snapshot: %+v", snap)
			}
			if got, want := s.bytes, generationOverhead+eventSize(snap.Events[0]); got != want {
				t.Fatalf("accounting: got %d, want %d", got, want)
			}
		})
	}
}

func TestOversizeJSON(t *testing.T) {
	s := New(Config{EventBytes: 64})
	k := Key{"a", 1}
	s.Start(k, "")
	dropped := `{"type":"data_message","data":"` + strings.Repeat("x", 100) + `"}`
	audio := `{"type":"session.output_audio.delta","data":"` + strings.Repeat("x", 100) + `"}`
	s.Append(k, "", "", "", []byte(dropped), false)
	s.Append(k, "", "", "", []byte(audio), false)
	x, _ := s.Snapshot(k, 0, 10)
	if x.Dropped != 1 || x.AudioEvents != 1 || x.AudioBytes != uint64(len(audio)) || x.NewestSeq != 0 || len(x.Events) != 0 {
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

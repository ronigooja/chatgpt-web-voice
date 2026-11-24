package eventlog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFrameRings(t *testing.T) {
	c := Defaults()
	c.FrameRingSize = 2
	c.AudioRingSize = 2
	s := New(c)
	k := Key{"s", 1}
	s.Start(k, "")
	for _, value := range []string{"one", "two", "three"} {
		s.AppendFrame(k, "client_to_upstream", "datachannel", "queued", []byte(value), false)
	}
	audio := []byte(`{"type":"data_message","data":"{\"type\":\"session.input_audio.append\",\"audio\":\"SECRET_MARKER\"}"}`)
	s.AppendFrame(k, "upstream_to_client", "datachannel", "queued", audio, false)
	s.Append(k, "upstream_to_client", "datachannel", "queued", audio, false)
	snap, ok := s.Frames(k, "all")
	if !ok || snap.Rings["conversation"].SizeFrames != 2 || snap.Rings["conversation"].Dropped != 1 || snap.Rings["audio"].SizeFrames != 1 {
		t.Fatalf("rings: %+v", snap.Rings)
	}
	for i := 0; i < 4; i++ {
		s.AppendFrame(k, "client_to_upstream", "datachannel", "queued", []byte(strings.Repeat("x", oversizeBytes+1)), false)
	}
	snap, _ = s.Frames(k, "oversize")
	if len(snap.Frames) != 3 || snap.Rings["oversize"].Dropped != 1 {
		t.Fatal(snap.Rings)
	}
	s.AppendFrame(k, "client_to_upstream", "datachannel", "queued", []byte(strings.Repeat("x", frameBytes+1)), false)
	snap, _ = s.Frames(k, "conversation")
	f := snap.Frames[len(snap.Frames)-1]
	if !f.Truncated || f.Length != frameBytes+1 || len(f.Payload) != frameBytes {
		t.Fatal("frame cap failed")
	}
	list := s.List("s")
	raw, _ := json.Marshal(list)
	if strings.Contains(string(raw), "SECRET_MARKER") || !strings.Contains(string(raw), "audio_samples") || list[0].AudioEvents != 1 {
		t.Fatalf("audio facts: %s", raw)
	}
}

func TestFrameCaptureAtCapacityDoesNotWaitForSubscribers(t *testing.T) {
	c := Defaults()
	c.FrameRingSize = 2
	c.AudioRingSize = 2
	s := New(c)
	k := Key{"s", 1}
	s.Start(k, "")
	_, _, cancel, _ := s.Subscribe(k, 0, 1)
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			s.AppendFrame(k, "client_to_upstream", "datachannel", "queued", []byte(`{"type":"session.input_audio.append"}`), false)
			s.AppendFrame(k, "client_to_upstream", "datachannel", "queued", []byte("text"), false)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("capture stalled")
	}
}

func TestFrameRingsByteLimitAndDisabled(t *testing.T) {
	c := Defaults()
	c.FrameRingSize = 20
	c.AudioRingSize = 0
	s := New(c)
	k := Key{"s", 1}
	s.Start(k, "")
	for i := 0; i < 10; i++ {
		s.AppendFrame(k, "d", "s", "queued", []byte(strings.Repeat("x", frameBytes)), false)
	}
	s.AppendFrame(k, "d", "s", "queued", []byte(`{"type":"session.output_audio.delta"}`), false)
	snap, _ := s.Frames(k, "all")
	if snap.Rings["conversation"].SizeFrames != 8 || snap.Rings["audio"].SizeFrames != 0 {
		t.Fatal(snap.Rings)
	}
}

func TestAudioFactsByDirectionSourceAndType(t *testing.T) {
	s := New(Defaults())
	k := Key{"s", 1}
	s.Start(k, "")
	input := []byte(`{"type":"session.input_audio.append","audio":"PRIVATE_MARKER"}`)
	output := []byte(`{"type":"session.output_audio.delta","delta":"PRIVATE_MARKER"}`)
	for i := 0; i < 5; i++ {
		s.Append(k, "client_to_upstream", "datachannel", "queued", input, false)
	}
	s.Append(k, "upstream_to_client", "sideband", "queued", output, false)
	large := []byte(`{"type":"session.output_audio.delta","delta":"` + strings.Repeat("x", 70000) + `"}`)
	s.Append(k, "upstream_to_client", "sideband", "queued", large, false)
	snap, _ := s.Snapshot(k, 0, 20)
	if snap.AudioEvents != 7 || len(snap.Audio) != 2 || snap.Audio[0].Events != 5 || snap.Audio[1].Events != 2 || snap.Audio[1].MaxBytes != len(large) || len(snap.AudioSamples) != 5 {
		t.Fatalf("facts: %+v", snap)
	}
	raw, _ := json.Marshal(snap.AudioSamples)
	if strings.Contains(string(raw), "PRIVATE_MARKER") || strings.Contains(string(raw), strings.Repeat("x", 100)) {
		t.Fatal("sample contains values")
	}
}

func TestAudioShapeKeysAndCharsets(t *testing.T) {
	raw := []byte(`{"type":"session.input_audio.append","audio_format":"audio/pcm","encoded":"SGVsbG8=","hex_data":"deadbeef","note":"hello world!","unicode":"你好","mixed":"a\nb","values":[1,true],"123e4567-e89b-12d3-a456-426614174000":"PRIVATE_MARKER","ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij":"PRIVATE_MARKER","data":"{\"nested_field\":\"PRIVATE_MARKER\"}"}`)
	paths := audioShape(raw)
	text := strings.Join(paths, "\n")
	for _, expected := range []string{"audio_format=string(len=9,charset=b64chars)", "note=string(len=12,charset=ascii)", "encoded=string(len=8,charset=b64chars)", "hex_data=string(len=8,charset=hexchars)", "unicode=string(len=6,charset=utf8)", "mixed=string(len=3,charset=mixed)", "values=array(count=2)", "data.<json>.nested_field=string", "<key>=string"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("missing %q in %s", expected, text)
		}
	}
	for _, secret := range []string{"PRIVATE_MARKER", "123e4567-e89b-12d3-a456-426614174000", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"} {
		if strings.Contains(text, secret) {
			t.Fatalf("shape leaked %q", secret)
		}
	}
	for _, tc := range []struct{ key, want string }{
		{"field_name", "field_name"}, {"abcdefabcdefabcdefabcdef", "<key>"}, {"123456789", "<key>"}, {"contains spaces", "<key>"},
	} {
		if got := safeShapeKey(tc.key); got != tc.want {
			t.Fatalf("key %q: %q", tc.key, got)
		}
	}
}

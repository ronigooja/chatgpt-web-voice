package eventlog

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCaptureWAVRotationAndNaming(t *testing.T) {
	dir := t.TempDir()
	c := Defaults()
	c.AudioCapture = true
	c.AudioCaptureDir = dir
	c.AudioCaptureMaxBytes = 1 << 20
	c.AudioCaptureSegment = 180 * time.Second
	s := New(c)
	k := Key{"session.name", 3}
	s.Start(k, "")
	clock := time.Date(2026, 11, 25, 0, 15, 0, 123000000, time.UTC)
	s.capture.now = func() time.Time { return clock }
	input := []byte(`{"type":"data_message","data":"{\"type\":\"session.input_audio.append\",\"audio\":\"AQIDBA==\"}"}`)
	output := []byte(`{"type":"session.output_audio.delta","delta":"BQY="}`)
	s.CaptureAudio(k, input)
	s.CaptureAudio(k, output)
	clock = clock.Add(179900 * time.Millisecond)
	s.CaptureAudio(k, input)
	clock = clock.Add(100 * time.Millisecond)
	s.CaptureAudio(k, input)
	s.End(k)
	s.Close()
	files, err := filepath.Glob(filepath.Join(dir, "session_name", "gen3", "*.wav"))
	if err != nil || len(files) != 3 {
		t.Fatalf("files %d, error %v", len(files), err)
	}
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" || binary.LittleEndian.Uint16(b[20:22]) != 1 || binary.LittleEndian.Uint16(b[22:24]) != 1 || binary.LittleEndian.Uint32(b[24:28]) != 24000 || binary.LittleEndian.Uint16(b[34:36]) != 16 || int(binary.LittleEndian.Uint32(b[40:44])) != len(b)-44 {
			t.Fatal("invalid WAV")
		}
		info, _ := os.Stat(path)
		if info.Mode().Perm() != 0600 {
			t.Fatal("file mode")
		}
	}
	first := filepath.Join(dir, "session_name", "gen3", "20261125T001500.123Z_session_name_gen3_in.wav")
	b, err := os.ReadFile(first)
	if err != nil || string(b[44:]) != "\x01\x02\x03\x04\x01\x02\x03\x04" {
		t.Fatal("input bytes or filename")
	}
	info, _ := os.Stat(filepath.Dir(first))
	if info.Mode().Perm() != 0700 {
		t.Fatal("directory mode")
	}
	if _, ok := sanitizedSession("../escape"); ok {
		t.Fatal("separator accepted")
	}
}
func TestCaptureDisabled(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	c := Defaults()
	c.AudioCaptureDir = dir
	s := New(c)
	s.CaptureAudio(Key{"s", 1}, []byte("anything"))
	s.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("directory created")
	}
}
func TestDecodeAudio(t *testing.T) {
	direction, b, err := decodeAudio([]byte(`{"type":"session.input_audio.append","audio":"AQIDBA=="}`))
	if err != nil || direction != "in" || string(b) != "\x01\x02\x03\x04" {
		t.Fatal("decode failed")
	}
	if got, ok := sanitizedSession("a b.中"); !ok || got != "a_b__" || strings.ContainsAny(got, "/\\") {
		t.Fatal("sanitize failed")
	}
}

func TestCaptureRetentionAndShutdown(t *testing.T) {
	dir := t.TempDir()
	c := Defaults()
	c.AudioCapture = true
	c.AudioCaptureDir = dir
	c.AudioCaptureMaxBytes = 50
	c.AudioCaptureSegment = time.Second
	s := New(c)
	k := Key{"keep", 1}
	s.Start(k, "")
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.capture.now = func() time.Time { return at }
	frame := []byte(`{"type":"session.input_audio.append","audio":"AQIDBA=="}`)
	s.CaptureAudio(k, frame)
	at = at.Add(2 * time.Second)
	s.CaptureAudio(k, frame)
	s.Close()
	files, err := filepath.Glob(filepath.Join(dir, "keep", "gen1", "*.wav"))
	if err != nil || len(files) != 1 {
		t.Fatalf("retention file count %d", len(files))
	}
	b, err := os.ReadFile(files[0])
	if err != nil || binary.LittleEndian.Uint32(b[40:44]) != 4 {
		t.Fatal("shutdown did not finalize")
	}
}
func TestCaptureQueueFullDoesNotBlock(t *testing.T) {
	w := &audioWriter{queue: make(chan audioMessage, 1), now: time.Now}
	w.queue <- audioMessage{}
	s := &Store{capture: w}
	done := make(chan struct{})
	go func() { s.CaptureAudio(Key{"s", 1}, []byte("x")); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capture blocked")
	}
}
func TestCaptureWriteFailureDoesNotEscape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(dir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	c := Defaults()
	c.AudioCapture = true
	c.AudioCaptureDir = dir
	c.AudioCaptureSegment = time.Second
	s := New(c)
	k := Key{"s", 1}
	s.Start(k, "")
	s.CaptureAudio(k, []byte(`{"type":"session.input_audio.append","audio":"AQIDBA=="}`))
	s.End(k)
	s.Close()
}

func TestCaptureEndDoesNotBlockFullQueue(t *testing.T) {
	w := &audioWriter{queue: make(chan audioMessage, 1), wake: make(chan struct{}, 1), now: time.Now}
	w.queue <- audioMessage{}
	done := make(chan struct{})
	go func() { w.end(Key{"s", 1}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("end blocked")
	}
}
func TestRetentionProtectsOpenFile(t *testing.T) {
	dir := t.TempDir()
	older := filepath.Join(dir, "old.wav")
	opened := filepath.Join(dir, "open.wav")
	if err := os.WriteFile(older, make([]byte, 60), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opened, make([]byte, 60), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(older, old, old); err != nil {
		t.Fatal(err)
	}
	w := &audioWriter{cfg: Config{AudioCaptureDir: dir, AudioCaptureMaxBytes: 60}}
	w.retain(map[Key]map[string]*audioSegment{{"s", 1}: {"in": {path: opened}}})
	if _, err := os.Stat(older); !os.IsNotExist(err) {
		t.Fatal("old finished file retained")
	}
	if _, err := os.Stat(opened); err != nil {
		t.Fatal("open file removed")
	}
}
func TestWriteErrorDisablesOnlyThatCall(t *testing.T) {
	dir := t.TempDir()
	c := Defaults()
	c.AudioCapture = true
	c.AudioCaptureDir = dir
	c.AudioCaptureMaxBytes = 1 << 20
	c.AudioCaptureSegment = time.Minute
	if err := os.WriteFile(filepath.Join(dir, "bad"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	s := New(c)
	bad := Key{"bad", 1}
	good := Key{"good", 1}
	s.Start(bad, "")
	s.Start(good, "")
	frame := []byte(`{"type":"session.input_audio.append","audio":"AQIDBA=="}`)
	s.CaptureAudio(bad, frame)
	// A FIFO barrier confirms the failing write was processed before fixing the path.
	barrier := Key{"barrier", 1}
	s.CaptureAudio(barrier, frame)
	s.End(barrier)
	// Closing drains the queue; use the writer wake and a segment on disk as an observable barrier.
	deadline := time.Now().Add(time.Second * 5)
	for {
		files, _ := filepath.Glob(filepath.Join(dir, "barrier", "gen1", "*.wav"))
		if len(files) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not process barrier")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.Remove(filepath.Join(dir, "bad")); err != nil {
		t.Fatal(err)
	}
	s.CaptureAudio(bad, frame)
	s.CaptureAudio(good, frame)
	s.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "bad", "gen1", "*.wav"))
	if len(files) != 0 {
		t.Fatal("failed call resumed capture")
	}
	files, _ = filepath.Glob(filepath.Join(dir, "good", "gen1", "*.wav"))
	if len(files) != 1 {
		t.Fatal("new call did not capture")
	}
}

func TestCaptureByteLimitRotation(t *testing.T) {
	at := time.Now()
	s := &audioSegment{start: at, bytes: maxSegmentBytes - 4}
	if shouldRotate(s, at, time.Hour, 4) || !shouldRotate(s, at, time.Hour, 5) {
		t.Fatal("incorrect byte rotation boundary")
	}
}

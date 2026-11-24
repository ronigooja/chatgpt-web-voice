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

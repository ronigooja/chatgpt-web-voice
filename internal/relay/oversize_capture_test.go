package relay

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOversizeCapture(t *testing.T) {
	dir := t.TempDir()
	c := &oversizeCapture{dir: dir, mediaID: "media", logger: slog.New(slog.DiscardHandler)}
	m := dataChannelMessage{data: []byte("exact\x00bytes"), source: "avas_sideband"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	status, path, truncated, _ := c.write(m, now)
	if status != "written" || truncated {
		t.Fatalf("capture: %s truncated=%v", status, truncated)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, m.data) {
		t.Fatalf("content mismatch: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode: %v %v", info, err)
	}
	status, _, _, _ = c.write(dataChannelMessage{data: []byte("different!!"), source: m.source}, now)
	if status != "error" {
		t.Fatalf("duplicate: %s", status)
	}
	got, _ = os.ReadFile(path)
	if !bytes.Equal(got, m.data) {
		t.Fatal("duplicate overwrote capture")
	}
	for i := 1; i < 4; i++ {
		status, _, _, _ = c.write(m, now.Add(time.Duration(i)))
		if status != "written" {
			t.Fatal(status)
		}
	}
	status, _, _, _ = c.write(m, now.Add(5))
	if status != "skipped_limit" {
		t.Fatal(status)
	}
	disabled := &oversizeCapture{}
	status, _, _, _ = disabled.write(m, now)
	if status != "disabled" {
		t.Fatal(status)
	}
	limited := &oversizeCapture{dir: filepath.Join(dir, "other"), mediaID: "other", capBytes: 3}
	status, path, truncated, _ = limited.write(m, now)
	if status != "written" || !truncated {
		t.Fatal(status, truncated)
	}
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, m.data[:3]) {
		t.Fatal("truncation mismatch", err)
	}
}

func TestOversizeSidebandFailsAndCaptures(t *testing.T) {
	dir := t.TempDir()
	r := newRelay(t, 1)
	r.oversizeCaptureDir = dir
	g := connect(t, r)
	payload := []byte(`{"type":"event","data":"` + strings.Repeat("x", mediaDataMessageMaxSize) + `"}`)
	reason := make(chan string, 1)
	g.session.SetCloseHandler(func(s string) { reason <- s })
	if err := g.session.(SidebandEventInjector).InjectSidebandEvent(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reason:
		if got != "data_channel_failed" {
			t.Fatalf("reason %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("session did not fail")
	}
	deadline := time.Now().Add(testWait)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			// The asynchronous writer may not have closed the file yet.
			got, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
			if err == nil && bytes.Equal(got, payload) {
				if !strings.Contains(entries[0].Name(), "-avas_sideband-") {
					t.Fatal(entries[0].Name())
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("capture missing or incomplete")
}

package relay

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
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
	if status != "written" {
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

func TestCaptureRollingLimitRestartSafe(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := dataChannelMessage{data: make([]byte, mediaDataMessageMaxSize+1), source: "test"}
	unrelated := filepath.Join(dir, "unrelated.bin")
	if err := os.WriteFile(unrelated, nil, 0600); err != nil {
		t.Fatal(err)
	}
	c := &oversizeCapture{dir: dir, mediaID: "media"}
	var paths []string
	for i := 0; i < 33; i++ {
		if i == 31 {
			c = &oversizeCapture{dir: dir, mediaID: "media"}
		}
		status, path, _, _ := c.write(m, now.Add(time.Duration(i)))
		if status != "written" {
			t.Fatal(status)
		}
		paths = append(paths, path)
	}
	for i, path := range paths {
		_, err := os.Stat(path)
		if (i < 3 && !os.IsNotExist(err)) || (i >= 3 && err != nil) {
			t.Fatalf("capture %d: %v", i, err)
		}
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 31 {
		t.Fatalf("entries %d: %v", len(entries), err)
	}
}

func TestCaptureEvictsOldestDespiteCreationOrder(t *testing.T) {
	c := &oversizeCapture{dir: t.TempDir(), mediaID: "media"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := dataChannelMessage{data: []byte("small"), source: "test"}
	var oldest string
	for i := captureLimit - 1; i >= 0; i-- {
		status, path, _, _ := c.write(m, now.Add(time.Duration(i)))
		if status != "written" {
			t.Fatal(status)
		}
		if i == 0 {
			oldest = path
		}
	}
	if status, _, _, _ := c.write(m, now.Add(time.Duration(captureLimit))); status != "written" {
		t.Fatal(status)
	}
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Fatalf("oldest capture: %v", err)
	}
}

func TestConcurrentCapturesRespectLimit(t *testing.T) {
	c := &oversizeCapture{dir: t.TempDir(), mediaID: "media"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := dataChannelMessage{data: []byte("small"), source: "test"}
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if status, _, _, _ := c.write(m, now.Add(time.Duration(i))); status != "written" {
				t.Errorf("capture: %s", status)
			}
		}(i)
	}
	wg.Wait()
	entries, err := os.ReadDir(c.dir)
	if err != nil || len(entries) != captureLimit {
		t.Fatalf("entries %d: %v", len(entries), err)
	}
}

func TestCaptureSizeBoundary(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{OversizeCaptureDir: dir, includeLoopback: true}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	g := connect(t, r)
	for _, size := range []int{5, mediaDataMessageMaxSize, mediaDataMessageMaxSize + 1} {
		payload := strings.Repeat("x", size)
		if err := g.session.(SidebandEventInjector).InjectSidebandEvent([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-g.clientEv:
			if got != payload {
				t.Fatalf("payload size %d differs", size)
			}
		case <-time.After(testWait):
			t.Fatal("message not delivered")
		}
	}
	deadline := time.Now().Add(testWait)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			if len(entries) != 1 {
				t.Fatalf("got %d captures", len(entries))
			}
			data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
			if err == nil && len(data) == mediaDataMessageMaxSize+1 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("boundary capture missing or incomplete")
}

func TestOversizeSidebandFailsAndCaptures(t *testing.T) {
	dir := t.TempDir()
	r := newRelay(t, 1)
	r.oversizeCaptureDir = dir
	logFile, err := os.CreateTemp(t.TempDir(), "relay-log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	r.logger = slog.New(slog.NewJSONHandler(logFile, nil))
	g := connectWithOffer(t, r, func(offer string) string {
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
	if g.events.ReadyState() != webrtc.DataChannelStateOpen {
		t.Fatal("client channel not open")
	}
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
			logData, logErr := os.ReadFile(logFile.Name())
			if err == nil && bytes.Equal(got, payload) && logErr == nil && bytes.Contains(logData, []byte(`"msg":"relay_oversize_message"`)) && bytes.Contains(logData, []byte(`"capture":"written"`)) {
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

func TestDefaultLoopbackAcceptsOversizeSideband(t *testing.T) {
	r := newRelay(t, 1)
	g := connect(t, r)
	session := g.session.(*pionSession)
	select {
	case <-session.bridge.upToDown.ready:
	case <-time.After(testWait):
		t.Fatal("relay downstream channel not open")
	}
	if got := session.downstream.SCTP().GetCapabilities().MaxMessageSize; got != 1073741823 {
		t.Fatalf("default send limit = %d", got)
	}
	payload := []byte(`{"type":"event","data":"` + strings.Repeat("x", mediaDataMessageMaxSize) + `"}`)
	if err := g.session.(SidebandEventInjector).InjectSidebandEvent(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-g.clientEv:
		if got != string(payload) {
			t.Fatal("payload differs")
		}
	case <-time.After(testWait):
		t.Fatal("message was not delivered")
	}
	if tr, ok := g.session.Termination(); ok {
		t.Fatalf("session terminated: %+v", tr)
	}
}

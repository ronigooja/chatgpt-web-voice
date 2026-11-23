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

func TestCaptureRollingQuotas(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	small := dataChannelMessage{data: []byte("small"), source: "test"}
	large := dataChannelMessage{data: make([]byte, mediaDataMessageMaxSize+1), source: "test"}
	unrelated := filepath.Join(dir, "unrelated.bin")
	if err := os.WriteFile(unrelated, nil, 0600); err != nil {
		t.Fatal(err)
	}
	c := &oversizeCapture{dir: dir, mediaID: "media"}
	var paths []string
	for i := 0; i < 32; i++ {
		if i == 31 {
			c = &oversizeCapture{dir: dir, mediaID: "media"}
		}
		status, path, _, _ := c.write(small, now.Add(time.Duration(i)))
		if status != "written" {
			t.Fatal(status)
		}
		paths = append(paths, path)
	}
	for i, path := range paths {
		_, err := os.Stat(path)
		if (i < 2 && !os.IsNotExist(err)) || (i >= 2 && err != nil) {
			t.Fatalf("small %d: %v", i, err)
		}
	}
	var largePaths []string
	for i := 0; i < 20; i++ {
		if i%4 == 0 {
			c = &oversizeCapture{dir: dir, mediaID: "media"}
		}
		status, path, _, _ := c.write(large, now.Add(time.Duration(100+i)))
		if status != "written" {
			t.Fatal(status)
		}
		largePaths = append(largePaths, path)
	}
	if status, _, _, _ := c.write(large, now.Add(200)); status != "skipped_limit" {
		t.Fatal(status)
	}
	c = &oversizeCapture{dir: dir, mediaID: "media"}
	if status, _, _, _ := c.write(large, now.Add(201)); status != "skipped_limit" {
		t.Fatal(status)
	}
	if status, _, _, _ := c.write(small, now.Add(202)); status != "written" {
		t.Fatal(status)
	}
	if _, err := os.Stat(paths[2]); !os.IsNotExist(err) {
		t.Fatalf("oldest small still present: %v", err)
	}
	for _, path := range append(largePaths, unrelated) {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 51 {
		t.Fatalf("entries %d: %v", len(entries), err)
	}
	if (&oversizeCapture{}).threshold() != mediaDataMessageMaxSize {
		t.Fatal("wrong default threshold")
	}
}

func TestCaptureEvictsOldestDespiteCreationOrder(t *testing.T) {
	c := &oversizeCapture{dir: t.TempDir(), mediaID: "media"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := dataChannelMessage{data: []byte("small"), source: "test"}
	var oldest string
	for i := smallCaptureLimit - 1; i >= 0; i-- {
		status, path, _, _ := c.write(m, now.Add(time.Duration(i)))
		if status != "written" {
			t.Fatal(status)
		}
		if i == 0 {
			oldest = path
		}
	}
	if status, _, _, _ := c.write(m, now.Add(time.Duration(smallCaptureLimit))); status != "written" {
		t.Fatal(status)
	}
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Fatalf("oldest capture: %v", err)
	}
}

func TestConcurrentSmallCapturesRespectLimit(t *testing.T) {
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
	if err != nil || len(entries) != smallCaptureLimit {
		t.Fatalf("entries %d: %v", len(entries), err)
	}
}

func TestCaptureThresholdSuccessfulSend(t *testing.T) {
	dir := t.TempDir()
	logFile, err := os.CreateTemp(t.TempDir(), "log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	r, err := New(Config{OversizeCaptureDir: dir, OversizeCaptureThresholdBytes: 5, includeLoopback: true}, slog.New(slog.NewJSONHandler(logFile, nil)))
	if err != nil {
		t.Fatal(err)
	}
	g := connect(t, r)
	for _, payload := range []string{"12345", "123456"} {
		if err := g.session.(SidebandEventInjector).InjectSidebandEvent([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-g.clientEv:
			if got != payload {
				t.Fatalf("got %q", got)
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
		logs, err := os.ReadFile(logFile.Name())
		if err == nil && len(entries) == 1 && bytes.Contains(logs, []byte(`"capture":"written"`)) {
			data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
			if err != nil || string(data) != "123456" {
				t.Fatalf("capture %q: %v", data, err)
			}
			if !bytes.Contains(logs, []byte(`"level":"INFO"`)) || !bytes.Contains(logs, []byte(`"threshold_bytes":5`)) || bytes.Contains(logs, []byte(`"capture":"send_failed"`)) {
				t.Fatalf("logs: %s", logs)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("capture missing")
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

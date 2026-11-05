package voice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
)

type safeForwardLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *safeForwardLog) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(b)
}
func (l *safeForwardLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

type forwardSink struct {
	mu       sync.Mutex
	attempts []transcriptBatch
	status   func(int) int
	hold     <-chan struct{}
	server   *httptest.Server
}

func newForwardSink(t *testing.T, status func(int) int) *forwardSink {
	t.Helper()
	s := &forwardSink{status: status}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/runner/v1/voice/turns" || r.Header.Get("Authorization") != "Bearer test-secret-do-not-log" {
			t.Error("incorrect endpoint or authorization")
		}
		var b transcriptBatch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Error(err)
		}
		s.mu.Lock()
		s.attempts = append(s.attempts, b)
		n := len(s.attempts)
		hold := s.hold
		s.mu.Unlock()
		if hold != nil {
			select {
			case <-hold:
			case <-r.Context().Done():
				return
			}
		}
		code := 200
		if s.status != nil {
			code = s.status(n)
		}
		w.WriteHeader(code)
		// Response content must never be copied into logs, even on errors.
		_, _ = w.Write([]byte(`{"accepted":1,"echo":"test-secret-do-not-log"}`))
	}))
	t.Cleanup(s.server.Close)
	return s
}
func (s *forwardSink) snapshot() []transcriptBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]transcriptBatch(nil), s.attempts...)
}
func waitForward(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("forwarding condition not met within 5s")
}
func forwardTestWorker(t *testing.T, s *forwardSink, l *safeForwardLog, start bool) *transcriptForward {
	t.Helper()
	f := newTranscriptForward(config.Config{ZoeURL: s.server.URL}, "test-secret-do-not-log", "gw-session", 7, slog.New(slog.NewTextHandler(l, nil)))
	f.interval = 5 * time.Millisecond
	f.retryBase = 10 * time.Millisecond
	f.flushDeadline = 2 * time.Second
	if start {
		go f.run()
		t.Cleanup(func() { f.Close(); <-f.done })
	} else {
		t.Cleanup(f.cancel)
	}
	return f
}
func forwardTurn(f *transcriptForward, role, id, text string) {
	f.Observe(avasData(map[string]any{"type": "turn.done", "turn": map[string]any{"id": id, "role": role, "transcript": text}}))
}
func flattenForward(b []transcriptBatch) []forwardedTurn {
	var turns []forwardedTurn
	for _, v := range b {
		turns = append(turns, v.Turns...)
	}
	return turns
}

func TestForwardsUserAndAssistantTurnsInOrderWithContiguousSeq(t *testing.T) {
	s := newForwardSink(t, nil)
	l := &safeForwardLog{}
	r := newAVASRig(t, func(svc *Service) {
		svc.cfg.ZoeURL = s.server.URL
		svc.zoeToken = "test-secret-do-not-log"
		svc.logger = slog.New(slog.NewTextHandler(l, nil))
		svc.WithMediaEvidence(mediaevidence.NewStore(svc.pool.(*accounts.Pool).DB()))
	})
	t.Cleanup(func() { _ = r.media.Close() })
	r.upstream.push(t, map[string]any{"type": "turn.created", "turn": map[string]any{"id": "u1", "role": "user"}})
	r.delegate(t, "deleg-1", "hello Zoe")
	// AVAS can report the same user completion via both representations.
	r.upstream.push(t, map[string]any{"type": "turn.done", "turn": map[string]any{"id": "u1", "role": "user", "transcript": "hello Zoe"}})
	r.upstream.push(t, map[string]any{"type": "turn.created", "turn": map[string]any{"id": "a1", "role": "assistant"}})
	r.upstream.push(t, map[string]any{"type": "turn.done", "turn": map[string]any{"id": "a1", "role": "assistant", "transcript": "hello user"}})
	waitForward(t, func() bool { return len(flattenForward(s.snapshot())) == 2 })
	turns := flattenForward(s.snapshot())
	if turns[0].Seq != 0 || turns[1].Seq != 1 || turns[0].Role != "user" || turns[1].Role != "assistant" || turns[0].Text != "hello Zoe" || turns[1].Text != "hello user" {
		t.Fatalf("incorrect turns: %+v", turns)
	}
	for _, b := range s.snapshot() {
		if b.Session != r.id || b.Generation != 1 {
			t.Fatal("incorrect gateway identity or media generation")
		}
	}
}

func TestForwardingDoesNotTrimLongCalls(t *testing.T) {
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	text := strings.Repeat("long voice turn ", 1200)
	for i := 0; i < 100; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		forwardTurn(f, role, fmt.Sprint(i), text)
	}
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 100 {
		t.Fatalf("received %d of 100", len(turns))
	}
	for i, v := range turns {
		if v.Seq != int64(i) || v.Text != text {
			t.Fatalf("turn %d trimmed or out of order", i)
		}
	}
}

func TestForwardIsIdempotentOnRetryAfter5xx(t *testing.T) {
	s := newForwardSink(t, func(n int) int {
		if n == 1 {
			return 500
		}
		return 200
	})
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	forwardTurn(f, "user", "u", "verbatim")
	waitForward(t, func() bool { return len(s.snapshot()) >= 2 })
	a := s.snapshot()
	x, _ := json.Marshal(a[0])
	y, _ := json.Marshal(a[1])
	if !bytes.Equal(x, y) {
		t.Fatal("retry changed idempotency payload")
	}
}

func TestForward404IsRetriedUntilSessionBoundThenDelivered(t *testing.T) {
	s := newForwardSink(t, func(n int) int {
		if n < 3 {
			return 404
		}
		return 200
	})
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	forwardTurn(f, "user", "u", "late bind")
	waitForward(t, func() bool { return len(s.snapshot()) == 3 })
	f.Close()
	<-f.done
	a := s.snapshot()
	if len(a) != 3 || a[2].Turns[0].Seq != 0 || a[2].Turns[0].Text != "late bind" {
		t.Fatal("404 retry lost turn")
	}
}

func TestForward409IsNotRetriedAndLoggedOnce(t *testing.T) {
	s := newForwardSink(t, func(int) int { return 409 })
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	forwardTurn(f, "assistant", "a", "conflict")
	waitForward(t, func() bool { return strings.Contains(l.String(), "zoe_transcript_dropped") })
	f.Close()
	<-f.done
	if len(s.snapshot()) != 1 || strings.Count(l.String(), "zoe_transcript_dropped") != 1 {
		t.Fatal("409 retried or logged more than once")
	}
}

func TestForward401IsRetriedLaterAndDoesNotBlockTheCall(t *testing.T) {
	s := newForwardSink(t, func(n int) int {
		if n < 3 {
			return 401
		}
		return 200
	})
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	start := time.Now()
	forwardTurn(f, "user", "u", "auth outage")
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("observer blocked")
	}
	waitForward(t, func() bool { return len(s.snapshot()) >= 3 })
	if strings.Count(l.String(), "zoe_transcript_auth") != 1 {
		t.Fatal("auth logs not throttled")
	}
}

func TestForwardQueueOverflowDropsOldestAndNeverBlocksEventPath(t *testing.T) {
	s := newForwardSink(t, nil)
	release := make(chan struct{})
	s.hold = release
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	f.capacity = 3
	media := &fakeMedia{}
	media.SetEventObserver(f.Observe)
	forwardTurn(f, "user", "0", "zero")
	waitForward(t, func() bool { return len(s.snapshot()) == 1 })
	start := time.Now()
	for i := 1; i <= 100; i++ {
		media.emit(string(avasData(map[string]any{"type": "turn.done", "turn": map[string]any{"id": fmt.Sprint(i), "role": "user", "transcript": "later"}})))
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("blocked on hanging Zoe")
	}
	f.mu.Lock()
	first, n, dropped := f.queue[0].Seq, len(f.queue), f.dropped
	f.mu.Unlock()
	if first != 98 || n != 3 || dropped != 98 {
		t.Fatalf("first=%d queued=%d dropped=%d", first, n, dropped)
	}
	close(release)
	f.Close()
	<-f.done
	if !strings.Contains(l.String(), "zoe_transcript_overflow") {
		t.Fatal("overflow not logged")
	}
}

func TestForwardFlushesPartialLastTurnOnHangup(t *testing.T) {
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	f.Observe([]byte(deltaFrame("assistant", "partial", "not quite")))
	f.Observe([]byte(appendFrame("/message/content/parts/0/text", " finished")))
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 1 || turns[0].Text != "not quite finished" {
		t.Fatal("hangup lost partial last turn")
	}
}

func TestForwardAVASPartialTextFlushesAndDuplicatesDoNotRepeat(t *testing.T) {
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	f.Observe(avasData(map[string]any{"type": "turn.created", "turn": map[string]any{"id": "partial", "role": "assistant"}}))
	f.Observe(avasData(map[string]any{"type": "output_audio_transcript.delta", "delta": "unfinished speech"}))
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 1 || turns[0].Text != "unfinished speech" {
		t.Fatal("AVAS partial text lost")
	}
}

func TestForwardOversizedTurnDoesNotDropNeighbors(t *testing.T) {
	s := newForwardSink(t, nil)
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, false)
	forwardTurn(f, "user", "before", "before")
	forwardTurn(f, "assistant", "oversize", strings.Repeat("x", (64<<10)+10))
	forwardTurn(f, "user", "after", "after")
	go f.run()
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 2 || turns[0].Seq != 0 || turns[1].Seq != 2 {
		t.Fatalf("oversize affected neighbors: %+v", turns)
	}
	if strings.Count(l.String(), "zoe_transcript_dropped") != 1 {
		t.Fatal("oversized turn not logged once")
	}
}

func TestForwardHangupDeadlineCancelsUnreachableDestination(t *testing.T) {
	s := newForwardSink(t, nil)
	release := make(chan struct{})
	s.hold = release
	defer close(release)
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	f.flushDeadline = 25 * time.Millisecond
	forwardTurn(f, "user", "u", "pending")
	waitForward(t, func() bool { return len(s.snapshot()) == 1 })
	start := time.Now()
	f.Close()
	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("hangup drain did not stop")
	}
	if time.Since(start) > time.Second || !strings.Contains(l.String(), "zoe_transcript_flush_expired") {
		t.Fatal("deadline or loss logging missing")
	}
}

func TestForwardingDisabledWhenURLOrTokenEmpty(t *testing.T) {
	s := newForwardSink(t, nil)
	for _, c := range []config.Config{{ZoeURL: s.server.URL}, {ZoeTokenFile: "unused"}, {ZoeURL: s.server.URL, ZoeTokenFile: filepath.Join(t.TempDir(), "empty")}} {
		if c.ZoeURL != "" && c.ZoeTokenFile != "" {
			if err := os.WriteFile(c.ZoeTokenFile, []byte(" \n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		token, err := c.ZoeTranscriptToken()
		if err != nil {
			t.Fatal(err)
		}
		f := newTranscriptForward(c, token, "session", 1, slog.Default())
		if f != nil {
			t.Fatal("incomplete config enabled forwarding")
		}
		forwardTurn(f, "user", "u", "ignored")
		f.Close()
	}
	if len(s.snapshot()) != 0 {
		t.Fatal("disabled forwarding made requests")
	}
}

func TestForwardRejectsInsecureURLAndMissingTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("test-secret-do-not-log"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"http://zoe.example", "https://user:pass@zoe.example", "https://zoe.example/?token=x", ":bad"} {
		if _, err := (config.Config{ZoeURL: u, ZoeTokenFile: path}).ZoeTranscriptToken(); err == nil {
			t.Fatalf("accepted insecure URL %s", u)
		}
	}
	if _, err := (config.Config{ZoeURL: "https://zoe.example", ZoeTokenFile: path + "missing"}).ZoeTranscriptToken(); err == nil {
		t.Fatal("accepted missing token file")
	}
	for _, u := range []string{"https://zoe.example", "http://localhost:1234", "http://127.0.0.1:1234", "http://[::1]:1234"} {
		if _, err := (config.Config{ZoeURL: u, ZoeTokenFile: path}).ZoeTranscriptToken(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestForwardLogsNoTurnTextWhenDebugTextOff(t *testing.T) {
	s := newForwardSink(t, func(int) int { return 409 })
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	forwardTurn(f, "user", "u", "PRIVATE-CONVERSATION-TEXT")
	f.Close()
	<-f.done
	if strings.Contains(l.String(), "PRIVATE-CONVERSATION-TEXT") || !strings.Contains(l.String(), "status=409") {
		t.Fatal("unsafe or missing status log")
	}
}

func TestForwardDoesNotChangeTranscriptTailHandoffBehavior(t *testing.T) {
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, true)
	with, without := newTranscriptTail(500), newTranscriptTail(500)
	for i := 0; i < 100; i++ {
		b := []byte(deltaFrame("user", fmt.Sprint(i), strings.Repeat("z", 200)))
		with.Observe(b)
		f.Observe(b)
		without.Observe(b)
		if with.Handoff() != without.Handoff() || with.ContextChars() != without.ContextChars() {
			t.Fatal("handoff behavior changed")
		}
	}
	f.Close()
	<-f.done
	if len(flattenForward(s.snapshot())) != 100 {
		t.Fatal("forwarder inherited handoff trimming")
	}
	if len(with.turns) >= 100 {
		t.Fatal("handoff no longer trims")
	}
}

func TestForwardTokenNeverAppearsInLogsOrErrors(t *testing.T) {
	s := newForwardSink(t, func(int) int { return 403 })
	l := &safeForwardLog{}
	f := forwardTestWorker(t, s, l, true)
	f.flushDeadline = 100 * time.Millisecond
	forwardTurn(f, "user", "u", "text")
	waitForward(t, func() bool { return len(s.snapshot()) >= 2 })
	f.Close()
	<-f.done
	if strings.Contains(l.String(), f.token) || !strings.Contains(l.String(), "zoe_transcript_auth") {
		t.Fatal("credential leaked or auth log missing")
	}
	p := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(p, []byte("test-secret-do-not-log\nbad"), 0600)
	_, err := (config.Config{ZoeURL: "https://zoe.example", ZoeTokenFile: p}).ZoeTranscriptToken()
	if err == nil || strings.Contains(err.Error(), "test-secret-do-not-log") {
		t.Fatal("unsafe credential validation error")
	}
}

package voice

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

func TestOversizedTurnIsCutAtRuneBoundaryNeverExceeding65536Bytes(t *testing.T) {
	text := strings.Repeat("语", 30000) + "ASCII tail"
	got := boundedTurnText(text)
	if len(got) > 65536 || !utf8.ValidString(got) || !strings.HasPrefix(text, got) || got != strings.Repeat("语", 65536/3) {
		t.Fatalf("invalid truncated prefix: bytes=%d valid_utf8=%v", len(got), utf8.ValidString(got))
	}
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, false)
	forwardTurn(f, "user", "oversized", text)
	f.mu.Lock()
	stored := f.queue[0].Text
	f.mu.Unlock()
	if stored != got {
		t.Fatal("stored text differs from bounded prefix")
	}
	go f.run()
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 1 || turns[0].Text != got {
		t.Fatal("truncated turn was not delivered unchanged")
	}
}

func TestExactly65536ByteTurnIsSentUnchanged(t *testing.T) {
	s := newForwardSink(t, nil)
	f := forwardTestWorker(t, s, &safeForwardLog{}, false)
	text := strings.Repeat("语", 21845) + "x"
	if len(text) != 65536 {
		t.Fatal("invalid test fixture")
	}
	forwardTurn(f, "assistant", "exact", text)
	go f.run()
	f.Close()
	<-f.done
	turns := flattenForward(s.snapshot())
	if len(turns) != 1 || turns[0].Text != text {
		t.Fatal("exactly 65536 bytes must be sent unchanged")
	}
}

// rejectForwardBatch proves actual batch rejection, singleton isolation, stable
// sequence gaps, and structured terminal log privacy for each rejected status.
func rejectForwardBatch(t *testing.T, status int) {
	t.Helper()
	var mu sync.Mutex
	var accepted []forwardedTurn
	var attempts [][]forwardedTurn
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch transcriptBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, batch.Turns)
		for _, turn := range batch.Turns {
			if len(turn.Text) > 65536 {
				t.Error("oversized wire turn")
			}
			if turn.Seq == 1 {
				w.WriteHeader(status)
				return
			}
		}
		accepted = append(accepted, batch.Turns...)
		w.WriteHeader(200)
	}))
	defer s.Close()
	logs := &safeForwardLog{}
	f := newTranscriptForward(config.Config{ZoeURL: s.URL}, "test-secret-do-not-log", "session", 1, slog.New(slog.NewJSONHandler(logs, nil)))
	defer f.cancel()
	for i := 0; i < 4; i++ {
		forwardTurn(f, "user", fmt.Sprint(i), fmt.Sprintf("PRIVATE-%d", i))
	}
	go f.run()
	f.Close()
	<-f.done
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) < 2 || len(attempts[0]) != 4 {
		t.Fatal("did not reject an actual multi-turn batch")
	}
	if len(accepted) != 3 || accepted[0].Seq != 0 || accepted[1].Seq != 2 || accepted[2].Seq != 3 {
		t.Fatalf("neighbours or later seq lost: %+v", accepted)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one terminal log, got %d", len(lines))
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatal(err)
	}
	if event["msg"] != "zoe_transcript_dropped" || event["seq"] != float64(1) || event["status"] != float64(status) {
		t.Fatalf("terminal log must identify only rejected seq and status: %s", lines[0])
	}
	for k := range event {
		if k != "time" && k != "level" && k != "msg" && k != "seq" && k != "status" {
			t.Fatalf("unexpected terminal log field %s", k)
		}
	}
	if strings.Contains(logs.String(), "PRIVATE") || strings.Contains(logs.String(), "test-secret") {
		t.Fatal("private data leaked")
	}
}

func TestOversizedTurnDoesNotDropNeighboursWhenZoeRejectsBatchWith413(t *testing.T) {
	// Size truncation is tested independently above. Zoe may still reject a
	// within-limit turn; simulate that response to exercise isolation itself.
	rejectForwardBatch(t, http.StatusRequestEntityTooLarge)
}

func TestRejectedTurnDoesNotDropNeighboursWhenZoeRejectsBatchWith400(t *testing.T) {
	rejectForwardBatch(t, http.StatusBadRequest)
}

func TestSeqGapDoesNotStallLaterTurns(t *testing.T) {
	for _, status := range []int{400, 413} {
		t.Run(fmt.Sprint(status), func(t *testing.T) { rejectForwardBatch(t, status) })
	}
}

package voice

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func delegationEvent(id, text string) map[string]any {
	return map[string]any{"type": "delegation.created", "item": map[string]any{"target": "client", "id": id,
		"content": []map[string]string{{"type": "input_text", "text": text}}}}
}

func TestDelegationUpgradesAlreadyForwardedTurnImmediately(t *testing.T) {
	sink := newForwardSink(t, nil)
	f := forwardTestWorker(t, sink, &safeForwardLog{}, false)
	f.interval = 500 * time.Millisecond
	go f.run()
	defer func() { f.Close(); <-f.done }()
	forwardTurn(f, "user", "u1", "unchanged text")
	waitForward(t, func() bool { return len(sink.snapshot()) == 1 })
	started := time.Now()
	f.Observe(avasData(delegationEvent("d1", "unchanged text")))
	f.Observe(avasData(delegationEvent("d1", "unchanged text")))
	waitForward(t, func() bool { return len(sink.snapshot()) >= 2 })
	if time.Since(started) >= 450*time.Millisecond {
		t.Fatal("marked turn waited for ordinary batching")
	}
	turns := flattenForward(sink.snapshot())
	if len(turns) != 2 || turns[0].Seq != turns[1].Seq || turns[0].Text != turns[1].Text || turns[0].At != turns[1].At || turns[1].DelegationID != "d1" {
		t.Fatalf("turns=%+v", turns)
	}
}

func TestMarkedTurnSurvivesOverflowAndDoesNotWaitBehindOrdinaryRetry(t *testing.T) {
	sink := newForwardSink(t, func(n int) int {
		if n == 1 {
			return 503
		}
		return 200
	})
	f := forwardTestWorker(t, sink, &safeForwardLog{}, false)
	f.capacity = 3
	f.retryBase = time.Second
	forwardTurn(f, "assistant", "old", "ordinary")
	go f.run()
	defer func() { f.Close(); <-f.done }()
	waitForward(t, func() bool { return len(sink.snapshot()) == 1 })
	f.Observe(avasData(delegationEvent("critical", "please help")))
	for i := range 25 {
		forwardTurn(f, "assistant", fmt.Sprint(i), "overflow")
	}
	waitForward(t, func() bool { return len(sink.snapshot()) >= 2 })
	if got := sink.snapshot()[1].Turns; len(got) != 1 || got[0].DelegationID != "critical" {
		t.Fatalf("marked turn starved or dropped: %+v", got)
	}
}

func TestDelegationShortRetryAndTerminalClassification(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409, 413, 429, 503, 502, 0} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			seen := make(chan forwardedTurn, 30)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var b transcriptBatch
				if json.NewDecoder(r.Body).Decode(&b) != nil {
					t.Error("invalid batch")
					return
				}
				for _, turn := range b.Turns {
					seen <- turn
					if turn.DelegationID == "" {
						continue
					}
					if code == 0 {
						c, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = c.Close()
						}
						return
					}
					w.WriteHeader(code)
					return
				}
			}))
			defer server.Close()
			f := forwardTestWorker(t, &forwardSink{server: server}, &safeForwardLog{}, false)
			if f.markedWindow != 10*time.Second {
				t.Fatal("wrong retry window")
			}
			f.markedWindow = 325 * time.Millisecond
			failed := make(chan string, 1)
			f.failed = func(id string) { failed <- id }
			go f.run()
			defer func() { f.Close(); <-f.done }()
			started := time.Now()
			f.Observe(avasData(delegationEvent("d1", "delegated")))
			if id := takeOwned(t, failed); id != "d1" {
				t.Fatal(id)
			}
			elapsed := time.Since(started)
			count := 0
			for {
				turn := takeOwned(t, seen)
				if turn.Seq != 0 || turn.Text != "delegated" {
					t.Fatalf("unstable turn: %+v", turn)
				}
				if turn.DelegationID == "" {
					break
				}
				count++
			}
			if code == 0 || code >= 500 {
				if elapsed < f.markedWindow || elapsed > time.Second || count < 2 {
					t.Fatalf("attempts=%d elapsed=%v", count, elapsed)
				}
			} else if count != 1 {
				t.Fatalf("terminal attempts=%d", count)
			}
		})
	}
}

func TestOversizedDelegationIsVisibleWithoutMarkOrTruncatingToExecute(t *testing.T) {
	sink := newForwardSink(t, nil)
	f := forwardTestWorker(t, sink, &safeForwardLog{}, false)
	failed := make(chan string, 1)
	f.failed = func(id string) { failed <- id }
	text := strings.Repeat("界", zoeFinalTurnMax+1)
	f.Observe(avasData(delegationEvent("oversize", text)))
	go f.run()
	defer func() { f.Close(); <-f.done }()
	if id := takeOwned(t, failed); id != "oversize" {
		t.Fatal(id)
	}
	waitForward(t, func() bool { return len(sink.snapshot()) != 0 })
	if got := flattenForward(sink.snapshot()); len(got) != 1 || got[0].DelegationID != "" || got[0].Text != text {
		t.Fatal("oversized delegation executed or lost its ordinary transcript")
	}
}

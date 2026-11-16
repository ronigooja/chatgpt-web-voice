package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/httptest"
	"github.com/bogdanfinn/websocket"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
)

type openingCreationRecords struct {
	*callsessions.Store
	afterUpsert func(callsessions.Session)
}

func (s *openingCreationRecords) Upsert(row callsessions.Session) error {
	if err := s.Store.Upsert(row); err != nil {
		return err
	}
	if s.afterUpsert != nil {
		s.afterUpsert(row)
	}
	return nil
}

// TestOpeningCreationReceipt exercises real CreateSession, HTTP serialization,
// initial_items conversion, sideband setup, binding and SQLite finalization.
// All HTTP/WebSocket peers and account credentials are local synthetic fixtures.
func TestOpeningCreationReceipt(t *testing.T) {
	for _, mode := range []string{"success", "replacement", "generation", "closed", "write_failure"} {
		t.Run(mode, func(t *testing.T) {
			svc, db, _ := newCreationService(t, []int{http.StatusCreated}, 1)
			opening := OpeningContext{Kind: "full", Version: 3, Items: []OpeningContextItem{{Role: "user", Text: "你好 <>&\u2028\"\\😀"}, {Role: "system", Text: "context only"}}}
			canonical, _ := json.Marshal(opening)
			sum := sha256.Sum256(canonical)
			wantHash := hex.EncodeToString(sum[:])
			captured := make(chan avasSessionPayload, 1)
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Session avasSessionPayload `json:"session"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				captured <- request.Session
				w.Header().Set("Location", "https://example.invalid/live/opening-test")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte("v=0\r\n"))
			}))
			defer upstream.Close()
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			sideband := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer sideband.Close()
			svc.avasURL = upstream.URL
			svc.avasSidebandURL = "ws" + strings.TrimPrefix(sideband.URL, "http") + "/"
			relay := &fakeRelay{}
			svc.WithRelay(relay).WithMediaEvidence(mediaevidence.NewStore(db))
			records := &openingCreationRecords{Store: callsessions.NewStore(db)}
			records.afterUpsert = func(row callsessions.Session) {
				switch mode {
				case "replacement":
					svc.mu.Lock()
					copy := *svc.bindings[row.VoiceSessionID]
					svc.bindings[row.VoiceSessionID] = &copy
					svc.mu.Unlock()
				case "generation":
					svc.mu.Lock()
					svc.bindings[row.VoiceSessionID].mediaID.MediaGeneration++
					svc.mu.Unlock()
				case "closed":
					_ = relay.session(0).CloseWithReason("test_closed_before_receipt")
				}
			}
			svc.WithCallSessions(records)
			if mode == "write_failure" {
				if _, err := db.Conn().Exec(`CREATE TRIGGER fail_opening_hash BEFORE UPDATE OF opening_context_hash ON call_sessions BEGIN SELECT RAISE(FAIL,'synthetic metadata write failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			result, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"), OpeningContext: opening})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { svc.ReleaseSession("api_key:1", result.VoiceSessionID) })
			// Closure/replacement can detach resources from the live binding; close the
			// concrete original sideband as well, without relying on HTTP server cleanup.
			svc.mu.Lock()
			binding := svc.bindings[result.VoiceSessionID]
			svc.mu.Unlock()
			if binding != nil {
				t.Cleanup(func() { binding.avasSideband.close() })
			}
			payload := <-captured
			// Independent expectation: do not reuse the production conversion helper.
			wantInitialItems := []map[string]any{
				{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": opening.Items[0].Text}}},
				{"type": "message", "role": "system", "content": []any{map[string]any{"type": "input_text", "text": opening.Items[1].Text}}},
			}
			gotItems, _ := json.Marshal(payload.InitialItems)
			wantItems, _ := json.Marshal(wantInitialItems)
			if string(gotItems) != string(wantItems) {
				t.Fatalf("initial_items=%s want=%s", gotItems, wantItems)
			}
			// Exercise the actual response JSON tags, not just in-memory hash helpers.
			wire, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var response struct {
				Opening *OpeningContextResult `json:"opening_context"`
			}
			if err := json.Unmarshal(wire, &response); err != nil || response.Opening == nil {
				t.Fatal(string(wire), err)
			}
			receipt := response.Opening
			if mode == "success" {
				if !receipt.Applied || receipt.Silent != true || receipt.Version != opening.Version || receipt.ContentHash != wantHash {
					t.Fatal(receipt)
				}
				var hash string
				var generation int64
				if err := db.Conn().QueryRow(`SELECT opening_context_hash,opening_context_generation FROM call_sessions WHERE voice_session_id=?`, result.VoiceSessionID).Scan(&hash, &generation); err != nil || hash != wantHash || generation != result.VoiceSessionGeneration {
					t.Fatal(hash, generation, err)
				}
			} else if receipt.Applied || receipt.ContentHash != "" || strings.Contains(string(wire), `"content_hash"`) {
				t.Fatalf("%s invented receipt: %s", mode, wire)
			}
		})
	}
}

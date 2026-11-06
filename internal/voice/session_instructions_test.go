package voice

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
)

func TestAVASSessionInstructionsCreateBody(t *testing.T) {
	for _, instructions := range []string{"", "  常驻规则：保持\"原文\"。\nSecond line\n", strings.Repeat("界", 2000)} {
		t.Run(fmt.Sprintf("bytes_%d", len(instructions)), func(t *testing.T) {
			svc, _, _ := newCreationService(t, []int{http.StatusCreated}, 1)
			svc.WithCallSessions(&testCallSessionStore{rows: make(map[string]callsessions.Session)})
			var received struct {
				Session avasSessionPayload `json:"session"`
			}
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
					t.Error(err)
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, "v=0\r\n")
			}))
			t.Cleanup(upstream.Close)
			svc.avasURL = upstream.URL
			body, _ := json.Marshal(map[string]any{"instructions": instructions})
			var req CreateSessionRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatal(err)
			}
			req.Owner = "api_key:1"
			req.OfferSDP = readTestdata(t, "wm_offer.sdp")
			req.OpeningContext = OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "opening context"}}}
			result, err := svc.CreateSession(req)
			if err != nil {
				t.Fatal(err)
			}
			if received.Session.Instructions != instructions {
				t.Fatalf("session.instructions = %q, want exact input %q", received.Session.Instructions, instructions)
			}
			if received.Session.Delegation.Type != "client" || len(received.Session.InitialItems) != 1 {
				t.Fatalf("delegation/initial_items = %+v", received.Session)
			}
			item := received.Session.InitialItems[0]
			content := item["content"].([]any)[0].(map[string]any)
			if item["role"] != "system" || content["text"] != "opening context" {
				t.Fatalf("opening context = %+v", item)
			}
			if result.OpeningContext == nil || !result.OpeningContext.Applied || result.OpeningContext.Silent != true {
				t.Fatalf("AVAS acknowledgement = %+v", result.OpeningContext)
			}
		})
	}
}

func TestSessionInstructionsInvalidNeverCallsUpstream(t *testing.T) {
	for _, instructions := range []string{"\xff", strings.Repeat("界", 2001)} {
		svc, _, upstream := newCreationService(t, []int{http.StatusCreated}, 1)
		_, err := svc.CreateSession(CreateSessionRequest{Instructions: instructions, Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
		openingContextErr(t, err, http.StatusBadRequest)
		if len(upstream.bodies) != 0 {
			t.Fatalf("invalid instructions reached upstream: %d calls", len(upstream.bodies))
		}
	}
}

func TestSessionInstructionsIgnoredOnWM(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, func(w http.ResponseWriter, _ *http.Request, captured wmCapture) {
		body, _ := json.Marshal(captured.Fields)
		if strings.Contains(string(body), "private-standing-rule") {
			t.Error("WM received AVAS-only instructions")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, readTestdata(t, "wm_answer.sdp"))
	})
	_, err := f.svc.CreateSession(CreateSessionRequest{Instructions: "private-standing-rule", Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
}

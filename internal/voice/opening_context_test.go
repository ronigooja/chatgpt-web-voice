package voice

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func openingContextFixture(t *testing.T) (*wmFixture, *fakeRelay, *testCallSessionStore, string) {
	t.Helper()
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), 201))
	store := f.svc.records.(*testCallSessionStore)
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	return f, fr, store, res.VoiceSessionID
}

func openingContextErr(t *testing.T, err error, status int) {
	t.Helper()
	se, ok := err.(*ServiceError)
	if !ok || se.StatusCode != status {
		t.Fatalf("error=%T %v want status %d", err, err, status)
	}
}

func TestOpeningContextMonotonicVersions(t *testing.T) {
	f, fr, store, id := openingContextFixture(t)
	first := OpeningContext{Kind: "delta", Version: 2, Items: []OpeningContextItem{{Role: "user", Text: "first"}}}
	got, err := f.svc.ApplyOpeningContext("api_key:1", id, first)
	if err != nil || !got.Applied {
		t.Fatalf("first apply=%+v err=%v", got, err)
	}
	media := fr.session(0)
	older := OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "older"}}}
	before := len(media.upstreamEvents)
	_, err = f.svc.ApplyOpeningContext("api_key:1", id, older)
	openingContextErr(t, err, 409)
	if len(media.upstreamEvents) != before {
		t.Fatal("lower version injected an event")
	}
	repeat := OpeningContext{Kind: "delta", Version: 2, Items: []OpeningContextItem{{Role: "user", Text: "repeat"}}}
	got, err = f.svc.ApplyOpeningContext("api_key:1", id, repeat)
	if err != nil || !got.Applied || !got.AlreadyApplied {
		t.Fatalf("repeat=%+v err=%v", got, err)
	}
	if len(media.upstreamEvents) != before {
		t.Fatal("idempotent call injected an event")
	}
	if store.rows[id].OpeningContextVersion != 2 {
		t.Fatalf("stored version=%d", store.rows[id].OpeningContextVersion)
	}
}

func TestOpeningContextFullIntoConfirmedResumedIsRejected(t *testing.T) {
	f, fr, _, id := openingContextFixture(t)
	confirmed := true
	f.svc.mu.Lock()
	f.svc.bindings[id].conversation = "resumed"
	f.svc.bindings[id].conversationConfirmed = &confirmed
	f.svc.mu.Unlock()
	_, err := f.svc.ApplyOpeningContext("api_key:1", id, OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "secret"}}})
	openingContextErr(t, err, 409)
	if len(fr.session(0).upstreamEvents) != 0 {
		t.Fatal("rejected full context was injected")
	}
}

func TestOpeningContextFullIntoNewIsAccepted(t *testing.T) {
	f, fr, _, id := openingContextFixture(t)
	got, err := f.svc.ApplyOpeningContext("api_key:1", id, OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "new thread"}}})
	if err != nil || !got.Applied || len(fr.session(0).upstreamEvents) != 1 {
		t.Fatalf("got=%+v err=%v events=%v", got, err, fr.session(0).upstreamEvents)
	}
	if got.Silent != "unverified" {
		t.Fatalf("silent=%v want unverified", got.Silent)
	}
}

func TestOpeningContextDeltaAcceptedForNewAndConfirmedResumed(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		name := "new"
		if resumed {
			name = "resumed"
		}
		t.Run(name, func(t *testing.T) {
			f, fr, _, id := openingContextFixture(t)
			if resumed {
				confirmed := true
				f.svc.mu.Lock()
				f.svc.bindings[id].conversation = "resumed"
				f.svc.bindings[id].conversationConfirmed = &confirmed
				f.svc.mu.Unlock()
			}
			got, err := f.svc.ApplyOpeningContext("api_key:1", id, OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "delta"}}})
			if err != nil || !got.Applied || len(fr.session(0).upstreamEvents) != 1 {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}

func TestOpeningContextAVASUnsupportedReturns501(t *testing.T) {
	f, fr, _, id := openingContextFixture(t)
	f.svc.cfg.VoiceUpstreamRoute = "avas"
	_, err := f.svc.ApplyOpeningContext("api_key:1", id, OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "not injected"}}})
	openingContextErr(t, err, 501)
	if len(fr.session(0).upstreamEvents) != 0 {
		t.Fatal("AVAS route injected live event")
	}
}

func TestOpeningContextTextIsNeverPersisted(t *testing.T) {
	f, _, store, id := openingContextFixture(t)
	marker := "never-persist-" + strings.Repeat("x", 20)
	if _, err := f.svc.ApplyOpeningContext("api_key:1", id, OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: marker}}}); err != nil {
		t.Fatal(err)
	}
	row := store.rows[id]
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), marker) {
		t.Fatalf("opening text persisted: %s", raw)
	}
	if row.OpeningContextItems != 1 || row.OpeningContextChars != len([]rune(marker)) || row.OpeningContextKind != "delta" || row.OpeningContextVersion != 1 || row.OpeningContextAppliedAt == "" {
		t.Fatalf("metadata incomplete: %+v", row)
	}
}

func TestOpeningContextContextCharsUpdated(t *testing.T) {
	f, _, store, id := openingContextFixture(t)
	before := store.rows[id].ContextChars
	c := OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "four"}}}
	if _, err := f.svc.ApplyOpeningContext("api_key:1", id, c); err != nil {
		t.Fatal(err)
	}
	if store.rows[id].ContextChars != before+4 {
		t.Fatalf("context chars=%d want %d", store.rows[id].ContextChars, before+4)
	}
	if _, err := f.svc.ApplyOpeningContext("api_key:1", id, c); err != nil {
		t.Fatal(err)
	}
	if store.rows[id].ContextChars != before+4 {
		t.Fatal("idempotent call changed context chars")
	}
}

func TestOpeningContextCreationFailureDoesNotFailCall(t *testing.T) {
	f, fr, _, _ := openingContextFixture(t)
	fr.acceptFn = func(m *fakeMedia) error {
		m.upstreamErr = errors.New("injection failed")
		return nil
	}
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"), OpeningContext: OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "creation context"}}}})
	if err != nil || res == nil || res.OpeningContext == nil || res.OpeningContext.Applied || res.OpeningContext.Reason == "" {
		t.Fatalf("creation result=%+v err=%v", res, err)
	}
}

func TestOpeningContextCreationReportsApplied(t *testing.T) {
	f, _, _, _ := openingContextFixture(t)
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"), OpeningContext: OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "creation context"}}}})
	if err != nil || res == nil || res.OpeningContext == nil || !res.OpeningContext.Applied || res.OpeningContext.Silent != "unverified" {
		t.Fatalf("creation result=%+v err=%v", res, err)
	}
}

func TestOpeningContextValidationBoundsAndControls(t *testing.T) {
	valid := OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "a\n\tb"}}}
	if chars, err := validateOpeningContext(valid); err != nil || chars != 4 {
		t.Fatalf("valid context chars=%d err=%v", chars, err)
	}
	for _, item := range []OpeningContextItem{{Role: "assistant", Text: "x"}, {Role: "user", Text: "x\x00"}} {
		if _, err := validateOpeningContext(OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{item}}); err == nil {
			t.Fatalf("accepted invalid item: %+v", item)
		}
	}
	tooMany := OpeningContext{Kind: "delta", Version: 1, Items: make([]OpeningContextItem, maxOpeningItems+1)}
	if _, err := validateOpeningContext(tooMany); err == nil {
		t.Fatal("accepted too many items")
	}
	tooLong := OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: strings.Repeat("x", maxOpeningItemChars+1)}}}
	if _, err := validateOpeningContext(tooLong); err == nil {
		t.Fatal("accepted oversized item")
	}
}

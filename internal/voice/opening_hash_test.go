package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func TestOpeningHashCanonicalGolden(t *testing.T) {
	c := OpeningContext{Kind: "full", Version: 7, Items: []OpeningContextItem{{Role: "user", Text: "你好 <>&\u2028\u2029\"\\\n😀"}}}
	want := `{"kind":"full","version":7,"items":[{"role":"user","text":"你好 \u003c\u003e\u0026\u2028\u2029\"\\\n😀"}]}`
	raw, err := json.Marshal(c)
	if err != nil || string(raw) != want {
		t.Fatalf("canonical=%s err=%v", raw, err)
	}
	h := sha256.Sum256([]byte(want))
	if openingContextContentHash(c) != hex.EncodeToString(h[:]) {
		t.Fatal("hash differs from canonical bytes")
	}
}

func openingHashFixture(t *testing.T) (*Service, *store.DB, *sessionBinding, OpeningContext) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "opening.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Conn().Exec(`INSERT INTO call_sessions(voice_session_id,owner) VALUES('s','api_key:1'); INSERT INTO voice_media_sessions(voice_session_id,media_generation,owner,account_id,lease_id,lease_generation,relay_session_id,state,created_at,updated_at) VALUES('s',1,'api_key:1',1,'l',1,'relay','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`)
	if err != nil {
		t.Fatal(err)
	}
	s := New(config.Config{VoiceUpstreamRoute: "avas", SessionTTLSeconds: 180}, nil, nil).WithCallSessions(callsessions.NewStore(db))
	b := &sessionBinding{Owner: "api_key:1", CreatedAt: time.Now(), UpdatedAt: time.Now(), mediaID: mediaevidence.Identity{VoiceSessionID: "s", MediaGeneration: 1}}
	s.bindings["s"] = b
	return s, db, b, OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: "hello"}}}
}

func TestOpeningHashConcreteFinalization(t *testing.T) {
	s, db, b, c := openingHashFixture(t)
	r := s.finalizeCreatedOpening("api_key:1", "s", b, c, 5)
	if !r.Applied || r.Silent != true || r.Version != 1 || r.ContentHash != openingContextContentHash(c) {
		t.Fatal(r)
	}
	if err := s.checkOpeningHash("api_key:1", "s", c); err != nil {
		t.Fatal(err)
	}
	again := s.finalizeCreatedOpening("api_key:1", "s", b, c, 5)
	if !again.Applied {
		t.Fatal(again)
	}
	var chars int
	db.Conn().QueryRow(`SELECT context_chars FROM call_sessions WHERE voice_session_id='s'`).Scan(&chars)
	if chars != 5 {
		t.Fatal(chars)
	}
	c.Items[0].Text = "different"
	if err := s.checkOpeningHash("api_key:1", "s", c); err == nil {
		t.Fatal("changed content accepted")
	}
	if r := s.finalizeCreatedOpening("api_key:1", "s", b, c, 9); r.Applied || r.ContentHash != "" {
		t.Fatal(r)
	}
}

func TestOpeningHashNoCrossIdentityOrMissingProof(t *testing.T) {
	s, db, b, c := openingHashFixture(t)
	db.Conn().Exec(`UPDATE call_sessions SET opening_context_version=1`)
	if err := s.checkOpeningHash("api_key:1", "s", c); err == nil {
		t.Fatal("missing old hash accepted")
	}
	if r := s.finalizeCreatedOpening("api_key:1", "s", b, c, 5); r.Applied || r.ContentHash != "" {
		t.Fatal(r)
	}
	db.Conn().Exec(`UPDATE call_sessions SET opening_context_version=0`)
	next := *b
	next.mediaID.MediaGeneration = 2
	s.bindings["s"] = &next
	for _, r := range []*OpeningContextResult{s.finalizeCreatedOpening("api_key:1", "s", b, c, 5), s.finalizeCreatedOpening("api_key:1", "other", &next, c, 5), s.finalizeCreatedOpening("api_key:1", "s", &next, c, 5)} {
		if r.Applied || r.ContentHash != "" {
			t.Fatal(r)
		}
	}
	s.bindings["s"] = b
	db.Conn().Exec(`DROP TABLE call_sessions`)
	if r := s.finalizeCreatedOpening("api_key:1", "s", b, c, 5); r.Applied || r.ContentHash != "" {
		t.Fatal(r)
	}
}

func TestOpeningHashConflictBeforeNetwork(t *testing.T) {
	s, db, _, c := openingHashFixture(t)
	db.Conn().Exec(`UPDATE call_sessions SET opening_context_version=1,opening_context_hash='different'`)
	// No account pool is configured: reaching account selection/network would panic.
	_, err := s.CreateSession(CreateSessionRequest{Owner: "api_key:1", VoiceSessionID: "s", OfferSDP: readTestdata(t, "wm_offer.sdp"), OpeningContext: c})
	var serviceErr *ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.StatusCode != 409 {
		t.Fatal(err)
	}
}

func TestOpeningHashConcurrentFinalization(t *testing.T) {
	s, db, b, c := openingHashFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := s.finalizeCreatedOpening("api_key:1", "s", b, c, 5)
			if !r.Applied {
				t.Error(r)
			}
		}()
	}
	wg.Wait()
	var chars int
	db.Conn().QueryRow(`SELECT context_chars FROM call_sessions WHERE voice_session_id='s'`).Scan(&chars)
	if chars != 5 {
		t.Fatal(chars)
	}
}

func TestOpeningHashWMUnverified(t *testing.T) {
	s, db, _, c := openingHashFixture(t)
	s.cfg.VoiceUpstreamRoute = "wm"
	db.Conn().Exec(`UPDATE call_sessions SET opening_context_version=1`)
	r, err := s.ApplyOpeningContext("api_key:1", "s", c)
	if err != nil || r.Silent != "unverified" || r.ContentHash != "" {
		t.Fatal(r, err)
	}
}

package mediaevidence

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func openTest(t *testing.T) (*Store, *store.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "voice.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db), db, path
}

func ident() Identity {
	return Identity{VoiceSessionID: "vs_1", MediaGeneration: 1, AccountID: 7, LeaseID: "lease-1", LeaseGeneration: 1}
}

var clean = Components{PeerAClosed: true, PeerBClosed: true, DataChannelsClosed: true, TunnelsClosed: true}

func TestCleanCloseWritesEvidenceAndTerminates(t *testing.T) {
	s, _, _ := openTest(t)
	id := ident()
	if err := s.Register("api_key:1", "media-a", id); err != nil {
		t.Fatal(err)
	}
	m, err := s.Close(id, clean, "released")
	if err != nil {
		t.Fatal(err)
	}
	if m.State != StateTerminated || m.EvidenceID == "" || m.TerminatedAt == "" || m.CloseReason != "released" {
		t.Fatalf("unexpected row: %+v", m)
	}
	ev, err := s.Evidence(id)
	if err != nil || ev.EvidenceID != m.EvidenceID || ev.Reason != "released" {
		t.Fatalf("evidence mismatch: %+v %v", ev, err)
	}
}

func TestRepeatedCloseIsIdempotentAndKeepsOneEvidence(t *testing.T) {
	s, db, _ := openTest(t)
	id := ident()
	_ = s.Register("o", "m", id)
	first, err := s.Close(id, clean, "released")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Close(id, clean, "superseded")
	if err != nil {
		t.Fatal(err)
	}
	if second.EvidenceID != first.EvidenceID || second.CloseReason != "released" || second.TerminatedAt != first.TerminatedAt {
		t.Fatalf("second close changed the record: %+v vs %+v", second, first)
	}
	var n int
	if err := db.Conn().QueryRow(`SELECT COUNT(*) FROM voice_termination_evidence`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("evidence rows=%d err=%v", n, err)
	}
}

func TestUncleanCloseStaysUnknownWithoutEvidence(t *testing.T) {
	cases := map[string]Components{
		"peer A":        {PeerBClosed: true, DataChannelsClosed: true, TunnelsClosed: true},
		"peer B":        {PeerAClosed: true, DataChannelsClosed: true, TunnelsClosed: true},
		"data channels": {PeerAClosed: true, PeerBClosed: true, TunnelsClosed: true},
		"tunnels":       {PeerAClosed: true, PeerBClosed: true, DataChannelsClosed: true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, _ := openTest(t)
			id := ident()
			_ = s.Register("o", "m", id)
			m, err := s.Close(id, c, "peer_a_failed")
			if err != nil {
				t.Fatal(err)
			}
			if m.State != StateUnknown || m.EvidenceID != "" || m.EvidenceError == "" {
				t.Fatalf("unclean close must be unknown without evidence: %+v", m)
			}
			if _, err := s.Evidence(id); !errors.Is(err, ErrNotFound) {
				t.Fatalf("no evidence expected, got %v", err)
			}
			// A later clean close must not upgrade an unknown row.
			again, err := s.Close(id, clean, "released")
			if err != nil || again.State != StateUnknown {
				t.Fatalf("unknown must stay unknown: %+v %v", again, err)
			}
		})
	}
}

func TestEvidenceWriteFailureKeepsLeaseBlocked(t *testing.T) {
	s, db, _ := openTest(t)
	id := ident()
	_ = s.Register("o", "m", id)
	// Make the evidence insert fail without touching the media session table.
	if _, err := db.Conn().Exec(`DROP TABLE voice_termination_evidence`); err != nil {
		t.Fatal(err)
	}
	m, err := s.Close(id, clean, "released")
	if err != nil {
		t.Fatal(err)
	}
	if m.State != StateUnknown || m.EvidenceID != "" || m.EvidenceError != "evidence write failed" {
		t.Fatalf("evidence failure must leave the row unknown: %+v", m)
	}
}

func TestIdentityMismatchIsRejected(t *testing.T) {
	s, _, _ := openTest(t)
	id := ident()
	_ = s.Register("o", "m", id)
	for name, mutate := range map[string]func(*Identity){
		"lease id":         func(i *Identity) { i.LeaseID = "lease-other" },
		"lease generation": func(i *Identity) { i.LeaseGeneration = 2 },
		"account":          func(i *Identity) { i.AccountID = 8 },
	} {
		bad := id
		mutate(&bad)
		if _, err := s.Close(bad, clean, "released"); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("%s: expected mismatch, got %v", name, err)
		}
	}
	if _, err := s.Close(Identity{VoiceSessionID: "vs_1", MediaGeneration: 2, AccountID: 7, LeaseID: "lease-1", LeaseGeneration: 1}, clean, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale/unknown generation must be not found, got %v", err)
	}
	m, _ := s.Get("vs_1", 1)
	if m.State != StateActive {
		t.Fatalf("rejected closes must not change the row: %+v", m)
	}
}

func TestRegisterRejectsReuseAndIncompleteIdentity(t *testing.T) {
	s, _, _ := openTest(t)
	id := ident()
	if err := s.Register("o", "m", id); err != nil {
		t.Fatal(err)
	}
	if err := s.Register("o", "m2", id); err == nil {
		t.Fatal("a generation must not be registered twice")
	}
	for name, bad := range map[string]Identity{
		"no session":    {MediaGeneration: 1, AccountID: 7, LeaseID: "l", LeaseGeneration: 1},
		"no lease":      {VoiceSessionID: "v", MediaGeneration: 1, AccountID: 7, LeaseGeneration: 1},
		"zero gen":      {VoiceSessionID: "v", AccountID: 7, LeaseID: "l", LeaseGeneration: 1},
		"zero lease gn": {VoiceSessionID: "v", MediaGeneration: 1, AccountID: 7, LeaseID: "l"},
		"no account":    {VoiceSessionID: "v", MediaGeneration: 1, LeaseID: "l", LeaseGeneration: 1},
	} {
		if err := s.Register("o", "m", bad); err == nil {
			t.Fatalf("%s: incomplete identity must be rejected", name)
		}
	}
}

func TestEvidenceIsImmutableAndUnique(t *testing.T) {
	s, db, _ := openTest(t)
	id := ident()
	_ = s.Register("o", "m", id)
	if _, err := s.Close(id, clean, "released"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn().Exec(`UPDATE voice_termination_evidence SET reason = 'tampered'`); err == nil {
		t.Fatal("evidence must not be updatable")
	}
	if _, err := db.Conn().Exec(`DELETE FROM voice_termination_evidence`); err == nil {
		t.Fatal("evidence must not be deletable")
	}
	_, err := db.Conn().Exec(`INSERT INTO voice_termination_evidence
		(evidence_id, voice_session_id, media_generation, account_id, lease_id, lease_generation, observed_at, reason)
		VALUES ('ev_dup', 'vs_1', 1, 7, 'lease-1', 1, 'now', 'dup')`)
	if err == nil {
		t.Fatal("second evidence for the same lease generation must be rejected")
	}
}

func TestRestartTurnsUnfinishedRowsUnknown(t *testing.T) {
	s, db, path := openTest(t)
	active, closing, done := ident(), ident(), ident()
	active.VoiceSessionID, closing.VoiceSessionID, done.VoiceSessionID = "vs_active", "vs_closing", "vs_done"
	for _, id := range []Identity{active, closing, done} {
		if err := s.Register("o", "m", id); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkClosing(closing, "released"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(done, clean, "released"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	s2 := NewStore(db2)
	n, err := s2.RecoverUnfinished()
	if err != nil || n != 2 {
		t.Fatalf("recovered=%d err=%v", n, err)
	}
	for _, id := range []Identity{active, closing} {
		m, err := s2.Get(id.VoiceSessionID, 1)
		if err != nil || m.State != StateUnknown || m.EvidenceID != "" {
			t.Fatalf("%s must be unknown after restart: %+v %v", id.VoiceSessionID, m, err)
		}
		if _, err := s2.Evidence(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: restart must not invent evidence", id.VoiceSessionID)
		}
	}
	m, _ := s2.Get("vs_done", 1)
	if m.State != StateTerminated || m.EvidenceID == "" {
		t.Fatalf("terminated row must survive restart: %+v", m)
	}
	if n, _ := s2.RecoverUnfinished(); n != 0 {
		t.Fatalf("second recovery must be a no-op, got %d", n)
	}
}

func TestNextGenerationAndCurrent(t *testing.T) {
	s, _, _ := openTest(t)
	if g, err := s.NextGeneration("vs_1"); err != nil || g != 1 {
		t.Fatalf("first generation=%d err=%v", g, err)
	}
	id := ident()
	_ = s.Register("o", "m", id)
	if g, _ := s.NextGeneration("vs_1"); g != 2 {
		t.Fatalf("next generation=%d", g)
	}
	cur, err := s.Current("vs_1")
	if err != nil || cur.MediaGeneration != 1 {
		t.Fatalf("current=%+v %v", cur, err)
	}
	if _, err := s.Current("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

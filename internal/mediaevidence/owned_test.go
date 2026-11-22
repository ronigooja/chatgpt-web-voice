package mediaevidence

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func TestOwnedReservationBlocksUnresolvedAccountAcrossOwners(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "voices.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStore(db)
	first := Identity{VoiceSessionID: "first", MediaGeneration: 1, AccountID: 42, LeaseID: "first-lease", LeaseGeneration: 1}
	second := Identity{VoiceSessionID: "second", MediaGeneration: 1, AccountID: 42, LeaseID: "second-lease", LeaseGeneration: 1}
	if err := s.ReserveOwned("owner1", first, "zoe1", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveOwned("owner2", second, "zoe2", 1); !errors.Is(err, ErrAccountOccupied) {
		t.Fatal(err)
	}
	if err := s.MarkClosing(first, "user_hangup"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecoverUnfinished(); err != nil {
		t.Fatal(err)
	}
	if row, err := s.Current("first"); err != nil || row.CloseAttemptFinished() {
		t.Fatal("restart falsely finished close", row, err)
	}
	if err := s.ReserveOwned("owner2", second, "zoe2", 1); !errors.Is(err, ErrAccountOccupied) {
		t.Fatal(err)
	}
	if lookup, err := s.LookupTermination("owner1", first); err != nil || lookup.Outcome != "unknown" || lookup.Evidence != nil {
		t.Fatal(lookup, err)
	}
	if _, _, err := s.OwnedBinding("owner2", "first"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if id, gen, err := s.OwnedBinding("owner1", "first"); err != nil || id != "zoe1" || gen != 1 {
		t.Fatal(id, gen, err)
	}
	second.AccountID++
	if err := s.ReserveOwned("owner2", second, "zoe2", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(second, Components{true, true, true, true}, "user_hangup"); err != nil {
		t.Fatal(err)
	}
	third := second
	third.VoiceSessionID, third.LeaseID = "third", "third-lease"
	if err := s.ReserveOwned("owner1", third, "zoe1", 3); err != nil {
		t.Fatal(err)
	}
}

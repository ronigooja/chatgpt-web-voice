package store

import (
	"path/filepath"
	"testing"
)

func TestVoiceCreationOutcomesStartupCleanupRemovesOldRowsAndKeepsFreshRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "voice.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("owner", "old"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("owner", "fresh"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn().Exec(`UPDATE voice_creation_outcomes SET updated_at = datetime('now', '-8 days') WHERE request_id = 'old'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.GetVoiceCreationOutcome("owner", "old"); err != ErrVoiceCreationOutcomeNotFound {
		t.Fatalf("old row err=%v", err)
	}
	if _, err := db.GetVoiceCreationOutcome("owner", "fresh"); err != nil {
		t.Fatalf("fresh row err=%v", err)
	}
}

func TestVoiceCreationOutcomesPendingAndMediaStatesPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "voice.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("owner", "crash"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row, err := db.GetVoiceCreationOutcome("owner", "crash")
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != "unknown" {
		t.Fatalf("reopened pending outcome=%q", row.Outcome)
	}
}

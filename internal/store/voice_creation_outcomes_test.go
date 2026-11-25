package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestMediaRequestIdentityMigrationFailsClosedForUnattributableReservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.conn.Exec(`DROP INDEX idx_voice_media_unresolved_request;
		ALTER TABLE voice_media_sessions DROP COLUMN creation_request_id;
		INSERT INTO voice_media_sessions (voice_session_id,media_generation,owner,account_id,lease_id,lease_generation,state,created_at,updated_at,zoe_session_id,zoe_generation)
		VALUES ('exact',1,'owner',1,'lease1',1,'active','now','now','zoe1',1),
		       ('unlinked',2,'owner',2,'lease2',1,'unknown','now','now','zoe2',1);
		INSERT INTO voice_creation_outcomes (owner,request_id,outcome,voice_session_id,media_generation,gateway_owned)
		VALUES ('owner','exact-request','established','exact',1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for session, want := range map[string]string{"exact": "exact-request", "unlinked": ""} {
		var request string
		if err := db.conn.QueryRow(`SELECT creation_request_id FROM voice_media_sessions WHERE voice_session_id=?`, session).Scan(&request); err != nil || request != want {
			t.Fatalf("unexpected migration identity for %s: %q, %v", session, request, err)
		}
	}
	for _, tc := range []struct{ owner, request, want string }{
		{"owner", "exact-request", VoiceCreationFenceBusy},
		{"owner", "not-linked", VoiceCreationFenceBusy},
		{"other", "not-linked", VoiceCreationFencedNoLiveMedia},
	} {
		if result, _, err := db.FenceVoiceCreation(tc.owner, tc.request); err != nil || result != tc.want {
			t.Fatalf("migration fence returned %q, %v; want %q", result, err, tc.want)
		}
	}
}

func TestRequestFenceMigrationPreservesExistingOwnedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "before-request-fence.db")
	sqlite, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqlite.Exec(`CREATE TABLE voice_creation_outcomes (
		request_id TEXT NOT NULL, owner TEXT NOT NULL, outcome TEXT NOT NULL,
		voice_session_id TEXT, media_generation INTEGER, created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL, attempt_id TEXT NOT NULL DEFAULT '', gateway_owned INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(owner, request_id));
		INSERT INTO voice_creation_outcomes VALUES
		('owned', 'owner', 'established', 'session', 3, '2026-01-01', '2026-01-02', 'attempt', 1),
		('unresolved', 'owner', 'unknown', NULL, NULL, '2026-01-01', '2026-01-02', 'unresolved-attempt', 1)`)
	if err := errors.Join(err, sqlite.Close()); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row, err := db.GetVoiceCreationOutcome("owner", "owned")
	if err != nil || row.Outcome != "established" || row.AttemptID != "attempt" || row.VoiceSessionID.String != "session" ||
		row.MediaGeneration.Int64 != 3 || row.CreatedAt != "2026-01-01" || row.UpdatedAt != "2026-01-02" {
		t.Fatalf("outcome changed during migration: %+v, %v", row, err)
	}
	if outcome, _, err := db.FenceVoiceCreation("owner", "unresolved"); err != nil || outcome != VoiceCreationFencedNoLiveMedia {
		t.Fatalf("existing unresolved claim was not fenced: %q %v", outcome, err)
	}
	if err := db.UpdateVoiceCreationOutcome("owner", "unresolved", "never_established", "", 0, "unresolved-attempt"); !errors.Is(err, ErrVoiceCreationAttemptStale) {
		t.Fatalf("fenced claim was still mutable: %v", err)
	}
	if _, err := db.BeginVoiceCreationOutcome("owner", "unresolved"); !errors.Is(err, ErrVoiceCreationFenced) {
		t.Fatalf("fenced claim could be reopened: %v", err)
	}
}

func TestOwnedAttemptFenceSurvivesLegacyCleanupAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := db.ClaimOwnedVoiceCreation("owner", "owned-request")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateVoiceCreationOutcome("owner", "owned-request", "never_established", "", 0, attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(`UPDATE voice_creation_outcomes SET updated_at='2001-01-01'`); err != nil {
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
	if _, err := db.ClaimOwnedVoiceCreation("owner", "owned-request"); !errors.Is(err, ErrVoiceCreationAttemptStale) {
		t.Fatal(err)
	}
	if _, err := db.BeginVoiceCreationOutcome("owner", "owned-request"); !errors.Is(err, ErrVoiceCreationAttemptStale) {
		t.Fatal(err)
	}
	if outcome, _, err := db.VoiceCreationOutcomeSnapshot("owner", "owned-request"); err != nil || outcome != "never_established" {
		t.Fatal(outcome, err)
	}
}

func TestVoiceCreationOutcomeOldSchemaMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE voice_creation_outcomes (
		request_id TEXT NOT NULL, owner TEXT NOT NULL, outcome TEXT NOT NULL,
		voice_session_id TEXT, media_generation INTEGER,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY(owner,request_id)); INSERT INTO voice_creation_outcomes(request_id,owner,outcome) VALUES('duplicate','owner','never_established')`)
	if err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if has, err := db.hasColumn("voice_creation_outcomes", "attempt_id"); err != nil || !has {
		t.Fatalf("attempt_id exists=%v err=%v", has, err)
	}
	first, err := db.BeginVoiceCreationOutcome("owner", "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginVoiceCreationOutcome("owner", "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || second == "" || first == second {
		t.Fatal("UPSERT did not allocate distinct attempts")
	}
	if err := db.UpdateVoiceCreationOutcome("owner", "duplicate", "never_established", "", 0, second); err != nil {
		t.Fatal(err)
	}
	row, err := db.GetVoiceCreationOutcome("owner", "duplicate")
	if err != nil || row.AttemptID != second || row.Outcome != "never_established" {
		t.Fatalf("row=%+v err=%v", row, err)
	}
}

func TestCreationAttemptDuplicateResetRace(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "voice.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old, err := db.BeginVoiceCreationOutcome("owner", "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	reset := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		<-reset
		result <- db.UpdateVoiceCreationOutcome("owner", "duplicate", "established", "old-media", 1, old)
	}()
	current, err := db.BeginVoiceCreationOutcome("owner", "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	close(reset)
	if err := <-result; !errors.Is(err, ErrVoiceCreationAttemptStale) {
		t.Fatalf("stale update=%v", err)
	}
	row, err := db.GetVoiceCreationOutcome("owner", "duplicate")
	if err != nil || row.Outcome != "pending" || row.AttemptID != current || row.VoiceSessionID.Valid {
		t.Fatalf("row=%+v err=%v", row, err)
	}
}

func TestCreationOutcomeAtomicSnapshotRace(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "voice.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Conn().Exec(`INSERT INTO voice_media_sessions (voice_session_id, media_generation, owner, account_id, lease_id, lease_generation, relay_session_id, state, evidence_id, peer_a_closed, peer_b_closed, data_channels_closed, tunnels_closed, created_at, updated_at) VALUES ('vs',1,'owner',1,'lease',1,'relay','terminated','ev',1,1,1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn().Exec(`INSERT INTO voice_termination_evidence (evidence_id,voice_session_id,media_generation,account_id,lease_id,lease_generation,observed_at,reason) VALUES ('ev','vs',1,1,'lease',1,CURRENT_TIMESTAMP,'test')`); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateVoiceCreationOutcome("owner", "duplicate"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			attempt, err := db.BeginVoiceCreationOutcome("owner", "duplicate")
			if err != nil {
				done <- err
				return
			}
			if err := db.UpdateVoiceCreationOutcome("owner", "duplicate", "established", "vs", 1, attempt); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 100; i++ {
		outcome, evidence, err := db.VoiceCreationOutcomeSnapshot("owner", "duplicate")
		if err != nil || (outcome == "released" && evidence != "ev") || (outcome != "released" && (outcome != "unknown" || evidence != "")) {
			t.Errorf("snapshot=%q,%q err=%v", outcome, evidence, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn().Exec(`UPDATE voice_media_sessions SET evidence_id='missing-evidence'`); err != nil {
		t.Fatal(err)
	}
	if outcome, evidence, err := db.VoiceCreationOutcomeSnapshot("owner", "duplicate"); err != nil || outcome != "unknown" || evidence != "" {
		t.Fatalf("missing evidence snapshot=%q,%q err=%v", outcome, evidence, err)
	}
}

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

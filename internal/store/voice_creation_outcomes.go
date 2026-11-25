package store

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrVoiceCreationOutcomeNotFound = errors.New("voice creation outcome not found")
var ErrVoiceCreationAttemptStale = errors.New("voice creation attempt is stale or missing")
var ErrVoiceCreationFenced = errors.New("voice creation request is fenced")

const VoiceCreationFencedNoLiveMedia = "fenced_no_live_media"

type VoiceCreationOutcome struct {
	RequestID       string
	Owner           string
	Outcome         string
	AttemptID       string
	VoiceSessionID  sql.NullString
	MediaGeneration sql.NullInt64
	CreatedAt       string
	UpdatedAt       string
}

func (db *DB) CreateVoiceCreationOutcome(owner, requestID string) error {
	_, err := db.BeginVoiceCreationOutcome(owner, requestID)
	return err
}

// BeginVoiceCreationOutcome resets a duplicate request to a new, non-idempotent
// attempt. Only the returned attempt identity may finalize this row.
func (db *DB) BeginVoiceCreationOutcome(owner, requestID string) (string, error) {
	return db.beginVoiceCreationOutcome(owner, requestID, false)
}

// ClaimOwnedVoiceCreation inserts a permanent fence without reopening a replay.
func (db *DB) ClaimOwnedVoiceCreation(owner, requestID string) (string, error) {
	return db.beginVoiceCreationOutcome(owner, requestID, true)
}

func (db *DB) beginVoiceCreationOutcome(owner, requestID string, owned bool) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate creation attempt: %w", err)
	}
	attemptID := fmt.Sprintf("%x", random[:])
	now := time.Now().UTC().Format(time.RFC3339Nano)
	conflict := ` ON CONFLICT(owner, request_id) DO NOTHING`
	if !owned {
		conflict = ` ON CONFLICT(owner, request_id) DO UPDATE SET outcome='pending', attempt_id=excluded.attempt_id, voice_session_id=NULL, media_generation=NULL, updated_at=excluded.updated_at WHERE voice_creation_outcomes.gateway_owned=0 AND voice_creation_outcomes.fenced=0`
	}
	db.Lock()
	defer db.Unlock()
	res, err := db.conn.Exec(`INSERT INTO voice_creation_outcomes (request_id, owner, outcome, attempt_id, created_at, updated_at, gateway_owned) VALUES (?, ?, 'pending', ?, ?, ?, ?)`+conflict, requestID, owner, attemptID, now, now, owned)
	if err != nil {
		return "", fmt.Errorf("create voice outcome: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return "", err
	} else if n != 1 {
		var fenced bool
		if err := db.conn.QueryRow(`SELECT fenced FROM voice_creation_outcomes WHERE owner=? AND request_id=?`, owner, requestID).Scan(&fenced); err != nil {
			return "", err
		}
		if fenced {
			return "", ErrVoiceCreationFenced
		}
		return "", ErrVoiceCreationAttemptStale
	}
	return attemptID, nil
}

func (db *DB) UpdateVoiceCreationOutcome(owner, requestID, outcome, sessionID string, generation int64, attemptID string) error {
	if attemptID == "" {
		return ErrVoiceCreationAttemptStale
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var sid any
	if strings.TrimSpace(sessionID) != "" {
		sid = sessionID
	}
	var gen any
	if generation > 0 {
		gen = generation
	}
	result, err := db.conn.Exec(`UPDATE voice_creation_outcomes SET outcome=?, voice_session_id=?, media_generation=?, updated_at=? WHERE owner=? AND request_id=? AND attempt_id=? AND fenced=0`, outcome, sid, gen, now, owner, requestID, attemptID)
	if err != nil {
		return fmt.Errorf("update voice outcome: %w", err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ErrVoiceCreationAttemptStale
	}
	return nil
}

func (db *DB) GetVoiceCreationOutcome(owner, requestID string) (VoiceCreationOutcome, error) {
	var row VoiceCreationOutcome
	err := db.conn.QueryRow(`SELECT request_id, owner, outcome, voice_session_id, media_generation, created_at, updated_at, attempt_id FROM voice_creation_outcomes WHERE owner=? AND request_id=?`, owner, requestID).Scan(&row.RequestID, &row.Owner, &row.Outcome, &row.VoiceSessionID, &row.MediaGeneration, &row.CreatedAt, &row.UpdatedAt, &row.AttemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return row, ErrVoiceCreationOutcomeNotFound
	}
	if err != nil {
		return row, fmt.Errorf("get voice outcome: %w", err)
	}
	return row, nil
}

func (db *DB) MediaOutcome(voiceSessionID string, generation int64) (string, error) {
	var state, evidence sql.NullString
	err := db.conn.QueryRow(`SELECT state, evidence_id FROM voice_media_sessions WHERE voice_session_id=? AND media_generation=?`, voiceSessionID, generation).Scan(&state, &evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return "unknown", nil
	}
	if err != nil {
		return "", fmt.Errorf("get media outcome: %w", err)
	}
	switch {
	case state.String == "active" || state.String == "closing":
		return "open", nil
	case state.String == "terminated" && evidence.Valid && strings.TrimSpace(evidence.String) != "":
		return "released", nil
	default:
		return "unknown", nil
	}
}

// VoiceCreationOutcomeSnapshot derives both public fields from one SQLite read
// snapshot, joining evidence by the complete owner/session/lease identity.
func (db *DB) VoiceCreationOutcomeSnapshot(owner, requestID string) (outcome, evidenceID string, err error) {
	return voiceCreationOutcomeSnapshot(db.conn, owner, requestID)
}

type creationOutcomeQuery interface {
	QueryRow(string, ...any) *sql.Row
}

func voiceCreationOutcomeSnapshot(q creationOutcomeQuery, owner, requestID string) (outcome, evidenceID string, err error) {
	var stored string
	var state, evidence sql.NullString
	err = q.QueryRow(`SELECT c.outcome, m.state, e.evidence_id
		FROM voice_creation_outcomes c
		LEFT JOIN voice_media_sessions m ON c.voice_session_id=m.voice_session_id AND c.media_generation=m.media_generation AND c.owner=m.owner
		LEFT JOIN voice_termination_evidence e ON e.evidence_id=m.evidence_id
		 AND e.voice_session_id=m.voice_session_id AND e.media_generation=m.media_generation
		 AND e.account_id=m.account_id AND e.lease_id=m.lease_id AND e.lease_generation=m.lease_generation
		 AND m.peer_a_closed=1 AND m.peer_b_closed=1 AND m.data_channels_closed=1 AND m.tunnels_closed=1 AND m.retired_legs_unclean=0
		WHERE c.owner=? AND c.request_id=?`, owner, requestID).Scan(&stored, &state, &evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return "unknown", "", ErrVoiceCreationOutcomeNotFound
	}
	if err != nil {
		return "unknown", "", fmt.Errorf("read creation outcome snapshot: %w", err)
	}
	switch {
	case stored == "never_established":
		return stored, "", nil
	case stored == "established" && (state.String == "active" || state.String == "closing"):
		return "open", "", nil
	case stored == "established" && state.String == "terminated" && strings.TrimSpace(evidence.String) != "":
		return "released", evidence.String, nil
	default:
		return "unknown", "", nil
	}
}

// FenceVoiceCreation preserves decided outcomes and atomically tombstones an
// absent or unresolved claim. The caller serializes this operation with local
// create admission and ensures the request has no live or in-flight media.
func (db *DB) FenceVoiceCreation(owner, requestID string) (outcome, evidenceID string, err error) {
	db.Lock()
	defer db.Unlock()
	tx, err := db.conn.Begin()
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	outcome, evidenceID, err = voiceCreationOutcomeSnapshot(tx, owner, requestID)
	if err != nil && !errors.Is(err, ErrVoiceCreationOutcomeNotFound) {
		return "", "", err
	}
	if outcome != "unknown" {
		return outcome, evidenceID, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.Exec(`INSERT INTO voice_creation_outcomes (owner, request_id, outcome, created_at, updated_at, gateway_owned, fenced)
		VALUES (?, ?, 'unknown', ?, ?, 1, 1)
		ON CONFLICT(owner, request_id) DO UPDATE SET fenced=1, gateway_owned=1, updated_at=excluded.updated_at
		WHERE voice_creation_outcomes.fenced=0`, owner, requestID, now, now)
	if err != nil {
		return "", "", err
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return VoiceCreationFencedNoLiveMedia, "", nil
}

// DeleteOldVoiceCreationOutcomes is exposed for startup-cleanup tests and
// delegates to the same migration cleanup rule used by Open.
func (db *DB) DeleteOldVoiceCreationOutcomes() error { return db.cleanupVoiceCreationOutcomes() }

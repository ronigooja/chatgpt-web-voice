package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrVoiceCreationOutcomeNotFound = errors.New("voice creation outcome not found")

type VoiceCreationOutcome struct {
	RequestID       string
	Owner           string
	Outcome         string
	VoiceSessionID  sql.NullString
	MediaGeneration sql.NullInt64
	CreatedAt       string
	UpdatedAt       string
}

func (db *DB) CreateVoiceCreationOutcome(owner, requestID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := db.conn.Exec(`INSERT INTO voice_creation_outcomes (request_id, owner, outcome, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?) ON CONFLICT(owner, request_id) DO UPDATE SET outcome='pending', voice_session_id=NULL, media_generation=NULL, updated_at=excluded.updated_at`, requestID, owner, now, now)
	if err != nil {
		return fmt.Errorf("create voice outcome: %w", err)
	}
	return nil
}

func (db *DB) UpdateVoiceCreationOutcome(owner, requestID, outcome, sessionID string, generation int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var sid any
	if strings.TrimSpace(sessionID) != "" {
		sid = sessionID
	}
	var gen any
	if generation > 0 {
		gen = generation
	}
	result, err := db.conn.Exec(`UPDATE voice_creation_outcomes SET outcome=?, voice_session_id=?, media_generation=?, updated_at=? WHERE owner=? AND request_id=?`, outcome, sid, gen, now, owner, requestID)
	if err != nil {
		return fmt.Errorf("update voice outcome: %w", err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ErrVoiceCreationOutcomeNotFound
	}
	return nil
}

func (db *DB) GetVoiceCreationOutcome(owner, requestID string) (VoiceCreationOutcome, error) {
	var row VoiceCreationOutcome
	err := db.conn.QueryRow(`SELECT request_id, owner, outcome, voice_session_id, media_generation, created_at, updated_at FROM voice_creation_outcomes WHERE owner=? AND request_id=?`, owner, requestID).Scan(&row.RequestID, &row.Owner, &row.Outcome, &row.VoiceSessionID, &row.MediaGeneration, &row.CreatedAt, &row.UpdatedAt)
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

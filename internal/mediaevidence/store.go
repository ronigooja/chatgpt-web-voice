// Package mediaevidence persists gateway-attested media termination records.
//
// A record is a local observation: the gateway closed its own peers and the
// close calls returned without error. It does not prove the upstream service
// reclaimed the account.
package mediaevidence

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

// Media session states.
const (
	StateActive     = "active"
	StateClosing    = "closing"
	StateTerminated = "terminated"
	StateUnknown    = "unknown"
)

// ErrNotFound means no media session matches the exact identity.
var ErrNotFound = errors.New("media session not found")

// ErrIdentityMismatch means the session exists but its lease identity differs.
var ErrIdentityMismatch = errors.New("lease identity does not match media session")

// Identity is the exact tuple evidence is bound to.
type Identity struct {
	VoiceSessionID  string
	MediaGeneration int64
	AccountID       int64
	LeaseID         string
	LeaseGeneration int64
}

// Components records per-component close results.
type Components struct {
	PeerAClosed        bool
	PeerBClosed        bool
	DataChannelsClosed bool
	TunnelsClosed      bool
}

// Clean reports whether every component closed without error.
func (c Components) Clean() bool {
	return c.PeerAClosed && c.PeerBClosed && c.DataChannelsClosed && c.TunnelsClosed
}

// MediaSession is one row of voice_media_sessions.
type MediaSession struct {
	Identity
	Owner        string
	RelaySession string
	State        string
	Components   Components
	CloseReason  string
	// RetiredLegs counts upstream legs replaced during the call;
	// RetiredLegsUnclean counts those that did not close cleanly.
	RetiredLegs        int
	RetiredLegsUnclean int
	// CurrentAccountID is the account of the leg now carrying the call.
	CurrentAccountID int64
	CreatedAt        string
	UpdatedAt        string
	TerminatedAt     string
	EvidenceID       string
	EvidenceError    string
}

// Evidence is one row of voice_termination_evidence.
type Evidence struct {
	Identity
	EvidenceID string
	ObservedAt string
	Reason     string
}

// Store is the SQLite repository for media sessions and evidence.
type Store struct {
	db  *store.DB
	now func() time.Time
}

// NewStore wraps an opened database. Tables are created by store.Open.
func NewStore(db *store.DB) *Store {
	return &Store{db: db, now: time.Now}
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func newEvidenceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate evidence id: %w", err)
	}
	return "ev_" + hex.EncodeToString(b[:]), nil
}

func (id Identity) valid() error {
	if strings.TrimSpace(id.VoiceSessionID) == "" || strings.TrimSpace(id.LeaseID) == "" ||
		id.MediaGeneration < 1 || id.LeaseGeneration < 1 || id.AccountID <= 0 {
		return errors.New("media identity is incomplete")
	}
	return nil
}

// Register records a new active media session. Registering the same
// (session, generation) twice is an error: generations are never reused.
func (s *Store) Register(owner, relaySession string, id Identity) error {
	if err := id.valid(); err != nil {
		return err
	}
	now := ts(s.now())
	s.db.Lock()
	defer s.db.Unlock()
	_, err := s.db.Conn().Exec(`
		INSERT INTO voice_media_sessions (
			voice_session_id, media_generation, owner, account_id, lease_id, lease_generation,
			relay_session_id, state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id.VoiceSessionID, id.MediaGeneration, owner, id.AccountID, id.LeaseID, id.LeaseGeneration,
		relaySession, StateActive, now, now)
	if err != nil {
		return fmt.Errorf("register media session: %w", err)
	}
	return nil
}

// Close records the outcome of closing a media session. It moves the row to
// terminated only when every component closed cleanly and the evidence row was
// written; otherwise it moves it to unknown. The evidence write and the state
// change happen in one transaction, so a terminated row always has evidence.
//
// Close is idempotent: a row already terminated or unknown is returned as is.
// Evidence for a (session, generation, lease, lease generation) is never
// overwritten.
func (s *Store) Close(id Identity, c Components, reason string) (MediaSession, error) {
	if err := id.valid(); err != nil {
		return MediaSession{}, err
	}
	s.db.Lock()
	defer s.db.Unlock()

	current, err := s.getUnlocked(id.VoiceSessionID, id.MediaGeneration)
	if err != nil {
		return MediaSession{}, err
	}
	if !sameLease(current.Identity, id) {
		return MediaSession{}, ErrIdentityMismatch
	}
	if current.State == StateTerminated || current.State == StateUnknown {
		return current, nil
	}

	now := ts(s.now())
	if current.RetiredLegsUnclean > 0 {
		if err := s.markUnknownUnlocked(id, c, reason, now, "a replaced upstream leg did not close cleanly"); err != nil {
			return MediaSession{}, err
		}
		return s.getUnlocked(id.VoiceSessionID, id.MediaGeneration)
	}
	if c.Clean() {
		if _, err := s.terminateUnlocked(id, c, reason, now); err == nil {
			return s.getUnlocked(id.VoiceSessionID, id.MediaGeneration)
		}
		// Evidence write failed (or the row was no longer closable): keep the lease blocked.
		if err := s.markUnknownUnlocked(id, c, reason, now, "evidence write failed"); err != nil {
			return MediaSession{}, err
		}
		return s.getUnlocked(id.VoiceSessionID, id.MediaGeneration)
	}
	if err := s.markUnknownUnlocked(id, c, reason, now, "component close not clean"); err != nil {
		return MediaSession{}, err
	}
	return s.getUnlocked(id.VoiceSessionID, id.MediaGeneration)
}

// terminateUnlocked writes the evidence row and the terminated state in one
// transaction, so a terminated row always has evidence and vice versa.
func (s *Store) terminateUnlocked(id Identity, c Components, reason, now string) (string, error) {
	evidenceID, err := newEvidenceID()
	if err != nil {
		return "", err
	}
	tx, err := s.db.Conn().Begin()
	if err != nil {
		return "", fmt.Errorf("begin terminate: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		INSERT INTO voice_termination_evidence (
			evidence_id, voice_session_id, media_generation, account_id,
			lease_id, lease_generation, observed_at, reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		evidenceID, id.VoiceSessionID, id.MediaGeneration, id.AccountID,
		id.LeaseID, id.LeaseGeneration, now, reason); err != nil {
		return "", fmt.Errorf("write evidence: %w", err)
	}
	res, err := tx.Exec(`
		UPDATE voice_media_sessions SET
			state = ?, peer_a_closed = ?, peer_b_closed = ?, data_channels_closed = ?, tunnels_closed = ?,
			close_reason = ?, evidence_id = ?, evidence_error = '', terminated_at = ?, updated_at = ?
		WHERE voice_session_id = ? AND media_generation = ? AND state IN (?, ?)`,
		StateTerminated, b2i(c.PeerAClosed), b2i(c.PeerBClosed), b2i(c.DataChannelsClosed), b2i(c.TunnelsClosed),
		reason, evidenceID, now, now,
		id.VoiceSessionID, id.MediaGeneration, StateActive, StateClosing)
	if err != nil {
		return "", fmt.Errorf("mark terminated: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return "", errors.New("media session is no longer closable")
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit terminate: %w", err)
	}
	return evidenceID, nil
}

func (s *Store) markUnknownUnlocked(id Identity, c Components, reason, now, why string) error {
	_, err := s.db.Conn().Exec(`
		UPDATE voice_media_sessions SET
			state = ?, peer_a_closed = ?, peer_b_closed = ?, data_channels_closed = ?, tunnels_closed = ?,
			close_reason = ?, evidence_error = ?, updated_at = ?
		WHERE voice_session_id = ? AND media_generation = ? AND state IN (?, ?)`,
		StateUnknown, b2i(c.PeerAClosed), b2i(c.PeerBClosed), b2i(c.DataChannelsClosed), b2i(c.TunnelsClosed),
		reason, why, now,
		id.VoiceSessionID, id.MediaGeneration, StateActive, StateClosing)
	if err != nil {
		return fmt.Errorf("mark media session unknown: %w", err)
	}
	return nil
}

// RecordLegSwap records that the active upstream leg was replaced by one on
// newAccountID. clean says whether the retired leg closed without error. Only an
// active row can change; a closing or finished row is left alone.
func (s *Store) RecordLegSwap(id Identity, newAccountID int64, clean bool) error {
	if err := id.valid(); err != nil {
		return err
	}
	unclean := 0
	if !clean {
		unclean = 1
	}
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`
		UPDATE voice_media_sessions SET
			retired_legs = retired_legs + 1,
			retired_legs_unclean = retired_legs_unclean + ?,
			current_account_id = ?,
			updated_at = ?
		WHERE voice_session_id = ? AND media_generation = ? AND lease_id = ? AND lease_generation = ? AND state = ?`,
		unclean, newAccountID, ts(s.now()),
		id.VoiceSessionID, id.MediaGeneration, id.LeaseID, id.LeaseGeneration, StateActive)
	if err != nil {
		return fmt.Errorf("record leg swap: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("media session is not active")
	}
	return nil
}

// MarkClosing moves an active row to closing so a crash during close is
// recoverable as unknown. It is a no-op for other states.
func (s *Store) MarkClosing(id Identity, reason string) error {
	if err := id.valid(); err != nil {
		return err
	}
	s.db.Lock()
	defer s.db.Unlock()
	_, err := s.db.Conn().Exec(`
		UPDATE voice_media_sessions SET state = ?, close_reason = ?, updated_at = ?
		WHERE voice_session_id = ? AND media_generation = ? AND lease_id = ? AND lease_generation = ? AND state = ?`,
		StateClosing, reason, ts(s.now()),
		id.VoiceSessionID, id.MediaGeneration, id.LeaseID, id.LeaseGeneration, StateActive)
	if err != nil {
		return fmt.Errorf("mark media session closing: %w", err)
	}
	return nil
}

// RecoverUnfinished turns every active or closing row into unknown. Call it on
// startup: a restart that finds only a session row cannot show the peers closed.
func (s *Store) RecoverUnfinished() (int64, error) {
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`
		UPDATE voice_media_sessions SET
			state = ?, evidence_error = 'gateway restarted before close was observed',
			updated_at = ?
		WHERE state IN (?, ?)`,
		StateUnknown, ts(s.now()), StateActive, StateClosing)
	if err != nil {
		return 0, fmt.Errorf("recover media sessions: %w", err)
	}
	return res.RowsAffected()
}

// Get returns one media session by exact (session, generation).
func (s *Store) Get(voiceSessionID string, generation int64) (MediaSession, error) {
	s.db.Lock()
	defer s.db.Unlock()
	return s.getUnlocked(voiceSessionID, generation)
}

// Evidence returns the evidence row for the exact identity, or ErrNotFound.
func (s *Store) Evidence(id Identity) (Evidence, error) {
	s.db.Lock()
	defer s.db.Unlock()
	row := s.db.Conn().QueryRow(`
		SELECT evidence_id, voice_session_id, media_generation, account_id,
			lease_id, lease_generation, observed_at, reason
		FROM voice_termination_evidence
		WHERE voice_session_id = ? AND media_generation = ? AND lease_id = ? AND lease_generation = ?`,
		id.VoiceSessionID, id.MediaGeneration, id.LeaseID, id.LeaseGeneration)
	var e Evidence
	err := row.Scan(&e.EvidenceID, &e.VoiceSessionID, &e.MediaGeneration, &e.AccountID,
		&e.LeaseID, &e.LeaseGeneration, &e.ObservedAt, &e.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return Evidence{}, ErrNotFound
	}
	if err != nil {
		return Evidence{}, fmt.Errorf("read evidence: %w", err)
	}
	return e, nil
}

// Current returns the highest-generation media session for a voice session.
func (s *Store) Current(voiceSessionID string) (MediaSession, error) {
	s.db.Lock()
	defer s.db.Unlock()
	row := s.db.Conn().QueryRow(mediaSelect+` WHERE voice_session_id = ? ORDER BY media_generation DESC LIMIT 1`, voiceSessionID)
	m, err := scanMedia(row)
	if errors.Is(err, sql.ErrNoRows) {
		return MediaSession{}, ErrNotFound
	}
	return m, err
}

// NextGeneration returns the next unused media generation for a voice session.
func (s *Store) NextGeneration(voiceSessionID string) (int64, error) {
	s.db.Lock()
	defer s.db.Unlock()
	var max sql.NullInt64
	if err := s.db.Conn().QueryRow(
		`SELECT MAX(media_generation) FROM voice_media_sessions WHERE voice_session_id = ?`, voiceSessionID,
	).Scan(&max); err != nil {
		return 0, fmt.Errorf("read media generation: %w", err)
	}
	return max.Int64 + 1, nil
}

const mediaSelect = `
	SELECT voice_session_id, media_generation, owner, account_id, lease_id, lease_generation,
		relay_session_id, state, peer_a_closed, peer_b_closed, data_channels_closed, tunnels_closed,
		close_reason, created_at, updated_at, terminated_at, evidence_id, evidence_error,
		retired_legs, retired_legs_unclean, current_account_id
	FROM voice_media_sessions`

func (s *Store) getUnlocked(voiceSessionID string, generation int64) (MediaSession, error) {
	row := s.db.Conn().QueryRow(mediaSelect+` WHERE voice_session_id = ? AND media_generation = ?`, voiceSessionID, generation)
	m, err := scanMedia(row)
	if errors.Is(err, sql.ErrNoRows) {
		return MediaSession{}, ErrNotFound
	}
	return m, err
}

func scanMedia(row store.Scanner) (MediaSession, error) {
	var m MediaSession
	var pa, pb, dc, tn int
	if err := row.Scan(&m.VoiceSessionID, &m.MediaGeneration, &m.Owner, &m.AccountID, &m.LeaseID, &m.LeaseGeneration,
		&m.RelaySession, &m.State, &pa, &pb, &dc, &tn,
		&m.CloseReason, &m.CreatedAt, &m.UpdatedAt, &m.TerminatedAt, &m.EvidenceID, &m.EvidenceError,
		&m.RetiredLegs, &m.RetiredLegsUnclean, &m.CurrentAccountID); err != nil {
		return MediaSession{}, err
	}
	m.Components = Components{PeerAClosed: pa == 1, PeerBClosed: pb == 1, DataChannelsClosed: dc == 1, TunnelsClosed: tn == 1}
	return m, nil
}

func sameLease(a, b Identity) bool {
	return a.VoiceSessionID == b.VoiceSessionID && a.MediaGeneration == b.MediaGeneration &&
		a.AccountID == b.AccountID && a.LeaseID == b.LeaseID && a.LeaseGeneration == b.LeaseGeneration
}

func b2i(v bool) int {
	if v {
		return 1
	}
	return 0
}

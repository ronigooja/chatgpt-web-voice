package mediaevidence

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var ErrAccountOccupied = errors.New("account already has unresolved media")

// ReserveOwned claims the account before the gateway touches the relay or AVAS.
// A crash after reservation leaves the account unavailable, not reusable.
func (s *Store) ReserveOwned(owner string, id Identity, zoeID string, zoeGeneration int64) error {
	if err := id.valid(); err != nil {
		return err
	}
	now := ts(s.now())
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`INSERT INTO voice_media_sessions
		(voice_session_id, media_generation, owner, account_id, lease_id, lease_generation, state, created_at, updated_at, zoe_session_id, zoe_generation)
		SELECT ?,?,?,?,?,?,'active',?,?,?,?
		WHERE NOT EXISTS (SELECT 1 FROM voice_media_sessions WHERE state <> 'terminated' AND (account_id=? OR current_account_id=?))`,
		id.VoiceSessionID, id.MediaGeneration, owner, id.AccountID, id.LeaseID, id.LeaseGeneration, now, now, zoeID, zoeGeneration, id.AccountID, id.AccountID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return ErrAccountOccupied
	}
	return nil
}

func (s *Store) AttachOwnedRelay(id Identity, relayID string) error {
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`UPDATE voice_media_sessions SET relay_session_id=?
		WHERE voice_session_id=? AND media_generation=? AND state='active' AND zoe_generation>0`, relayID, id.VoiceSessionID, id.MediaGeneration)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return ErrNotFound
	}
	return nil
}

// AbandonOwnedReservation is only for relay capacity refusal before allocation.
func (s *Store) AbandonOwnedReservation(id Identity) error {
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`DELETE FROM voice_media_sessions WHERE voice_session_id=? AND media_generation=?
		AND state='active' AND relay_session_id='' AND zoe_generation>0`, id.VoiceSessionID, id.MediaGeneration)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) OwnedBinding(owner, handle string) (string, int64, error) {
	s.db.Lock()
	defer s.db.Unlock()
	var id string
	var generation int64
	err := s.db.Conn().QueryRow(`SELECT zoe_session_id, zoe_generation FROM voice_media_sessions
		WHERE owner=? AND voice_session_id=? AND zoe_generation>0`, owner, handle).Scan(&id, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return id, generation, err
}

type TerminationRecord struct {
	VoiceSessionID     string `json:"voice_session_id"`
	MediaGeneration    int64  `json:"voice_session_generation"`
	LeaseID            string `json:"account_lease_id"`
	LeaseGeneration    int64  `json:"account_lease_generation"`
	AccountAlias       string `json:"account_id"`
	Terminated         bool   `json:"terminated"`
	EvidenceID         string `json:"evidence_id"`
	ObservedAt         string `json:"observed_at"`
	Reason             string `json:"reason"`
	PeerAClosed        bool   `json:"peer_a_closed"`
	PeerBClosed        bool   `json:"peer_b_closed"`
	DataChannelsClosed bool   `json:"data_channels_closed"`
	TunnelsClosed      bool   `json:"tunnels_closed"`
	RetiredLegs        int    `json:"retired_legs"`
	RetiredLegsUnclean int    `json:"retired_legs_unclean"`
}

type TerminationLookup struct {
	Outcome  string             `json:"outcome"`
	Evidence *TerminationRecord `json:"termination_evidence,omitempty"`
}

var evidenceIDPattern = regexp.MustCompile(`^ev_[0-9a-f]{32}$`)

func (s *Store) LookupTermination(owner string, key Identity) (TerminationLookup, error) {
	result := TerminationLookup{Outcome: "unknown"}
	s.db.Lock()
	defer s.db.Unlock()
	var state string
	var id, observed, reason sql.NullString
	var account int64
	record := TerminationRecord{VoiceSessionID: key.VoiceSessionID, MediaGeneration: key.MediaGeneration, LeaseID: key.LeaseID, LeaseGeneration: key.LeaseGeneration}
	err := s.db.Conn().QueryRow(`SELECT m.state, m.account_id, m.peer_a_closed, m.peer_b_closed, m.data_channels_closed, m.tunnels_closed,
		m.retired_legs, m.retired_legs_unclean, e.evidence_id, e.observed_at, e.reason
		FROM voice_media_sessions m LEFT JOIN voice_termination_evidence e ON e.evidence_id=m.evidence_id
		AND e.voice_session_id=m.voice_session_id AND e.media_generation=m.media_generation
		AND e.account_id=m.account_id AND e.lease_id=m.lease_id AND e.lease_generation=m.lease_generation
		AND e.reason=m.close_reason
		WHERE m.owner=? AND m.voice_session_id=? AND m.media_generation=? AND m.lease_id=? AND m.lease_generation=?`,
		owner, key.VoiceSessionID, key.MediaGeneration, key.LeaseID, key.LeaseGeneration).Scan(&state, &account, &record.PeerAClosed,
		&record.PeerBClosed, &record.DataChannelsClosed, &record.TunnelsClosed, &record.RetiredLegs, &record.RetiredLegsUnclean, &id, &observed, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if state == StateActive || state == StateClosing {
		result.Outcome = "not_yet"
		return result, nil
	}
	at, err := time.Parse(time.RFC3339Nano, observed.String)
	if state != StateTerminated || !record.PeerAClosed || !record.PeerBClosed || !record.DataChannelsClosed || !record.TunnelsClosed ||
		record.RetiredLegs < 0 || record.RetiredLegsUnclean != 0 || !evidenceIDPattern.MatchString(id.String) || reason.String == "" ||
		err != nil || at.IsZero() || at.After(s.now()) || account <= 0 {
		return result, nil
	}
	alias := sha256.Sum256([]byte(fmt.Sprintf("gateway-account:%d", account)))
	record.AccountAlias = fmt.Sprintf("acct_%x", alias[:16])
	record.Terminated, record.EvidenceID, record.ObservedAt, record.Reason = true, id.String, observed.String, reason.String
	return TerminationLookup{Outcome: "released", Evidence: &record}, nil
}

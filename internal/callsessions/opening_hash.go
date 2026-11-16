package callsessions

import (
	"errors"
)

var ErrOpeningConflict = errors.New("opening context identity conflict")

// CheckOpeningHash applies before existing-session context injection. Empty
// prior hashes are not evidence for an identical version.
func (s *Store) CheckOpeningHash(owner, id string, version int, hash string) error {
	s.db.Lock()
	defer s.db.Unlock()
	var oldVersion int
	var oldHash string
	if err := s.db.Conn().QueryRow(`SELECT opening_context_version,opening_context_hash FROM call_sessions WHERE owner=? AND voice_session_id=?`, owner, id).Scan(&oldVersion, &oldHash); err != nil {
		return err
	}
	if version < oldVersion || version == oldVersion && (oldHash == "" || oldHash != hash) {
		return ErrOpeningConflict
	}
	return nil
}

// FinalizeOpeningHash applies only to a successful concrete media creation.
// The service excludes binding replacement while this atomic update executes.
func (s *Store) FinalizeOpeningHash(owner, id string, generation int64, kind string, version, items, chars int, hash, at string) error {
	s.db.Lock()
	defer s.db.Unlock()
	res, err := s.db.Conn().Exec(`UPDATE call_sessions SET opening_context_version=?,opening_context_kind=?,opening_context_items=?,opening_context_chars=?,opening_context_applied_at=?,opening_context_hash=?,opening_context_generation=?,context_chars=context_chars+CASE WHEN opening_context_generation=? AND opening_context_version=? AND opening_context_hash=? THEN 0 ELSE ? END WHERE owner=? AND voice_session_id=? AND (opening_context_version<? OR (opening_context_version=? AND opening_context_hash=?)) AND EXISTS(SELECT 1 FROM voice_media_sessions m WHERE m.owner=call_sessions.owner AND m.voice_session_id=call_sessions.voice_session_id AND m.media_generation=? AND m.state='active')`, version, kind, items, chars, at, hash, generation, generation, version, hash, chars, owner, id, version, version, hash, generation)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return ErrOpeningConflict
	}
	return err
}

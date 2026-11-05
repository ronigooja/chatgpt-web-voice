package voice

import (
	"errors"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

type CreationOutcomeStore interface {
	CreateVoiceCreationOutcome(owner, requestID string) error
	UpdateVoiceCreationOutcome(owner, requestID, outcome, sessionID string, generation int64) error
	GetVoiceCreationOutcome(owner, requestID string) (store.VoiceCreationOutcome, error)
	MediaOutcome(voiceSessionID string, generation int64) (string, error)
}

func (s *Service) creationOutcome(owner, requestID string) (string, bool) {
	if s.creationOutcomes == nil {
		return "unknown", false
	}
	row, err := s.creationOutcomes.GetVoiceCreationOutcome(owner, requestID)
	if errors.Is(err, store.ErrVoiceCreationOutcomeNotFound) {
		return "unknown", false
	}
	if err != nil {
		return "unknown", false
	}
	if row.Outcome == "pending" {
		// A pending create may still be in flight or may have crashed; neither is provable.
		return "unknown", true
	}
	if row.Outcome != "established" {
		return row.Outcome, true
	}
	if !row.VoiceSessionID.Valid || !row.MediaGeneration.Valid {
		return "unknown", true
	}
	outcome, err := s.creationOutcomes.MediaOutcome(row.VoiceSessionID.String, row.MediaGeneration.Int64)
	if err != nil {
		return "unknown", true
	}
	return outcome, true
}

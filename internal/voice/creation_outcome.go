package voice

import "github.com/dyhhhhhh/chatgpt-web-voice/internal/store"

type CreationOutcomeStore interface {
	BeginVoiceCreationOutcome(owner, requestID string) (string, error)
	UpdateVoiceCreationOutcome(owner, requestID, outcome, sessionID string, generation int64, attemptID string) error
	VoiceCreationOutcomeSnapshot(owner, requestID string) (string, string, error)
}

// CreationOutcomeSnapshot returns owner-scoped outcome and evidence atomically.
// A released outcome always carries matching durable local termination evidence.
func (s *Service) CreationOutcomeSnapshot(owner, requestID string) (string, string, bool) {
	if s.creationOutcomes == nil {
		return "unknown", "", false
	}
	outcome, evidence, err := s.creationOutcomes.VoiceCreationOutcomeSnapshot(owner, requestID)
	if err != nil {
		return "unknown", "", false
	}
	if outcome == "released" && evidence == "" {
		return "unknown", "", true
	}
	return outcome, evidence, true
}

func (s *Service) CreationOutcomeEvidence(owner, requestID string) string {
	_, evidence, _ := s.CreationOutcomeSnapshot(owner, requestID)
	return evidence
}

func (s *Service) creationOutcome(owner, requestID string) (string, bool) {
	outcome, _, exists := s.CreationOutcomeSnapshot(owner, requestID)
	return outcome, exists
}

var _ CreationOutcomeStore = (*store.DB)(nil)

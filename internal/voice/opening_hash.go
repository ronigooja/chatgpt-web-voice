package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type openingHashStore interface {
	CheckOpeningHash(string, string, int, string) error
	FinalizeOpeningHash(string, string, int64, string, int, int, int, string, string) error
}

func openingContextContentHash(c OpeningContext) string {
	raw, _ := json.Marshal(c)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func (s *Service) checkOpeningHash(owner, id string, c OpeningContext) error {
	records, ok := s.records.(openingHashStore)
	if !ok {
		return &ServiceError{Message: "opening hash persistence unavailable", StatusCode: 503}
	}
	if err := records.CheckOpeningHash(owner, id, c.Version, openingContextContentHash(c)); err != nil {
		return &ServiceError{Message: "opening context identity conflict", StatusCode: 409}
	}
	return nil
}

func (s *Service) finalizeCreatedOpening(owner, id string, b *sessionBinding, c OpeningContext, chars int) *OpeningContextResult {
	r := &OpeningContextResult{Version: c.Version, ItemCount: len(c.Items), Reason: "metadata_persist_failed"}
	records, ok := s.records.(openingHashStore)
	if !ok || b == nil || b.mediaID.MediaGeneration <= 0 {
		return r
	}
	hash := openingContextContentHash(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings[id] != b || b.Owner != owner {
		r.Reason = "media_superseded"
		return r
	}
	if b.media != nil {
		if _, closed := b.media.Termination(); closed {
			r.Reason = "media_closed"
			return r
		}
	}
	if err := records.FinalizeOpeningHash(owner, id, b.mediaID.MediaGeneration, c.Kind, c.Version, len(c.Items), chars, hash, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return r
	}
	r.Applied = true
	r.Silent = true
	r.ContentHash = hash
	r.CharCount = chars
	r.Reason = ""
	return r
}

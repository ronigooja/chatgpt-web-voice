package voice

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
	"github.com/google/uuid"
)

type ResultReceipt = store.ResultReceipt
type ResultDeliveryRequest struct {
	MediaGeneration int64  `json:"media_generation"`
	DeliveryID      string `json:"delivery_id"`
	ContentHash     string `json:"content_hash"`
	Text            string `json:"text"`
}
type ResultReceiptStore interface {
	GetResultReceipt(string, string, int64, string) (ResultReceipt, error)
	InsertResultReceipt(string, string, ResultReceipt, string) (bool, error)
	ClaimResultReceipt(string, string, ResultReceipt, string) (bool, error)
	FinishResultReceipt(string, string, ResultReceipt, string, string) error
}

func (s *Service) WithResultReceipts(db ResultReceiptStore) *Service { s.resultReceipts = db; return s }

// ResultDeliveryCapability applies only when durable receipt storage is wired.
func (s *Service) ResultDeliveryCapability() bool { return s.resultReceipts != nil }

func ValidDeliveryID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
func ValidateResultDelivery(p ResultDeliveryRequest) error {
	h := sha256.Sum256([]byte(p.Text))
	if p.MediaGeneration <= 0 || !ValidDeliveryID(p.DeliveryID) || len(p.Text) > avasContextMaxBytes || !utf8.ValidString(p.Text) || strings.TrimSpace(p.Text) == "" || p.ContentHash != hex.EncodeToString(h[:]) {
		return &ServiceError{Message: "invalid result delivery", StatusCode: 400}
	}
	return nil
}
func receiptError(err error) error {
	if errors.Is(err, store.ErrResultNotFound) {
		return &ServiceError{Message: "result receipt not found", StatusCode: 404}
	}
	if errors.Is(err, store.ErrResultConflict) {
		return &ServiceError{Message: "result delivery content conflict", StatusCode: 409}
	}
	return &ServiceError{Message: "result receipt storage unavailable", StatusCode: 503}
}
func (s *Service) LookupResultDelivery(owner, session string, generation int64, id string) (ResultReceipt, error) {
	if s.resultReceipts == nil {
		return ResultReceipt{}, receiptError(nil)
	}
	r, err := s.resultReceipts.GetResultReceipt(normalizeSessionOwner(owner), session, generation, id)
	if err != nil {
		return r, receiptError(err)
	}
	return r, nil
}

// SubmitResultDelivery never retries an inserted receipt. Applied proves completed
// local writes plus durable finalization, not upstream consumption or playback.
func (s *Service) SubmitResultDelivery(owner, session string, p ResultDeliveryRequest) (ResultReceipt, error) {
	r := ResultReceipt{MediaGeneration: p.MediaGeneration, DeliveryID: p.DeliveryID, ContentHash: p.ContentHash, State: "pending"}
	if err := ValidateResultDelivery(p); err != nil {
		return r, err
	}
	if s.resultReceipts == nil {
		return r, receiptError(nil)
	}
	owner = normalizeSessionOwner(owner)
	// This lock also fences binding replacement/release through actual dispatch.
	// No SQL transaction is held during network IO.
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.resultReceipts.GetResultReceipt(owner, session, p.MediaGeneration, p.DeliveryID)
	if err == nil {
		if old.ContentHash != p.ContentHash {
			return r, receiptError(store.ErrResultConflict)
		}
		return old, nil
	}
	if !errors.Is(err, store.ErrResultNotFound) {
		return r, receiptError(err)
	}
	b := s.bindings[session]
	if b == nil || b.Owner != owner {
		return r, &ServiceError{Message: "voice session not found", StatusCode: 404}
	}
	if b.call != nil && b.call.owned != nil && !b.call.owned.active() {
		return r, &ServiceError{Message: "voice session not active", StatusCode: 409}
	}
	if b.mediaID.MediaGeneration != p.MediaGeneration {
		return r, &ServiceError{Message: "media generation mismatch", StatusCode: 409}
	}
	inserted, err := s.resultReceipts.InsertResultReceipt(owner, session, r, p.Text)
	if err != nil {
		return r, receiptError(err)
	}
	if !inserted {
		return s.LookupResultDelivery(owner, session, p.MediaGeneration, p.DeliveryID)
	}
	attempt := uuid.NewString()
	claimed, err := s.resultReceipts.ClaimResultReceipt(owner, session, r, attempt)
	if err != nil {
		return r, receiptError(err)
	}
	if !claimed {
		return s.LookupResultDelivery(owner, session, p.MediaGeneration, p.DeliveryID)
	}
	attempted, sendErr := s.sendResultContextLocked(b, p.Text)
	r.State = "applied"
	if sendErr != nil {
		r.State = "rejected"
		if attempted {
			r.State = "unknown"
		}
	}
	if err = s.resultReceipts.FinishResultReceipt(owner, session, r, attempt, r.State); err != nil {
		// A failed applied commit must never be represented as success, nor resent.
		r.State = "unknown"
		_ = s.resultReceipts.FinishResultReceipt(owner, session, r, attempt, "unknown")
		return r, receiptError(err)
	}
	return r, nil
}

func (s *Service) sendResultContextLocked(b *sessionBinding, text string) (bool, error) {
	if s.cfg.VoiceUpstreamRoute == "avas" {
		if b.avasSideband == nil {
			return false, errors.New("sideband unavailable")
		}
		tracker := &resultAVASSender{write: b.avasSideband.sendAttempt}
		err := sendAVASContext(tracker, b.openDelegation, text)
		return tracker.attempted, err
	}
	sender, ok := b.media.(interface{ SendResultText(string) (bool, error) })
	if !ok {
		return false, errors.New("synchronous text transport unavailable")
	}
	return sender.SendResultText(text)
}

type resultAVASSender struct {
	write     func([]byte) (bool, error)
	attempted bool
}

func (s *resultAVASSender) send(data []byte) error {
	attempted, err := s.write(data)
	s.attempted = s.attempted || attempted
	return err
}

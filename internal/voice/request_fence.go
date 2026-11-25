package voice

import "github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"

type ownedRequest struct{ owner, id string }

// Activity starts with the claim, before a reservation or relay can exist.
// It lasts through binding acknowledgement and through media cleanup, which
// can complete independently. An unverified local close stays busy for this
// process lifetime; a restart discards only local activity, not reservations.
type ownedRequestActivity struct {
	creating, media, unverified bool
	sidebandCleanup             int
}

func (s *Service) FenceCreationRequest(owner, requestID string) (result, evidenceID string, err error) {
	store, ok := s.creationOutcomes.(interface {
		FenceVoiceCreation(string, string) (string, string, error)
	})
	if !ok || !s.GatewayOwnsCalls() {
		return "", "", unavailableCall
	}
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.requestActivity[ownedRequest{owner, requestID}] != nil {
		return "busy", "", nil
	}
	return store.FenceVoiceCreation(owner, requestID)
}

func (s *Service) updateRequestActivity(request ownedRequest, update func(*ownedRequestActivity)) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if activity := s.requestActivity[request]; activity != nil {
		update(activity)
		if !activity.creating && !activity.media && !activity.unverified && activity.sidebandCleanup == 0 {
			delete(s.requestActivity, request)
		}
	}
}

func (s *Service) finishRequestCreate(request ownedRequest, panicked bool) {
	s.updateRequestActivity(request, func(a *ownedRequestActivity) {
		a.creating = false
		a.unverified = a.unverified || panicked
	})
}

func (s *Service) startRequestMedia(c *ownedCall) {
	s.updateRequestActivity(ownedRequest{c.owner, c.requestID}, func(a *ownedRequestActivity) { a.media = true })
}

func (s *Service) finishRequestMedia(c *ownedCall, media relay.Session) {
	term, closed := media.Termination()
	s.updateRequestActivity(ownedRequest{c.owner, c.requestID}, func(a *ownedRequestActivity) {
		a.media = false
		a.unverified = a.unverified || !closed || !term.Clean()
	})
}

func (c *ownedCall) requestCleanupUnverified(s *Service) {
	s.updateRequestActivity(ownedRequest{c.owner, c.requestID}, func(a *ownedRequestActivity) { a.unverified = true })
}

// The ordinary evidence path waits only briefly for sideband workers. Keep
// the fence busy until those workers actually exit, without holding up a dial.
func (s *Service) trackRequestSidebandCleanup(c *ownedCall, w *avasSidebandWriter) <-chan struct{} {
	done := make(chan struct{})
	if w.done == nil {
		c.requestCleanupUnverified(s)
		close(done)
		return done
	}
	request := ownedRequest{c.owner, c.requestID}
	s.updateRequestActivity(request, func(a *ownedRequestActivity) { a.sidebandCleanup++ })
	go func() {
		defer close(done)
		<-w.done
		w.mu.Lock()
		clean := w.closed && w.closeErr == nil
		w.mu.Unlock()
		s.updateRequestActivity(request, func(a *ownedRequestActivity) {
			a.sidebandCleanup--
			a.unverified = a.unverified || !clean
		})
	}()
	return done
}

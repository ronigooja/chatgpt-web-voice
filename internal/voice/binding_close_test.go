package voice

import (
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
)

// replayCloseMedia reproduces the real relay's synchronous closed registration.
type replayCloseMedia struct {
	*fakeMedia
	registering chan struct{}
	resume      chan struct{}
}

func (m *replayCloseMedia) SetCloseHandler(h func(string)) {
	if m.registering != nil {
		close(m.registering)
		<-m.resume
	}
	m.mu.Lock()
	m.handler = h
	closed := m.closed
	m.mu.Unlock()
	if closed {
		h("already_closed")
	}
}

func TestBindingAlreadyClosedRegistration(t *testing.T) {
	s := New(config.Config{SessionTTLSeconds: 180}, nil, nil)
	m := &replayCloseMedia{fakeMedia: &fakeMedia{closed: true}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.bindVoiceSessionMedia("api_key:1", "s", "", accounts.Account{}, UpstreamContext{}, m, mediaevidence.Identity{}, nil, "wm")
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("synchronous close registration deadlocked")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings["s"] != nil {
		t.Fatal("already-closed binding was not reclaimed")
	}
}

func TestBindingDelayedRegistrationReplacementFence(t *testing.T) {
	s := New(config.Config{SessionTTLSeconds: 180}, nil, nil)
	old := &replayCloseMedia{fakeMedia: &fakeMedia{}, registering: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.bindVoiceSessionMedia("api_key:1", "s", "", accounts.Account{}, UpstreamContext{}, old, mediaevidence.Identity{}, nil, "wm")
	}()
	select {
	case <-old.registering:
	case <-time.After(2 * time.Second):
		t.Fatal("registration not reached")
	}
	next := &replayCloseMedia{fakeMedia: &fakeMedia{}}
	replaced := make(chan struct{})
	go func() {
		defer close(replaced)
		s.bindVoiceSessionMedia("api_key:1", "s", "", accounts.Account{}, UpstreamContext{}, next, mediaevidence.Identity{}, nil, "wm")
	}()
	select {
	case <-replaced:
	case <-time.After(2 * time.Second):
		t.Fatal("registration retained binding mutex")
	}
	// Close before installing the old handler; registration must replay the close
	// without removing the newer binding, even if async replacement cleanup ran.
	old.CloseWithReason("replaced")
	close(old.resume)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delayed callback deadlocked")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings["s"] == nil || s.bindings["s"].media != next {
		t.Fatal("old close removed replacement")
	}
}

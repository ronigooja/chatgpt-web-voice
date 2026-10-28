package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
)

type bridgeAccountStore interface {
	BridgeGet(string) (accounts.BridgeView, error)
	BridgePut(string, string, string, accounts.BridgePayload) (accounts.BridgeView, error)
}

// RegisterBridge mounts the strictly Basic-authenticated compatibility surface.
// The composition root must wrap this mux in the administrator Basic middleware.
func (s *Server) RegisterBridge(mux *http.ServeMux) {
	mux.HandleFunc("GET /bridge/v1/capabilities", s.bridgeCapabilities)
	mux.HandleFunc("GET /bridge/v1/accounts/{remote_id}", s.bridgeGet)
	mux.HandleFunc("PUT /bridge/v1/accounts/{remote_id}", s.bridgePut)
}
func (s *Server) bridgeCapabilities(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.accounts.(bridgeAccountStore); !ok {
		writeJSON(w, 503, map[string]string{"error": "bridge storage unavailable"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"protocol": "bridge-v1", "explicit_route": true, "credential_digest": true})
}
func (s *Server) bridgeGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	store, ok := s.accounts.(bridgeAccountStore)
	if !ok {
		writeJSON(w, 503, map[string]string{"error": "bridge storage unavailable"})
		return
	}
	id := r.PathValue("remote_id")
	if !accounts.ValidBridgeID(id) {
		writeJSON(w, 400, map[string]string{"error": "invalid remote id"})
		return
	}
	view, err := store.BridgeGet(id)
	if err != nil {
		bridgeError(w, err)
		return
	}
	writeJSON(w, 200, view)
}
func (s *Server) bridgePut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	store, ok := s.accounts.(bridgeAccountStore)
	if !ok {
		writeJSON(w, 503, map[string]string{"error": "bridge storage unavailable"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var payload accounts.BridgePayload
	if err := dec.Decode(&payload); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid bridge payload"})
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		writeJSON(w, 400, map[string]string{"error": "invalid bridge payload"})
		return
	}
	if err := payload.Validate(); err != nil {
		bridgeError(w, err)
		return
	}
	// Digest the decoded request with Python-compatible canonical strings. The
	// header is a client idempotency token, not trusted evidence of credential state.
	digest, err := accounts.BridgeDigest(payload)
	if err != nil {
		bridgeError(w, err)
		return
	}
	id := r.PathValue("remote_id")
	key := r.Header.Get("Idempotency-Key")
	view, err := store.BridgePut(id, key, digest, payload)
	if err != nil {
		bridgeError(w, err)
		return
	}
	writeJSON(w, 200, view)
}
func bridgeError(w http.ResponseWriter, err error) {
	status := 500
	message := "bridge storage operation failed"
	var invalid *accounts.Error
	switch {
	case errors.Is(err, accounts.ErrNotFound):
		status = 404
		message = "bridge account not found"
	case errors.Is(err, accounts.ErrConflict):
		status = 409
		message = "bridge account conflict"
	case errors.As(err, &invalid):
		status = 400
		message = "invalid bridge account request"
	}
	writeJSON(w, status, map[string]string{"error": message})
}

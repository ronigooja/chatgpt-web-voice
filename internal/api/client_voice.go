package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/auth"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
)

type clientVoiceService interface {
	DialClient(context.Context, string, voice.ClientDialRequest) (*voice.ClientDialResult, error)
	HangupClient(context.Context, string, string) error
}

func (s *Server) legacyVoiceRoute(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc, ok := s.voice.(interface{ GatewayOwnsCalls() bool }); ok && svc.GatewayOwnsCalls() {
			writeDownstreamError(w, 404, "not_found", "route not available in gateway-owned mode")
			return
		}
		next(w, r)
	}
}

func (s *Server) RegisterClientVoice(mux *http.ServeMux) {
	mux.HandleFunc("POST /client/v1/voice/calls", s.clientVoice(s.dialClient))
	mux.HandleFunc("POST /client/v1/voice/calls/{gateway_session_id}/hangup", s.clientVoice(s.hangupClient))
}

func (s *Server) clientVoice(next func(clientVoiceService, string, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		var key string
		if len(values) == 1 {
			scheme, token, _ := strings.Cut(values[0], " ")
			if strings.EqualFold(scheme, "Bearer") {
				key = strings.TrimLeft(token, " ")
			}
		}
		if !voice.ValidInstanceBearer(key) {
			writeDownstreamError(w, 401, "unauthenticated", "valid instance bearer required")
			return
		}
		// Do not give the handler or its logging a request containing the key.
		r.Header.Del("Authorization")
		r.Header.Del("X-Zoe-Instance-Key")
		svc, ok := s.voice.(clientVoiceService)
		if !ok {
			if r.Method == http.MethodPost && r.URL.Path == "/client/v1/voice/calls" {
				voice.LogOwnedVoiceUnavailable()
			}
			writeDownstreamError(w, 503, "voice_unavailable", "voice unavailable")
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		next(svc, key, w, r)
	}
}

func writeClientCallError(w http.ResponseWriter, err error) {
	var call *voice.CallError
	if errors.As(err, &call) {
		writeDownstreamError(w, call.Status, call.Code, strings.ReplaceAll(call.Code, "_", " "))
		return
	}
	writeDownstreamError(w, 503, "voice_unavailable", "voice unavailable")
}

func (s *Server) dialClient(svc clientVoiceService, key string, w http.ResponseWriter, r *http.Request) {
	// A valid 1 MiB SDP of control characters may use six JSON bytes per byte.
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 7<<20))
	var p *voice.ClientDialRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err != nil || !utf8.Valid(raw) || d.Decode(&p) != nil || p == nil || d.Decode(new(any)) != io.EOF {
		writeDownstreamError(w, 400, "invalid_request", "invalid dial body")
		return
	}
	result, err := svc.DialClient(r.Context(), key, *p)
	if err != nil {
		writeClientCallError(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) hangupClient(svc clientVoiceService, key string, w http.ResponseWriter, r *http.Request) {
	if err := svc.HangupClient(r.Context(), key, r.PathValue("gateway_session_id")); err != nil {
		writeClientCallError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) downstreamTerminationEvidence(w http.ResponseWriter, r *http.Request) {
	owner := auth.APIKeyOwner(r.Context())
	if owner == "" {
		writeDownstreamError(w, 401, "invalid_api_key", "valid Bearer API key required")
		return
	}
	q := r.URL.Query()
	positiveDecimal := func(name string) int64 {
		if len(q[name]) != 1 {
			return 0
		}
		raw := q.Get(name)
		if strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return 0
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0
		}
		return value
	}
	key := mediaevidence.Identity{VoiceSessionID: r.PathValue("gateway_session_id"), MediaGeneration: positiveDecimal("media_generation"),
		LeaseID: q.Get("account_lease_id"), LeaseGeneration: positiveDecimal("account_lease_generation")}
	if strings.TrimSpace(key.VoiceSessionID) == "" || strings.TrimSpace(key.LeaseID) == "" || len(q["account_lease_id"]) != 1 || key.MediaGeneration < 1 || key.LeaseGeneration < 1 {
		writeDownstreamError(w, 400, "invalid_request", "invalid termination evidence key")
		return
	}
	svc, ok := s.voice.(interface {
		LookupTermination(string, mediaevidence.Identity) (mediaevidence.TerminationLookup, error)
	})
	if !ok {
		writeDownstreamError(w, 503, "voice_unavailable", "termination evidence unavailable")
		return
	}
	result, err := svc.LookupTermination(owner, key)
	if err != nil {
		writeDownstreamError(w, 503, "voice_unavailable", "termination evidence unavailable")
		return
	}
	writeJSON(w, 200, result)
}

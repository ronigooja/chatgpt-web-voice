package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
)

var viewerHeartbeatInterval = 15 * time.Second

// RegisterViewer mounts the read-only viewer surface separately for its auth policy.
func (s *Server) RegisterViewer(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/call-sessions/{id}/generations", s.viewerGenerations)
	mux.HandleFunc("GET /api/call-sessions/{id}/generations/{gen}/events", s.viewerEvents)
	mux.HandleFunc("GET /api/call-sessions/{id}/generations/{gen}/events/stream", s.viewerStream)
	mux.HandleFunc("GET /api/call-sessions/{id}/generations/{gen}/frames", s.viewerFrames)
}

func (s *Server) viewerFrames(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	key, valid := viewerKey(r)
	if !valid || s.events == nil {
		http.NotFound(w, r)
		return
	}
	ring := r.URL.Query().Get("ring")
	if ring == "" {
		ring = "all"
	}
	if ring != "all" && ring != "conversation" && ring != "audio" && ring != "oversize" {
		http.Error(w, "invalid ring", http.StatusBadRequest)
		return
	}
	snapshot, ok := s.events.Frames(key, ring)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) generationCount(id string) int {
	if s.events == nil {
		return 0
	}
	return len(s.events.List(id))
}

func (s *Server) viewerGenerations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.events == nil {
		http.NotFound(w, r)
		return
	}
	gens := s.events.List(r.PathValue("id"))
	if len(gens) == 0 {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"generations": gens})
}

func viewerKey(r *http.Request) (eventlog.Key, bool) {
	gen, err := strconv.ParseInt(r.PathValue("gen"), 10, 64)
	return eventlog.Key{SessionID: r.PathValue("id"), Generation: gen}, err == nil && gen > 0
}

func viewerAfter(r *http.Request, stream bool) (uint64, bool) {
	raw := r.URL.Query().Get("after")
	if stream && r.Header.Get("Last-Event-ID") != "" {
		raw = r.Header.Get("Last-Event-ID")
	}
	if raw == "" {
		return 0, true
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil
}

func (s *Server) viewerEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	key, valid := viewerKey(r)
	after, validAfter := viewerAfter(r, false)
	if !valid || s.events == nil {
		http.NotFound(w, r)
		return
	}
	if !validAfter {
		http.Error(w, "invalid after", http.StatusBadRequest)
		return
	}
	limit := 500
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = n
	}
	if limit > 2000 {
		limit = 2000
	}
	snap, ok := s.events.Snapshot(key, after, limit)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) viewerStream(w http.ResponseWriter, r *http.Request) {
	key, valid := viewerKey(r)
	after, validAfter := viewerAfter(r, true)
	if !valid || s.events == nil {
		http.NotFound(w, r)
		return
	}
	if !validAfter {
		http.Error(w, "invalid after", http.StatusBadRequest)
		return
	}
	snap, wake, cancel, ok := s.events.Subscribe(key, after, 500)
	if !ok {
		http.NotFound(w, r)
		return
	}
	defer cancel()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	if controller.Flush() != nil {
		return
	}
	ticker := time.NewTicker(viewerHeartbeatInterval)
	defer ticker.Stop()
	for {
		if snap.Gap && snap.OldestSeq > after+1 {
			if _, err := fmt.Fprintf(w, "event: gap\ndata: {\"from\":%d,\"to\":%d}\n\n", after+1, snap.OldestSeq-1); err != nil {
				return
			}
			after = snap.OldestSeq - 1
		}
		for _, event := range snap.Events {
			if event.Seq <= after {
				continue
			}
			raw, err := json.Marshal(event)
			if err != nil {
				return
			}
			if _, err = fmt.Fprintf(w, "id: %d\nevent: event\ndata: %s\n\n", event.Seq, raw); err != nil {
				return
			}
			after = event.Seq
		}
		if err := controller.Flush(); err != nil {
			return
		}
		if after < snap.NewestSeq {
			snap, ok = s.events.Snapshot(key, after, 500)
			if !ok {
				return
			}
			continue
		}
		if !snap.Active {
			_, _ = fmt.Fprint(w, "event: end\ndata: {}\n\n")
			_ = controller.Flush()
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			if controller.Flush() != nil {
				return
			}
		case <-wake:
		}
		snap, ok = s.events.Snapshot(key, after, 500)
		if !ok {
			return
		}
	}
}

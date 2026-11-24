// Package eventlog retains bounded, in-memory upstream event history.
package eventlog

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// These are accounting estimates, not exact heap size.
const eventOverhead = 256
const generationOverhead = 1024

type Config struct {
	MaxEvents   int
	MaxBytes    int
	GlobalBytes int
	EventBytes  int
	TTL         time.Duration
}

func Defaults() Config { return Config{2000, 4 << 20, 64 << 20, 64 << 10, 6 * time.Hour} }

type Key struct {
	SessionID  string
	Generation int64
}

type Event struct {
	Seq       uint64    `json:"seq"`
	At        time.Time `json:"at"`
	Direction string    `json:"direction"`
	Type      string    `json:"type"`
	Source    string    `json:"source"`
	Payload   string    `json:"payload"`
	Size      int       `json:"size"`
	Truncated bool      `json:"truncated"`
	Delivery  string    `json:"delivery"`
}

type Generation struct {
	Generation   int64      `json:"generation"`
	ZoeSessionID string     `json:"zoe_session_id"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at"`
	Active       bool       `json:"active"`
	EventCount   int        `json:"event_count"`
	NewestSeq    uint64     `json:"newest_seq"`
	Dropped      uint64     `json:"dropped"`
}

type Snapshot struct {
	SessionID    string  `json:"session_id"`
	Generation   int64   `json:"generation"`
	ZoeSessionID string  `json:"zoe_session_id"`
	OldestSeq    uint64  `json:"oldest_seq"`
	NewestSeq    uint64  `json:"newest_seq"`
	Dropped      uint64  `json:"dropped"`
	Gap          bool    `json:"gap"`
	Active       bool    `json:"active"`
	Events       []Event `json:"events"`
}

type entry struct {
	meta   Generation
	events []Event
	bytes  int
	subs   map[chan struct{}]struct{}
}

type Store struct {
	mu      sync.Mutex
	cfg     Config
	entries map[Key]*entry
	bytes   int
}

func New(c Config) *Store {
	d := Defaults()
	if c.MaxEvents <= 0 {
		c.MaxEvents = d.MaxEvents
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = d.MaxBytes
	}
	if c.GlobalBytes <= 0 {
		c.GlobalBytes = d.GlobalBytes
	}
	if c.GlobalBytes < generationOverhead {
		c.GlobalBytes = generationOverhead
	}
	if c.EventBytes <= 0 {
		c.EventBytes = d.EventBytes
	}
	if c.TTL <= 0 {
		c.TTL = d.TTL
	}
	return &Store{cfg: c, entries: make(map[Key]*entry)}
}

func (s *Store) Start(k Key, zoeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	if e := s.entries[k]; e != nil {
		if zoeID != "" {
			e.meta.ZoeSessionID = zoeID
		}
		return
	}
	s.entries[k] = &entry{meta: Generation{Generation: k.Generation, ZoeSessionID: zoeID, StartedAt: time.Now().UTC(), Active: true}, subs: make(map[chan struct{}]struct{})}
	s.bytes += generationOverhead
	s.enforceGlobal()
}

func (s *Store) End(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[k]; e != nil && e.meta.Active {
		now := time.Now().UTC()
		e.meta.Active = false
		e.meta.EndedAt = &now
		notify(e)
	}
}

// Append never waits for subscribers. Payload bytes are copied into the store.
func (s *Store) Append(k Key, direction, source, delivery string, payload []byte, binary bool) {
	typ := "text"
	if binary {
		typ = "binary"
	} else {
		var v struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(payload, &v) == nil && v.Type != "" {
			typ = v.Type
		}
	}
	size := len(payload)
	limit := s.cfg.EventBytes
	if len(payload) > limit {
		payload = payload[:limit]
	}
	event := Event{At: time.Now().UTC(), Direction: direction, Type: typ, Source: source, Payload: string(payload), Size: size, Truncated: size > len(payload), Delivery: delivery}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[k]
	if e == nil || !e.meta.Active {
		return
	}
	e.meta.NewestSeq++
	event.Seq = e.meta.NewestSeq
	e.events = append(e.events, event)
	n := eventSize(event)
	e.bytes += n
	s.bytes += n
	for len(e.events) > s.cfg.MaxEvents || e.bytes > s.cfg.MaxBytes {
		s.drop(e)
	}
	s.expire(time.Now())
	s.enforceGlobal()
	notify(e)
}

func eventSize(e Event) int {
	return eventOverhead + len(e.Payload) + len(e.Type) + len(e.Source) + len(e.Direction) + len(e.Delivery)
}

func (s *Store) enforceGlobal() {
	for s.bytes > s.cfg.GlobalBytes {
		var oldest Key
		var victim *entry
		for key, candidate := range s.entries {
			if candidate.meta.Active || candidate.meta.EndedAt == nil {
				continue
			}
			if victim == nil || candidate.meta.EndedAt.Before(*victim.meta.EndedAt) {
				oldest, victim = key, candidate
			}
		}
		if victim != nil {
			s.remove(oldest, victim)
			continue
		}
		// Active generations remain addressable, but their oldest events can be evicted.
		for _, candidate := range s.entries {
			if len(candidate.events) > 0 && (victim == nil || candidate.events[0].At.Before(victim.events[0].At)) {
				victim = candidate
			}
		}
		if victim == nil {
			// If active generation metadata alone exhausts the cap, evict the oldest.
			for key, candidate := range s.entries {
				if victim == nil || candidate.meta.StartedAt.Before(victim.meta.StartedAt) {
					oldest, victim = key, candidate
				}
			}
			if victim == nil {
				break
			}
			s.remove(oldest, victim)
			continue
		}
		s.drop(victim)
		notify(victim)
	}
}

func (s *Store) drop(e *entry) {
	n := eventSize(e.events[0])
	e.events[0] = Event{}
	e.events = e.events[1:]
	e.bytes -= n
	s.bytes -= n
	e.meta.Dropped++
}
func notify(e *entry) {
	for ch := range e.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
func (s *Store) remove(k Key, e *entry) {
	s.bytes -= e.bytes + generationOverhead
	delete(s.entries, k)
	notify(e)
}
func (s *Store) expire(now time.Time) {
	for k, e := range s.entries {
		if e.meta.EndedAt != nil && now.Sub(*e.meta.EndedAt) >= s.cfg.TTL {
			s.remove(k, e)
		}
	}
}

func (s *Store) List(id string) []Generation {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	out := []Generation{}
	for k, e := range s.entries {
		if k.SessionID == id {
			m := e.meta
			m.EventCount = len(e.events)
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Generation < out[j].Generation })
	return out
}

func (s *Store) snapshot(k Key, after uint64, limit int) (Snapshot, bool) {
	e := s.entries[k]
	if e == nil {
		return Snapshot{}, false
	}
	out := Snapshot{SessionID: k.SessionID, Generation: k.Generation, ZoeSessionID: e.meta.ZoeSessionID, NewestSeq: e.meta.NewestSeq, Dropped: e.meta.Dropped, Active: e.meta.Active, Events: []Event{}}
	if len(e.events) > 0 {
		out.OldestSeq = e.events[0].Seq
	} else {
		out.OldestSeq = e.meta.NewestSeq + 1
	}
	out.Gap = after < out.OldestSeq-1
	for _, event := range e.events {
		if event.Seq > after {
			out.Events = append(out.Events, event)
			if len(out.Events) == limit {
				break
			}
		}
	}
	return out, true
}

func (s *Store) Snapshot(k Key, after uint64, limit int) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	return s.snapshot(k, after, limit)
}

// Subscribe registers the wakeup before taking the initial snapshot, avoiding a handoff race.
// Notifications are coalesced; readers detect missed events using sequence numbers.
func (s *Store) Subscribe(k Key, after uint64, limit int) (Snapshot, <-chan struct{}, func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	e := s.entries[k]
	if e == nil {
		return Snapshot{}, nil, nil, false
	}
	ch := make(chan struct{}, 1)
	e.subs[ch] = struct{}{}
	snap, _ := s.snapshot(k, after, limit)
	cancel := func() { s.mu.Lock(); defer s.mu.Unlock(); delete(e.subs, ch) }
	return snap, ch, cancel, true
}

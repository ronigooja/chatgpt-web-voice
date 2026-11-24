// Package eventlog retains bounded, in-memory upstream event history.
package eventlog

import (
	"bytes"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// These are accounting estimates, not exact heap size.
const eventOverhead = 256
const generationOverhead = 1024

type Config struct {
	MaxEvents     int
	MaxBytes      int
	GlobalBytes   int
	EventBytes    int
	TTL           time.Duration
	AudioCapture bool
	AudioCaptureDir string
	AudioCaptureMaxBytes int64
	AudioCaptureSegment time.Duration
}

func Defaults() Config {
	return Config{MaxEvents: 2000, MaxBytes: 4 << 20, GlobalBytes: 64 << 20, EventBytes: 64 << 10, TTL: 6 * time.Hour}
}

type Key struct {
	SessionID  string
	Generation int64
}

type Event struct {
	Seq       uint64    `json:"seq"`
	At        time.Time `json:"at"`
	Direction string    `json:"direction"`
	Type      string    `json:"type"`
	Wrapper   string    `json:"wrapper,omitempty"`
	Source    string    `json:"source"`
	Payload   string    `json:"payload"`
	Size      int       `json:"size"`
	Truncated bool      `json:"truncated"`
	Delivery  string    `json:"delivery"`
}

type Generation struct {
	Generation    int64         `json:"generation"`
	ZoeSessionID  string        `json:"zoe_session_id"`
	ZoeGeneration int64         `json:"zoe_generation"`
	StartedAt     time.Time     `json:"started_at"`
	EndedAt       *time.Time    `json:"ended_at"`
	Active        bool          `json:"active"`
	EventCount    int           `json:"event_count"`
	NewestSeq     uint64        `json:"newest_seq"`
	Dropped       uint64        `json:"dropped"`
	AudioEvents   uint64        `json:"audio_events"`
	AudioBytes    uint64        `json:"audio_bytes"`
	Audio         []AudioFact   `json:"audio"`
	AudioSamples  []AudioSample `json:"audio_samples"`
	AudioCapture bool `json:"audio_capture"`
}

type Snapshot struct {
	SessionID     string        `json:"session_id"`
	Generation    int64         `json:"generation"`
	ZoeSessionID  string        `json:"zoe_session_id"`
	ZoeGeneration int64         `json:"zoe_generation"`
	OldestSeq     uint64        `json:"oldest_seq"`
	NewestSeq     uint64        `json:"newest_seq"`
	Dropped       uint64        `json:"dropped"`
	AudioEvents   uint64        `json:"audio_events"`
	AudioBytes    uint64        `json:"audio_bytes"`
	Audio         []AudioFact   `json:"audio"`
	AudioSamples  []AudioSample `json:"audio_samples"`
	Gap           bool          `json:"gap"`
	Active        bool          `json:"active"`
	Events        []Event       `json:"events"`
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
	capture *audioWriter
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
	s := &Store{cfg: c, entries: make(map[Key]*entry)}
	if c.AudioCapture { s.capture = newAudioWriter(c) }
	return s
}

func (s *Store) Start(k Key, zoeID string, zoeGeneration ...int64) {
	if s == nil {
		return
	}
	var zg int64
	if len(zoeGeneration) > 0 {
		zg = zoeGeneration[0]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	if e := s.entries[k]; e != nil {
		if zoeID != "" {
			e.meta.ZoeSessionID = zoeID
			e.meta.ZoeGeneration = zg
		}
		return
	}
	s.entries[k] = &entry{meta: Generation{Generation: k.Generation, ZoeSessionID: zoeID, ZoeGeneration: zg, StartedAt: time.Now().UTC(), Active: true, AudioCapture: s.capture != nil}, subs: make(map[chan struct{}]struct{})}
	s.bytes += generationOverhead
	s.enforceGlobal()
}

func (s *Store) End(k Key) {
	if s == nil {
		return
	}
	if s.capture != nil { s.capture.end(k) }
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
	if s == nil {
		return
	}
	// Do not retain oversized JSON: its type may appear after the cap, so it
	// cannot safely be distinguished from audio using bounded work.
	if !binary && len(payload) > s.cfg.EventBytes && bytes.HasPrefix(bytes.TrimLeft(payload[:s.cfg.EventBytes], " \t\r\n"), []byte("{")) {
		prefix := payload[:s.cfg.EventBytes]
		s.mu.Lock()
		if e := s.entries[k]; e != nil && e.meta.Active {
			if typ := audioPrefixType(prefix); typ != "" {
				s.countAudio(e, direction, source, typ, payload)
			} else {
				e.meta.Dropped++
			}
			notify(e)
		}
		s.mu.Unlock()
		return
	}
	typ := "text"
	wrapper := ""
	audio := false
	if binary {
		typ = "binary"
	} else if len(payload) <= s.cfg.EventBytes {
		var v struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(payload, &v) == nil && v.Type != "" {
			typ = v.Type
			inner := v.Type
			if len(v.Data) > 0 {
				var raw string
				data := []byte(v.Data)
				if json.Unmarshal(v.Data, &raw) == nil {
					data = []byte(raw)
				}
				var nested struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(data, &nested) == nil && nested.Type != "" {
					inner = nested.Type
					if v.Type == "data_message" && inner != v.Type {
						typ, wrapper = inner, v.Type
					}
				}
			}
			audio = inner == "session.input_audio.append" || inner == "session.output_audio.delta"
		}
	}
	if audio {
		s.mu.Lock()
		defer s.mu.Unlock()
		if e := s.entries[k]; e != nil && e.meta.Active {
			s.countAudio(e, direction, source, audioType(payload), payload)
			notify(e)
		}
		return
	}
	size := len(payload)
	limit := s.cfg.EventBytes
	if len(payload) > limit {
		payload = payload[:limit]
	}
	event := Event{At: time.Now().UTC(), Direction: direction, Type: typ, Wrapper: wrapper, Source: source, Payload: string(payload), Size: size, Truncated: size > len(payload), Delivery: delivery}
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
	return eventOverhead + len(e.Payload) + len(e.Type) + len(e.Wrapper) + len(e.Source) + len(e.Direction) + len(e.Delivery)
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
			m.Audio = append([]AudioFact{}, e.meta.Audio...)
			m.AudioSamples = append([]AudioSample{}, e.meta.AudioSamples...)
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
	out := Snapshot{SessionID: k.SessionID, Generation: k.Generation, ZoeSessionID: e.meta.ZoeSessionID, ZoeGeneration: e.meta.ZoeGeneration, NewestSeq: e.meta.NewestSeq, Dropped: e.meta.Dropped, AudioEvents: e.meta.AudioEvents, AudioBytes: e.meta.AudioBytes, Audio: append([]AudioFact{}, e.meta.Audio...), AudioSamples: append([]AudioSample{}, e.meta.AudioSamples...), Active: e.meta.Active, Events: []Event{}}
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

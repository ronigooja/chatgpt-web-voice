package eventlog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"
)

const frameBytes = 1 << 20
const ringBytes = 8 << 20
const oversizeBytes = 256 << 10

type AudioFact struct {
	Direction string `json:"direction"`
	Source    string `json:"source"`
	Type      string `json:"type"`
	Events    uint64 `json:"events"`
	Bytes     uint64 `json:"bytes"`
	MaxBytes  int    `json:"max_bytes"`
}
type AudioSample struct {
	Direction  string   `json:"direction"`
	Type       string   `json:"type"`
	ByteLength int      `json:"byte_length"`
	Paths      []string `json:"paths"`
}

// audioType follows the viewer's bounded oversize-prefix classification.
func audioType(raw []byte) string {
	if len(raw) <= 64<<10 {
		var outer struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &outer) != nil {
			return ""
		}
		typ := outer.Type
		if len(outer.Data) > 0 {
			data := []byte(outer.Data)
			var str string
			if json.Unmarshal(outer.Data, &str) == nil {
				data = []byte(str)
			}
			var inner struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &inner) == nil && inner.Type != "" {
				typ = inner.Type
			}
		}
		if typ == "session.input_audio.append" || typ == "session.output_audio.delta" {
			return typ
		}
		return ""
	}
	return audioPrefixType(raw[:64<<10])
}

func audioPrefixType(raw []byte) string {
	for _, typ := range []string{"session.input_audio.append", "session.output_audio.delta"} {
		if bytes.Contains(raw, []byte(typ)) {
			return typ
		}
	}
	return ""
}

func (s *Store) countAudio(e *entry, direction, source, typ string, raw []byte) {
	e.meta.AudioEvents++
	e.meta.AudioBytes += uint64(len(raw))
	index := -1
	for i, f := range e.meta.Audio {
		if f.Direction == direction && f.Source == source && f.Type == typ {
			index = i
			break
		}
	}
	if index < 0 {
		e.meta.Audio = append(e.meta.Audio, AudioFact{Direction: direction, Source: source, Type: typ})
		index = len(e.meta.Audio) - 1
	}
	f := &e.meta.Audio[index]
	f.Events++
	f.Bytes += uint64(len(raw))
	if len(raw) > f.MaxBytes {
		f.MaxBytes = len(raw)
	}
	n := 0
	for _, sample := range e.meta.AudioSamples {
		if sample.Direction == direction && sample.Type == typ {
			n++
		}
	}
	if n < 3 {
		e.meta.AudioSamples = append(e.meta.AudioSamples, AudioSample{direction, typ, len(raw), audioShape(raw)})
	}
}

// Shape keys are allowlisted so arbitrary JSON keys cannot leak content.
func audioShape(raw []byte) []string {
	if len(raw) > frameBytes {
		return []string{"<oversized>:object"}
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return []string{"<malformed>:object"}
	}
	paths := []string{}
	known := map[string]bool{"type": true, "data": true, "audio": true, "delta": true, "event": true, "payload": true}
	var walk func(any, string, int)
	walk = func(v any, path string, depth int) {
		if len(paths) >= 128 || depth > 7 {
			return
		}
		switch x := v.(type) {
		case string:
			paths = append(paths, path+"=string(len="+strconv.Itoa(len(x))+")")
			if path == "data" && len(x) <= frameBytes {
				var inner any
				if json.Unmarshal([]byte(x), &inner) == nil {
					walk(inner, "data.<json>", depth+1)
				}
			}
		case map[string]any:
			paths = append(paths, path+"=object")
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				safe := k
				if !known[k] {
					safe = "<key>"
				}
				p := safe
				if path != "" {
					p = path + "." + safe
				}
				walk(x[k], p, depth+1)
			}
		case []any:
			paths = append(paths, path+"=array")
			for _, child := range x {
				walk(child, path+".[]", depth+1)
			}
		case bool:
			paths = append(paths, path+"=bool")
		case float64:
			paths = append(paths, path+"=number")
		default:
			paths = append(paths, path+"=null")
		}
	}
	walk(v, "", 0)
	return paths
}

type Frame struct {
	Seq         uint64    `json:"seq"`
	At          time.Time `json:"at"`
	Direction   string    `json:"direction"`
	Source      string    `json:"source"`
	Length      int       `json:"length"`
	IsString    bool      `json:"is_string"`
	Delivery    string    `json:"delivery"`
	Truncated   bool      `json:"truncated"`
	Ring        string    `json:"ring"`
	Oversize    bool      `json:"oversize"`
	Encoding    string    `json:"encoding"`
	Payload     string    `json:"payload"`
	storedBytes int
}
type frameRing struct {
	frames  []Frame
	bytes   int
	dropped uint64
	seq     uint64
}
type RingStats struct {
	SizeFrames int    `json:"size_frames"`
	SizeBytes  int    `json:"size_bytes"`
	Dropped    uint64 `json:"dropped"`
}
type FrameSnapshot struct {
	SessionID  string               `json:"session_id"`
	Generation int64                `json:"generation"`
	Rings      map[string]RingStats `json:"rings"`
	Frames     []Frame              `json:"frames"`
}

func (s *Store) AppendFrame(k Key, direction, source, delivery string, data []byte, binary bool) {
	if s == nil {
		return
	}
	typ := ""
	if !binary {
		typ = audioType(data)
	}
	index := 0
	max := s.cfg.FrameRingSize
	name := "conversation"
	if typ != "" {
		index = 1
		max = s.cfg.AudioRingSize
		name = "audio"
	}
	if max <= 0 && len(data) <= oversizeBytes {
		return
	}
	n := len(data)
	if len(data) > frameBytes {
		data = data[:frameBytes]
	}
	encoding := "base64"
	payload := ""
	if !binary && utf8.Valid(data) {
		encoding = "utf8"
		payload = string(data)
	} else {
		payload = base64.StdEncoding.EncodeToString(data)
	}
	f := Frame{At: time.Now().UTC(), Direction: direction, Source: source, Length: n, IsString: !binary, Delivery: delivery, Truncated: n > len(data), Ring: name, Oversize: n > oversizeBytes, Encoding: encoding, Payload: payload, storedBytes: len(data)}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[k]
	if e == nil || !e.meta.Active {
		return
	}
	if max > 0 {
		s.addFrame(&e.rings[index], f, max, ringBytes)
	}
	if f.Oversize {
		f.Ring = "oversize"
		s.addFrame(&e.rings[2], f, 3, 3*frameBytes*2)
	}
	s.enforceGlobal()
}

func (s *Store) addFrame(r *frameRing, f Frame, max, maxBytes int) {
	r.seq++
	f.Seq = r.seq
	r.frames = append(r.frames, f)
	n := f.storedBytes
	r.bytes += n
	s.bytes += n
	for len(r.frames) > max || r.bytes > maxBytes {
		s.dropFrame(r)
	}
}
func (s *Store) dropFrame(r *frameRing) {
	n := r.frames[0].storedBytes
	r.frames[0] = Frame{}
	r.frames = r.frames[1:]
	r.bytes -= n
	s.bytes -= n
	r.dropped++
}

func (s *Store) Frames(k Key, which string) (FrameSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire(time.Now())
	e := s.entries[k]
	if e == nil {
		return FrameSnapshot{}, false
	}
	out := FrameSnapshot{SessionID: k.SessionID, Generation: k.Generation, Rings: map[string]RingStats{}, Frames: []Frame{}}
	for i, name := range []string{"conversation", "audio", "oversize"} {
		r := &e.rings[i]
		out.Rings[name] = RingStats{len(r.frames), r.bytes, r.dropped}
		if which == "all" || which == name {
			out.Frames = append(out.Frames, r.frames...)
		}
	}
	sort.SliceStable(out.Frames, func(i, j int) bool { return out.Frames[i].At.Before(out.Frames[j].At) })
	return out, true
}

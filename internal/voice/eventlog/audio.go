package eventlog

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const frameBytes = 1 << 20

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

var shapeKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,39}$`)
var shapeUUIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// shapeCharset emits only a class, never string contents.
func shapeCharset(s string) string {
	if !utf8.ValidString(s) {
		return "mixed"
	}
	if len(s) == 0 {
		return "ascii"
	}
	hex, b64, ascii := true, true, true
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			hex = false
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '+' || r == '/' || r == '=' || r == '-' || r == '_') {
			b64 = false
		}
		if r < 32 || r > 126 {
			ascii = false
		}
	}
	if hex {
		return "hexchars"
	}
	if b64 {
		return "b64chars"
	}
	if ascii {
		return "ascii"
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return "mixed"
		}
	}
	return "utf8"
}

func safeShapeKey(key string) string {
	if !shapeKeyPattern.MatchString(key) || shapeUUIDPattern.MatchString(key) {
		return "<key>"
	}
	if len(key) >= 24 && shapeCharset(key) == "hexchars" {
		return "<key>"
	}
	if len(key) > 24 && shapeCharset(key) == "b64chars" {
		return "<key>"
	}
	return key
}

// Shape samples retain safe keys and value types, lengths and charset classes only.
func audioShape(raw []byte) []string {
	if len(raw) > 64<<10 {
		return []string{"<oversized>:object"}
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return []string{"<malformed>:object"}
	}
	paths := []string{}
	var walk func(any, string, int)
	walk = func(v any, path string, depth int) {
		if len(paths) >= 128 || depth > 7 {
			return
		}
		switch x := v.(type) {
		case string:
			paths = append(paths, path+"=string(len="+strconv.Itoa(len(x))+",charset="+shapeCharset(x)+")")
			if (path == "data" || strings.HasSuffix(path, ".data")) && len(x) <= frameBytes {
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
				safe := safeShapeKey(k)
				p := safe
				if path != "" {
					p = path + "." + safe
				}
				walk(x[k], p, depth+1)
			}
		case []any:
			paths = append(paths, path+"=array(count="+strconv.Itoa(len(x))+")")
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

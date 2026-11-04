package relay

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	defaultEventShapeMaxFrames = 400
	maxEventShapeFrames        = 2000
	maxEventShapePaths         = 64
	maxEventShapeDepth         = 5
	maxEventShapeParseBytes    = 1 << 20
)

type eventShapeConfig struct {
	enabled   bool
	maxFrames int
	route     string
}
type eventShapeSample struct {
	data              []byte
	direction, source string
	sequence          int
}

type eventShapeSampler struct {
	logger                *slog.Logger
	mediaSessionID        string
	mu                    sync.Mutex
	voiceSessionID, route string
	count                 int
	finalish              bool
	counts                map[eventShapeCount]int
	queue                 chan eventShapeSample
	done                  chan struct{}
	closeOnce             sync.Once
	closed                bool
	maxFrames             int
}
type eventShapeCount struct{ direction, outer, inner string }

var allowedShapeValueKeys = map[string]bool{
	"type": true, "role": true, "direction": true, "content_type": true, "status": true,
	"finish_type": true, "finish_details": true, "end_turn": true, "is_final": true,
	"final": true, "done": true, "complete": true, "completed": true, "o": true, "p": true,
}
var safeShapeValue = regexp.MustCompile(`^[A-Za-z0-9_./:\-\[\]]{1,64}$`)
var uuidShapeValue = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var hexShapeValue = regexp.MustCompile(`(?i)^[0-9a-f]{24,}$`)

func newEventShapeSampler(logger *slog.Logger, mediaID, route string, maxFrames int) *eventShapeSampler {
	if maxFrames <= 0 {
		maxFrames = defaultEventShapeMaxFrames
	}
	if maxFrames > maxEventShapeFrames {
		maxFrames = maxEventShapeFrames
	}
	s := &eventShapeSampler{logger: logger, mediaSessionID: mediaID, route: normalizeShapeRoute(route), counts: make(map[eventShapeCount]int), queue: make(chan eventShapeSample, 128), done: make(chan struct{}), maxFrames: maxFrames}
	go s.run(maxFrames)
	return s
}

func normalizeShapeRoute(route string) string {
	if strings.EqualFold(strings.TrimSpace(route), "avas") {
		return "avas"
	}
	return "wm"
}

func (s *eventShapeSampler) setMetadata(voiceID, route string) {
	s.mu.Lock()
	s.voiceSessionID = voiceID
	s.route = normalizeShapeRoute(route)
	s.mu.Unlock()
}

func (s *eventShapeSampler) observe(data []byte, source, direction string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.count >= s.maxFrames {
		return
	}
	s.count++
	seq := s.count
	item := eventShapeSample{data: append([]byte(nil), data...), direction: direction, source: source, sequence: seq}
	select {
	case s.queue <- item:
	default:
	}
}

func (s *eventShapeSampler) run(maxFrames int) {
	for item := range s.queue {
		s.emit(item)
	}
	close(s.done)
}

func (s *eventShapeSampler) emit(item eventShapeSample) {
	outer, inner, paths, finalish := shapeFrame(item.data)
	s.mu.Lock()
	s.counts[eventShapeCount{item.direction, outer, inner}]++
	if finalish {
		s.finalish = true
	}
	voiceID, route := s.voiceSessionID, s.route
	s.mu.Unlock()
	attrs := []any{"voice_session_id", voiceID, "media_session_id", s.mediaSessionID, "route", route, "source", item.source, "direction", item.direction, "frame_sequence", item.sequence, "byte_length", len(item.data), "outer_type", outer, "inner_type", inner, "paths", paths}
	s.logger.Info("oai_event_shape", attrs...)
}

func (s *eventShapeSampler) close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.queue)
		s.mu.Unlock()
		<-s.done
		s.emitSummary()
	})
}

func (s *eventShapeSampler) emitSummary() {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]string, 0, 64)
	for key, count := range s.counts {
		if len(entries) == 64 {
			break
		}
		entries = append(entries, fmt.Sprintf("%s|%s|%s=%d", key.direction, key.outer, key.inner, count))
	}
	s.logger.Info("oai_event_shape_summary", "voice_session_id", s.voiceSessionID, "media_session_id", s.mediaSessionID, "route", s.route, "entries", entries, "finalish_key_observed", s.finalish)
}

func shapeFrame(raw []byte) (outerType, innerType string, paths []string, finalish bool) {
	if len(raw) > maxEventShapeParseBytes {
		return "", "", []string{"<oversized>:object"}, false
	}
	var outer map[string]any
	if json.Unmarshal(raw, &outer) != nil {
		return "", "", []string{"<malformed>:object"}, false
	}
	outerType = shapeString(outer["type"])
	innerRaw := outer["data"]
	if str, ok := innerRaw.(string); ok {
		innerRaw = str
	}
	inner, ok := innerRaw.(map[string]any)
	if !ok {
		if str, ok := innerRaw.(string); ok {
			_ = json.Unmarshal([]byte(str), &inner)
		}
	}
	if inner != nil {
		innerType = shapeString(inner["type"])
	}
	paths, finalish = collectShapePaths(outer)
	if inner != nil {
		innerPaths, innerFinal := collectShapePaths(inner)
		paths = append(paths, innerPaths...)
		if len(paths) > maxEventShapePaths {
			paths = paths[:maxEventShapePaths]
		}
		finalish = finalish || innerFinal
	}
	return
}

func collectShapePaths(root map[string]any) ([]string, bool) {
	paths := make([]string, 0, maxEventShapePaths)
	finalish := false
	var walk func(any, string, int)
	walk = func(value any, path string, depth int) {
		if len(paths) >= maxEventShapePaths || depth > maxEventShapeDepth {
			return
		}
		kind := shapeKind(value)
		valueText := ""
		if str, ok := value.(string); ok {
			valueText = shapeDisclosure(path, str)
		}
		paths = append(paths, path+"="+kind+valueText)
		key := path[strings.LastIndex(path, ".")+1:]
		if allowedShapeValueKeys[key] && isFinalShapeKey(key) {
			finalish = true
		}
		switch v := value.(type) {
		case map[string]any:
			if depth == maxEventShapeDepth {
				return
			}
			for k, child := range v {
				walk(child, path+"."+k, depth+1)
				if len(paths) >= maxEventShapePaths {
					return
				}
			}
		case []any:
			if depth < maxEventShapeDepth {
				for _, child := range v {
					walk(child, path+".[]", depth+1)
					if len(paths) >= maxEventShapePaths {
						return
					}
				}
			}
		}
	}
	for key, value := range root {
		walk(value, key, 1)
	}
	return paths, finalish
}

func shapeKind(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return "unknown"
	}
}
func shapeString(v any) string {
	if s, ok := v.(string); ok && safeShapeValue.MatchString(s) {
		return s
	}
	return ""
}
func isFinalShapeKey(key string) bool {
	return key == "end_turn" || key == "is_final" || key == "final" || key == "done" || key == "complete" || key == "completed" || key == "finish_type" || key == "finish_details"
}
func shapeDisclosure(path, value string) string {
	key := path[strings.LastIndex(path, ".")+1:]
	if !allowedShapeValueKeys[key] {
		return "(" + shapeLenBucket(value) + ")"
	}
	if key == "p" && strings.IndexFunc(value, func(r rune) bool { return r >= '0' && r <= '9' }) >= 0 {
		return "=" + strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return '#'
			}
			return r
		}, value)
	}
	if !safeShapeValue.MatchString(value) || uuidShapeValue.MatchString(value) || hexShapeValue.MatchString(value) || len(value) > 24 && looksLikeEncoded(value) {
		return "(redacted)"
	}
	return "=" + value
}
func shapeLenBucket(s string) string {
	n := utf8.RuneCountInString(s)
	switch {
	case n == 0:
		return "len=0"
	case n <= 10:
		return "len=1-10"
	case n <= 100:
		return "len=11-100"
	case n <= 1000:
		return "len=101-1000"
	default:
		return "len=>1000"
	}
}
func looksLikeEncoded(s string) bool {
	if len(s) <= 24 {
		return false
	}
	_, err := base64.RawStdEncoding.DecodeString(s)
	return err == nil
}

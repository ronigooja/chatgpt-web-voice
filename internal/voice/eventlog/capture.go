package eventlog

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The 24 kHz rate is inferred from observed 9600-byte, 200 ms PCM frames.
const audioSampleRate = 24000
const captureQueueSize = 256
const maxSegmentBytes = 1 << 30

type audioMessage struct {
	key  Key
	data []byte
	at   time.Time
}
type audioSegment struct {
	file  *os.File
	path  string
	start time.Time
	bytes uint32
}
type audioWriter struct {
	cfg      Config
	queue    chan audioMessage
	done     chan struct{}
	now      func() time.Time
	closed   atomic.Bool
	inflight atomic.Int64
	stop     chan struct{}
	wake     chan struct{}
	stopOnce sync.Once
	ended    sync.Map
	drops    sync.Map
}

func newAudioWriter(c Config) *audioWriter {
	w := &audioWriter{cfg: c, queue: make(chan audioMessage, captureQueueSize), done: make(chan struct{}), stop: make(chan struct{}), wake: make(chan struct{}, 1), now: time.Now}
	go w.run()
	return w
}

// CaptureAudio queues a copy without waiting for disk or the writer.
func (s *Store) CaptureAudio(k Key, data []byte) {
	if s == nil || s.capture == nil {
		return
	}
	w := s.capture
	if w.closed.Load() {
		return
	}
	w.inflight.Add(1)
	defer w.inflight.Add(-1)
	if w.closed.Load() {
		return
	}
	if _, ended := w.ended.Load(k); ended {
		return
	}
	if len(w.queue) == cap(w.queue) {
		v, _ := w.drops.LoadOrStore(k, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
		return
	}
	select {
	case w.queue <- audioMessage{key: k, data: append([]byte(nil), data...), at: w.now().UTC()}:
	default:
		v, _ := w.drops.LoadOrStore(k, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
	}
}
func (w *audioWriter) end(k Key) {
	w.ended.Store(k, struct{}{})
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (s *Store) Close() {
	if s == nil || s.capture == nil {
		return
	}
	w := s.capture
	w.stopOnce.Do(func() {
		w.closed.Store(true)
		for w.inflight.Load() != 0 {
			runtime.Gosched()
		}
		close(w.stop)
	})
	<-w.done
}

func sanitizedSession(id string) (string, bool) {
	if id == "" || strings.ContainsAny(id, `/\`) {
		return "", false
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, id), true
}
func decodeAudio(raw []byte) (string, []byte, error) {
	var outer struct {
		Type  string          `json:"type"`
		Data  json.RawMessage `json:"data"`
		Audio string          `json:"audio"`
		Delta string          `json:"delta"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return "", nil, err
	}
	if outer.Type == "data_message" {
		var text string
		data := []byte(outer.Data)
		if json.Unmarshal(outer.Data, &text) == nil {
			data = []byte(text)
		}
		if err := json.Unmarshal(data, &outer); err != nil {
			return "", nil, err
		}
	}
	dir, encoded := "", ""
	switch outer.Type {
	case "session.input_audio.append":
		dir, encoded = "in", outer.Audio
	case "session.output_audio.delta":
		dir, encoded = "out", outer.Delta
	default:
		return "", nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	return dir, data, err
}
func shouldRotate(seg *audioSegment, at time.Time, duration time.Duration, size int) bool {
	return at.Sub(seg.start) >= duration || uint64(seg.bytes)+uint64(size) > maxSegmentBytes
}
func wavHeader(n uint32) []byte {
	b := make([]byte, 44)
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], 36+n)
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], audioSampleRate)
	binary.LittleEndian.PutUint32(b[28:], audioSampleRate*2)
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], n)
	return b
}
func finishSegment(seg *audioSegment) error {
	_, err := seg.file.WriteAt(wavHeader(seg.bytes), 0)
	closeErr := seg.file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func (w *audioWriter) run() {
	defer close(w.done)
	segments := map[Key]map[string]*audioSegment{}
	disabled := map[Key]bool{}
	finish := func(k Key) {
		for _, seg := range segments[k] {
			if err := finishSegment(seg); err != nil {
				slog.Warn("audio_capture_write_failed", "class", "io")
			}
		}
		delete(segments, k)
		w.retain(segments)
	}
	process := func(m audioMessage) {
		defer func() {
			if _, ok := w.ended.Load(m.key); ok {
				select {
				case w.wake <- struct{}{}:
				default:
				}
			}
		}()
		if disabled[m.key] {
			return
		}
		direction, pcm, err := decodeAudio(m.data)
		if err != nil || direction == "" || len(pcm) == 0 {
			return
		}
		if len(pcm) > maxSegmentBytes {
			disabled[m.key] = true
			slog.Warn("audio_capture_write_failed", "class", "frame_too_large")
			finish(m.key)
			return
		}
		id, ok := sanitizedSession(m.key.SessionID)
		if !ok {
			disabled[m.key] = true
			slog.Warn("audio_capture_write_failed", "class", "invalid_session")
			return
		}
		if segments[m.key] == nil {
			segments[m.key] = map[string]*audioSegment{}
		}
		seg := segments[m.key][direction]
		if seg != nil && shouldRotate(seg, m.at, w.cfg.AudioCaptureSegment, len(pcm)) {
			err = finishSegment(seg)
			delete(segments[m.key], direction)
			seg = nil
			w.retain(segments)
		}
		if err == nil && seg == nil {
			dir := filepath.Join(w.cfg.AudioCaptureDir, id, fmt.Sprintf("gen%d", m.key.Generation))
			err = os.MkdirAll(dir, 0700)
			if err == nil {
				name := fmt.Sprintf("%s_%s_gen%d_%s.wav", m.at.UTC().Format("20060102T150405.000Z"), id, m.key.Generation, direction)
				path := filepath.Join(dir, name)
				var f *os.File
				f, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if err == nil {
					seg = &audioSegment{file: f, path: path, start: m.at}
					segments[m.key][direction] = seg
					_, err = f.Write(wavHeader(0))
				}
			}
		}
		if err == nil {
			var n int
			n, err = seg.file.Write(pcm)
			seg.bytes += uint32(n)
			if err == nil && n != len(pcm) {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			slog.Warn("audio_capture_write_failed", "class", "io")
			disabled[m.key] = true
			finish(m.key)
		}
	}
	drain := func() {
		n := len(w.queue)
		for i := 0; i < n; i++ {
			process(<-w.queue)
		}
	}
	finishEnded := func() {
		w.ended.Range(func(key, _ any) bool {
			k := key.(Key)
			if _, ok := segments[k]; ok {
				finish(k)
			}
			return true
		})
	}
	for {
		select {
		case m := <-w.queue:
			process(m)
		case <-w.wake:
			drain()
			finishEnded()
		case <-w.stop:
			drain()
			finishEnded()
			for k := range segments {
				finish(k)
			}
			goto done
		}
	}
done:
	w.drops.Range(func(key, value any) bool {
		slog.Warn("audio_capture_queue_full", "dropped", value.(*atomic.Uint64).Load())
		return true
	})
}
func (w *audioWriter) retain(open map[Key]map[string]*audioSegment) {
	if w.cfg.AudioCaptureMaxBytes <= 0 {
		return
	}
	opened := map[string]bool{}
	for _, dirs := range open {
		for _, s := range dirs {
			opened[s.path] = true
		}
	}
	type file struct {
		path string
		size int64
		mod  time.Time
	}
	var files []file
	var total int64
	_ = filepath.WalkDir(w.cfg.AudioCaptureDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".wav") {
			return nil
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		total += info.Size()
		if !opened[path] {
			files = append(files, file{path, info.Size(), info.ModTime()})
		}
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].mod.Before(files[j].mod) })
	for _, f := range files {
		if total <= w.cfg.AudioCaptureMaxBytes {
			break
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}

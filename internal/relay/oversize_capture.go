package relay

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const oversizeFileCap = 16 << 20

// Serializes directory limits across all sessions in this process.
var oversizeCaptureMu sync.Mutex

type oversizeCapture struct {
	dir, mediaID string
	logger       *slog.Logger
	count        int // guarded by oversizeCaptureMu
	capBytes     int // zero selects the production limit
}

func (c *oversizeCapture) start(message dataChannelMessage, direction string) {
	// The queued message owns its bytes and is not subsequently modified.
	go c.record(message, direction)
}

func (c *oversizeCapture) failure(message dataChannelMessage, direction string) {
	c.log(message, direction, time.Now().UTC(), "send_failed", "", false, "")
}

func (c *oversizeCapture) log(message dataChannelMessage, direction string, now time.Time, status, path string, truncated bool, reason string) {
	outer, inner, _, _ := shapeFrame(message.data)
	c.logger.Warn("relay_oversize_message", "direction", direction, "source", message.source, "byte_length", len(message.data), "is_string", message.isString, "outer_type", outer, "inner_type", inner, "utc_time", now.Format(time.RFC3339Nano), "capture", status, "capture_path", path, "truncated", truncated, "reason", reason)
}

func (c *oversizeCapture) record(message dataChannelMessage, direction string) {
	now := time.Now().UTC()
	status, path, truncated, reason := c.write(message, now)
	c.log(message, direction, now, status, path, truncated, reason)
}

func (c *oversizeCapture) write(message dataChannelMessage, now time.Time) (status, path string, truncated bool, reason string) {
	if c.dir == "" {
		return "disabled", "", false, ""
	}
	oversizeCaptureMu.Lock()
	defer oversizeCaptureMu.Unlock()
	if c.count >= 4 {
		return "skipped_limit", "", false, ""
	}
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return "error", "", false, "mkdir"
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return "error", "", false, "readdir"
	}
	if len(entries) >= 20 {
		return "skipped_limit", "", false, ""
	}
	name := fmt.Sprintf("%s-%s-%s-%d.bin", now.Format("20060102T150405.000000000Z"), c.mediaID, message.source, len(message.data))
	path = filepath.Join(c.dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "error", path, false, "create"
	}
	c.count++
	capBytes := c.capBytes
	if capBytes <= 0 || capBytes > oversizeFileCap {
		capBytes = oversizeFileCap
	}
	data := message.data
	if len(data) > capBytes {
		data = data[:capBytes]
		truncated = true
	}
	n, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil || n != len(data) || closeErr != nil {
		return "error", path, truncated, "write"
	}
	return "written", path, truncated, ""
}

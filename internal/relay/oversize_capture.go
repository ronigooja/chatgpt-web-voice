package relay

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

const (
	oversizeFileCap      = 16 << 20
	smallCaptureLimit    = 30
	oversizeCaptureLimit = 20
	totalCaptureLimit    = smallCaptureLimit + oversizeCaptureLimit
)

var captureNamePattern = regexp.MustCompile(`^([0-9]{8}T[0-9]{6}\.[0-9]{9}Z)-(small|oversize)-[A-Za-z0-9_-]+-[A-Za-z0-9_]+-[0-9]+\.bin$`)

// Serializes directory limits across all sessions in this process.
var oversizeCaptureMu sync.Mutex

type oversizeCapture struct {
	dir, mediaID   string
	logger         *slog.Logger
	count          int // guarded by oversizeCaptureMu
	capBytes       int // zero selects the production limit
	thresholdBytes int
}

func (c *oversizeCapture) threshold() int {
	if c.thresholdBytes <= 0 {
		return mediaDataMessageMaxSize
	}
	return c.thresholdBytes
}

func (c *oversizeCapture) start(message dataChannelMessage, direction string) {
	// The queue owns an immutable copy of the message bytes.
	go c.record(message, direction)
}

func (c *oversizeCapture) failure(message dataChannelMessage, direction string) {
	c.log(message, direction, time.Now().UTC(), "send_failed", "", false, "")
}

func (c *oversizeCapture) log(message dataChannelMessage, direction string, now time.Time, status, path string, truncated bool, reason string) {
	outer, inner, _, _ := shapeFrame(message.data)
	level := slog.LevelWarn
	if status != "send_failed" && len(message.data) <= mediaDataMessageMaxSize {
		level = slog.LevelInfo
	}
	c.logger.Log(context.Background(), level, "relay_oversize_message", "direction", direction, "source", message.source, "byte_length", len(message.data), "threshold_bytes", c.threshold(), "is_string", message.isString, "outer_type", outer, "inner_type", inner, "utc_time", now.Format(time.RFC3339Nano), "capture", status, "capture_path", path, "truncated", truncated, "reason", reason)
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
	small := len(message.data) <= mediaDataMessageMaxSize
	if !small && c.count >= 4 {
		return "skipped_limit", "", false, ""
	}
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return "error", "", false, "mkdir"
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return "error", "", false, "readdir"
	}
	// os.ReadDir returns names sorted, so the first small file is the oldest.
	var smallFiles []string
	oversizeCount := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		match := captureNamePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		if _, err := time.Parse("20060102T150405.000000000Z", match[1]); err != nil {
			continue
		}
		if match[2] == "small" {
			smallFiles = append(smallFiles, entry.Name())
		} else {
			oversizeCount++
		}
	}
	// Small captures roll within 30 slots; oversize captures keep 20 slots and are never evicted.
	// Only recognized capture files count toward the combined 50-file limit.
	if small {
		for len(smallFiles) > 0 && (len(smallFiles) >= smallCaptureLimit || len(smallFiles)+oversizeCount >= totalCaptureLimit) {
			if err := os.Remove(filepath.Join(c.dir, smallFiles[0])); err != nil {
				return "error", "", false, "evict"
			}
			smallFiles = smallFiles[1:]
		}
		if len(smallFiles)+oversizeCount >= totalCaptureLimit {
			return "skipped_limit", "", false, ""
		}
	} else if oversizeCount >= oversizeCaptureLimit || len(smallFiles)+oversizeCount >= totalCaptureLimit {
		return "skipped_limit", "", false, ""
	}
	tier := "oversize"
	if small {
		tier = "small"
	}
	name := fmt.Sprintf("%s-%s-%s-%s-%d.bin", now.Format("20060102T150405.000000000Z"), tier, c.mediaID, message.source, len(message.data))
	path = filepath.Join(c.dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "error", path, false, "create"
	}
	if !small {
		c.count++
	}
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

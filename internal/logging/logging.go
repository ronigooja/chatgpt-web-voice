package logging

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
)

type contextKey struct{}
type requestIDKey struct{}

// New creates the process logger used by both application and access logs.
func New(format, level string) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: slogLevel}
	if strings.EqualFold(strings.TrimSpace(format), "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// FromContext returns the request-scoped logger when available.
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(contextKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *responseRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseRecorder) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func (w *responseRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// HTTPMiddleware adds request IDs, panic recovery, and structured access logs.
func HTTPMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		clientVoice := strings.HasPrefix(r.URL.Path, "/client/v1/")
		id, path, method, userAgent := "", "/client/v1/", "", ""
		if clientVoice {
			id = uuid.NewString()
		} else {
			id, path, method, userAgent = requestID(r), sanitize(r.URL.Path, 200), sanitize(r.Method, 32), sanitize(r.UserAgent(), 120)
		}
		requestID := id
		w.Header().Set("X-Request-ID", requestID)

		requestLogger := logger.With(
			"request_id", requestID,
			"method", method,
			"path", path,
		)
		ctx := context.WithValue(r.Context(), contextKey{}, requestLogger)
		ctx = context.WithValue(ctx, requestIDKey{}, requestID)
		r = r.WithContext(ctx)
		recorder := &responseRecorder{ResponseWriter: w}

		defer func() {
			if recovered := recover(); recovered != nil {
				if clientVoice {
					requestLogger.Error("http_panic")
				} else {
					requestLogger.Error("http_panic",
						"panic_type", fmt.Sprintf("%T", recovered),
						"stack", stackFrames(),
					)
				}
				if recorder.status == 0 {
					http.Error(recorder, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}

			status := recorder.status
			if status == 0 {
				status = http.StatusOK
			}
			attrs := []any{
				"route_pattern", sanitize(r.Pattern, 200),
				"status", status,
				"response_bytes", recorder.bytes,
				"duration_ms", float64(time.Since(started).Microseconds()) / 1000,
				"remote_ip", remoteIP(r.RemoteAddr),
				"user_agent", userAgent,
			}
			switch {
			case status >= 500:
				requestLogger.Error("http_request", attrs...)
			case status >= 400:
				requestLogger.Warn("http_request", attrs...)
			default:
				requestLogger.Info("http_request", attrs...)
			}
		}()

		next.ServeHTTP(recorder, r)
	})
}

// RequestID returns the middleware request ID, including a client-supplied X-Request-ID.
func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

func requestID(r *http.Request) string {
	incoming := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if validRequestID(incoming) {
		return sanitize(incoming, 120)
	}
	return uuid.NewString()
}

func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return remoteAddr
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func sanitize(s string, limit int) string {
	var b strings.Builder
	for _, r := range s {
		if b.Len() >= limit {
			break
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("/_.-~", r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func stackFrames() string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var b strings.Builder
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&b, "%s %s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return b.String()
}

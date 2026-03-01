package sync

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	gosync "sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// logger is the package-level structured logger for all sync operations.
// Defaults to a no-op (discard) handler until InitLogger is called.
var logger *slog.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

// InitLogger configures the sync package logger.
// Always enables console output: INFO→stdout, WARN/ERROR→stderr.
// If logDir is non-empty, also writes to level-split log files:
//   - sync_warn.log  — WARN + ERROR (no practical size limit)
//   - sync_info.log  — INFO only (1MB, 1 backup)
//   - sync_debug.log — DEBUG only (1MB, 1 backup)
func InitLogger(logDir string) {
	console := &consoleHandler{
		stdout: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		stderr: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}),
	}
	errCapture := &errorCaptureHandler{}

	handlers := []slog.Handler{console, errCapture}

	if logDir != "" {
		os.MkdirAll(logDir, 0750) //nolint:errcheck

		warnFile := slog.NewTextHandler(&lumberjack.Logger{
			Filename:   filepath.Join(logDir, "sync_warn.log"),
			MaxSize:    1000,
			MaxBackups: 3,
		}, &slog.HandlerOptions{Level: slog.LevelWarn})

		infoFile := &levelRangeHandler{
			min: slog.LevelInfo,
			max: slog.LevelInfo,
			inner: slog.NewTextHandler(&lumberjack.Logger{
				Filename:   filepath.Join(logDir, "sync_info.log"),
				MaxSize:    1,
				MaxBackups: 1,
			}, &slog.HandlerOptions{Level: slog.LevelInfo}),
		}

		debugFile := &levelRangeHandler{
			min: slog.LevelDebug,
			max: slog.LevelDebug,
			inner: slog.NewTextHandler(&lumberjack.Logger{
				Filename:   filepath.Join(logDir, "sync_debug.log"),
				MaxSize:    1,
				MaxBackups: 1,
			}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		}

		handlers = append(handlers, warnFile, infoFile, debugFile)
	}

	logger = slog.New(&multiHandler{handlers: handlers})
}

// sub returns a child logger tagged with the given component name.
func sub(component string) *slog.Logger {
	return logger.With("comp", component)
}

// logEnabled reports whether the given log level is enabled.
// Use this to guard expensive DEBUG logging in hot paths.
func logEnabled(level slog.Level) bool {
	return logger.Enabled(context.Background(), level)
}

// --- consoleHandler: routes INFO→stdout, WARN+→stderr ---

type consoleHandler struct {
	stdout slog.Handler
	stderr slog.Handler
}

func (h *consoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

func (h *consoleHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		return h.stderr.Handle(ctx, r)
	}
	return h.stdout.Handle(ctx, r)
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &consoleHandler{
		stdout: h.stdout.WithAttrs(attrs),
		stderr: h.stderr.WithAttrs(attrs),
	}
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	return &consoleHandler{
		stdout: h.stdout.WithGroup(name),
		stderr: h.stderr.WithGroup(name),
	}
}

// --- errorCapture: captures recent error-level log messages ---

// LogEntry represents a captured error log entry.
type LogEntry struct {
	Time    time.Time `json:"time"`
	Comp    string    `json:"comp"`
	Message string    `json:"message"`
	Error   string    `json:"error,omitempty"`
}

var errorRing struct {
	mu      gosync.Mutex
	entries [2]LogEntry
	count   int
}

// RecentErrors returns the most recent error log entries (up to 2).
func RecentErrors() []LogEntry {
	errorRing.mu.Lock()
	defer errorRing.mu.Unlock()
	n := errorRing.count
	if n > 2 {
		n = 2
	}
	out := make([]LogEntry, n)
	// Return newest first
	for i := 0; i < n; i++ {
		out[i] = errorRing.entries[(2-1-i+errorRing.count)%2]
	}
	return out
}

type errorCaptureHandler struct{}

func (h *errorCaptureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (h *errorCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	entry := LogEntry{
		Time:    r.Time,
		Message: r.Message,
	}
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "comp":
			entry.Comp = a.Value.String()
		case "err":
			entry.Error = a.Value.String()
		}
		return true
	})
	errorRing.mu.Lock()
	errorRing.entries[errorRing.count%2] = entry
	errorRing.count++
	errorRing.mu.Unlock()
	return nil
}

func (h *errorCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *errorCaptureHandler) WithGroup(_ string) slog.Handler      { return h }

// --- levelRangeHandler: passes only a specific level range ---

type levelRangeHandler struct {
	min, max slog.Level
	inner    slog.Handler
}

func (h *levelRangeHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.min && level <= h.max
}

func (h *levelRangeHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *levelRangeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelRangeHandler{min: h.min, max: h.max, inner: h.inner.WithAttrs(attrs)}
}

func (h *levelRangeHandler) WithGroup(name string) slog.Handler {
	return &levelRangeHandler{min: h.min, max: h.max, inner: h.inner.WithGroup(name)}
}

// --- multiHandler: fans out to multiple handlers ---

type multiHandler struct {
	handlers []slog.Handler
}

func (h *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, hh := range h.handlers {
		if hh.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, hh := range h.handlers {
		if hh.Enabled(ctx, r.Level) {
			if err := hh.Handle(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(h.handlers))
	for i, hh := range h.handlers {
		hs[i] = hh.WithAttrs(attrs)
	}
	return &multiHandler{handlers: hs}
}

func (h *multiHandler) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(h.handlers))
	for i, hh := range h.handlers {
		hs[i] = hh.WithGroup(name)
	}
	return &multiHandler{handlers: hs}
}

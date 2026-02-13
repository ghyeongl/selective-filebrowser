package sync

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// logger is the package-level structured logger for all sync operations.
// Defaults to a no-op (discard) handler until InitLogger is called.
var logger *slog.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

// InitLogger configures the sync package logger.
// Always enables console output: INFO→stdout, WARN/ERROR→stderr.
// If debugWriter is non-nil, also writes DEBUG+ level logs to it.
func InitLogger(debugWriter io.Writer) {
	console := &consoleHandler{
		stdout: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		stderr: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}),
	}

	if debugWriter == nil {
		logger = slog.New(console)
		return
	}

	file := slog.NewTextHandler(debugWriter, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger = slog.New(&multiHandler{handlers: []slog.Handler{console, file}})
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

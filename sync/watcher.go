package sync

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const debounceInterval = 300 * time.Millisecond

// Watcher monitors Archives and Spaces directories for filesystem changes
// and feeds relative paths into the eval queue.
type Watcher struct {
	archivesRoot string
	spacesRoot   string
	queue        *EvalQueue
	watcher      *fsnotify.Watcher
	Overflow     chan struct{}
}

// NewWatcher creates a filesystem watcher for both roots.
func NewWatcher(archivesRoot, spacesRoot string, queue *EvalQueue) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		queue:        queue,
		watcher:      w,
		Overflow:     make(chan struct{}, 1),
	}, nil
}

// Start begins watching and debouncing events. Blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	l := sub("watcher")

	// Add recursive watches
	if err := w.addRecursive(w.archivesRoot); err != nil {
		return err
	}
	l.Info("watching", "root", w.archivesRoot, "type", "archives")

	if err := w.addRecursive(w.spacesRoot); err != nil {
		return err
	}
	l.Info("watching", "root", w.spacesRoot, "type", "spaces")

	// Debounce timer and pending paths
	pending := make(map[string]struct{})
	timer := time.NewTimer(debounceInterval)
	timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.watcher.Close()
			l.Info("watcher stopping")
			return ctx.Err()

		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil
			}

			if logEnabled(slog.LevelDebug) {
				l.Debug("event", "name", event.Name, "op", event.Op.String())
			}

			relPath := w.toRelPath(event.Name)
			if relPath == "" {
				continue
			}

			// Skip .sync-conflict and hidden files
			base := filepath.Base(event.Name)
			if strings.HasPrefix(base, ".") || strings.Contains(base, ".sync-conflict-") {
				if logEnabled(slog.LevelDebug) {
					l.Debug("skip", "name", event.Name, "reason", "hidden or conflict")
				}
				continue
			}

			pending[relPath] = struct{}{}
			if logEnabled(slog.LevelDebug) {
				l.Debug("pending", "path", relPath, "op", event.Op.String())
			}

			// Reset debounce timer
			timer.Reset(debounceInterval)

			// If a new directory was created, add it to watch
			if event.Has(fsnotify.Create) {
				if err := w.watcher.Add(event.Name); err == nil {
					l.Debug("added new dir", "path", event.Name)
				}
			}

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil
			}
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				l.Warn("event overflow detected")
				select {
				case w.Overflow <- struct{}{}:
				default:
				}
			} else {
				l.Error("watcher error", "err", err)
			}

		case <-timer.C:
			// Debounce timer fired — flush pending paths to queue
			if len(pending) > 0 {
				paths := make([]string, 0, len(pending))
				for p := range pending {
					paths = append(paths, p)
				}
				w.queue.PushMany(paths)
				l.Info("flushed", "count", len(paths))
				if logEnabled(slog.LevelDebug) {
					l.Debug("flush paths", "paths", paths)
				}
				pending = make(map[string]struct{})
			}
		}
	}
}

// toRelPath converts an absolute path to the relative path used by the pipeline.
// It tries Archives first, then Spaces.
func (w *Watcher) toRelPath(absPath string) string {
	if rel, err := filepath.Rel(w.archivesRoot, absPath); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	if rel, err := filepath.Rel(w.spacesRoot, absPath); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return ""
}

// addRecursive adds a directory and all subdirectories to the watcher.
func (w *Watcher) addRecursive(root string) error {
	l := sub("watcher")
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			l.Warn("skip inaccessible", "path", path, "err", err)
			return nil // skip inaccessible dirs
		}
		if d.IsDir() {
			base := filepath.Base(path)
			if strings.HasPrefix(base, ".") && path != root {
				return filepath.SkipDir
			}
			if err := w.watcher.Add(path); err != nil {
				return err
			}
			l.Debug("added dir", "path", path)
			return nil
		}
		return nil
	})
}

// Close closes the underlying fsnotify watcher.
func (w *Watcher) Close() error {
	return w.watcher.Close()
}

package sync

import (
	"context"
	"io/fs"
	"log"
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
	}, nil
}

// Start begins watching and debouncing events. Blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	// Add recursive watches
	if err := w.addRecursive(w.archivesRoot); err != nil {
		return err
	}
	if err := w.addRecursive(w.spacesRoot); err != nil {
		return err
	}

	log.Printf("[watcher] Watching %s and %s", w.archivesRoot, w.spacesRoot)

	// Debounce timer and pending paths
	pending := make(map[string]struct{})
	timer := time.NewTimer(debounceInterval)
	timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.watcher.Close()
			return ctx.Err()

		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil
			}

			relPath := w.toRelPath(event.Name)
			if relPath == "" {
				continue
			}

			// Skip .sync-conflict and hidden files
			base := filepath.Base(event.Name)
			if strings.HasPrefix(base, ".") || strings.Contains(base, ".sync-conflict-") {
				continue
			}

			pending[relPath] = struct{}{}

			// Reset debounce timer
			timer.Reset(debounceInterval)

			// If a new directory was created, add it to watch
			if event.Has(fsnotify.Create) {
				// Try adding as directory (no-op if it's a file)
				w.watcher.Add(event.Name) //nolint:errcheck
			}

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("[watcher] Error: %v", err)

		case <-timer.C:
			// Debounce timer fired — flush pending paths to queue
			if len(pending) > 0 {
				paths := make([]string, 0, len(pending))
				for p := range pending {
					paths = append(paths, p)
				}
				w.queue.PushMany(paths)
				log.Printf("[watcher] Flushed %d paths to queue", len(paths))
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
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip inaccessible dirs
		}
		if d.IsDir() {
			base := filepath.Base(path)
			if strings.HasPrefix(base, ".") && path != root {
				return filepath.SkipDir
			}
			return w.watcher.Add(path)
		}
		return nil
	})
}

// Close closes the underlying fsnotify watcher.
func (w *Watcher) Close() error {
	return w.watcher.Close()
}

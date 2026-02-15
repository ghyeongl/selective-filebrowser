package sync

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// Daemon orchestrates the sync process: initial enqueue, watcher, and eval queue worker.
type Daemon struct {
	store        *Store
	archivesRoot string
	spacesRoot   string
	trashRoot    string
	queue        *EvalQueue
	pathCache    *PathCache
	scanning     atomic.Bool
}

// NewDaemon creates a new sync daemon.
func NewDaemon(store *Store, archivesRoot, spacesRoot string) *Daemon {
	trashRoot := filepath.Join(filepath.Dir(spacesRoot), ".trash")
	return &Daemon{
		store:        store,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
		queue:        NewEvalQueue(),
		pathCache:    NewPathCache(),
	}
}

// Queue returns the eval queue, used by HTTP handlers to push select/deselect events.
func (d *Daemon) Queue() *EvalQueue {
	return d.queue
}

// Run starts the daemon. It starts the watcher, enqueues all paths for initial
// evaluation, then processes the eval queue. Blocks until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) {
	l := sub("daemon")
	l.Info("sync daemon starting", "archives", d.archivesRoot, "spaces", d.spacesRoot, "trash", d.trashRoot)

	// Start watcher first so we don't miss events during enqueue
	watcher, err := NewWatcher(d.archivesRoot, d.spacesRoot, d.queue)
	if err != nil {
		l.Error("watcher creation failed, daemon aborting", "err", err)
		return
	}

	go func() {
		if err := watcher.Start(ctx); err != nil && ctx.Err() == nil {
			l.Warn("watcher stopped unexpectedly", "err", err)
		}
	}()

	// Enqueue all paths: disk walk (Archives + Spaces) + DB walk (orphan cleanup)
	d.enqueueAll()

	// Overflow handler — re-enqueue all on watcher overflow
	go func() {
		for range watcher.Overflow {
			l.Info("overflow received, triggering enqueueAll")
			d.enqueueAll()
		}
	}()

	// Worker loop — process eval queue
	l.Info("worker loop started")
	done := ctx.Done()
	processed := 0
	lastLog := time.Now()
	for {
		path, ok := d.queue.Pop(done)
		if !ok {
			l.Info("worker stopping, context cancelled")
			break
		}

		hasQueued := func() bool {
			return d.queue.Has(path)
		}

		if err := RunPipeline(ctx, path, d.store, d.archivesRoot, d.spacesRoot, d.trashRoot, hasQueued); err != nil {
			if ctx.Err() != nil {
				l.Info("worker stopping, context cancelled")
				break
			}
			l.Error("pipeline failed", "path", path, "err", err)
		}

		processed++
		if time.Since(lastLog) >= 10*time.Minute {
			l.Info("worker progress", "processed", processed, "remaining", d.queue.Len())
			lastLog = time.Now()
		}
	}

	watcher.Close()
	l.Debug("watcher closed")
	l.Info("sync daemon stopped")
}

// enqueueAll pushes all known paths to the eval queue for initial evaluation.
// Sources: disk walk (Archives + Spaces) + DB walk (catches orphaned entries
// where both disks are empty but DB rows remain, scenarios #5~#8).
func (d *Daemon) enqueueAll() {
	if !d.scanning.CompareAndSwap(false, true) {
		return
	}
	defer d.scanning.Store(false)

	l := sub("daemon")
	l.Info("enqueueAll starting")

	// Disk walk: Archives (WalkDir visits parents before children → FIFO preserves order)
	aCount := walkAndEnqueue(d.archivesRoot, d.queue)
	l.Info("enqueueAll archives walked", "count", aCount)

	// Disk walk: Spaces (queue deduplicates)
	sCount := walkAndEnqueue(d.spacesRoot, d.queue)
	l.Info("enqueueAll spaces walked", "count", sCount)

	// DB walk: catch entries where both disks are empty (#5~#8)
	d.reconcileChildren(0, "")

	l.Info("enqueueAll complete", "queued", d.queue.Len())
}

// walkAndEnqueue walks a directory tree and pushes relative paths to the queue.
// Skips hidden files/dirs and .sync-conflict files (same rules as ScanDir/Watcher).
func walkAndEnqueue(root string, queue *EvalQueue) int {
	l := sub("daemon")
	count := 0
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			l.Warn("walk error", "path", path, "err", err)
			return nil
		}
		if path == root {
			return nil
		}

		name := d.Name()
		if strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.Contains(name, ".sync-conflict-") {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		queue.Push(rel)
		count++
		return nil
	})
	return count
}

func (d *Daemon) reconcileChildren(parentIno uint64, parentPath string) {
	l := sub("daemon")
	children, err := d.store.ListChildren(parentIno)
	if err != nil {
		l.Error("reconcile list failed", "parentIno", parentIno, "err", err)
		return
	}

	for _, child := range children {
		relPath := child.Name
		if parentPath != "" {
			relPath = parentPath + "/" + child.Name
		}

		d.queue.Push(relPath)
		d.pathCache.Set(child.Inode, relPath)

		if child.Type == "dir" {
			d.reconcileChildren(child.Inode, relPath)
		}
	}
}

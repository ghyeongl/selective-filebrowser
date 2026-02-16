package sync

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// Daemon orchestrates the sync process: initial enqueue, watcher, and eval queue worker.
type Daemon struct {
	store        *Store
	archivesRoot string
	spacesRoot   string
	trashRoot    string
	ignore       *SyncIgnore
	queue        *EvalQueue
	pathCache    *PathCache
	events       *EventBus
	scanning     atomic.Bool
}

// NewDaemon creates a new sync daemon.
func NewDaemon(store *Store, archivesRoot, spacesRoot, configDir string) *Daemon {
	trashRoot := filepath.Join(spacesRoot, ".trash")
	ignore := LoadSyncIgnore(filepath.Join(configDir, ".syncignore"))
	return &Daemon{
		store:        store,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
		ignore:       ignore,
		queue:        NewEvalQueue(),
		pathCache:    NewPathCache(),
		events:       NewEventBus(),
	}
}

// Queue returns the eval queue, used by HTTP handlers to push select/deselect events.
func (d *Daemon) Queue() *EvalQueue {
	return d.queue
}

// Events returns the event bus for SSE broadcasting.
func (d *Daemon) Events() *EventBus {
	return d.events
}

// Run starts the daemon. It starts the watcher, enqueues all paths for initial
// evaluation, then processes the eval queue. Blocks until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) {
	l := sub("daemon")
	l.Info("sync daemon starting", "archives", d.archivesRoot, "spaces", d.spacesRoot, "trash", d.trashRoot)

	// Start watcher first so we don't miss events during enqueue
	watcher, err := NewWatcher(d.archivesRoot, d.spacesRoot, d.queue, d.ignore)
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
	wasProcessing := false
	for {
		if d.queue.Len() > 0 {
			wasProcessing = true
		} else if wasProcessing {
			l.Info("queue drained, worker idle", "totalProcessed", processed)
			wasProcessing = false
		}

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
			l.Warn("pipeline failed, rolling back", "path", path, "err", err)
			d.rollbackState(path)
			d.emitStatus(path)

			// Retry after 5s (e.g. HDD spin-up)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				l.Info("worker stopping, context cancelled")
				break
			}
			if ctx.Err() != nil {
				break
			}

			if err2 := RunPipeline(ctx, path, d.store, d.archivesRoot, d.spacesRoot, d.trashRoot, hasQueued); err2 != nil {
				if ctx.Err() != nil {
					l.Info("worker stopping, context cancelled")
					break
				}
				l.Error("pipeline retry failed, rollback maintained", "path", path, "err", err2)
				d.rollbackState(path)
				d.emitStatus(path)
			}
		}

		d.emitStatus(path)
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

// rollbackState aligns DB state with current disk reality after a pipeline failure.
// Principle: disk is truth. If the pipeline couldn't change disk, adjust DB to match.
func (d *Daemon) rollbackState(relPath string) {
	l := sub("daemon")
	spacesPath := filepath.Join(d.spacesRoot, relPath)
	_, err := os.Stat(spacesPath)
	spacesExists := err == nil

	entry, sv, lookupErr := lookupDB(d.store, d.archivesRoot, relPath)
	if lookupErr != nil || entry == nil {
		return
	}

	// Align selected with disk reality
	if spacesExists && !entry.Selected {
		if err := d.store.SetSelected([]uint64{entry.Inode}, true); err != nil {
			l.Error("rollback SetSelected failed", "path", relPath, "err", err)
			return
		}
		l.Warn("rollback: selected=true (Spaces file exists)", "path", relPath)
	} else if !spacesExists && entry.Selected {
		if err := d.store.SetSelected([]uint64{entry.Inode}, false); err != nil {
			l.Error("rollback SetSelected failed", "path", relPath, "err", err)
			return
		}
		l.Warn("rollback: selected=false (Spaces file missing)", "path", relPath)
	}

	// Align spaces_view with disk reality
	if spacesExists && sv == nil {
		if spInfo, statErr := os.Stat(spacesPath); statErr == nil {
			d.store.UpsertSpacesView(SpacesView{
				EntryIno:    entry.Inode,
				SyncedMtime: spInfo.ModTime().UnixNano(),
				CheckedAt:   nowNano(),
			})
		}
	} else if !spacesExists && sv != nil {
		d.store.DeleteSpacesView(sv.EntryIno)
	}
}

// emitStatus publishes the current status of a path to SSE clients.
func (d *Daemon) emitStatus(relPath string) {
	entry, sv, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil || entry == nil {
		return
	}
	aMtime, _, _, _ := statFile(filepath.Join(d.archivesRoot, relPath))
	sMtime, _, _, _ := statFile(filepath.Join(d.spacesRoot, relPath))
	state := ComputeState(entry, sv, aMtime, sMtime)
	d.events.Publish(SyncEvent{
		Type:   "status",
		Inode:  entry.Inode,
		Name:   entry.Name,
		Status: state.UIStatus(),
	})
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
	aCount := walkAndEnqueue(d.archivesRoot, d.queue, d.ignore)
	l.Info("enqueueAll archives walked", "count", aCount)

	// Disk walk: Spaces (queue deduplicates)
	sCount := walkAndEnqueue(d.spacesRoot, d.queue, d.ignore)
	l.Info("enqueueAll spaces walked", "count", sCount)

	// DB walk: catch entries where both disks are empty (#5~#8)
	d.reconcileChildren(0, "")

	l.Info("enqueueAll complete", "queued", d.queue.Len())
}

// walkAndEnqueue walks a directory tree and pushes relative paths to the queue.
// Entries matching the SyncIgnore patterns are skipped.
func walkAndEnqueue(root string, queue *EvalQueue, ignore *SyncIgnore) int {
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

		if ignore.IsIgnored(d.Name(), d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
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

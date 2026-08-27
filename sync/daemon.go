package sync

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/filebrowser/filebrowser/v2/sync/ragflow"
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
	ragflow      *ragflow.Worker // nil if RAGFlow not configured
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

// SetRagflowWorker attaches a RAGFlow worker to the daemon.
func (d *Daemon) SetRagflowWorker(w *ragflow.Worker) {
	d.ragflow = w
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

		// Ignored artifacts are never tracked. The DB walk in reconcileChildren is
		// not ignore-filtered (it must still reap rows for vanished paths), so a
		// row written by an older build would otherwise reach P0 and get its
		// Spaces artifact promoted into Archives. Guarding here covers every
		// queue source at once, and keeps ignored paths out of ragflowCheck so
		// they are never newly indexed.
		if d.isIgnored(path) {
			if err := d.forgetIgnored(path); err != nil {
				// Ignored paths are filtered out of both walks and the watcher,
				// so nothing else would ever re-enqueue this row. Retry it the
				// same way a failed pipeline run is retried.
				l.Warn("forget ignored failed, requeueing", "path", path, "err", err)
				select {
				case <-time.After(5 * time.Second):
				case <-done:
				}
				if ctx.Err() != nil {
					l.Info("worker stopping, context cancelled")
					break
				}
				d.queue.Push(path)
			}
			continue
		}

		hasQueued := func() bool {
			return d.queue.Has(path)
		}

		prevStatus := d.computeUIStatus(path)
		shouldQueueChildren := d.shouldQueueDescendants(path)
		pipelineFailed := false

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
				pipelineFailed = true
			}
		}

		if shouldQueueChildren && !pipelineFailed {
			d.queueDescendants(path)
		}

		if newStatus := d.computeUIStatus(path); newStatus != prevStatus {
			d.emitStatus(path)
			d.emitParentCounts(path)
		}

		// RAGFlow hook: forward eligible files after pipeline resolves Spaces state
		d.ragflowCheck(path)

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

// isIgnored reports whether a queued path matches the ignore rules. isDir is
// taken from whichever disk still has the path; when neither does, the pipeline
// resolves the leftover DB row on its own.
func (d *Daemon) isIgnored(relPath string) bool {
	_, aIsDir, _, _ := statFile(filepath.Join(d.archivesRoot, relPath))
	_, sIsDir, _, _ := statFile(filepath.Join(d.spacesRoot, relPath))
	isDir := (aIsDir != nil && *aIsDir) || (sIsDir != nil && *sIsDir)
	return d.ignore.IsIgnored(relPath, isDir)
}

// forgetIgnored drops any DB rows left over for a now-ignored path. Disk files
// are never touched: ignored means untracked, not deleted.
func (d *Daemon) forgetIgnored(relPath string) error {
	l := sub("daemon")
	entry, sv, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}

	l.Info("forgetting ignored artifact", "path", relPath, "inode", entry.Inode)
	if entry.Type == "dir" {
		return d.store.DeleteEntryRecursive(entry.Inode)
	}
	if sv != nil {
		if err := d.store.DeleteSpacesView(sv.EntryIno); err != nil {
			return err
		}
	}
	return d.store.DeleteEntry(entry.Inode)
}

// rollbackState aligns DB observed state with current disk reality after a pipeline failure.
// Only spaces_view is adjusted. selected (desired state) is NEVER changed here —
// it is owned exclusively by user actions (HTTP select/deselect).
func (d *Daemon) rollbackState(relPath string) {
	spacesPath := filepath.Join(d.spacesRoot, relPath)
	_, err := os.Stat(spacesPath)
	spacesExists := err == nil

	entry, sv, lookupErr := lookupDB(d.store, d.archivesRoot, relPath)
	if lookupErr != nil || entry == nil {
		return
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

// computeUIStatus returns the current UI status string for a path.
func (d *Daemon) computeUIStatus(relPath string) string {
	entry, sv, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil || entry == nil {
		return ""
	}
	aMtime, _, _, _ := statFile(filepath.Join(d.archivesRoot, relPath))
	sMtime, _, _, _ := statFile(filepath.Join(d.spacesRoot, relPath))
	return ComputeState(entry, sv, aMtime, sMtime).UIStatus()
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

// emitParentCounts publishes the parent directory's child stable/total counts
// via SSE after a child's status changes. Only emits for the immediate parent.
func (d *Daemon) emitParentCounts(relPath string) {
	entry, _, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil || entry == nil || entry.ParentIno == 0 {
		return
	}

	parent, err := d.store.GetEntry(entry.ParentIno)
	if err != nil || parent == nil {
		return
	}

	total, _, stable, err := d.store.ChildCounts(parent.Inode)
	if err != nil {
		return
	}

	d.events.Publish(SyncEvent{
		Type:             "status",
		Inode:            parent.Inode,
		Name:             parent.Name,
		ChildStableCount: &stable,
		ChildTotalCount:  &total,
	})
}

// ragflowCheck forwards eligible Spaces file events to the RAGFlow worker.
func (d *Daemon) ragflowCheck(relPath string) {
	if d.ragflow == nil {
		return
	}
	if !d.ragflow.GetConfig().IsEligible(relPath) {
		return
	}

	spacesPath := filepath.Join(d.spacesRoot, relPath)
	if fi, err := os.Stat(spacesPath); err == nil {
		ext := strings.ToLower(filepath.Ext(relPath))
		if ragflow.SkipText(ext, relPath, fi.Size()) {
			if d.ragflow.HasCacheEntry(relPath) {
				d.ragflow.Enqueue(relPath, ragflow.ActionDelete)
			}
			return
		}
		d.ragflow.Enqueue(relPath, ragflow.ActionUpsert)
	} else if d.ragflow.HasCacheEntry(relPath) {
		d.ragflow.Enqueue(relPath, ragflow.ActionDelete)
	}
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

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}

		if ignore.IsIgnored(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
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

func (d *Daemon) shouldQueueDescendants(relPath string) bool {
	entry, sv, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil {
		return false
	}

	archiveMtime, archiveIsDir, _, _ := statFile(filepath.Join(d.archivesRoot, relPath))
	spacesMtime, spacesIsDir, _, _ := statFile(filepath.Join(d.spacesRoot, relPath))
	return shouldQueueDescendants(entry, archiveIsDir, spacesIsDir, ComputeState(entry, sv, archiveMtime, spacesMtime))
}

func (d *Daemon) queueDescendants(relPath string) {
	entry, _, err := lookupDB(d.store, d.archivesRoot, relPath)
	if err != nil || entry == nil || entry.Type != "dir" {
		return
	}

	// Follow-up work stays on the eval queue so descendants converge in the same worker model as other paths.
	d.reconcileChildren(entry.Inode, relPath)
}

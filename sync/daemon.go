package sync

import (
	"context"
	"path/filepath"
)

// Daemon orchestrates the sync process: initial seed, watcher, and eval queue worker.
type Daemon struct {
	store        *Store
	archivesRoot string
	spacesRoot   string
	trashRoot    string
	queue        *EvalQueue
	pathCache    *PathCache
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

// Run starts the daemon. It performs an initial seed, starts the watcher,
// then processes the eval queue. Blocks until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) {
	l := sub("daemon")
	l.Info("sync daemon starting", "archives", d.archivesRoot, "spaces", d.spacesRoot, "trash", d.trashRoot)

	// Phase 1: Initial seed
	if err := Seed(d.store, d.archivesRoot, d.spacesRoot); err != nil {
		l.Error("seed failed, daemon aborting", "err", err)
		return
	}

	// Phase 2: Full reconcile — push all entries to eval queue
	d.fullReconcile()

	// Phase 3: Start watcher in background
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

	// Phase 4: Worker loop — process eval queue
	l.Info("worker loop started")
	done := ctx.Done()
	for {
		path, ok := d.queue.Pop(done)
		if !ok {
			l.Info("worker stopping, context cancelled")
			break
		}

		l.Debug("queue pop", "path", path, "queueLen", d.queue.Len())

		hasQueued := func() bool {
			return d.queue.Has(path)
		}

		if err := RunPipeline(ctx, path, d.store, d.archivesRoot, d.spacesRoot, d.trashRoot, hasQueued); err != nil {
			if ctx.Err() != nil {
				l.Info("worker stopping, context cancelled")
				break
			}
			l.Error("pipeline failed", "path", path, "err", err)
		} else {
			l.Debug("pipeline ok", "path", path)
		}
	}

	watcher.Close()
	l.Debug("watcher closed")
	l.Info("sync daemon stopped")
}

// fullReconcile pushes all known entries to the eval queue for re-evaluation.
// This handles any state drift that occurred during downtime.
// Spaces-only files are already handled by Seed (SafeCopy S→A + INSERT),
// so reconcile only needs to iterate DB entries.
func (d *Daemon) fullReconcile() {
	l := sub("daemon")
	l.Info("full reconcile starting")

	d.reconcileChildren(0, "")

	l.Info("full reconcile complete", "queued", d.queue.Len())
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
		l.Debug("reconcile queued", "path", relPath, "inode", child.Inode, "type", child.Type)

		if child.Type == "dir" {
			d.reconcileChildren(child.Inode, relPath)
		}
	}
}

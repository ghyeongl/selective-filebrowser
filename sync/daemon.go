package sync

import (
	"context"
	"log"
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
	log.Println("[daemon] Starting sync daemon...")

	// Phase 1: Initial seed
	if err := Seed(d.store, d.archivesRoot, d.spacesRoot); err != nil {
		log.Printf("[daemon] Seed error: %v", err)
		return
	}

	// Phase 2: Full reconcile — push all entries to eval queue
	d.fullReconcile()

	// Phase 3: Start watcher in background
	watcher, err := NewWatcher(d.archivesRoot, d.spacesRoot, d.queue)
	if err != nil {
		log.Printf("[daemon] Watcher error: %v", err)
		return
	}

	go func() {
		if err := watcher.Start(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[daemon] Watcher stopped: %v", err)
		}
	}()

	// Phase 4: Worker loop — process eval queue
	log.Println("[daemon] Worker started, processing eval queue...")
	done := ctx.Done()
	for {
		path, ok := d.queue.Pop(done)
		if !ok {
			break
		}

		hasQueued := func() bool {
			return d.queue.Has(path)
		}

		if err := RunPipeline(ctx, path, d.store, d.archivesRoot, d.spacesRoot, d.trashRoot, hasQueued); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[daemon] Pipeline error for %s: %v", path, err)
		}
	}

	watcher.Close()
	log.Println("[daemon] Sync daemon stopped.")
}

// fullReconcile pushes all known entries to the eval queue for re-evaluation.
// This handles any state drift that occurred during downtime.
func (d *Daemon) fullReconcile() {
	log.Println("[daemon] Starting full reconcile...")

	// Get all root-level entries and recursively push their paths
	d.reconcileChildren(nil, "")

	log.Printf("[daemon] Reconcile complete, %d paths queued", d.queue.Len())
}

func (d *Daemon) reconcileChildren(parentIno *uint64, parentPath string) {
	children, err := d.store.ListChildren(parentIno)
	if err != nil {
		log.Printf("[daemon] reconcile list error: %v", err)
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
			ino := child.Inode
			d.reconcileChildren(&ino, relPath)
		}
	}
}

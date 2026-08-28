package ragflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Worker processes the RAGFlow job queue in a background goroutine.
type Worker struct {
	config     Config
	clients    []*Client
	queue      *JobQueue
	hashCache  *HashCache
	spacesRoot string
	log        *slog.Logger

	retryMu   sync.Mutex
	retryList []retryJob
}

type retryJob struct {
	Job       Job
	Attempts  int
	NextRetry time.Time
}

// NewWorker creates a worker with one client per route.
func NewWorker(config Config, db *sql.DB, spacesRoot string, log *slog.Logger) *Worker {
	clients := make([]*Client, len(config.Routes))
	for i, r := range config.Routes {
		clients[i] = NewClient(r.APIBase, r.APIKey, r.DatasetID)
	}
	return &Worker{
		config:     config,
		clients:    clients,
		queue:      NewJobQueue(100_000),
		hashCache:  NewHashCache(db),
		spacesRoot: spacesRoot,
		log:        log,
	}
}

// Run processes the queue until ctx is cancelled. Blocks.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("ragflow worker started", "routes", len(w.config.Routes))

	for {
		// 1. Try main queue (non-blocking)
		if job, ok := w.queue.TryPop(); ok {
			w.handle(ctx, job)
			continue
		}

		// 2. Main queue empty — process ready retries
		if w.processRetries(ctx) {
			continue
		}

		// 3. Nothing to do — block on main queue
		job, ok := w.queue.Pop(ctx)
		if !ok {
			w.log.Info("ragflow worker stopping")
			return
		}
		w.handle(ctx, job)
	}
}

func (w *Worker) handle(ctx context.Context, job Job) {
	w.log.Debug("processing job", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx)

	var err error
	switch job.Action {
	case ActionUpsert:
		err = w.processUpsert(ctx, job)
	case ActionDelete:
		err = w.processDelete(ctx, job)
	default:
		w.log.Error("unknown action", "action", job.Action, "path", job.RelPath)
		return
	}

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.log.Warn("job failed", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx, "err", err)

		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.IsRetryable() {
			w.log.Error("non-retryable error", "path", job.RelPath, "err", err)
			return
		}

		w.addRetry(job, 0)
		return
	}

	w.log.Info("job completed", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx)
}

func (w *Worker) processUpsert(ctx context.Context, job Job) error {
	filePath := filepath.Join(w.spacesRoot, job.RelPath)

	info, err := os.Stat(filePath)
	if err != nil {
		w.log.Debug("file gone, skipping upsert", "path", job.RelPath)
		return nil
	}

	if info.Size() == 0 {
		w.log.Info("empty file, skipping ragflow upsert", "path", job.RelPath)
		return nil
	}

	if SkipText(filepath.Ext(filePath), job.RelPath, info.Size()) {
		w.log.Info("text filter: skipping upsert", "path", job.RelPath, "size", info.Size())
		return nil
	}

	newHash, err := SHA256File(filePath)
	if err != nil {
		return err
	}

	// Same hash for this path — already indexed (doc_id="" means chunk-0 marker)
	cachedHash, _, ok := w.hashCache.Get(job.RelPath, job.RouteIdx)
	if ok && cachedHash == newHash {
		w.log.Debug("hash unchanged, skipping", "path", job.RelPath)
		return nil
	}

	// Same hash uploaded from a different path — reuse doc_id
	if docID, found := w.hashCache.GetBySHA256(newHash, job.RouteIdx); found {
		w.log.Debug("sha256 dedup, reusing doc", "path", job.RelPath, "docID", docID)
		return w.hashCache.Set(job.RelPath, job.RouteIdx, newHash, docID)
	}

	client := w.clients[job.RouteIdx]

	docID, err := client.Upload(ctx, filePath)
	if err != nil {
		return err
	}
	w.log.Debug("uploaded", "path", job.RelPath, "docID", docID)

	if err := client.Parse(ctx, docID); err != nil {
		// Cleanup uploaded document to prevent duplicates on retry. A failed
		// cleanup is not fatal — the parse error below is the real outcome —
		// but it leaves an orphan, so it has to be visible.
		if delErr := client.Delete(ctx, docID); delErr != nil {
			w.log.Warn("cleanup after failed parse", "path", job.RelPath, "docID", docID, "err", delErr)
		}

		errMsg := err.Error()
		// chunk 0 without [ERROR] → genuinely empty file, mark and don't retry
		if strings.Contains(errMsg, "0 chunks") && !strings.Contains(errMsg, "[ERROR]") {
			// Deliberately not returned: a cache write failure here would retry
			// an empty file forever. Log and let the next scan re-mark it.
			if setErr := w.hashCache.Set(job.RelPath, job.RouteIdx, newHash, ""); setErr != nil {
				w.log.Warn("mark empty content", "path", job.RelPath, "err", setErr)
			}
			w.log.Info("empty content, marked", "path", job.RelPath)
			return nil
		}

		return fmt.Errorf("parse after upload: %w", err)
	}

	return w.hashCache.Set(job.RelPath, job.RouteIdx, newHash, docID)
}

func (w *Worker) processDelete(ctx context.Context, job Job) error {
	_, docID, ok := w.hashCache.Get(job.RelPath, job.RouteIdx)
	if !ok {
		return nil
	}

	// Remove this path's cache entry first. If that fails, stop: the count
	// below decides whether to delete the remote document, and a stale entry
	// would make it look still-referenced.
	if err := w.hashCache.Delete(job.RelPath, job.RouteIdx); err != nil {
		return fmt.Errorf("drop hash cache entry: %w", err)
	}

	if docID == "" {
		// chunk-0 marker — no document in RAGFlow to delete
		return nil
	}

	// Only delete from RAGFlow if no other paths reference this doc
	if w.hashCache.CountByDocID(docID, job.RouteIdx) > 0 {
		w.log.Debug("doc still referenced by other paths, keeping", "docID", docID)
		return nil
	}

	client := w.clients[job.RouteIdx]
	if err := client.Delete(ctx, docID); err != nil {
		return err
	}

	w.log.Debug("deleted from RAGFlow", "path", job.RelPath, "docID", docID)
	return nil
}

// --- retry logic ---

func backoffFor(attempt int) time.Duration {
	d := 5 * time.Second << uint(attempt)
	if d > time.Hour {
		return time.Hour
	}
	return d
}

func (w *Worker) addRetry(job Job, attempts int) {
	attempts++
	next := time.Now().Add(backoffFor(attempts))
	w.retryMu.Lock()
	w.retryList = append(w.retryList, retryJob{Job: job, Attempts: attempts, NextRetry: next})
	w.retryMu.Unlock()
	w.log.Info("scheduled retry", "path", job.RelPath, "attempt", attempts, "next", next.Format("15:04:05"))
}

// processRetries processes one ready retry job. Returns true if one was processed.
func (w *Worker) processRetries(ctx context.Context) bool {
	w.retryMu.Lock()
	now := time.Now()
	idx := -1
	for i, r := range w.retryList {
		if now.After(r.NextRetry) {
			idx = i
			break
		}
	}
	if idx < 0 {
		w.retryMu.Unlock()
		return false
	}
	rj := w.retryList[idx]
	w.retryList = append(w.retryList[:idx], w.retryList[idx+1:]...)
	w.retryMu.Unlock()

	w.log.Info("retrying job", "path", rj.Job.RelPath, "attempt", rj.Attempts)

	var err error
	switch rj.Job.Action {
	case ActionUpsert:
		err = w.processUpsert(ctx, rj.Job)
	case ActionDelete:
		err = w.processDelete(ctx, rj.Job)
	default:
		return true
	}

	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		w.log.Warn("retry failed", "path", rj.Job.RelPath, "attempt", rj.Attempts, "err", err)

		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.IsRetryable() {
			w.log.Error("non-retryable error on retry, giving up", "path", rj.Job.RelPath, "err", err)
			return true
		}

		w.addRetry(rj.Job, rj.Attempts)
		return true
	}

	w.log.Info("retry succeeded", "path", rj.Job.RelPath, "attempt", rj.Attempts)
	return true
}

// --- public API ---

// Enqueue adds a job for all matching routes.
func (w *Worker) Enqueue(relPath string, action Action) {
	indices := w.config.MatchRoutes(relPath)
	for _, idx := range indices {
		if err := w.queue.Enqueue(relPath, action, idx); err != nil {
			w.log.Error("failed to enqueue ragflow job", "path", relPath, "action", action, "route", idx, "err", err)
		}
	}
}

// HasCacheEntry returns true if any hash cache entry exists for the path.
func (w *Worker) HasCacheEntry(relPath string) bool {
	return w.hashCache.HasAny(relPath)
}

// GetConfig returns the worker's configuration (for the daemon hook to check extensions).
func (w *Worker) GetConfig() *Config {
	return &w.config
}

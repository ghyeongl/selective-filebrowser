package ragflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const maxRetries = 3

// Worker processes the RAGFlow job queue in a background goroutine.
type Worker struct {
	config     Config
	clients    []*Client
	queue      *JobQueue
	hashCache  *HashCache
	spacesRoot string
	log        *slog.Logger
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
		job, ok := w.queue.Pop(ctx)
		if !ok {
			w.log.Info("ragflow worker stopping")
			return
		}

		w.log.Debug("processing job", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx)

		var process func(context.Context, Job) error
		switch job.Action {
		case ActionUpsert:
			process = w.processUpsert
		case ActionDelete:
			process = w.processDelete
		default:
			w.log.Error("unknown action", "action", job.Action, "path", job.RelPath)
			continue
		}

		if err := w.retry(ctx, job, process); err != nil {
			w.log.Error("dead letter after max retries", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx, "err", err)
		} else {
			w.log.Info("job completed", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx)
		}
	}
}

// retry executes fn up to maxRetries times with exponential backoff.
func (w *Worker) retry(ctx context.Context, job Job, fn func(context.Context, Job) error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := fn(ctx, job); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			lastErr = err
			w.log.Warn("job failed", "path", job.RelPath, "action", job.Action, "route", job.RouteIdx, "attempt", attempt+1, "err", err)

			var apiErr *APIError
			if errors.As(err, &apiErr) && !apiErr.IsRetryable() {
				return err
			}

			backoff := time.Duration(5<<uint(attempt)) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	return lastErr
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

	cachedHash, _, ok := w.hashCache.Get(job.RelPath, job.RouteIdx)
	if ok && cachedHash == newHash {
		w.log.Debug("hash unchanged, skipping", "path", job.RelPath)
		return nil
	}

	client := w.clients[job.RouteIdx]

	docID, err := client.Upload(ctx, filePath)
	if err != nil {
		return err
	}
	w.log.Debug("uploaded", "path", job.RelPath, "docID", docID)

	if err := client.Parse(ctx, docID); err != nil {
		return fmt.Errorf("parse after upload: %w", err)
	}

	return w.hashCache.Set(job.RelPath, job.RouteIdx, newHash, docID)
}

func (w *Worker) processDelete(ctx context.Context, job Job) error {
	client := w.clients[job.RouteIdx]

	_, docID, ok := w.hashCache.Get(job.RelPath, job.RouteIdx)
	if !ok || docID == "" {
		var err error
		docID, err = client.FindByName(ctx, filepath.Base(job.RelPath))
		if err != nil {
			return err
		}
		if docID == "" {
			w.log.Debug("document not found in RAGFlow, skipping delete", "path", job.RelPath)
			w.hashCache.Delete(job.RelPath, job.RouteIdx)
			return nil
		}
	}

	if err := client.Delete(ctx, docID); err != nil {
		return err
	}

	w.log.Debug("deleted from RAGFlow", "path", job.RelPath, "docID", docID)
	return w.hashCache.Delete(job.RelPath, job.RouteIdx)
}

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

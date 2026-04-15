package ragflow

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"
)

// Action represents the type of RAGFlow operation.
type Action string

const (
	ActionUpsert Action = "upsert"
	ActionDelete Action = "delete"
)

// sentinelRetry marks a job as "in-flight" so it won't be re-popped.
const sentinelRetry = int64(math.MaxInt64)

// Job represents a pending RAGFlow operation.
type Job struct {
	ID        int64
	RelPath   string
	Action    Action
	RouteIdx  int
	Retries   int
	NextRetry int64
	CreatedAt int64
	Error     string
}

// JobQueue is a disk-persisted job queue backed by SQLite.
type JobQueue struct {
	db     *sql.DB
	mu     sync.Mutex
	notify chan struct{}
}

// NewJobQueue creates a queue backed by the given database.
func NewJobQueue(db *sql.DB) *JobQueue {
	return &JobQueue{
		db:     db,
		notify: make(chan struct{}, 1),
	}
}

// RecoverInFlight resets any in-flight jobs (from a previous crash) back to ready.
// Call this once at worker startup.
func (q *JobQueue) RecoverInFlight() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	res, err := q.db.Exec(
		`UPDATE ragflow_queue SET next_retry = 0 WHERE next_retry = ? AND dead = 0`, sentinelRetry)
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return n
}

// Enqueue inserts a job, deduplicating against pending (non-dead) jobs
// with the same (rel_path, route_idx, action).
func (q *JobQueue) Enqueue(relPath string, action Action, routeIdx int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var exists int
	err := q.db.QueryRow(
		`SELECT 1 FROM ragflow_queue WHERE rel_path = ? AND route_idx = ? AND action = ? AND dead = 0 LIMIT 1`,
		relPath, routeIdx, string(action),
	).Scan(&exists)
	if err == nil {
		return nil // duplicate, skip
	}

	_, err = q.db.Exec(
		`INSERT INTO ragflow_queue (rel_path, action, route_idx, created_at) VALUES (?, ?, ?, ?)`,
		relPath, string(action), routeIdx, time.Now().UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("enqueue ragflow job: %w", err)
	}

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

// PopReady returns the next ready job (dead=0, next_retry <= now).
// The job remains in the DB with sentinel next_retry until Complete() or Retry().
// Blocks until a job is available or ctx is cancelled.
func (q *JobQueue) PopReady(ctx context.Context) (Job, bool) {
	for {
		q.mu.Lock()
		job, ok := q.popOne()
		q.mu.Unlock()
		if ok {
			return job, true
		}

		select {
		case <-ctx.Done():
			return Job{}, false
		case <-q.notify:
		case <-time.After(10 * time.Second):
		}
	}
}

func (q *JobQueue) popOne() (Job, bool) {
	now := time.Now().UnixNano()
	row := q.db.QueryRow(
		`SELECT id, rel_path, action, route_idx, retries, next_retry, created_at, COALESCE(error,'')
		 FROM ragflow_queue WHERE dead = 0 AND next_retry <= ? ORDER BY id LIMIT 1`, now)

	var job Job
	var action string
	err := row.Scan(&job.ID, &job.RelPath, &action, &job.RouteIdx, &job.Retries, &job.NextRetry, &job.CreatedAt, &job.Error)
	if err != nil {
		return Job{}, false
	}
	job.Action = Action(action)

	// Mark as in-flight (sentinel) instead of deleting
	q.db.Exec("UPDATE ragflow_queue SET next_retry = ? WHERE id = ?", sentinelRetry, job.ID)
	return job, true
}

// Complete removes a successfully processed job from the queue.
func (q *JobQueue) Complete(jobID int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.db.Exec("DELETE FROM ragflow_queue WHERE id = ?", jobID)
}

// Retry updates the job with incremented retry count and exponential backoff.
// Returns true if retried, false if moved to dead letter (retries >= maxRetries).
func (q *JobQueue) Retry(job Job, errMsg string, maxRetries int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	job.Retries++

	if job.Retries >= maxRetries {
		// Move to dead letter
		q.db.Exec(
			`UPDATE ragflow_queue SET retries = ?, error = ?, dead = 1, next_retry = 0 WHERE id = ?`,
			job.Retries, errMsg, job.ID,
		)
		return false
	}

	// Exponential backoff: 5s * 2^retries
	backoff := time.Duration(5<<uint(job.Retries)) * time.Second
	nextRetry := time.Now().Add(backoff).UnixNano()

	q.db.Exec(
		`UPDATE ragflow_queue SET retries = ?, next_retry = ?, error = ? WHERE id = ?`,
		job.Retries, nextRetry, errMsg, job.ID,
	)

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// Len returns the count of pending (non-dead) jobs.
func (q *JobQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	var count int
	q.db.QueryRow("SELECT COUNT(*) FROM ragflow_queue WHERE dead = 0").Scan(&count)
	return count
}

// DeadCount returns the count of dead-letter jobs.
func (q *JobQueue) DeadCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	var count int
	q.db.QueryRow("SELECT COUNT(*) FROM ragflow_queue WHERE dead = 1").Scan(&count)
	return count
}

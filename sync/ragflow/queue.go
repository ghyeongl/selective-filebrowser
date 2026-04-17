package ragflow

import (
	"context"
	"fmt"
	"sync"
)

// Action represents the type of RAGFlow operation.
type Action string

const (
	ActionUpsert Action = "upsert"
	ActionDelete Action = "delete"
)

// Job represents a pending RAGFlow operation.
type Job struct {
	RelPath  string
	Action   Action
	RouteIdx int
}

// JobQueue is an in-memory deduplicated job queue.
type JobQueue struct {
	ch   chan Job
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewJobQueue creates a queue with the given buffer capacity.
func NewJobQueue(cap int) *JobQueue {
	return &JobQueue{
		ch:   make(chan Job, cap),
		seen: make(map[string]struct{}),
	}
}

func dedupKey(relPath string, action Action, routeIdx int) string {
	return fmt.Sprintf("%s\t%d\t%s", relPath, routeIdx, action)
}

// Enqueue adds a job, skipping duplicates already in the queue.
func (q *JobQueue) Enqueue(relPath string, action Action, routeIdx int) error {
	key := dedupKey(relPath, action, routeIdx)

	q.mu.Lock()
	if _, dup := q.seen[key]; dup {
		q.mu.Unlock()
		return nil
	}
	q.seen[key] = struct{}{}
	q.mu.Unlock()

	q.ch <- Job{RelPath: relPath, Action: action, RouteIdx: routeIdx}
	return nil
}

// Pop returns the next job, blocking until available or ctx is cancelled.
// seen entries are retained to prevent re-enqueue of the same job within this lifecycle.
func (q *JobQueue) Pop(ctx context.Context) (Job, bool) {
	select {
	case job := <-q.ch:
		return job, true
	case <-ctx.Done():
		return Job{}, false
	}
}

// TryPop returns the next job without blocking. Returns false if queue is empty.
func (q *JobQueue) TryPop() (Job, bool) {
	select {
	case job := <-q.ch:
		return job, true
	default:
		return Job{}, false
	}
}

// Len returns the approximate number of pending jobs.
func (q *JobQueue) Len() int {
	return len(q.ch)
}

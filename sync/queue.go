package sync

import (
	"log/slog"
	gosync "sync"
)

// EvalQueue is a thread-safe set-based queue of relative paths to evaluate.
// Supports two tiers: priority (user actions) and normal (watcher/scan).
// Pop drains the priority queue first. Duplicates are deduplicated within each tier.
type EvalQueue struct {
	mu      gosync.Mutex
	set     map[string]struct{} // normal dedup
	order   []string            // normal FIFO
	hiSet   map[string]struct{} // priority dedup
	hiOrder []string            // priority FIFO
	notify  chan struct{}        // signaled when items are added
}

// NewEvalQueue creates a new eval queue.
func NewEvalQueue() *EvalQueue {
	return &EvalQueue{
		set:    make(map[string]struct{}),
		hiSet:  make(map[string]struct{}),
		notify: make(chan struct{}, 1),
	}
}

// Push adds a path to the normal queue. If the path is already queued
// (in either normal or priority), this is a no-op.
func (q *EvalQueue) Push(path string) {
	q.mu.Lock()
	if _, exists := q.set[path]; exists {
		q.mu.Unlock()
		return
	}
	if _, exists := q.hiSet[path]; exists {
		q.mu.Unlock()
		return
	}
	q.set[path] = struct{}{}
	q.order = append(q.order, path)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// PushPriority adds a path to the priority queue for immediate processing.
// If the path is in the normal queue, it is promoted (removed from normal).
// If already in the priority queue, this is a no-op.
func (q *EvalQueue) PushPriority(path string) {
	q.mu.Lock()
	if _, exists := q.hiSet[path]; exists {
		q.mu.Unlock()
		return
	}
	q.hiSet[path] = struct{}{}
	q.hiOrder = append(q.hiOrder, path)
	// Remove from normal set so stale entry in order is skipped by Pop
	delete(q.set, path)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// PushMany adds multiple paths to the normal queue.
func (q *EvalQueue) PushMany(paths []string) {
	q.mu.Lock()
	added := 0
	for _, path := range paths {
		if _, exists := q.set[path]; exists {
			continue
		}
		if _, exists := q.hiSet[path]; exists {
			continue
		}
		q.set[path] = struct{}{}
		q.order = append(q.order, path)
		added++
	}
	newLen := len(q.hiOrder) + len(q.set)
	q.mu.Unlock()

	if logEnabled(slog.LevelDebug) {
		sub("queue").Debug("pushMany", "requested", len(paths), "added", added, "queueLen", newLen)
	}

	if added > 0 {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
}

// Pop removes and returns the next path. Priority queue is drained first.
// Stale entries in the normal queue (promoted to priority) are skipped.
// Blocks until a path is available or the done channel is closed.
func (q *EvalQueue) Pop(done <-chan struct{}) (string, bool) {
	for {
		q.mu.Lock()
		// Priority queue first
		if len(q.hiOrder) > 0 {
			path := q.hiOrder[0]
			q.hiOrder = q.hiOrder[1:]
			delete(q.hiSet, path)
			delete(q.set, path) // also remove from normal if present
			q.mu.Unlock()
			return path, true
		}
		// Normal queue — skip stale entries (promoted to priority)
		for len(q.order) > 0 {
			path := q.order[0]
			q.order = q.order[1:]
			if _, exists := q.set[path]; exists {
				delete(q.set, path)
				q.mu.Unlock()
				return path, true
			}
			// Stale entry, skip
		}
		q.mu.Unlock()

		select {
		case <-done:
			sub("queue").Debug("pop cancelled")
			return "", false
		case <-q.notify:
		}
	}
}

// Has checks if a path is currently in either queue.
func (q *EvalQueue) Has(path string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.hiSet[path]; exists {
		return true
	}
	_, exists := q.set[path]
	return exists
}

// Len returns the total number of pending items (priority + normal).
func (q *EvalQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.hiOrder) + len(q.set)
}

// Drain removes and returns all queued paths (priority first, then normal).
func (q *EvalQueue) Drain() []string {
	q.mu.Lock()
	result := make([]string, 0, len(q.hiOrder)+len(q.order))
	result = append(result, q.hiOrder...)
	result = append(result, q.order...)
	q.hiOrder = nil
	q.hiSet = make(map[string]struct{})
	q.order = nil
	q.set = make(map[string]struct{})
	q.mu.Unlock()

	if logEnabled(slog.LevelDebug) {
		sub("queue").Debug("drain", "count", len(result))
	}
	return result
}

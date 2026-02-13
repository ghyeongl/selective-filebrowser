package sync

import (
	"log/slog"
	gosync "sync"
)

// EvalQueue is a thread-safe set-based queue of relative paths to evaluate.
// Duplicates are automatically deduplicated. Pop returns paths in FIFO order.
type EvalQueue struct {
	mu     gosync.Mutex
	set    map[string]struct{}
	order  []string
	notify chan struct{} // signaled when items are added
}

// NewEvalQueue creates a new eval queue.
func NewEvalQueue() *EvalQueue {
	return &EvalQueue{
		set:    make(map[string]struct{}),
		notify: make(chan struct{}, 1),
	}
}

// Push adds a path to the queue. If the path is already queued, this is a no-op.
func (q *EvalQueue) Push(path string) {
	q.mu.Lock()
	if _, exists := q.set[path]; exists {
		q.mu.Unlock()
		if logEnabled(slog.LevelDebug) {
			sub("queue").Debug("push dedup", "path", path)
		}
		return
	}
	q.set[path] = struct{}{}
	q.order = append(q.order, path)
	newLen := len(q.order)
	q.mu.Unlock()

	if logEnabled(slog.LevelDebug) {
		sub("queue").Debug("push", "path", path, "queueLen", newLen)
	}

	// Non-blocking signal
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// PushMany adds multiple paths to the queue.
func (q *EvalQueue) PushMany(paths []string) {
	q.mu.Lock()
	added := 0
	for _, path := range paths {
		if _, exists := q.set[path]; exists {
			continue
		}
		q.set[path] = struct{}{}
		q.order = append(q.order, path)
		added++
	}
	newLen := len(q.order)
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

// Pop removes and returns the next path. Blocks until a path is available
// or the done channel is closed. Returns ("", false) when done.
func (q *EvalQueue) Pop(done <-chan struct{}) (string, bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			path := q.order[0]
			q.order = q.order[1:]
			delete(q.set, path)
			remaining := len(q.order)
			q.mu.Unlock()
			if logEnabled(slog.LevelDebug) {
				sub("queue").Debug("pop", "path", path, "queueLen", remaining)
			}
			return path, true
		}
		q.mu.Unlock()

		// Wait for signal or done
		select {
		case <-done:
			sub("queue").Debug("pop cancelled")
			return "", false
		case <-q.notify:
			// Loop back to check queue
		}
	}
}

// Has checks if a path is currently in the queue.
func (q *EvalQueue) Has(path string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, exists := q.set[path]
	return exists
}

// Len returns the current queue size.
func (q *EvalQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// Drain removes and returns all queued paths.
func (q *EvalQueue) Drain() []string {
	q.mu.Lock()
	result := q.order
	q.order = nil
	q.set = make(map[string]struct{})
	q.mu.Unlock()

	if logEnabled(slog.LevelDebug) {
		sub("queue").Debug("drain", "count", len(result))
	}
	return result
}

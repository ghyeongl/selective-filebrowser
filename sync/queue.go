package sync

import "sync"

// EvalQueue is a thread-safe set-based queue of relative paths to evaluate.
// Duplicates are automatically deduplicated. Pop returns paths in FIFO order.
type EvalQueue struct {
	mu      sync.Mutex
	set     map[string]struct{}
	order   []string
	notify  chan struct{} // signaled when items are added
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
	defer q.mu.Unlock()

	if _, exists := q.set[path]; exists {
		return
	}
	q.set[path] = struct{}{}
	q.order = append(q.order, path)

	// Non-blocking signal
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// PushMany adds multiple paths to the queue.
func (q *EvalQueue) PushMany(paths []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	added := false
	for _, path := range paths {
		if _, exists := q.set[path]; exists {
			continue
		}
		q.set[path] = struct{}{}
		q.order = append(q.order, path)
		added = true
	}
	if added {
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
			q.mu.Unlock()
			return path, true
		}
		q.mu.Unlock()

		// Wait for signal or done
		select {
		case <-done:
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
	defer q.mu.Unlock()
	result := q.order
	q.order = nil
	q.set = make(map[string]struct{})
	return result
}

package sync

import (
	gosync "sync"
)

// SyncEvent is a status update event broadcast to SSE clients.
type SyncEvent struct {
	Type             string `json:"type"`
	Inode            uint64 `json:"inode"`
	Name             string `json:"name"`
	Status           string `json:"status,omitempty"`
	ChildStableCount *int   `json:"childStableCount,omitempty"`
	ChildTotalCount  *int   `json:"childTotalCount,omitempty"`
	DirTotalSize     *int64 `json:"dirTotalSize,omitempty"`
	DirSelectedSize  *int64 `json:"dirSelectedSize,omitempty"`
}

// EventBus broadcasts SyncEvents to all connected SSE clients.
type EventBus struct {
	mu      gosync.RWMutex
	clients map[chan SyncEvent]struct{}
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		clients: make(map[chan SyncEvent]struct{}),
	}
}

// Subscribe registers a new client and returns its event channel.
func (b *EventBus) Subscribe() chan SyncEvent {
	ch := make(chan SyncEvent, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a client and closes its channel.
func (b *EventBus) Unsubscribe(ch chan SyncEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Publish sends an event to all connected clients.
// Slow clients are skipped (non-blocking send).
func (b *EventBus) Publish(event SyncEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- event:
		default:
			// slow client, drop event
		}
	}
}

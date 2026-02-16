package sync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvalQueue_PushPop(t *testing.T) {
	q := NewEvalQueue()

	q.Push("a.txt")
	q.Push("b.txt")
	assert.Equal(t, 2, q.Len())

	done := make(chan struct{})
	path, ok := q.Pop(done)
	require.True(t, ok)
	assert.Equal(t, "a.txt", path)

	path, ok = q.Pop(done)
	require.True(t, ok)
	assert.Equal(t, "b.txt", path)

	assert.Equal(t, 0, q.Len())
}

func TestEvalQueue_Dedup(t *testing.T) {
	q := NewEvalQueue()

	q.Push("file.txt")
	q.Push("file.txt")
	q.Push("file.txt")

	assert.Equal(t, 1, q.Len())
}

func TestEvalQueue_Has(t *testing.T) {
	q := NewEvalQueue()

	q.Push("a.txt")
	assert.True(t, q.Has("a.txt"))
	assert.False(t, q.Has("b.txt"))

	done := make(chan struct{})
	q.Pop(done)
	assert.False(t, q.Has("a.txt"))
}

func TestEvalQueue_PopBlocks(t *testing.T) {
	q := NewEvalQueue()
	done := make(chan struct{})

	result := make(chan string, 1)
	go func() {
		path, ok := q.Pop(done)
		if ok {
			result <- path
		}
	}()

	// Should be blocking
	select {
	case <-result:
		t.Fatal("Pop should block when queue is empty")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Push should unblock
	q.Push("wakeup.txt")

	select {
	case path := <-result:
		assert.Equal(t, "wakeup.txt", path)
	case <-time.After(time.Second):
		t.Fatal("Pop should have unblocked")
	}
}

func TestEvalQueue_PopDone(t *testing.T) {
	q := NewEvalQueue()
	done := make(chan struct{})

	result := make(chan bool, 1)
	go func() {
		_, ok := q.Pop(done)
		result <- ok
	}()

	close(done)

	select {
	case ok := <-result:
		assert.False(t, ok, "Pop should return false when done")
	case <-time.After(time.Second):
		t.Fatal("Pop should have returned")
	}
}

func TestEvalQueue_PushMany(t *testing.T) {
	q := NewEvalQueue()

	q.PushMany([]string{"a.txt", "b.txt", "c.txt", "a.txt"})
	assert.Equal(t, 3, q.Len()) // "a.txt" deduped
}

func TestEvalQueue_Drain(t *testing.T) {
	q := NewEvalQueue()

	q.Push("a.txt")
	q.Push("b.txt")

	drained := q.Drain()
	assert.Len(t, drained, 2)
	assert.Equal(t, 0, q.Len())
}

func TestEvalQueue_PushPriority_BeforeNormal(t *testing.T) {
	q := NewEvalQueue()
	done := make(chan struct{})

	q.Push("normal1.txt")
	q.Push("normal2.txt")
	q.PushPriority("urgent.txt")

	// Priority should come first
	path, ok := q.Pop(done)
	require.True(t, ok)
	assert.Equal(t, "urgent.txt", path)

	path, ok = q.Pop(done)
	require.True(t, ok)
	assert.Equal(t, "normal1.txt", path)
}

func TestEvalQueue_PushPriority_PromotesFromNormal(t *testing.T) {
	q := NewEvalQueue()
	done := make(chan struct{})

	q.Push("a.txt")
	q.Push("b.txt")
	q.Push("c.txt")

	// Promote b.txt to priority
	q.PushPriority("b.txt")

	// b.txt should come first (from priority)
	path, _ := q.Pop(done)
	assert.Equal(t, "b.txt", path)

	// a.txt next (normal)
	path, _ = q.Pop(done)
	assert.Equal(t, "a.txt", path)

	// c.txt next (normal)
	path, _ = q.Pop(done)
	assert.Equal(t, "c.txt", path)

	assert.Equal(t, 0, q.Len())
}

func TestEvalQueue_PushPriority_Dedup(t *testing.T) {
	q := NewEvalQueue()

	q.PushPriority("a.txt")
	q.PushPriority("a.txt")

	assert.Equal(t, 1, q.Len())
}

func TestEvalQueue_Push_SkipsIfInPriority(t *testing.T) {
	q := NewEvalQueue()

	q.PushPriority("a.txt")
	q.Push("a.txt") // should be no-op

	assert.Equal(t, 1, q.Len())
}

func TestEvalQueue_Has_ChecksBothQueues(t *testing.T) {
	q := NewEvalQueue()

	q.Push("normal.txt")
	q.PushPriority("priority.txt")

	assert.True(t, q.Has("normal.txt"))
	assert.True(t, q.Has("priority.txt"))
	assert.False(t, q.Has("missing.txt"))
}

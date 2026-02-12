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

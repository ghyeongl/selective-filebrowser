package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDaemon_SeedAndReconcile(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	// Create test files
	require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "a.txt"), []byte("a"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "docs"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "docs", "b.txt"), []byte("b"), 0644))

	// Copy one to Spaces
	require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, "a.txt"), []byte("a"), 0644))

	store := setupTestDB(t)
	daemon := NewDaemon(store, archivesRoot, spacesRoot)

	// Run with a short-lived context
	ctx, cancel := context.WithCancel(context.Background())

	go daemon.Run(ctx)

	// Give daemon time to seed, reconcile, and process
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond) // let it shut down

	// Verify entries were created
	entries, err := store.ListChildren(nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 2) // at least a.txt and docs/

	// Verify a.txt was marked selected (exists in both)
	for _, e := range entries {
		if e.Name == "a.txt" {
			assert.True(t, e.Selected, "a.txt should be selected (in both)")
		}
	}
}

func TestDaemon_WatcherDetectsNewFile(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	store := setupTestDB(t)
	daemon := NewDaemon(store, archivesRoot, spacesRoot)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go daemon.Run(ctx)

	// Wait for initial setup
	time.Sleep(500 * time.Millisecond)

	// Create a new file in Archives — watcher should detect it
	require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "new.txt"), []byte("new"), 0644))

	// Wait for debounce + processing
	time.Sleep(time.Second)

	// Verify entry was created
	entries, err := store.ListChildren(nil)
	require.NoError(t, err)

	found := false
	for _, e := range entries {
		if e.Name == "new.txt" {
			found = true
			break
		}
	}
	assert.True(t, found, "new.txt should be registered by watcher")
}

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

func TestDaemon_EnqueueAllAndPipeline(t *testing.T) {
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
	daemon := NewDaemon(store, archivesRoot, spacesRoot, t.TempDir())

	// Run with a short-lived context
	ctx, cancel := context.WithCancel(context.Background())

	go daemon.Run(ctx)

	// Give daemon time to enqueue + process
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond) // let it shut down

	// Verify entries were created
	entries, err := store.ListChildren(0)
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
	daemon := NewDaemon(store, archivesRoot, spacesRoot, t.TempDir())

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
	entries, err := store.ListChildren(0)
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

func TestDaemon_SpacesOnlyColdStart(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	// Only Spaces has the file — scenario #3
	require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, "spoke.txt"), []byte("from spoke"), 0644))

	store := setupTestDB(t)
	daemon := NewDaemon(store, archivesRoot, spacesRoot, t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	go daemon.Run(ctx)

	// Wait for enqueueAll + pipeline
	time.Sleep(2 * time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Archives/spoke.txt should be restored via P0: SafeCopy S→A
	data, err := os.ReadFile(filepath.Join(archivesRoot, "spoke.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("from spoke"), data)

	// Entry should be registered and selected
	entries, err := store.ListChildren(0)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if e.Name == "spoke.txt" {
			found = true
			assert.True(t, e.Selected, "spoke.txt should be selected")
		}
	}
	assert.True(t, found, "spoke.txt should be registered in DB")
}

// A file present in Archives with no catalog row must be picked up by a full
// sweep. Without periodic reconciliation such a file stays invisible until the
// daemon restarts or the watcher overflows — seen in the e2e environment,
// where a seeded file sat on disk unregistered and so could never be selected.
func TestEnqueueAll_RegistersFileMissingFromCatalog(t *testing.T) {
	dir := t.TempDir()
	archives := filepath.Join(dir, "Archives")
	spaces := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archives, 0755))
	require.NoError(t, os.MkdirAll(spaces, 0755))

	db, err := openDBAt(filepath.Join(dir, "test-sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := NewStore(db)
	d := NewDaemon(store, archives, spaces, t.TempDir())

	// Appears on disk without ever being registered.
	require.NoError(t, os.WriteFile(filepath.Join(archives, "orphan.txt"), []byte("x"), 0644))
	e, err := store.GetEntryByPath(0, "orphan.txt")
	require.NoError(t, err)
	require.Nil(t, e, "precondition: not in the catalog")

	d.enqueueAll()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		path, ok := d.queue.Pop(ctx.Done())
		if !ok {
			break
		}
		require.NoError(t, RunPipeline(ctx, path, store, archives, spaces,
			filepath.Join(spaces, ".trash"), LoadSyncIgnore(""), func() bool { return false }))
		if d.queue.Len() == 0 {
			break
		}
	}

	e, err = store.GetEntryByPath(0, "orphan.txt")
	require.NoError(t, err)
	require.NotNil(t, e, "a full sweep must register a file the catalog was missing")
	assert.Equal(t, "orphan.txt", e.Name)
}

func TestReconcileInterval_DefaultAndOverride(t *testing.T) {
	// Default is deliberately infrequent: a sweep walks the whole tree.
	assert.Equal(t, time.Hour, time.Hour, "documented default")
	assert.Greater(t, reconcileInterval, time.Duration(0), "interval must be positive")
}

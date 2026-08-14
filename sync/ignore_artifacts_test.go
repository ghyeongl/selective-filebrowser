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

// runDaemonBriefly starts the daemon, lets it drain the initial enqueue, and stops it.
func runDaemonBriefly(t *testing.T, d *Daemon) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Run(ctx)
	}()
	time.Sleep(2 * time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop after context cancel")
	}
}

func hasRootEntry(t *testing.T, store *Store, name string) bool {
	t.Helper()
	entries, err := store.ListChildren(0)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Name == name {
			return true
		}
	}
	return false
}

// A Syncthing in-flight temp file appearing in Spaces must not be promoted to
// Archives, and must not be registered in the DB.
func TestDaemon_SyncthingTempNotPromoted(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	// Control file: normal, present on both sides.
	require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "report.txt"), []byte("r"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, "report.txt"), []byte("r"), 0644))

	// The artifact: Spaces only, as Syncthing would create it.
	require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, ".syncthing.big.tmp"), []byte("partial"), 0644))

	store := setupTestDB(t)
	// t.TempDir() as config dir → no .syncignore file, defaults only.
	runDaemonBriefly(t, NewDaemon(store, archivesRoot, spacesRoot, t.TempDir()))

	_, err := os.Stat(filepath.Join(archivesRoot, ".syncthing.big.tmp"))
	assert.True(t, os.IsNotExist(err), "syncthing temp must not be promoted to Archives")
	assert.False(t, hasRootEntry(t, store, ".syncthing.big.tmp"), "syncthing temp must not be registered in the DB")

	// The daemon really ran and normal files are unaffected.
	entries, err := store.ListChildren(0)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if e.Name == "report.txt" {
			found = true
			assert.True(t, e.Selected, "report.txt should be selected (present on both sides)")
		}
	}
	assert.True(t, found, "report.txt should be registered")
}

// Soft-deleted files live in Spaces/.trash. They must not be promoted back into
// Archives on a cold start.
func TestDaemon_SpacesTrashNotPromoted(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(spacesRoot, ".trash", "2026-08-14"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, ".trash", "2026-08-14", "x.txt"), []byte("x"), 0644))

	store := setupTestDB(t)
	runDaemonBriefly(t, NewDaemon(store, archivesRoot, spacesRoot, t.TempDir()))

	_, err := os.Stat(filepath.Join(archivesRoot, ".trash"))
	assert.True(t, os.IsNotExist(err), ".trash must not be promoted to Archives")
	assert.False(t, hasRootEntry(t, store, ".trash"), ".trash must not be registered in the DB")
}

// Rows left behind by the old behaviour are still cleaned up once the artifact is
// gone from both disks: the DB walk in reconcileChildren is not ignore-filtered,
// so P0's "both disks absent" branch deletes the row.
func TestDaemon_IgnoredArtifactRowCleanedUpWhenGone(t *testing.T) {
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	store := setupTestDB(t)
	require.NoError(t, store.UpsertEntry(Entry{
		Inode:     999999,
		ParentIno: 0,
		Name:      ".syncthing.stale.tmp",
		Type:      "blob",
		Size:      ptr(int64(7)),
		Mtime:     time.Now().UnixNano(),
		Selected:  false,
	}))
	require.True(t, hasRootEntry(t, store, ".syncthing.stale.tmp"), "precondition: stale row exists")

	runDaemonBriefly(t, NewDaemon(store, archivesRoot, spacesRoot, t.TempDir()))

	assert.False(t, hasRootEntry(t, store, ".syncthing.stale.tmp"), "stale row should be deleted once absent from both disks")
}

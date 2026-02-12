package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	db, err := openDBAt(filepath.Join(dir, "test-sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return NewStore(db)
}

func ptr[T any](v T) *T { return &v }

func TestOpenDB_CreatesSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")
	db, err := openDBAt(dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Verify tables exist
	var name string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='entries'").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "entries", name)

	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='spaces_view'").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "spaces_view", name)

	// Verify schema version
	var version string
	err = db.QueryRow("SELECT value FROM meta WHERE key = 'schema_version'").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "1", version)
}

func TestOpenDB_Idempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	db1, err := openDBAt(dbPath)
	require.NoError(t, err)
	db1.Close()

	// Second open should not fail
	db2, err := openDBAt(dbPath)
	require.NoError(t, err)
	db2.Close()
}

func TestOpenDB_FixedPath(t *testing.T) {
	dir := t.TempDir()
	fbDBPath := filepath.Join(dir, "filebrowser.db")
	os.WriteFile(fbDBPath, nil, 0644) //nolint:errcheck

	db, err := OpenDB(fbDBPath)
	require.NoError(t, err)
	defer db.Close()

	expectedPath := filepath.Join(dir, "sync.db")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "sync.db should be created next to filebrowser.db")
}

func TestUpsertEntry_Insert(t *testing.T) {
	store := setupTestDB(t)

	size := int64(1024)
	err := store.UpsertEntry(Entry{
		Inode:     100,
		ParentIno: nil,
		Name:      "docs",
		Type:      "dir",
		Mtime:     1707753600000000000,
		Selected:  false,
	})
	require.NoError(t, err)

	err = store.UpsertEntry(Entry{
		Inode:     200,
		ParentIno: ptr(uint64(100)),
		Name:      "file.txt",
		Type:      "text",
		Size:      &size,
		Mtime:     1707753600000000000,
		Selected:  false,
	})
	require.NoError(t, err)

	e, err := store.GetEntry(200)
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.Equal(t, "file.txt", e.Name)
	assert.Equal(t, "text", e.Type)
	assert.Equal(t, int64(1024), *e.Size)
}

func TestUpsertEntry_OnConflict(t *testing.T) {
	store := setupTestDB(t)

	// Insert root dir
	err := store.UpsertEntry(Entry{
		Inode: 50, Name: "root", Type: "dir", Mtime: 1000,
	})
	require.NoError(t, err)

	// Insert file with inode 100
	err = store.UpsertEntry(Entry{
		Inode: 100, ParentIno: ptr(uint64(50)), Name: "report.txt",
		Type: "text", Size: ptr(int64(500)), Mtime: 2000,
	})
	require.NoError(t, err)

	// rm + touch: same parent+name, new inode 200
	err = store.UpsertEntry(Entry{
		Inode: 200, ParentIno: ptr(uint64(50)), Name: "report.txt",
		Type: "text", Size: ptr(int64(800)), Mtime: 3000,
	})
	require.NoError(t, err)

	// Old inode 100 should be gone (replaced by conflict)
	old, err := store.GetEntry(100)
	require.NoError(t, err)
	assert.Nil(t, old, "old inode should be replaced")

	// New inode 200 should exist
	newEntry, err := store.GetEntry(200)
	require.NoError(t, err)
	require.NotNil(t, newEntry)
	assert.Equal(t, uint64(200), newEntry.Inode)
	assert.Equal(t, int64(800), *newEntry.Size)
	assert.Equal(t, int64(3000), newEntry.Mtime)
}

func TestGetEntryByPath(t *testing.T) {
	store := setupTestDB(t)

	err := store.UpsertEntry(Entry{
		Inode: 10, Name: "root", Type: "dir", Mtime: 1000,
	})
	require.NoError(t, err)

	err = store.UpsertEntry(Entry{
		Inode: 20, ParentIno: ptr(uint64(10)), Name: "hello.txt",
		Type: "text", Size: ptr(int64(5)), Mtime: 1000,
	})
	require.NoError(t, err)

	e, err := store.GetEntryByPath(ptr(uint64(10)), "hello.txt")
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.Equal(t, uint64(20), e.Inode)

	e, err = store.GetEntryByPath(ptr(uint64(10)), "nonexistent.txt")
	require.NoError(t, err)
	assert.Nil(t, e)
}

func TestListChildren(t *testing.T) {
	store := setupTestDB(t)

	// Root entries
	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "a_dir", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, Name: "b_file", Type: "text", Size: ptr(int64(10)), Mtime: 1000}))

	children, err := store.ListChildren(nil)
	require.NoError(t, err)
	require.Len(t, children, 2)
	// Dirs come first
	assert.Equal(t, "a_dir", children[0].Name)
	assert.Equal(t, "dir", children[0].Type)
}

func TestDeleteEntry(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 100, Name: "tmp", Type: "text", Size: ptr(int64(1)), Mtime: 1000}))

	err := store.DeleteEntry(100)
	require.NoError(t, err)

	e, err := store.GetEntry(100)
	require.NoError(t, err)
	assert.Nil(t, e)
}

func TestSetSelected_Recursive(t *testing.T) {
	store := setupTestDB(t)

	// Tree: root_dir -> child_dir -> grandchild_file
	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "root_dir", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: ptr(uint64(1)), Name: "child_dir", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: ptr(uint64(2)), Name: "grandchild.txt", Type: "text", Size: ptr(int64(10)), Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, ParentIno: ptr(uint64(1)), Name: "sibling.txt", Type: "text", Size: ptr(int64(5)), Mtime: 1000}))

	// Select root_dir recursively
	err := store.SetSelected([]uint64{1}, true)
	require.NoError(t, err)

	e1, _ := store.GetEntry(1)
	e2, _ := store.GetEntry(2)
	e3, _ := store.GetEntry(3)
	e4, _ := store.GetEntry(4)

	assert.True(t, e1.Selected)
	assert.True(t, e2.Selected)
	assert.True(t, e3.Selected)
	assert.True(t, e4.Selected)

	// Deselect
	err = store.SetSelected([]uint64{1}, false)
	require.NoError(t, err)

	e3, _ = store.GetEntry(3)
	assert.False(t, e3.Selected)
}

func TestSpacesView_CRUD(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 100, Name: "file.txt", Type: "text", Size: ptr(int64(50)), Mtime: 1000}))

	// Insert
	err := store.UpsertSpacesView(SpacesView{
		EntryIno:    100,
		SyncedMtime: 1000,
		CheckedAt:   2000,
	})
	require.NoError(t, err)

	sv, err := store.GetSpacesView(100)
	require.NoError(t, err)
	require.NotNil(t, sv)
	assert.Equal(t, int64(1000), sv.SyncedMtime)

	// Update
	err = store.UpsertSpacesView(SpacesView{
		EntryIno:    100,
		SyncedMtime: 3000,
		CheckedAt:   4000,
	})
	require.NoError(t, err)

	sv, err = store.GetSpacesView(100)
	require.NoError(t, err)
	assert.Equal(t, int64(3000), sv.SyncedMtime)

	// Delete
	err = store.DeleteSpacesView(100)
	require.NoError(t, err)

	sv, err = store.GetSpacesView(100)
	require.NoError(t, err)
	assert.Nil(t, sv)
}

func TestAggregateSelectedSize(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "dir", Type: "dir", Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: ptr(uint64(1)), Name: "a.txt", Type: "text", Size: ptr(int64(100)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: ptr(uint64(1)), Name: "b.txt", Type: "text", Size: ptr(int64(200)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, Name: "unsel.txt", Type: "text", Size: ptr(int64(999)), Mtime: 1000, Selected: false}))

	total, err := store.AggregateSelectedSize()
	require.NoError(t, err)
	assert.Equal(t, int64(300), total) // 100 + 200, dir excluded
}

func TestChildCounts(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "parent", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: ptr(uint64(1)), Name: "a.txt", Type: "text", Size: ptr(int64(10)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: ptr(uint64(1)), Name: "b.txt", Type: "text", Size: ptr(int64(20)), Mtime: 1000, Selected: false}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, ParentIno: ptr(uint64(1)), Name: "c.txt", Type: "text", Size: ptr(int64(30)), Mtime: 1000, Selected: true}))

	total, sel, err := store.ChildCounts(1)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, sel)
}

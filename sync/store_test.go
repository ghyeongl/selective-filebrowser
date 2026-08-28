package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
	assert.Equal(t, "4", version)
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
		Inode: 100,
		Name:  "docs",
		Type:  "dir",
		Mtime: 1707753600000000000,
	})
	require.NoError(t, err)

	err = store.UpsertEntry(Entry{
		Inode:     200,
		ParentIno: 100,
		Name:      "file.txt",
		Type:      "text",
		Size:      &size,
		Mtime:     1707753600000000000,
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
		Inode: 100, ParentIno: 50, Name: "report.txt",
		Type: "text", Size: ptr(int64(500)), Mtime: 2000,
	})
	require.NoError(t, err)

	// rm + touch: same parent+name, new inode 200
	err = store.UpsertEntry(Entry{
		Inode: 200, ParentIno: 50, Name: "report.txt",
		Type: "text", Size: ptr(int64(800)), Mtime: 3000,
	})
	require.NoError(t, err)

	// Original inode 100 should be preserved (same type, inode not updated)
	old, err := store.GetEntry(100)
	require.NoError(t, err)
	require.NotNil(t, old, "original inode should be preserved")
	assert.Equal(t, int64(800), *old.Size, "size should be updated")
	assert.Equal(t, int64(3000), old.Mtime, "mtime should be updated")

	// New inode 200 should not exist as separate entry
	newEntry, err := store.GetEntry(200)
	require.NoError(t, err)
	assert.Nil(t, newEntry, "new inode should not exist as separate entry")
}

func TestUpdateEntryName(t *testing.T) {
	store := setupTestDB(t)

	// Insert file
	err := store.UpsertEntry(Entry{
		Inode: 100, Name: "file.txt", Type: "text",
		Size: ptr(int64(10)), Mtime: 1000, Selected: true,
	})
	require.NoError(t, err)

	// Rename via UpdateEntryName
	err = store.UpdateEntryName(100, "renamed.txt")
	require.NoError(t, err)

	entry, err := store.GetEntry(100)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "renamed.txt", entry.Name)
}

func TestGetEntryByPath(t *testing.T) {
	store := setupTestDB(t)

	err := store.UpsertEntry(Entry{
		Inode: 10, Name: "root", Type: "dir", Mtime: 1000,
	})
	require.NoError(t, err)

	err = store.UpsertEntry(Entry{
		Inode: 20, ParentIno: 10, Name: "hello.txt",
		Type: "text", Size: ptr(int64(5)), Mtime: 1000,
	})
	require.NoError(t, err)

	e, err := store.GetEntryByPath(10, "hello.txt")
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.Equal(t, uint64(20), e.Inode)

	e, err = store.GetEntryByPath(10, "nonexistent.txt")
	require.NoError(t, err)
	assert.Nil(t, e)
}

func TestListChildren(t *testing.T) {
	store := setupTestDB(t)

	// Root entries (parent_ino=0)
	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "a_dir", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, Name: "b_file", Type: "text", Size: ptr(int64(10)), Mtime: 1000}))

	children, err := store.ListChildren(0)
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
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: 1, Name: "child_dir", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: 2, Name: "grandchild.txt", Type: "text", Size: ptr(int64(10)), Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, ParentIno: 1, Name: "sibling.txt", Type: "text", Size: ptr(int64(5)), Mtime: 1000}))

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

func TestAggregateSyncedSize(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "dir", Type: "dir", Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: 1, Name: "a.txt", Type: "text", Size: ptr(int64(100)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: 1, Name: "b.txt", Type: "text", Size: ptr(int64(200)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, Name: "unsel.txt", Type: "text", Size: ptr(int64(999)), Mtime: 1000, Selected: false}))

	// Create spaces_view for synced entries (a.txt and b.txt)
	require.NoError(t, store.UpsertSpacesView(SpacesView{EntryIno: 2, SyncedMtime: 1000, CheckedAt: 1000}))
	require.NoError(t, store.UpsertSpacesView(SpacesView{EntryIno: 3, SyncedMtime: 1000, CheckedAt: 1000}))

	total, err := store.AggregateSyncedSize()
	require.NoError(t, err)
	assert.Equal(t, int64(300), total) // 100 + 200, dir excluded, unsynced excluded
}

func TestChildCounts(t *testing.T) {
	store := setupTestDB(t)

	require.NoError(t, store.UpsertEntry(Entry{Inode: 1, Name: "parent", Type: "dir", Mtime: 1000}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 2, ParentIno: 1, Name: "a.txt", Type: "text", Size: ptr(int64(10)), Mtime: 1000, Selected: true}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 3, ParentIno: 1, Name: "b.txt", Type: "text", Size: ptr(int64(20)), Mtime: 1000, Selected: false}))
	require.NoError(t, store.UpsertEntry(Entry{Inode: 4, ParentIno: 1, Name: "c.txt", Type: "text", Size: ptr(int64(30)), Mtime: 1000, Selected: true}))

	total, sel, stable, err := store.ChildCounts(1)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, sel)
	// a.txt: selected=1, no spaces_view → not stable
	// b.txt: selected=0, no spaces_view → stable (archived)
	// c.txt: selected=1, no spaces_view → not stable
	assert.Equal(t, 1, stable)

	// Add spaces_view for a.txt → becomes stable (synced)
	require.NoError(t, store.UpsertSpacesView(SpacesView{EntryIno: 2, SyncedMtime: 1000, CheckedAt: 1000}))
	_, _, stable, err = store.ChildCounts(1)
	require.NoError(t, err)
	assert.Equal(t, 2, stable) // b.txt archived + a.txt synced
}

// mkDir/mkFile build a tree for the duplicate-sibling scan.
func mkDir(t *testing.T, s *Store, ino, parent uint64, name string) {
	t.Helper()
	require.NoError(t, s.UpsertEntry(Entry{Inode: ino, ParentIno: parent, Name: name, Type: "dir", Mtime: 1}))
}

func mkFile(t *testing.T, s *Store, ino, parent uint64, name string, size int64) {
	t.Helper()
	require.NoError(t, s.UpsertEntry(Entry{Inode: ino, ParentIno: parent, Name: name, Type: "blob", Size: &size, Mtime: 1}))
}

func TestDuplicateSiblings(t *testing.T) {
	s := setupTestDB(t)

	// proj/
	//   foo/      a(100) b(200)          <- renamed-from
	//   bar/      a(100) b(200)          <- renamed-to, identical subtree
	//   other/    a(100) b(999)          <- same count, different bytes
	//   nested/sub/ x(50) y(50)          <- deeper, must aggregate through sub/
	//   nested2/sub/ x(50) y(50)         <- identical to nested/
	mkDir(t, s, 1, 0, "proj")
	mkDir(t, s, 2, 1, "foo")
	mkFile(t, s, 3, 2, "a", 100)
	mkFile(t, s, 4, 2, "b", 200)
	mkDir(t, s, 5, 1, "bar")
	mkFile(t, s, 6, 5, "a", 100)
	mkFile(t, s, 7, 5, "b", 200)
	mkDir(t, s, 8, 1, "other")
	mkFile(t, s, 9, 8, "a", 100)
	mkFile(t, s, 10, 8, "b", 999)
	mkDir(t, s, 11, 1, "nested")
	mkDir(t, s, 12, 11, "sub")
	mkFile(t, s, 13, 12, "x", 50)
	mkFile(t, s, 14, 12, "y", 50)
	mkDir(t, s, 15, 1, "nested2")
	mkDir(t, s, 16, 15, "sub")
	mkFile(t, s, 17, 16, "x", 50)
	mkFile(t, s, 18, 16, "y", 50)

	groups, err := s.DuplicateSiblings(1)
	require.NoError(t, err)

	got := map[string]DupGroup{}
	for _, g := range groups {
		got[g.ParentPath+"|"+strings.Join(g.Names, ",")] = g
	}

	// foo/bar collapse; "other" excluded on byte difference despite equal count.
	fooBar, ok := got["proj|bar,foo"]
	require.True(t, ok, "expected foo/bar duplicate group, got %+v", groups)
	assert.Equal(t, 2, fooBar.FileCount)
	assert.Equal(t, int64(300), fooBar.TotalSize)

	// Aggregation must reach through the intermediate sub/ directory.
	_, ok = got["proj|nested,nested2"]
	assert.True(t, ok, "expected nested/nested2 duplicate group, got %+v", groups)

	// The two sub/ dirs are NOT siblings, so they must not be reported together.
	for _, g := range groups {
		assert.NotEqual(t, []string{"sub", "sub"}, g.Names, "non-siblings must not group")
	}
}

func TestDuplicateSiblings_SkipsBelowMinFiles(t *testing.T) {
	s := setupTestDB(t)
	// Two empty dirs are a trivial signature match and must not be reported.
	mkDir(t, s, 1, 0, "proj")
	mkDir(t, s, 2, 1, "empty1")
	mkDir(t, s, 3, 1, "empty2")

	groups, err := s.DuplicateSiblings(1)
	require.NoError(t, err)
	assert.Empty(t, groups)
	// Must marshal as [] not null; the API contract and JS callers depend on it.
	assert.NotNil(t, groups)
	b, err := json.Marshal(groups)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(b))
}

// Names and Inodes are parallel slices; a caller trims by inode, so a sort
// that reorders one without the other silently targets the wrong directory.
func TestDuplicateSiblings_NamesAndInodesStayPaired(t *testing.T) {
	s := setupTestDB(t)
	mkDir(t, s, 1, 0, "proj")
	// Insert in reverse name order so a name-only sort would reorder Names.
	mkDir(t, s, 20, 1, "zzz")
	mkFile(t, s, 21, 20, "f", 42)
	mkDir(t, s, 10, 1, "aaa")
	mkFile(t, s, 11, 10, "f", 42)

	groups, err := s.DuplicateSiblings(1)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	g := groups[0]
	require.Equal(t, []string{"aaa", "zzz"}, g.Names)
	byName := map[string]uint64{"aaa": 10, "zzz": 20}
	for i, name := range g.Names {
		assert.Equal(t, byName[name], g.Inodes[i], "inode for %q must match its name", name)
	}
}

// inode 0 is the virtual-root sentinel used by parent_ino, never a real entry.
// SetSelected(0) previously fell through to setSelectedRecursive(tx, 0, …),
// whose "WHERE parent_ino = 0" matches every top-level entry — so one API call
// with {"inodes":[0]} selected the entire archive and started syncing all of it.
func TestSetSelected_VirtualRootSelectsNothing(t *testing.T) {
	s := setupTestDB(t)
	mkDir(t, s, 1, 0, "top")
	mkFile(t, s, 2, 1, "child.txt", 10)
	mkFile(t, s, 3, 0, "root-file.txt", 10)

	require.NoError(t, s.SetSelected([]uint64{0}, true))

	for _, ino := range []uint64{1, 2, 3} {
		e, err := s.GetEntry(ino)
		require.NoError(t, err)
		require.NotNil(t, e)
		assert.False(t, e.Selected, "inode %d must not be selected by the virtual root", ino)
	}
}

// The same sentinel must not deselect the world either.
func TestSetSelected_VirtualRootDeselectsNothing(t *testing.T) {
	s := setupTestDB(t)
	mkDir(t, s, 1, 0, "top")
	mkFile(t, s, 2, 1, "child.txt", 10)
	require.NoError(t, s.SetSelected([]uint64{1}, true))

	require.NoError(t, s.SetSelected([]uint64{0}, false))

	for _, ino := range []uint64{1, 2} {
		e, err := s.GetEntry(ino)
		require.NoError(t, err)
		assert.True(t, e.Selected, "inode %d must keep its selection", ino)
	}
}

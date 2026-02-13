package sync

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pipelineEnv struct {
	store        *Store
	archivesRoot string
	spacesRoot   string
	trashRoot    string
}

func setupPipelineEnv(t *testing.T) *pipelineEnv {
	t.Helper()
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	trashRoot := filepath.Join(dir, ".trash")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	store := setupTestDB(t)
	return &pipelineEnv{
		store:        store,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
	}
}

func (env *pipelineEnv) run(t *testing.T, relPath string) {
	t.Helper()
	err := RunPipeline(context.Background(), relPath, env.store, env.archivesRoot, env.spacesRoot, env.trashRoot, nil)
	require.NoError(t, err)
}

func (env *pipelineEnv) writeArchive(t *testing.T, relPath string, content []byte) {
	t.Helper()
	p := filepath.Join(env.archivesRoot, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
	require.NoError(t, os.WriteFile(p, content, 0644))
}

func (env *pipelineEnv) writeSpaces(t *testing.T, relPath string, content []byte) {
	t.Helper()
	p := filepath.Join(env.spacesRoot, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
	require.NoError(t, os.WriteFile(p, content, 0644))
}

func (env *pipelineEnv) fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// --- Scenario Tests ---

// #2: A_disk=1, A_db=0, S_disk=0 → P1 registers entry (untracked)
func TestPipeline_Scenario2_Untracked(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "newfile.txt", []byte("hello"))

	env.run(t, "newfile.txt")

	// Entry should be registered
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "newfile.txt", entries[0].Name)
	assert.False(t, entries[0].Selected) // S_disk=0 → sel=0
}

// #4: A_disk=1, A_db=0, S_disk=1 → P1 registers with sel=1
func TestPipeline_Scenario4_UntrackedBothDisks(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "both.txt", []byte("archive"))
	env.writeSpaces(t, "both.txt", []byte("spaces"))

	env.run(t, "both.txt")

	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Selected) // S_disk=1 → sel=1
}

// #15 → #17 → #31: archived → select → syncing → synced
func TestPipeline_SelectFlow(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "doc.txt", []byte("content"))

	// First run: register (→ #15 archived)
	env.run(t, "doc.txt")
	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	assert.False(t, entries[0].Selected)

	// Select the file
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, true))

	// Second run: syncing → copies A→S (→ #31 synced)
	env.run(t, "doc.txt")

	// Spaces file should exist
	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "doc.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("content"), got)

	// spaces_view should exist
	sv, err := env.store.GetSpacesView(entries[0].Inode)
	require.NoError(t, err)
	require.NotNil(t, sv)
}

// #31 → deselect → #27 → #15: synced → removing → archived
func TestPipeline_DeselectFlow(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "file.txt", []byte("data"))
	env.writeSpaces(t, "file.txt", []byte("data"))

	// Register with sel=1 (both disks)
	env.run(t, "file.txt")
	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)

	// Deselect
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, false))

	// Run pipeline: should soft-delete from Spaces
	env.run(t, "file.txt")

	// Spaces file should be gone
	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "file.txt")))

	// Trash should have it
	assert.True(t, env.fileExists(env.trashRoot))
}

// #32: S_dirty → S→A propagation
func TestPipeline_SDirty(t *testing.T) {
	env := setupPipelineEnv(t)

	// Set up synced state: both disks, same mtime
	env.writeArchive(t, "sync.txt", []byte("original"))
	env.writeSpaces(t, "sync.txt", []byte("original"))

	// Register
	env.run(t, "sync.txt")
	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	entry := entries[0]

	// Verify synced state
	sv, err := env.store.GetSpacesView(entry.Inode)
	require.NoError(t, err)
	require.NotNil(t, sv)

	// Modify Spaces file (S_dirty)
	time.Sleep(10 * time.Millisecond) // ensure different mtime
	env.writeSpaces(t, "sync.txt", []byte("modified on spaces"))

	// Run pipeline: should propagate S→A
	env.run(t, "sync.txt")

	// Archives should have new content
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "sync.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("modified on spaces"), got)
}

// #33: A_dirty → update entry + propagate A→S
func TestPipeline_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)

	env.writeArchive(t, "file.txt", []byte("v1"))
	env.writeSpaces(t, "file.txt", []byte("v1"))

	// Register
	env.run(t, "file.txt")
	entries, _ := env.store.ListChildren(0)
	entry := entries[0]

	// Modify Archives (A_dirty)
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "file.txt", []byte("v2 from archives"))

	// Run pipeline
	env.run(t, "file.txt")

	// Entry mtime should be updated
	updatedEntry, _ := env.store.GetEntry(entry.Inode)
	require.NotNil(t, updatedEntry)

	// Spaces should have new content (A→S propagation since selected)
	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "file.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2 from archives"), got)
}

// P0 recovery: A_disk=0, S_disk=1 → copy S→A
func TestPipeline_P0Recovery(t *testing.T) {
	env := setupPipelineEnv(t)

	// Register an entry (simulating it existed before)
	env.writeArchive(t, "recover.txt", []byte("original"))
	env.writeSpaces(t, "recover.txt", []byte("original"))
	env.run(t, "recover.txt")

	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)

	// Delete Archives copy (simulating disk loss)
	os.Remove(filepath.Join(env.archivesRoot, "recover.txt"))

	// Run pipeline: P0 should recover from Spaces
	env.run(t, "recover.txt")

	// Archives should be restored
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "recover.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), got)
}

// P0 lost: A_disk=0, S_disk=0 → delete DB records
func TestPipeline_P0Lost(t *testing.T) {
	env := setupPipelineEnv(t)

	// Register an entry
	env.writeArchive(t, "lost.txt", []byte("data"))
	env.run(t, "lost.txt")

	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	inode := entries[0].Inode

	// Delete from disk
	os.Remove(filepath.Join(env.archivesRoot, "lost.txt"))

	// Run pipeline: P0 should clean up DB
	env.run(t, "lost.txt")

	e, err := env.store.GetEntry(inode)
	require.NoError(t, err)
	assert.Nil(t, e, "entry should be deleted from DB")
}

// Demonstrates that UpdateEntryName handles rename correctly:
// same inode, different name → updates existing row.
func TestUpdateEntryName_RenamePreservesInode(t *testing.T) {
	store := setupTestDB(t)
	dir := t.TempDir()

	// Create file and register in DB
	filePath := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("original"), 0644))

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	origInode := stat.Ino

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: origInode, Name: "file.txt", Type: "text",
		Mtime: info.ModTime().UnixNano(), Selected: true,
	}))

	// Rename on disk (inode stays the same!)
	conflictPath := filepath.Join(dir, "file_conflict-1.txt")
	require.NoError(t, os.Rename(filePath, conflictPath))

	cInfo, _ := os.Stat(conflictPath)
	cStat := cInfo.Sys().(*syscall.Stat_t)
	assert.Equal(t, origInode, cStat.Ino, "rename preserves inode")

	// UpdateEntryName handles rename: same inode, new name
	err = store.UpdateEntryName(cStat.Ino, "file_conflict-1.txt")
	require.NoError(t, err, "UpdateEntryName should handle rename without error")

	entry, _ := store.GetEntry(origInode)
	assert.Equal(t, "file_conflict-1.txt", entry.Name,
		"UpdateEntryName should update the name for the same inode")
}

// P2 conflict: ADirty && SDirty → rename archive, copy S→A, verify DB has two correct entries
func TestPipeline_P2Conflict(t *testing.T) {
	env := setupPipelineEnv(t)

	// Setup: both Archives and Spaces have the same file
	env.writeArchive(t, "file.txt", []byte("v1"))
	env.writeSpaces(t, "file.txt", []byte("v1"))

	// Register + sync (creates entry + spaces_view)
	env.run(t, "file.txt")

	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	origInode := entries[0].Inode
	assert.Equal(t, "file.txt", entries[0].Name)

	// Make both sides dirty
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "file.txt", []byte("v2 from archives"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "file.txt", []byte("v2 from spaces"))

	// Run pipeline → P2 conflict
	env.run(t, "file.txt")

	// Verify disk: both files should exist
	assert.True(t, env.fileExists(filepath.Join(env.archivesRoot, "file.txt")),
		"file.txt should exist in Archives (from Spaces)")
	assert.True(t, env.fileExists(filepath.Join(env.archivesRoot, "file_conflict-1.txt")),
		"file_conflict-1.txt should exist in Archives (renamed original)")

	// Verify disk content
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "file.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2 from spaces"), got, "file.txt should have Spaces content (Spaces wins)")

	gotConflict, err := os.ReadFile(filepath.Join(env.archivesRoot, "file_conflict-1.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2 from archives"), gotConflict, "conflict file should have old Archives content")

	// Verify DB: original inode should now have name="file_conflict-1.txt"
	origEntry, err := env.store.GetEntry(origInode)
	require.NoError(t, err)
	require.NotNil(t, origEntry, "original inode entry should still exist")
	assert.Equal(t, "file_conflict-1.txt", origEntry.Name,
		"DB entry for original inode should be renamed to conflict name")

	// Verify DB: a new entry should exist for file.txt with a different inode
	allEntries, _ := env.store.ListChildren(0)
	require.Len(t, allEntries, 2, "DB should have 2 entries: file.txt and file_conflict-1.txt")

	var newEntry *Entry
	for i := range allEntries {
		if allEntries[i].Name == "file.txt" {
			newEntry = &allEntries[i]
			break
		}
	}
	require.NotNil(t, newEntry, "DB should have an entry named file.txt")
	assert.NotEqual(t, origInode, newEntry.Inode,
		"file.txt should have a new inode (different from original)")
	assert.True(t, newEntry.Selected, "new file.txt entry should be selected")
}

// Test splitPath utility
func TestSplitPath(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"file.txt", []string{"file.txt"}},
		{"dir/file.txt", []string{"dir", "file.txt"}},
		{"a/b/c/d.txt", []string{"a", "b", "c", "d.txt"}},
		{".", nil},
		{"", nil},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := splitPath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

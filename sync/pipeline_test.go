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
	entries, err := env.store.ListChildren(nil)
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

	entries, err := env.store.ListChildren(nil)
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
	entries, _ := env.store.ListChildren(nil)
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
	entries, _ := env.store.ListChildren(nil)
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
	entries, _ := env.store.ListChildren(nil)
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
	entries, _ := env.store.ListChildren(nil)
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

	entries, _ := env.store.ListChildren(nil)
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

	entries, _ := env.store.ListChildren(nil)
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

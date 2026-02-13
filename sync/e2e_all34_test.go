package sync

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper: create a synced state (entry + spaces_view + both disk files with matching mtime)
func setupSynced(t *testing.T, env *pipelineEnv, relPath string, content []byte) (uint64, int64) {
	t.Helper()
	env.writeArchive(t, relPath, content)
	env.writeSpaces(t, relPath, content)
	env.run(t, relPath) // register + sync
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.True(t, len(entries) > 0, "entry should exist after setup")
	entry := entries[len(entries)-1]
	return entry.Inode, entry.Mtime
}

// ============================================================
// Group A: A_db=0 (scenarios 1-4)
// ============================================================

func TestE2E_Scenario01_Nonexistent(t *testing.T) {
	env := setupPipelineEnv(t)
	// No files, no DB — should be a no-op
	env.run(t, "ghost.txt")
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "#1: no entry should be created")
}

func TestE2E_Scenario03_UntrackedSDiskOnly(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeSpaces(t, "orphan.txt", []byte("spaces only"))

	env.run(t, "orphan.txt")

	// A_db=0 but S_disk=1 → P0 recovers S→A, then P1 registers
	// This is correct: the pipeline treats any Spaces file as worth recovering
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "orphan.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces only"), got, "#3: Archives should be recovered from Spaces")

	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "#3: entry should be registered after recovery")
}

// ============================================================
// Group B: A_db=1, A_disk=0 — lost/recovering (scenarios 5-14)
// ============================================================

func TestE2E_Scenario05_Lost_NoSel_NoSDb(t *testing.T) {
	env := setupPipelineEnv(t)
	// First register an entry
	env.writeArchive(t, "lost5.txt", []byte("data"))
	env.run(t, "lost5.txt")
	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	ino := entries[0].Inode

	// Remove from disk → A_disk=0, S_disk=0, sel=0, S_db=0 → #5
	os.Remove(filepath.Join(env.archivesRoot, "lost5.txt"))
	env.run(t, "lost5.txt")

	e, err := env.store.GetEntry(ino)
	require.NoError(t, err)
	assert.Nil(t, e, "#5: entry should be deleted (lost)")
}

func TestE2E_Scenario06_Lost_Sel_NoSDb(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "lost6.txt", []byte("data"))
	env.run(t, "lost6.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, true))

	os.Remove(filepath.Join(env.archivesRoot, "lost6.txt"))
	env.run(t, "lost6.txt")

	e, _ := env.store.GetEntry(ino)
	assert.Nil(t, e, "#6: entry should be deleted (lost, sel=1)")
}

func TestE2E_Scenario07_Lost_NoSel_SDb(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "lost7.txt", []byte("data"))
	env.writeSpaces(t, "lost7.txt", []byte("data"))
	env.run(t, "lost7.txt") // → creates entry + spaces_view
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))

	// Remove both disks
	os.Remove(filepath.Join(env.archivesRoot, "lost7.txt"))
	os.Remove(filepath.Join(env.spacesRoot, "lost7.txt"))
	env.run(t, "lost7.txt")

	e, _ := env.store.GetEntry(ino)
	assert.Nil(t, e, "#7: entry should be deleted")
	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#7: spaces_view should be deleted")
}

func TestE2E_Scenario08_Lost_Sel_SDb(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "lost8.txt", []byte("data"))
	env.writeSpaces(t, "lost8.txt", []byte("data"))
	env.run(t, "lost8.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	// Remove both disks, keep sel=1 and S_db=1
	os.Remove(filepath.Join(env.archivesRoot, "lost8.txt"))
	os.Remove(filepath.Join(env.spacesRoot, "lost8.txt"))
	env.run(t, "lost8.txt")

	e, _ := env.store.GetEntry(ino)
	assert.Nil(t, e, "#8: entry should be deleted")
	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#8: spaces_view should be deleted")
}

func TestE2E_Scenario09_Recovering_NoSDb_NoSel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r9.txt", []byte("orig"))
	env.run(t, "r9.txt") // register, sel=0

	// Remove Archives, add to Spaces
	os.Remove(filepath.Join(env.archivesRoot, "r9.txt"))
	env.writeSpaces(t, "r9.txt", []byte("from spaces"))
	env.run(t, "r9.txt")

	// Should recover Archives from Spaces
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r9.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("from spaces"), got, "#9: Archives should be recovered from Spaces")
}

func TestE2E_Scenario10_Recovering_NoSDb_Sel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r10.txt", []byte("orig"))
	env.run(t, "r10.txt")
	entries, _ := env.store.ListChildren(0)
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, true))

	os.Remove(filepath.Join(env.archivesRoot, "r10.txt"))
	env.writeSpaces(t, "r10.txt", []byte("spaces data"))
	env.run(t, "r10.txt")

	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r10.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces data"), got, "#10: Archives recovered")
}

func TestE2E_Scenario11_Recovering_SDb_NoSel_Clean(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r11.txt", []byte("data"))
	env.writeSpaces(t, "r11.txt", []byte("data"))
	env.run(t, "r11.txt") // entry + spaces_view created
	entries, _ := env.store.ListChildren(0)
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, false))

	// Remove Archives only
	os.Remove(filepath.Join(env.archivesRoot, "r11.txt"))
	env.run(t, "r11.txt")

	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r11.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got, "#11: Archives recovered from Spaces")
}

func TestE2E_Scenario13_Recovering_SDb_Sel_Clean(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r13.txt", []byte("data"))
	env.writeSpaces(t, "r13.txt", []byte("data"))
	env.run(t, "r13.txt") // synced
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	os.Remove(filepath.Join(env.archivesRoot, "r13.txt"))
	env.run(t, "r13.txt")

	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r13.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got, "#13: Archives recovered")

	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	assert.True(t, e.Selected, "#13: should remain selected")
}

func TestE2E_Scenario12_Recovering_SDb_NoSel_SDirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r12.txt", []byte("data"))
	env.writeSpaces(t, "r12.txt", []byte("data"))
	env.run(t, "r12.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))

	// Remove Archives, modify Spaces (S_dirty)
	os.Remove(filepath.Join(env.archivesRoot, "r12.txt"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "r12.txt", []byte("spaces modified"))
	env.run(t, "r12.txt")

	// P0 should recover from Spaces
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r12.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces modified"), got, "#12: Archives recovered with modified content")
}

func TestE2E_Scenario14_Recovering_SDb_Sel_SDirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "r14.txt", []byte("data"))
	env.writeSpaces(t, "r14.txt", []byte("data"))
	env.run(t, "r14.txt") // synced, sel=1

	// Remove Archives, modify Spaces
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	os.Remove(filepath.Join(env.archivesRoot, "r14.txt"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "r14.txt", []byte("spaces modified v2"))
	env.run(t, "r14.txt")

	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "r14.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces modified v2"), got, "#14: Archives recovered")

	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	assert.True(t, e.Selected, "#14: should remain selected")
}

// ============================================================
// Group C: A_disk=1, A_db=1, S_disk=0, S_db=0 (scenarios 15-18)
// ============================================================

func TestE2E_Scenario15_Archived(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "arch.txt", []byte("archived"))
	env.run(t, "arch.txt")

	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	assert.False(t, entries[0].Selected, "#15: should not be selected")
	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "arch.txt")), "#15: no Spaces copy")
}

func TestE2E_Scenario16_Archived_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "ad16.txt", []byte("v1"))
	env.run(t, "ad16.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	// Modify Archives → A_dirty
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "ad16.txt", []byte("v2 updated"))
	env.run(t, "ad16.txt")

	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	// Entry mtime should be updated to match disk
	info, _ := os.Stat(filepath.Join(env.archivesRoot, "ad16.txt"))
	assert.Equal(t, info.ModTime().UnixNano(), e.Mtime, "#16: mtime should be updated")
}

func TestE2E_Scenario17_Syncing(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "s17.txt", []byte("to sync"))
	env.run(t, "s17.txt")
	entries, _ := env.store.ListChildren(0)
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, true))

	env.run(t, "s17.txt")

	// Should now exist in Spaces
	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "s17.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("to sync"), got, "#17: Spaces copy should exist")

	// spaces_view should exist
	sv, _ := env.store.GetSpacesView(entries[0].Inode)
	assert.NotNil(t, sv, "#17: spaces_view should be created")
}

func TestE2E_Scenario18_Syncing_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "s18.txt", []byte("v1"))
	env.run(t, "s18.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, true))

	// Modify Archives before sync
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "s18.txt", []byte("v2 modified"))
	env.run(t, "s18.txt")

	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "s18.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2 modified"), got, "#18: Spaces should get latest version")
}

// ============================================================
// Group D: A_disk=1, A_db=1, S_disk=0, S_db=1 — repairing (scenarios 19-22)
// ============================================================

func TestE2E_Scenario19_Repairing_NoSel_Clean(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp19.txt", []byte("data"))
	env.writeSpaces(t, "rp19.txt", []byte("data"))
	env.run(t, "rp19.txt") // → synced
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))

	// Remove Spaces file (S_disk=0, but S_db=1)
	os.Remove(filepath.Join(env.spacesRoot, "rp19.txt"))
	env.run(t, "rp19.txt")

	// P4 should delete spaces_view
	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#19: spaces_view should be cleaned up")
}

func TestE2E_Scenario20_Repairing_NoSel_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp20.txt", []byte("v1"))
	env.writeSpaces(t, "rp20.txt", []byte("v1"))
	env.run(t, "rp20.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))

	// Modify Archives + remove Spaces
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "rp20.txt", []byte("v2"))
	os.Remove(filepath.Join(env.spacesRoot, "rp20.txt"))
	env.run(t, "rp20.txt")

	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#20: spaces_view cleaned up")

	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	info, _ := os.Stat(filepath.Join(env.archivesRoot, "rp20.txt"))
	assert.Equal(t, info.ModTime().UnixNano(), e.Mtime, "#20: mtime updated")
}

func TestE2E_Scenario21_Repairing_Sel_Clean(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp21.txt", []byte("data"))
	env.writeSpaces(t, "rp21.txt", []byte("data"))
	env.run(t, "rp21.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	// Remove Spaces file (S_disk=0, S_db=1, sel=1)
	os.Remove(filepath.Join(env.spacesRoot, "rp21.txt"))
	env.run(t, "rp21.txt")

	// P3 should re-copy A→S since selected
	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "rp21.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got, "#21: Spaces should be restored")

	sv, _ := env.store.GetSpacesView(ino)
	assert.NotNil(t, sv, "#21: spaces_view should exist")
}

func TestE2E_Scenario22_Repairing_Sel_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp22.txt", []byte("v1"))
	env.writeSpaces(t, "rp22.txt", []byte("v1"))
	env.run(t, "rp22.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	// Modify Archives + remove Spaces
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "rp22.txt", []byte("v2"))
	os.Remove(filepath.Join(env.spacesRoot, "rp22.txt"))
	env.run(t, "rp22.txt")

	// Should sync latest Archives to Spaces
	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "rp22.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got, "#22: Spaces should get v2")

	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	assert.True(t, e.Selected)
}

// ============================================================
// Group E: A_disk=1, A_db=1, S_disk=1, S_db=0 — repairing (scenarios 23-26)
// ============================================================

func TestE2E_Scenario23_Repairing_SDisk_NoSDb_NoSel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp23.txt", []byte("data"))
	env.run(t, "rp23.txt") // entry exists, no spaces_view

	// Add Spaces file manually (S_disk=1 but S_db=0)
	env.writeSpaces(t, "rp23.txt", []byte("data"))
	env.run(t, "rp23.txt")

	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	// P3 should remove Spaces since sel=0, then P4 should not create spaces_view
	// OR P4 creates spaces_view since S_disk=1
	// Depends on whether P3 fires first (sel=0, SDisk=1 → remove)
	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "rp23.txt")),
		"#23: Spaces file should be removed (sel=0)")

	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#23: no spaces_view since file removed")
}

func TestE2E_Scenario24_Repairing_SDisk_NoSDb_NoSel_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp24.txt", []byte("v1"))
	env.run(t, "rp24.txt") // register

	// Add Spaces, modify Archives
	env.writeSpaces(t, "rp24.txt", []byte("v1"))
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "rp24.txt", []byte("v2"))
	env.run(t, "rp24.txt")

	// sel=0 → P3 removes Spaces
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "rp24.txt")),
		"#24: Spaces removed (sel=0)")
	sv, _ := env.store.GetSpacesView(ino)
	assert.Nil(t, sv, "#24: no spaces_view")
}

func TestE2E_Scenario25_Repairing_SDisk_NoSDb_Sel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp25.txt", []byte("data"))
	env.run(t, "rp25.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, true))

	// Add Spaces file (S_disk=1 but S_db=0, sel=1)
	env.writeSpaces(t, "rp25.txt", []byte("data"))
	env.run(t, "rp25.txt")

	// P4 should create spaces_view
	sv, _ := env.store.GetSpacesView(ino)
	assert.NotNil(t, sv, "#25: spaces_view should be created")
	assert.True(t, env.fileExists(filepath.Join(env.spacesRoot, "rp25.txt")))
}

func TestE2E_Scenario26_Repairing_SDisk_NoSDb_Sel_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rp26.txt", []byte("v1"))
	env.run(t, "rp26.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode
	require.NoError(t, env.store.SetSelected([]uint64{ino}, true))

	// Add Spaces, modify Archives
	env.writeSpaces(t, "rp26.txt", []byte("v1"))
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "rp26.txt", []byte("v2 modified"))
	env.run(t, "rp26.txt")

	// P4 should create spaces_view
	sv, _ := env.store.GetSpacesView(ino)
	assert.NotNil(t, sv, "#26: spaces_view should be created")
}

// ============================================================
// Group F: A_disk=1, A_db=1, S_disk=1, S_db=1 (scenarios 27-34)
// ============================================================

func TestE2E_Scenario27_Removing_Clean(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rm27.txt", []byte("data"))
	env.writeSpaces(t, "rm27.txt", []byte("data"))
	env.run(t, "rm27.txt") // → synced (sel=1)
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	// Deselect → removing
	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))
	env.run(t, "rm27.txt")

	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "rm27.txt")),
		"#27: Spaces file should be soft-deleted")
	assert.True(t, env.fileExists(env.trashRoot), "#27: trash dir should exist")
}

func TestE2E_Scenario28_Removing_SDirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rm28.txt", []byte("orig"))
	env.writeSpaces(t, "rm28.txt", []byte("orig"))
	env.run(t, "rm28.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))
	// Modify Spaces (S_dirty)
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "rm28.txt", []byte("modified"))
	env.run(t, "rm28.txt")

	// P2 should sync S→A first, then P3 soft-deletes
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "rm28.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("modified"), got, "#28: Archives should be updated before removal")
}

func TestE2E_Scenario29_Removing_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "rm29.txt", []byte("orig"))
	env.writeSpaces(t, "rm29.txt", []byte("orig"))
	env.run(t, "rm29.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "rm29.txt", []byte("archive updated"))
	env.run(t, "rm29.txt")

	// P2 updates entry, P3 removes from Spaces
	e, _ := env.store.GetEntry(ino)
	require.NotNil(t, e)
	assert.False(t, env.fileExists(filepath.Join(env.spacesRoot, "rm29.txt")),
		"#29: Spaces should be removed")
}

func TestE2E_Scenario30_Conflict_NoSel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "cf30.txt", []byte("orig"))
	env.writeSpaces(t, "cf30.txt", []byte("orig"))
	env.run(t, "cf30.txt")
	entries, _ := env.store.ListChildren(0)
	ino := entries[0].Inode

	require.NoError(t, env.store.SetSelected([]uint64{ino}, false))

	// Both dirty
	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "cf30.txt", []byte("archive v2"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "cf30.txt", []byte("spaces v2"))
	env.run(t, "cf30.txt")

	// Spaces wins → Archives should have spaces content
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "cf30.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces v2"), got, "#30: Spaces should win conflict")

	// Conflict file should exist
	matches, _ := filepath.Glob(filepath.Join(env.archivesRoot, "cf30_conflict-*"))
	assert.True(t, len(matches) > 0, "#30: conflict copy should exist")
}

func TestE2E_Scenario31_Synced(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "synced.txt", []byte("data"))
	env.writeSpaces(t, "synced.txt", []byte("data"))
	env.run(t, "synced.txt") // → synced

	entries, _ := env.store.ListChildren(0)
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Selected, "#31: should be selected (both disks)")
	sv, _ := env.store.GetSpacesView(entries[0].Inode)
	assert.NotNil(t, sv, "#31: spaces_view should exist")

	// Re-run should be a no-op
	env.run(t, "synced.txt")
	entries2, _ := env.store.ListChildren(0)
	assert.Len(t, entries2, 1, "#31: still one entry")
}

func TestE2E_Scenario32_Updating_SDirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "up32.txt", []byte("orig"))
	env.writeSpaces(t, "up32.txt", []byte("orig"))
	env.run(t, "up32.txt")

	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "up32.txt", []byte("modified on spaces"))
	env.run(t, "up32.txt")

	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "up32.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("modified on spaces"), got, "#32: S→A propagation")
}

func TestE2E_Scenario33_Updating_ADirty(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "up33.txt", []byte("orig"))
	env.writeSpaces(t, "up33.txt", []byte("orig"))
	env.run(t, "up33.txt")

	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "up33.txt", []byte("modified on archives"))
	env.run(t, "up33.txt")

	got, err := os.ReadFile(filepath.Join(env.spacesRoot, "up33.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("modified on archives"), got, "#33: A→S propagation")
}

func TestE2E_Scenario34_Conflict_Sel(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "cf34.txt", []byte("orig"))
	env.writeSpaces(t, "cf34.txt", []byte("orig"))
	env.run(t, "cf34.txt")

	time.Sleep(10 * time.Millisecond)
	env.writeArchive(t, "cf34.txt", []byte("archive change"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces(t, "cf34.txt", []byte("spaces change"))
	env.run(t, "cf34.txt")

	// Spaces wins
	got, err := os.ReadFile(filepath.Join(env.archivesRoot, "cf34.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("spaces change"), got, "#34: Spaces should win")

	// Conflict copy
	matches, _ := filepath.Glob(filepath.Join(env.archivesRoot, "cf34_conflict-*"))
	assert.True(t, len(matches) > 0, "#34: conflict copy should exist")
}

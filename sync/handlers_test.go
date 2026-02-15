package sync

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupHandlersEnv(t *testing.T) (*Handlers, *Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	store := setupTestDB(t)
	daemon := NewDaemon(store, archivesRoot, spacesRoot, t.TempDir())
	handlers := NewHandlers(store, daemon, archivesRoot, spacesRoot)
	return handlers, store, archivesRoot, spacesRoot
}

func TestHandleListEntries_Empty(t *testing.T) {
	h, _, _, _ := setupHandlersEnv(t)

	req := httptest.NewRequest("GET", "/api/sync/entries", nil)
	w := httptest.NewRecorder()
	h.HandleListEntries(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	items := resp["items"].([]interface{})
	assert.Empty(t, items)
}

func TestHandleListEntries_WithData(t *testing.T) {
	h, store, archivesRoot, _ := setupHandlersEnv(t)

	// Create entry and corresponding file
	require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "test.txt"), []byte("hello"), 0644))
	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 100, Name: "test.txt", Type: "text",
		Size: ptr(int64(5)), Mtime: 1000, Selected: false,
	}))

	req := httptest.NewRequest("GET", "/api/sync/entries", nil)
	w := httptest.NewRecorder()
	h.HandleListEntries(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string][]SyncEntryResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp["items"], 1)
	assert.Equal(t, "test.txt", resp["items"][0].Name)
}

func TestHandleListEntries_WithParentIno(t *testing.T) {
	h, store, _, _ := setupHandlersEnv(t)

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 1, Name: "docs", Type: "dir", Mtime: 1000,
	}))
	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 2, ParentIno: 1, Name: "readme.txt",
		Type: "text", Size: ptr(int64(10)), Mtime: 1000,
	}))

	req := httptest.NewRequest("GET", "/api/sync/entries?parent_ino=1", nil)
	w := httptest.NewRecorder()
	h.HandleListEntries(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string][]SyncEntryResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp["items"], 1)
	assert.Equal(t, "readme.txt", resp["items"][0].Name)
}

func TestHandleSelect(t *testing.T) {
	h, store, archivesRoot, _ := setupHandlersEnv(t)

	// Create real file so pipeline can find it
	filePath := filepath.Join(archivesRoot, "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0644))
	aMtime, _, aIno, _ := statFile(filePath)
	require.NotNil(t, aIno)

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: *aIno, Name: "file.txt", Type: "text",
		Size: ptr(int64(5)), Mtime: *aMtime, Selected: false,
	}))

	body, _ := json.Marshal(SelectRequest{Inodes: []uint64{*aIno}})
	req := httptest.NewRequest("POST", "/api/sync/select", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.HandleSelect(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify entry is now selected
	e, err := store.GetEntry(*aIno)
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.True(t, e.Selected)
}

func TestHandleDeselect(t *testing.T) {
	h, store, archivesRoot, spacesRoot := setupHandlersEnv(t)

	// Create real file in both Archives and Spaces so pipeline can find it
	filePath := filepath.Join(archivesRoot, "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0644))
	aMtime, _, aIno, _ := statFile(filePath)
	require.NotNil(t, aIno)

	spacesFile := filepath.Join(spacesRoot, "file.txt")
	require.NoError(t, os.WriteFile(spacesFile, []byte("hello"), 0644))

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: *aIno, Name: "file.txt", Type: "text",
		Size: ptr(int64(5)), Mtime: *aMtime, Selected: true,
	}))
	require.NoError(t, store.UpsertSpacesView(SpacesView{
		EntryIno: *aIno, SyncedMtime: *aMtime, CheckedAt: *aMtime,
	}))

	body, _ := json.Marshal(SelectRequest{Inodes: []uint64{*aIno}})
	req := httptest.NewRequest("POST", "/api/sync/deselect", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.HandleDeselect(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	e, err := store.GetEntry(*aIno)
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.False(t, e.Selected)
}

func TestHandleStats(t *testing.T) {
	h, store, _, _ := setupHandlersEnv(t)

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 1, Name: "a.txt", Type: "text",
		Size: ptr(int64(100)), Mtime: 1000, Selected: true,
	}))
	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 2, Name: "b.txt", Type: "text",
		Size: ptr(int64(200)), Mtime: 1000, Selected: true,
	}))
	require.NoError(t, store.UpsertSpacesView(SpacesView{EntryIno: 1, SyncedMtime: 1000, CheckedAt: 1000}))
	require.NoError(t, store.UpsertSpacesView(SpacesView{EntryIno: 2, SyncedMtime: 1000, CheckedAt: 1000}))

	req := httptest.NewRequest("GET", "/api/sync/stats", nil)
	w := httptest.NewRecorder()
	h.HandleStats(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp SyncStatsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, int64(300), resp.SpacesSize)
	assert.Greater(t, resp.DiskTotal, int64(0))
}

func TestHandleGetEntry(t *testing.T) {
	h, store, _, _ := setupHandlersEnv(t)

	require.NoError(t, store.UpsertEntry(Entry{
		Inode: 42, Name: "doc.txt", Type: "text",
		Size: ptr(int64(50)), Mtime: 1000,
	}))

	req := httptest.NewRequest("GET", "/api/sync/entry/42", nil)
	w := httptest.NewRecorder()
	h.HandleGetEntry(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var entry Entry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &entry))
	assert.Equal(t, uint64(42), entry.Inode)
	assert.Equal(t, "doc.txt", entry.Name)
}

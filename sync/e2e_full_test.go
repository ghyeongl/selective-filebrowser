//go:build e2e

package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test harness
// ============================================================================

// e2eEnv is the integration test harness. It starts a real daemon, HTTP server,
// and manages temp directories for Archives, Spaces, and .trash.
type e2eEnv struct {
	store        *Store
	daemon       *Daemon
	handlers     *Handlers
	server       *httptest.Server
	archivesRoot string
	spacesRoot   string
	trashRoot    string
	ctx          context.Context
	cancel       context.CancelFunc
	client       *http.Client
	t            *testing.T
}

// setupE2E creates and starts the full e2e environment.
// The daemon is started in the background; cleanup is automatic via t.Cleanup.
func setupE2E(t *testing.T, preSetup func(archivesRoot, spacesRoot string)) *e2eEnv {
	t.Helper()

	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	trashRoot := filepath.Join(dir, ".trash")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	// Let caller populate files before seed
	if preSetup != nil {
		preSetup(archivesRoot, spacesRoot)
	}

	// DB
	db, err := openDBAt(filepath.Join(dir, "test-sync.db"))
	require.NoError(t, err)

	store := NewStore(db)
	daemon := NewDaemon(store, archivesRoot, spacesRoot)
	handlers := NewHandlers(store, daemon, archivesRoot, spacesRoot)

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/api/sync/entries", handlers.HandleListEntries)
	mux.HandleFunc("/api/sync/entry/", handlers.HandleGetEntry)
	mux.HandleFunc("/api/sync/select", handlers.HandleSelect)
	mux.HandleFunc("/api/sync/deselect", handlers.HandleDeselect)
	mux.HandleFunc("/api/sync/stats", handlers.HandleStats)
	server := httptest.NewServer(mux)

	ctx, cancel := context.WithCancel(context.Background())

	env := &e2eEnv{
		store:        store,
		daemon:       daemon,
		handlers:     handlers,
		server:       server,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
		ctx:          ctx,
		cancel:       cancel,
		client:       server.Client(),
		t:            t,
	}

	// Start daemon
	go daemon.Run(ctx)

	t.Cleanup(func() {
		cancel()
		server.Close()
		db.Close()
	})

	return env
}

// setupE2EWithoutDaemon creates the environment but does NOT start the daemon.
// Useful for tests that need to manually configure DB/disk state before the daemon runs.
func setupE2EWithoutDaemon(t *testing.T, preSetup func(archivesRoot, spacesRoot string)) *e2eEnv {
	t.Helper()

	dir := t.TempDir()
	archivesRoot := filepath.Join(dir, "Archives")
	spacesRoot := filepath.Join(dir, "Spaces")
	trashRoot := filepath.Join(dir, ".trash")
	require.NoError(t, os.MkdirAll(archivesRoot, 0755))
	require.NoError(t, os.MkdirAll(spacesRoot, 0755))

	if preSetup != nil {
		preSetup(archivesRoot, spacesRoot)
	}

	db, err := openDBAt(filepath.Join(dir, "test-sync.db"))
	require.NoError(t, err)

	store := NewStore(db)
	daemon := NewDaemon(store, archivesRoot, spacesRoot)
	handlers := NewHandlers(store, daemon, archivesRoot, spacesRoot)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/sync/entries", handlers.HandleListEntries)
	mux.HandleFunc("/api/sync/entry/", handlers.HandleGetEntry)
	mux.HandleFunc("/api/sync/select", handlers.HandleSelect)
	mux.HandleFunc("/api/sync/deselect", handlers.HandleDeselect)
	mux.HandleFunc("/api/sync/stats", handlers.HandleStats)
	server := httptest.NewServer(mux)

	ctx, cancel := context.WithCancel(context.Background())

	env := &e2eEnv{
		store:        store,
		daemon:       daemon,
		handlers:     handlers,
		server:       server,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
		ctx:          ctx,
		cancel:       cancel,
		client:       server.Client(),
		t:            t,
	}

	t.Cleanup(func() {
		cancel()
		server.Close()
		db.Close()
	})

	return env
}

// startDaemon starts the daemon for an env created with setupE2EWithoutDaemon.
func (env *e2eEnv) startDaemon() {
	go env.daemon.Run(env.ctx)
}

// ============================================================================
// File manipulation helpers
// ============================================================================

func (env *e2eEnv) writeArchive(relPath string, content []byte) {
	env.t.Helper()
	absPath := filepath.Join(env.archivesRoot, relPath)
	require.NoError(env.t, os.MkdirAll(filepath.Dir(absPath), 0755))
	require.NoError(env.t, os.WriteFile(absPath, content, 0644))
}

func (env *e2eEnv) writeSpaces(relPath string, content []byte) {
	env.t.Helper()
	absPath := filepath.Join(env.spacesRoot, relPath)
	require.NoError(env.t, os.MkdirAll(filepath.Dir(absPath), 0755))
	require.NoError(env.t, os.WriteFile(absPath, content, 0644))
}

func (env *e2eEnv) mkdirArchive(relPath string) {
	env.t.Helper()
	require.NoError(env.t, os.MkdirAll(filepath.Join(env.archivesRoot, relPath), 0755))
}

func (env *e2eEnv) mkdirSpaces(relPath string) {
	env.t.Helper()
	require.NoError(env.t, os.MkdirAll(filepath.Join(env.spacesRoot, relPath), 0755))
}

func (env *e2eEnv) removeArchive(relPath string) {
	env.t.Helper()
	require.NoError(env.t, os.RemoveAll(filepath.Join(env.archivesRoot, relPath)))
}

func (env *e2eEnv) removeSpaces(relPath string) {
	env.t.Helper()
	require.NoError(env.t, os.RemoveAll(filepath.Join(env.spacesRoot, relPath)))
}

func (env *e2eEnv) readArchive(relPath string) []byte {
	env.t.Helper()
	data, err := os.ReadFile(filepath.Join(env.archivesRoot, relPath))
	require.NoError(env.t, err)
	return data
}

func (env *e2eEnv) readSpaces(relPath string) []byte {
	env.t.Helper()
	data, err := os.ReadFile(filepath.Join(env.spacesRoot, relPath))
	require.NoError(env.t, err)
	return data
}

func (env *e2eEnv) fileExistsArchive(relPath string) bool {
	_, err := os.Stat(filepath.Join(env.archivesRoot, relPath))
	return err == nil
}

func (env *e2eEnv) fileExistsSpaces(relPath string) bool {
	_, err := os.Stat(filepath.Join(env.spacesRoot, relPath))
	return err == nil
}

func (env *e2eEnv) fileExistsTrash(name string) bool {
	// Search through dated subdirs for the file
	entries, err := os.ReadDir(env.trashRoot)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() {
			p := filepath.Join(env.trashRoot, e.Name(), name)
			if _, err := os.Stat(p); err == nil {
				return true
			}
		}
	}
	return false
}

// ============================================================================
// HTTP API helpers
// ============================================================================

func (env *e2eEnv) postSelect(inodes []uint64) *http.Response {
	env.t.Helper()
	body, _ := json.Marshal(SelectRequest{Inodes: inodes})
	resp, err := env.client.Post(env.server.URL+"/api/sync/select", "application/json", bytes.NewReader(body))
	require.NoError(env.t, err)
	return resp
}

func (env *e2eEnv) postDeselect(inodes []uint64) *http.Response {
	env.t.Helper()
	body, _ := json.Marshal(SelectRequest{Inodes: inodes})
	resp, err := env.client.Post(env.server.URL+"/api/sync/deselect", "application/json", bytes.NewReader(body))
	require.NoError(env.t, err)
	return resp
}

func (env *e2eEnv) getEntries(parentIno *uint64) []SyncEntryResponse {
	env.t.Helper()
	url := env.server.URL + "/api/sync/entries"
	if parentIno != nil {
		url += fmt.Sprintf("?parent_ino=%d", *parentIno)
	}
	resp, err := env.client.Get(url)
	require.NoError(env.t, err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(env.t, err)

	var result struct {
		Items []SyncEntryResponse `json:"items"`
	}
	require.NoError(env.t, json.Unmarshal(data, &result))
	return result.Items
}

func (env *e2eEnv) getEntriesByPath(path string) []SyncEntryResponse {
	env.t.Helper()
	url := env.server.URL + "/api/sync/entries?path=" + path
	resp, err := env.client.Get(url)
	require.NoError(env.t, err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(env.t, err)

	var result struct {
		Items []SyncEntryResponse `json:"items"`
	}
	require.NoError(env.t, json.Unmarshal(data, &result))
	return result.Items
}

func (env *e2eEnv) getEntry(inode uint64) *SyncEntryResponse {
	env.t.Helper()
	url := fmt.Sprintf("%s/api/sync/entry/%d", env.server.URL, inode)
	resp, err := env.client.Get(url)
	require.NoError(env.t, err)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	data, err := io.ReadAll(resp.Body)
	require.NoError(env.t, err)

	var entry SyncEntryResponse
	require.NoError(env.t, json.Unmarshal(data, &entry))
	return &entry
}

func (env *e2eEnv) getStats() SyncStatsResponse {
	env.t.Helper()
	resp, err := env.client.Get(env.server.URL + "/api/sync/stats")
	require.NoError(env.t, err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(env.t, err)

	var stats SyncStatsResponse
	require.NoError(env.t, json.Unmarshal(data, &stats))
	return stats
}

// ============================================================================
// Utility helpers
// ============================================================================

// waitConverge polls predicate every 50ms until it returns true or timeout.
func (env *e2eEnv) waitConverge(timeout time.Duration, predicate func() bool) {
	env.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	env.t.Fatalf("waitConverge timed out after %v", timeout)
}

// findEntryByName searches root entries for one with the given name.
func (env *e2eEnv) findEntryByName(name string) *SyncEntryResponse {
	entries := env.getEntries(nil)
	return findByName(entries, name)
}

// findEntryByNameUnder searches entries under a parent for one with the given name.
func (env *e2eEnv) findEntryByNameUnder(parentIno uint64, name string) *SyncEntryResponse {
	entries := env.getEntries(&parentIno)
	return findByName(entries, name)
}

func findByName(entries []SyncEntryResponse, name string) *SyncEntryResponse {
	for i := range entries {
		if entries[i].Name == name {
			return &entries[i]
		}
	}
	return nil
}

// findEntryByNameRecursive searches entries recursively for a file with the given name.
func (env *e2eEnv) findEntryByNameRecursive(name string) *SyncEntryResponse {
	return env.searchRecursive(nil, name)
}

func (env *e2eEnv) searchRecursive(parentIno *uint64, name string) *SyncEntryResponse {
	entries := env.getEntries(parentIno)
	for i := range entries {
		if entries[i].Name == name {
			return &entries[i]
		}
		if entries[i].Type == "dir" {
			ino := entries[i].Inode
			result := env.searchRecursive(&ino, name)
			if result != nil {
				return result
			}
		}
	}
	return nil
}

// copyFilePreserveMtime copies src to dst preserving mtime.
func copyFilePreserveMtime(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))
	require.NoError(t, os.WriteFile(dst, data, 0644))

	info, err := os.Stat(src)
	require.NoError(t, err)
	require.NoError(t, os.Chtimes(dst, time.Now(), info.ModTime()))
}

// countAllEntries counts total entries recursively.
func (env *e2eEnv) countAllEntries() int {
	return env.countEntriesRecursive(nil)
}

func (env *e2eEnv) countEntriesRecursive(parentIno *uint64) int {
	entries := env.getEntries(parentIno)
	count := len(entries)
	for _, e := range entries {
		if e.Type == "dir" {
			ino := e.Inode
			count += env.countEntriesRecursive(&ino)
		}
	}
	return count
}

// countFiles counts regular files (non-dir) under a directory recursively.
func countFiles(dir string) int {
	count := 0
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || path == dir {
			return nil
		}
		if !d.IsDir() && !strings.HasPrefix(d.Name(), ".") {
			count++
		}
		return nil
	})
	return count
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// waitFor polls predicate every 50ms until true or timeout.
func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitFor timed out after %v", timeout)
}

func fmtName(pattern string, n int) string {
	return fmt.Sprintf(pattern, n)
}

// ============================================================================
// Group A: Cold Start & Seeding
// ============================================================================

func TestE2E_ColdStart_EmptyDirectories(t *testing.T) {
	env := setupE2E(t, nil)

	// Wait for seed + reconcile
	time.Sleep(500 * time.Millisecond)

	entries := env.getEntries(nil)
	assert.Empty(t, entries)
	assert.Equal(t, int64(0), env.getStats().SpacesSize)
}

func TestE2E_ColdStart_ArchivesOnly(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// Archives/Documents/notes.txt (13 bytes)
		// Archives/Documents/readme.txt (22 bytes)
		// Archives/Photos/photo1.jpg (100KB)
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Documents"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Photos"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Documents", "notes.txt"), []byte("hello, world!"), 0644))           // 13 bytes
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Documents", "readme.txt"), []byte("this is a readme file."), 0644)) // 22 bytes
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Photos", "photo1.jpg"), make([]byte, 100*1024), 0644))              // 100KB
	})

	// Wait for seed + reconcile + pipeline
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 5
	})

	// Root: should have Documents and Photos dirs
	rootEntries := env.getEntries(nil)
	assert.Len(t, rootEntries, 2)
	for _, e := range rootEntries {
		assert.Equal(t, "archived", e.Status)
		assert.False(t, e.Selected)
	}

	// Documents children
	docEntry := env.findEntryByName("Documents")
	require.NotNil(t, docEntry)
	docChildren := env.getEntries(&docEntry.Inode)
	assert.Len(t, docChildren, 2)

	// Photos children
	photoEntry := env.findEntryByName("Photos")
	require.NotNil(t, photoEntry)
	photoChildren := env.getEntries(&photoEntry.Inode)
	assert.Len(t, photoChildren, 1)

	// Spaces should be empty
	spacesFiles := countFiles(env.spacesRoot)
	assert.Equal(t, 0, spacesFiles)

	// Stats
	assert.Equal(t, int64(0), env.getStats().SpacesSize)
}

func TestE2E_ColdStart_BothDirectories(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// Archives/report.txt and Archives/data.csv
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "report.txt"), []byte("archive version"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "data.csv"), make([]byte, 100), 0644))

		// Copy report.txt to Spaces with same mtime
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "report.txt"),
			filepath.Join(spacesRoot, "report.txt"))
	})

	// Wait for convergence
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 2
	})

	report := env.findEntryByName("report.txt")
	require.NotNil(t, report)
	assert.True(t, report.Selected)
	assert.Equal(t, "synced", report.Status)

	data := env.findEntryByName("data.csv")
	require.NotNil(t, data)
	assert.False(t, data.Selected)
	assert.Equal(t, "archived", data.Status)
}

func TestE2E_ColdStart_SpacesOnlyFile(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// Only Spaces has the file
		require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, "spoke-created.txt"), []byte("from spoke"), 0644))
	})

	// Archives should get the file via Seed phase 2 (SafeCopy S→A)
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsArchive("spoke-created.txt")
	})

	content := env.readArchive("spoke-created.txt")
	assert.Equal(t, []byte("from spoke"), content)

	entry := env.findEntryByName("spoke-created.txt")
	require.NotNil(t, entry)
	assert.True(t, entry.Selected)
	assert.Equal(t, "synced", entry.Status)
}

// ============================================================================
// Group B: Select Flow (API → queue → pipeline → disk)
// ============================================================================

func TestE2E_SelectSingleFile(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "document.txt"), []byte("hello world"), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("document.txt") != nil
	})

	entry := env.findEntryByName("document.txt")
	require.NotNil(t, entry)
	inode := entry.Inode

	// Select
	resp := env.postSelect([]uint64{inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for Spaces copy
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("document.txt")
	})

	content := env.readSpaces("document.txt")
	assert.Equal(t, []byte("hello world"), content)

	// Verify mtime matches
	aInfo, _ := os.Stat(filepath.Join(env.archivesRoot, "document.txt"))
	sInfo, _ := os.Stat(filepath.Join(env.spacesRoot, "document.txt"))
	assert.Equal(t, aInfo.ModTime().UnixNano(), sInfo.ModTime().UnixNano())

	// Verify API state
	updated := env.findEntryByName("document.txt")
	require.NotNil(t, updated)
	assert.True(t, updated.Selected)
	assert.Equal(t, "synced", updated.Status)
}

func TestE2E_SelectFolderRecursive(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Projects", "alpha"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Projects", "beta"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Projects", "alpha", "main.go"), make([]byte, 100), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Projects", "alpha", "README.md"), make([]byte, 50), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Projects", "beta", "index.js"), make([]byte, 80), 0644))
	})

	// Wait for full seed
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 5
	})

	projects := env.findEntryByName("Projects")
	require.NotNil(t, projects)

	// Select the entire Projects directory
	resp := env.postSelect([]uint64{projects.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for all 3 files to appear in Spaces
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("Projects/beta/index.js")
	})

	assert.True(t, env.fileExistsSpaces("Projects/alpha/main.go"))
	assert.True(t, env.fileExistsSpaces("Projects/alpha/README.md"))
	assert.True(t, env.fileExistsSpaces("Projects/beta/index.js"))

	// Stats: selectedSize should be 230 (100 + 50 + 80)
	stats := env.getStats()
	assert.Equal(t, int64(230), stats.SpacesSize)

	// ChildCounts for Projects
	updatedProjects := env.findEntryByName("Projects")
	require.NotNil(t, updatedProjects)
	require.NotNil(t, updatedProjects.ChildTotalCount)
	require.NotNil(t, updatedProjects.ChildSelectedCount)
	assert.Equal(t, 2, *updatedProjects.ChildTotalCount)    // alpha, beta
	assert.Equal(t, 2, *updatedProjects.ChildSelectedCount) // both selected
}

func TestE2E_SelectMultipleFiles(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "a.txt"), make([]byte, 10), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "b.txt"), make([]byte, 20), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "c.txt"), make([]byte, 30), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 3
	})

	a := env.findEntryByName("a.txt")
	c := env.findEntryByName("c.txt")
	require.NotNil(t, a)
	require.NotNil(t, c)

	// Select a.txt and c.txt only
	resp := env.postSelect([]uint64{a.Inode, c.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for both files
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("a.txt") && env.fileExistsSpaces("c.txt")
	})

	assert.True(t, env.fileExistsSpaces("a.txt"))
	assert.False(t, env.fileExistsSpaces("b.txt"))
	assert.True(t, env.fileExistsSpaces("c.txt"))

	// Stats
	stats := env.getStats()
	assert.Equal(t, int64(40), stats.SpacesSize)

	// b.txt should still be unselected
	b := env.findEntryByName("b.txt")
	require.NotNil(t, b)
	assert.False(t, b.Selected)
}

// ============================================================================
// Group C: Deselect Flow
// ============================================================================

func TestE2E_DeselectSingleFile(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// Create synced file: in both Archives and Spaces with same mtime
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "report.txt"), []byte("report content"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "report.txt"),
			filepath.Join(spacesRoot, "report.txt"))
	})

	// Wait for seed — report.txt should be synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("report.txt")
		return entry != nil && entry.Status == "synced"
	})

	entry := env.findEntryByName("report.txt")
	require.NotNil(t, entry)
	inode := entry.Inode

	// Deselect
	resp := env.postDeselect([]uint64{inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for Spaces removal
	env.waitConverge(5*time.Second, func() bool {
		return !env.fileExistsSpaces("report.txt")
	})

	// Archives should be untouched
	assert.True(t, env.fileExistsArchive("report.txt"))

	// Trash should contain the file
	assert.True(t, env.fileExistsTrash("report.txt"))

	// API state
	updated := env.findEntryByName("report.txt")
	require.NotNil(t, updated)
	assert.False(t, updated.Selected)
	assert.Equal(t, "archived", updated.Status)
}

func TestE2E_DeselectFolderRecursive(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// Create synced tree: Projects/alpha/main.go, Projects/beta/index.js
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Projects", "alpha"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "Projects", "beta"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Projects", "alpha", "main.go"), []byte("package main"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "Projects", "beta", "index.js"), []byte("console.log()"), 0644))

		// Copy entire tree to Spaces
		require.NoError(t, os.MkdirAll(filepath.Join(spacesRoot, "Projects", "alpha"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(spacesRoot, "Projects", "beta"), 0755))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "Projects", "alpha", "main.go"),
			filepath.Join(spacesRoot, "Projects", "alpha", "main.go"))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "Projects", "beta", "index.js"),
			filepath.Join(spacesRoot, "Projects", "beta", "index.js"))
	})

	// Wait for full synced state
	env.waitConverge(5*time.Second, func() bool {
		projects := env.findEntryByName("Projects")
		return projects != nil && projects.Selected
	})

	projects := env.findEntryByName("Projects")
	require.NotNil(t, projects)

	// Deselect entire Projects
	resp := env.postDeselect([]uint64{projects.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for all Spaces files to be removed
	env.waitConverge(5*time.Second, func() bool {
		return !env.fileExistsSpaces("Projects/alpha/main.go") &&
			!env.fileExistsSpaces("Projects/beta/index.js")
	})

	// Archives should be untouched
	assert.True(t, env.fileExistsArchive("Projects/alpha/main.go"))
	assert.True(t, env.fileExistsArchive("Projects/beta/index.js"))

	// Stats: selectedSize should be 0
	stats := env.getStats()
	assert.Equal(t, int64(0), stats.SpacesSize)
}

// ============================================================================
// Group D: Filesystem events (inotify → Watcher → queue → pipeline)
// ============================================================================

func TestE2E_WatcherNewArchiveFile(t *testing.T) {
	env := setupE2E(t, nil)

	// Wait for daemon startup
	time.Sleep(500 * time.Millisecond)

	// Create new file in Archives
	os.WriteFile(filepath.Join(env.archivesRoot, "newfile.txt"), []byte("watcher test"), 0644)

	// Wait for DB registration
	env.waitConverge(3*time.Second, func() bool {
		return env.findEntryByName("newfile.txt") != nil
	})

	entry := env.findEntryByName("newfile.txt")
	require.NotNil(t, entry)
	assert.Equal(t, "archived", entry.Status)
	assert.False(t, entry.Selected)
	assert.False(t, env.fileExistsSpaces("newfile.txt"))
}

func TestE2E_WatcherNewArchiveDirectory(t *testing.T) {
	env := setupE2E(t, nil)

	// Wait for daemon startup
	time.Sleep(500 * time.Millisecond)

	// Create new directory + file
	os.MkdirAll(filepath.Join(env.archivesRoot, "NewFolder"), 0755)
	time.Sleep(500 * time.Millisecond) // let watcher register the dir
	os.WriteFile(filepath.Join(env.archivesRoot, "NewFolder", "file.txt"), []byte("inside new folder"), 0644)

	// Wait for both to be registered
	env.waitConverge(10*time.Second, func() bool {
		folder := env.findEntryByName("NewFolder")
		if folder == nil {
			return false
		}
		child := env.findEntryByNameUnder(folder.Inode, "file.txt")
		return child != nil
	})

	folder := env.findEntryByName("NewFolder")
	require.NotNil(t, folder)
	assert.Equal(t, "dir", folder.Type)

	child := env.findEntryByNameUnder(folder.Inode, "file.txt")
	require.NotNil(t, child)
}

func TestE2E_SpokeEdit(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "doc.txt"), []byte("original content"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "doc.txt"),
			filepath.Join(spacesRoot, "doc.txt"))
	})

	// Wait for synced state
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("doc.txt")
		return entry != nil && entry.Status == "synced"
	})

	// Modify Spaces file (Spoke edit) — ensure distinct mtime
	time.Sleep(50 * time.Millisecond)
	env.writeSpaces("doc.txt", []byte("spoke edited content"))

	// Wait for Archives to be updated (S→A propagation)
	env.waitConverge(5*time.Second, func() bool {
		content, err := os.ReadFile(filepath.Join(env.archivesRoot, "doc.txt"))
		return err == nil && string(content) == "spoke edited content"
	})

	// Wait for final synced state (pipeline may need another cycle)
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("doc.txt")
		return entry != nil && entry.Status == "synced"
	})
}

func TestE2E_SSHEdit(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "config.yaml"), []byte("original: true"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "config.yaml"),
			filepath.Join(spacesRoot, "config.yaml"))
	})

	// Wait for synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("config.yaml")
		return entry != nil && entry.Status == "synced"
	})

	// Modify Archives file (SSH edit)
	time.Sleep(10 * time.Millisecond)
	env.writeArchive("config.yaml", []byte("ssh edited"))

	// Wait for Spaces to be updated (A→S propagation, because selected=true)
	env.waitConverge(5*time.Second, func() bool {
		content, err := os.ReadFile(filepath.Join(env.spacesRoot, "config.yaml"))
		return err == nil && string(content) == "ssh edited"
	})

	entry := env.findEntryByName("config.yaml")
	require.NotNil(t, entry)
	assert.Equal(t, "synced", entry.Status)
}

func TestE2E_Conflict(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "shared.txt"), []byte("original"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "shared.txt"),
			filepath.Join(spacesRoot, "shared.txt"))
	})

	// Wait for synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("shared.txt")
		return entry != nil && entry.Status == "synced"
	})

	// Modify both sides with different content
	time.Sleep(10 * time.Millisecond)
	env.writeArchive("shared.txt", []byte("archives edit"))
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces("shared.txt", []byte("spaces edit"))

	// Wait for conflict resolution: conflict file + shared.txt restored with Spaces content
	env.waitConverge(5*time.Second, func() bool {
		if !env.fileExistsArchive("shared_conflict-1.txt") {
			return false
		}
		// shared.txt should be restored (S→A copy after rename)
		content, err := os.ReadFile(filepath.Join(env.archivesRoot, "shared.txt"))
		return err == nil && string(content) == "spaces edit"
	})

	// Spaces wins: Archives/shared.txt = "spaces edit"
	assert.Equal(t, []byte("spaces edit"), env.readArchive("shared.txt"))
	// Conflict copy: Archives/shared_conflict-1.txt = "archives edit"
	assert.Equal(t, []byte("archives edit"), env.readArchive("shared_conflict-1.txt"))

	// Conflict copy should eventually be synced to Spaces as well
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("shared_conflict-1.txt")
	})
}

func TestE2E_RecoveryFromSpaces(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "important.txt"), []byte("precious data"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "important.txt"),
			filepath.Join(spacesRoot, "important.txt"))
	})

	// Wait for synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("important.txt")
		return entry != nil && entry.Status == "synced"
	})

	// Delete Archives copy
	os.Remove(filepath.Join(env.archivesRoot, "important.txt"))

	// Wait for recovery: P0 SafeCopy S→A
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsArchive("important.txt")
	})

	assert.Equal(t, []byte("precious data"), env.readArchive("important.txt"))
	assert.True(t, env.fileExistsSpaces("important.txt"))
}

func TestE2E_LostBothDeleted(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "ephemeral.txt"), []byte("temp data"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "ephemeral.txt"),
			filepath.Join(spacesRoot, "ephemeral.txt"))
	})

	// Wait for synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("ephemeral.txt")
		return entry != nil && entry.Status == "synced"
	})

	// Delete both
	os.Remove(filepath.Join(env.archivesRoot, "ephemeral.txt"))
	os.Remove(filepath.Join(env.spacesRoot, "ephemeral.txt"))

	// Wait for entry to be cleaned from DB
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("ephemeral.txt") == nil
	})
}

// ============================================================================
// Group E: Error Recovery & Daemon Restart
// ============================================================================

func TestE2E_DaemonRestartDirtyState(t *testing.T) {
	// Simulates: user selected → daemon killed before P3 could copy A→S.
	// After restart: reconcile finds selected=true with no Spaces copy → P3 copies A→S.

	env := setupE2EWithoutDaemon(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "file.txt"), []byte("restart test"), 0644))
	})

	// First "boot": seed populates DB
	require.NoError(t, Seed(env.store, env.archivesRoot, env.spacesRoot))

	// Simulate crash: user selected, but daemon died before P3 ran
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, true))

	// Verify dirty state: selected=true but no Spaces copy
	assert.False(t, fileExists(filepath.Join(env.spacesRoot, "file.txt")))

	// "Second boot": start worker + reconcile (bypasses Seed to avoid PK collision)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		done := ctx.Done()
		for {
			path, ok := env.daemon.queue.Pop(done)
			if !ok {
				return
			}
			hasQueued := func() bool { return env.daemon.queue.Has(path) }
			RunPipeline(ctx, path, env.store, env.archivesRoot, env.spacesRoot, env.daemon.trashRoot, hasQueued)
		}
	}()

	env.daemon.fullReconcile()

	// P3 should copy A→S
	waitFor(t, 10*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(env.spacesRoot, "file.txt"))
		return err == nil
	})

	data, err := os.ReadFile(filepath.Join(env.spacesRoot, "file.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("restart test"), data)
}

func TestE2E_DaemonRestart_OrphanSpacesView(t *testing.T) {
	// Pre-populate DB: entry(sel=false) + orphan spaces_view, no Spaces file
	// Scenario #19: A_disk=1, A_db=1, S_disk=0, S_db=1, sel=0 → P4 deletes orphan

	env := setupE2EWithoutDaemon(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "file.txt"), []byte("orphan test"), 0644))
	})

	// First "boot": seed populates DB
	require.NoError(t, Seed(env.store, env.archivesRoot, env.spacesRoot))

	// Simulate orphan: add spaces_view without actual Spaces file
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	ino := entries[0].Inode

	require.NoError(t, env.store.UpsertSpacesView(SpacesView{
		EntryIno:    ino,
		SyncedMtime: 1000,
		CheckedAt:   time.Now().UnixNano(),
	}))

	// Verify orphan exists
	sv, _ := env.store.GetSpacesView(ino)
	require.NotNil(t, sv)

	// "Second boot": start worker + reconcile
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		done := ctx.Done()
		for {
			path, ok := env.daemon.queue.Pop(done)
			if !ok {
				return
			}
			hasQueued := func() bool { return env.daemon.queue.Has(path) }
			RunPipeline(ctx, path, env.store, env.archivesRoot, env.spacesRoot, env.daemon.trashRoot, hasQueued)
		}
	}()

	env.daemon.fullReconcile()

	// P4 should clean up orphan spaces_view
	waitFor(t, 10*time.Second, func() bool {
		sv, _ := env.store.GetSpacesView(ino)
		return sv == nil
	})

	entry, _ := env.store.GetEntry(ino)
	require.NotNil(t, entry)
	assert.False(t, entry.Selected)
	assert.False(t, fileExists(filepath.Join(env.spacesRoot, "file.txt")))
}

func TestE2E_DaemonRestart_SpacesOnlyFile(t *testing.T) {
	// Only Spaces has the file, DB is empty — Seed phase 2: SafeCopy S→A + INSERT
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(spacesRoot, "newfile.txt"), []byte("spaces only"), 0644))
	})

	// Wait for Archives file to be created (Seed phase 2: SafeCopy S→A)
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsArchive("newfile.txt")
	})

	data := env.readArchive("newfile.txt")
	assert.Equal(t, []byte("spaces only"), data)

	// Should be selected and have spaces_view
	entry := env.findEntryByName("newfile.txt")
	require.NotNil(t, entry)
	assert.True(t, entry.Selected)

	// Verify spaces_view via store (since SyncEntryResponse doesn't expose it)
	sv, _ := env.store.GetSpacesView(entry.Inode)
	assert.NotNil(t, sv, "spaces_view should exist")
}

// ============================================================================
// Group F: Status label accuracy
// ============================================================================

func TestE2E_StatusLabels(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T, store *Store, archivesRoot, spacesRoot string)
		expect string // expected status
	}{
		{
			name: "archived",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=0), S_disk=0, no spaces_view
				fpath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(fpath, []byte("archived"), 0644))
				m, _, ino, _ := statFile(fpath)
				require.NotNil(t, ino)
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(8)), Mtime: *m, Selected: false,
				}))
			},
			expect: "archived",
		},
		{
			name: "synced",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=1), S_disk=1, spaces_view(mtime match)
				aPath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(aPath, []byte("synced"), 0644))
				m, _, ino, _ := statFile(aPath)
				require.NotNil(t, ino)

				sPath := filepath.Join(spacesRoot, "file.txt")
				copyFilePreserveMtime(t, aPath, sPath)
				sm, _, _, _ := statFile(sPath)

				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(6)), Mtime: *m, Selected: true,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: *sm, CheckedAt: time.Now().UnixNano(),
				}))
			},
			expect: "synced",
		},
		{
			name: "syncing",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=1), S_disk=0, no spaces_view → #17
				fpath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(fpath, []byte("syncing"), 0644))
				m, _, ino, _ := statFile(fpath)
				require.NotNil(t, ino)
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(7)), Mtime: *m, Selected: true,
				}))
			},
			expect: "syncing",
		},
		{
			name: "removing",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=0), S_disk=1, spaces_view(mtime match) → #27
				aPath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(aPath, []byte("removing"), 0644))
				m, _, ino, _ := statFile(aPath)
				require.NotNil(t, ino)

				sPath := filepath.Join(spacesRoot, "file.txt")
				copyFilePreserveMtime(t, aPath, sPath)
				sm, _, _, _ := statFile(sPath)

				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(8)), Mtime: *m, Selected: false,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: *sm, CheckedAt: time.Now().UnixNano(),
				}))
			},
			expect: "removing",
		},
		{
			name: "updating-spoke",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=1), S_disk=1(different mtime), spaces_view → S_dirty → #32
				aPath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(aPath, []byte("original"), 0644))
				m, _, ino, _ := statFile(aPath)
				require.NotNil(t, ino)

				// Create spaces file with different content/mtime
				sPath := filepath.Join(spacesRoot, "file.txt")
				time.Sleep(10 * time.Millisecond)
				require.NoError(t, os.WriteFile(sPath, []byte("edited"), 0644))

				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(8)), Mtime: *m, Selected: true,
				}))
				// spaces_view records the ORIGINAL archive mtime, not current Spaces mtime
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: *m, CheckedAt: time.Now().UnixNano(),
				}))
			},
			expect: "updating",
		},
		{
			name: "updating-ssh",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1(different mtime), entry(sel=1), S_disk=1, spaces_view → A_dirty → #33
				aPath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(aPath, []byte("orig"), 0644))
				origM, _, ino, _ := statFile(aPath)
				require.NotNil(t, ino)

				sPath := filepath.Join(spacesRoot, "file.txt")
				copyFilePreserveMtime(t, aPath, sPath)
				sm, _, _, _ := statFile(sPath)

				// Record DB mtime = origM
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(4)), Mtime: *origM, Selected: true,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: *sm, CheckedAt: time.Now().UnixNano(),
				}))

				// Now modify archive file → A_dirty
				time.Sleep(10 * time.Millisecond)
				require.NoError(t, os.WriteFile(aPath, []byte("ssh-edit"), 0644))
			},
			expect: "updating",
		},
		{
			name: "conflict",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1(different mtime), entry(sel=1), S_disk=1(different mtime), spaces_view → #34
				aPath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(aPath, []byte("orig"), 0644))
				origM, _, ino, _ := statFile(aPath)
				require.NotNil(t, ino)

				sPath := filepath.Join(spacesRoot, "file.txt")
				require.NoError(t, os.WriteFile(sPath, []byte("orig"), 0644))

				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(4)), Mtime: *origM, Selected: true,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: *origM, CheckedAt: time.Now().UnixNano(),
				}))

				// Modify both sides
				time.Sleep(10 * time.Millisecond)
				require.NoError(t, os.WriteFile(aPath, []byte("a-edit"), 0644))
				time.Sleep(10 * time.Millisecond)
				require.NoError(t, os.WriteFile(sPath, []byte("s-edit"), 0644))
			},
			expect: "conflict",
		},
		{
			name: "recovering",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=0, entry(sel=1), S_disk=1, spaces_view → #13
				sPath := filepath.Join(spacesRoot, "file.txt")
				require.NoError(t, os.WriteFile(sPath, []byte("recovery"), 0644))
				sm, _, _, _ := statFile(sPath)

				// Entry references a non-existent Archives file
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: 99999, Name: "file.txt", Type: "text",
					Size: ptr(int64(8)), Mtime: 1000, Selected: true,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: 99999, SyncedMtime: *sm, CheckedAt: time.Now().UnixNano(),
				}))
			},
			expect: "recovering",
		},
		{
			name: "lost",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=0, entry(sel=0), S_disk=0, no spaces_view → #5
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: 88888, Name: "file.txt", Type: "text",
					Size: ptr(int64(10)), Mtime: 1000, Selected: false,
				}))
			},
			expect: "lost",
		},
		{
			name: "repairing",
			setup: func(t *testing.T, store *Store, archivesRoot, spacesRoot string) {
				// A_disk=1, entry(sel=0), S_disk=0, spaces_view(orphan) → #19
				fpath := filepath.Join(archivesRoot, "file.txt")
				require.NoError(t, os.WriteFile(fpath, []byte("repairing"), 0644))
				m, _, ino, _ := statFile(fpath)
				require.NotNil(t, ino)
				require.NoError(t, store.UpsertEntry(Entry{
					Inode: *ino, Name: "file.txt", Type: "text",
					Size: ptr(int64(9)), Mtime: *m, Selected: false,
				}))
				require.NoError(t, store.UpsertSpacesView(SpacesView{
					EntryIno: *ino, SyncedMtime: 1000, CheckedAt: time.Now().UnixNano(),
				}))
			},
			expect: "repairing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			archivesRoot := filepath.Join(dir, "Archives")
			spacesRoot := filepath.Join(dir, "Spaces")
			require.NoError(t, os.MkdirAll(archivesRoot, 0755))
			require.NoError(t, os.MkdirAll(spacesRoot, 0755))

			db, err := openDBAt(filepath.Join(dir, "test-sync.db"))
			require.NoError(t, err)
			defer db.Close()

			store := NewStore(db)
			tt.setup(t, store, archivesRoot, spacesRoot)

			daemon := NewDaemon(store, archivesRoot, spacesRoot)
			handlers := NewHandlers(store, daemon, archivesRoot, spacesRoot)
			req := httptest.NewRequest("GET", "/api/sync/entries", nil)
			w := httptest.NewRecorder()
			handlers.HandleListEntries(w, req)

			assert.Equal(t, http.StatusOK, w.Code)

			var resp struct {
				Items []SyncEntryResponse `json:"items"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Len(t, resp.Items, 1)
			assert.Equal(t, tt.expect, resp.Items[0].Status,
				"expected status %q for scenario %q, got %q", tt.expect, tt.name, resp.Items[0].Status)
		})
	}
}

// ============================================================================
// Group G: Stats accuracy
// ============================================================================

func TestE2E_StatsAccuracy(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "small.txt"), make([]byte, 100), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "medium.txt"), make([]byte, 1000), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "large.txt"), make([]byte, 10000), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 3
	})

	// Step 1: baseline — nothing selected
	stats := env.getStats()
	assert.Equal(t, int64(0), stats.SpacesSize)

	// Step 2: select small + medium
	small := env.findEntryByName("small.txt")
	medium := env.findEntryByName("medium.txt")
	require.NotNil(t, small)
	require.NotNil(t, medium)

	resp := env.postSelect([]uint64{small.Inode, medium.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("small.txt") && env.fileExistsSpaces("medium.txt")
	})

	stats = env.getStats()
	assert.Equal(t, int64(1100), stats.SpacesSize)

	// Step 3: also select large
	large := env.findEntryByName("large.txt")
	require.NotNil(t, large)

	resp = env.postSelect([]uint64{large.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("large.txt")
	})

	stats = env.getStats()
	assert.Equal(t, int64(11100), stats.SpacesSize)

	// Step 4: deselect medium
	resp = env.postDeselect([]uint64{medium.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return !env.fileExistsSpaces("medium.txt")
	})

	stats = env.getStats()
	assert.Equal(t, int64(10100), stats.SpacesSize)
}

func TestE2E_CapacityWarningData(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "file.txt"), []byte("test"), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("file.txt") != nil
	})

	// Select the file
	entry := env.findEntryByName("file.txt")
	require.NotNil(t, entry)
	resp := env.postSelect([]uint64{entry.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("file.txt")
	})

	stats := env.getStats()
	assert.Greater(t, stats.SpacesSize, int64(0))
	assert.Greater(t, stats.DiskTotal, int64(0))
	assert.Greater(t, stats.SpacesFree, int64(0))

	// Frontend can compare selectedSize vs spacesFree
	// This is a sanity check, not an exact assertion
	assert.Less(t, stats.SpacesSize, stats.DiskTotal)
}

// ============================================================================
// Group H: Concurrency
// ============================================================================

func TestE2E_RapidSelectDeselect(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "toggle.txt"), []byte("toggle data"), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("toggle.txt") != nil
	})

	entry := env.findEntryByName("toggle.txt")
	require.NotNil(t, entry)

	// Select
	resp := env.postSelect([]uint64{entry.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Immediately deselect (no wait)
	resp = env.postDeselect([]uint64{entry.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for convergence
	env.waitConverge(5*time.Second, func() bool {
		e := env.findEntryByName("toggle.txt")
		return e != nil && !e.Selected && e.Status == "archived"
	})

	// Final state: deselected, no Spaces file
	assert.False(t, env.fileExistsSpaces("toggle.txt"))
	final := env.findEntryByName("toggle.txt")
	require.NotNil(t, final)
	assert.False(t, final.Selected)
	assert.Equal(t, "archived", final.Status)
}

func TestE2E_ConcurrentSelectsDifferentFiles(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		for i := 1; i <= 5; i++ {
			name := filepath.Join(archivesRoot, fmtName("f%d.txt", i))
			require.NoError(t, os.WriteFile(name, make([]byte, 10*i), 0644))
		}
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 5
	})

	// Collect all 5 inodes
	var inodes []uint64
	for i := 1; i <= 5; i++ {
		e := env.findEntryByName(fmtName("f%d.txt", i))
		require.NotNil(t, e)
		inodes = append(inodes, e.Inode)
	}

	// Select all 5 at once
	resp := env.postSelect(inodes)
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for all to appear in Spaces
	env.waitConverge(10*time.Second, func() bool {
		for i := 1; i <= 5; i++ {
			if !env.fileExistsSpaces(fmtName("f%d.txt", i)) {
				return false
			}
		}
		return true
	})

	// Verify all synced
	for i := 1; i <= 5; i++ {
		assert.True(t, env.fileExistsSpaces(fmtName("f%d.txt", i)))
	}
}

func TestE2E_SelectDuringFsChange(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "evolving.txt"), []byte("version1"), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("evolving.txt") != nil
	})

	entry := env.findEntryByName("evolving.txt")
	require.NotNil(t, entry)

	// Select the file
	resp := env.postSelect([]uint64{entry.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Simultaneously modify Archives (SSH edit simulation)
	time.Sleep(10 * time.Millisecond)
	env.writeArchive("evolving.txt", []byte("version2"))

	// Wait for Spaces to converge to the latest content
	env.waitConverge(10*time.Second, func() bool {
		if !env.fileExistsSpaces("evolving.txt") {
			return false
		}
		content, err := os.ReadFile(filepath.Join(env.spacesRoot, "evolving.txt"))
		if err != nil {
			return false
		}
		return string(content) == "version2"
	})

	content := env.readSpaces("evolving.txt")
	assert.Equal(t, []byte("version2"), content)
}

// ============================================================================
// Group I: Edge Cases
// ============================================================================

func TestE2E_HiddenFilesSkipped(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, ".hidden"), []byte("hidden"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, ".DS_Store"), []byte("ds"), 0644))
		// Also add a normal file to confirm scanner works
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "visible.txt"), []byte("visible"), 0644))
	})

	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("visible.txt") != nil
	})

	// Hidden files should NOT be registered
	assert.Nil(t, env.findEntryByName(".hidden"))
	assert.Nil(t, env.findEntryByName(".DS_Store"))
	assert.NotNil(t, env.findEntryByName("visible.txt"))
}

func TestE2E_SyncConflictFilesSkipped(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(
			filepath.Join(archivesRoot, "file.sync-conflict-20260101-123456.txt"),
			[]byte("conflict"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "normal.txt"), []byte("normal"), 0644))
	})

	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("normal.txt") != nil
	})

	// sync-conflict file should NOT be registered
	assert.Nil(t, env.findEntryByName("file.sync-conflict-20260101-123456.txt"))
	assert.NotNil(t, env.findEntryByName("normal.txt"))
}

func TestE2E_DeeplyNestedTree(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "a", "b", "c", "d", "e"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "a", "b", "c", "d", "e", "deep.txt"), []byte("deep file"), 0644))
	})

	// Wait for all entries to be registered (5 dirs + 1 file = 6 entries)
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 6
	})

	// Select root "a" directory
	a := env.findEntryByName("a")
	require.NotNil(t, a)

	resp := env.postSelect([]uint64{a.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for deep file to appear in Spaces
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("a/b/c/d/e/deep.txt")
	})

	content := env.readSpaces("a/b/c/d/e/deep.txt")
	assert.Equal(t, []byte("deep file"), content)
}

func TestE2E_LargeFileMtimePreserved(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		// 1MB random file
		data := make([]byte, 1024*1024)
		rand.Read(data)
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "bigfile.bin"), data, 0644))

		// Set a specific mtime
		mtime := time.Date(2025, 6, 15, 10, 30, 0, 123456000, time.UTC)
		require.NoError(t, os.Chtimes(filepath.Join(archivesRoot, "bigfile.bin"), time.Now(), mtime))
	})

	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("bigfile.bin") != nil
	})

	entry := env.findEntryByName("bigfile.bin")
	require.NotNil(t, entry)

	// Select
	resp := env.postSelect([]uint64{entry.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(10*time.Second, func() bool {
		return env.fileExistsSpaces("bigfile.bin")
	})

	// Verify content matches
	archiveData := env.readArchive("bigfile.bin")
	spacesData := env.readSpaces("bigfile.bin")
	assert.Equal(t, archiveData, spacesData)

	// Verify mtime matches at nanosecond level
	aInfo, err := os.Stat(filepath.Join(env.archivesRoot, "bigfile.bin"))
	require.NoError(t, err)
	sInfo, err := os.Stat(filepath.Join(env.spacesRoot, "bigfile.bin"))
	require.NoError(t, err)
	assert.Equal(t, aInfo.ModTime().UnixNano(), sInfo.ModTime().UnixNano())
}

func TestE2E_SameNameDifferentLevels(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "README.md"), []byte("root readme"), 0644))
		require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, "docs"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "docs", "README.md"), []byte("docs readme"), 0644))
	})

	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 3
	})

	// Select root README.md
	rootReadme := env.findEntryByName("README.md")
	require.NotNil(t, rootReadme)

	// Select docs/ (which includes docs/README.md)
	docs := env.findEntryByName("docs")
	require.NotNil(t, docs)

	resp := env.postSelect([]uint64{rootReadme.Inode, docs.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for both to appear in Spaces
	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("README.md") && env.fileExistsSpaces("docs/README.md")
	})

	// Verify content
	assert.Equal(t, []byte("root readme"), env.readSpaces("README.md"))
	assert.Equal(t, []byte("docs readme"), env.readSpaces("docs/README.md"))

	// Verify different inodes
	docsReadme := env.findEntryByNameUnder(docs.Inode, "README.md")
	require.NotNil(t, docsReadme)
	assert.NotEqual(t, rootReadme.Inode, docsReadme.Inode)
}

// ============================================================================
// Group J: Watcher-specific behavior
// ============================================================================

func TestE2E_WatcherDebounce(t *testing.T) {
	env := setupE2E(t, nil)

	// Wait for daemon startup
	time.Sleep(500 * time.Millisecond)

	// Rapid burst: write → overwrite → overwrite within 100ms
	fpath := filepath.Join(env.archivesRoot, "burst.txt")
	os.WriteFile(fpath, []byte("v1"), 0644)
	time.Sleep(50 * time.Millisecond)
	os.WriteFile(fpath, []byte("v2-longer"), 0644)
	time.Sleep(50 * time.Millisecond)
	os.WriteFile(fpath, []byte("v3-final-version"), 0644)

	// Wait for debounce (300ms) + pipeline processing
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("burst.txt") != nil
	})

	entry := env.findEntryByName("burst.txt")
	require.NotNil(t, entry)

	// Entry mtime should match the LAST write
	info, err := os.Stat(fpath)
	require.NoError(t, err)
	assert.Equal(t, info.ModTime().UnixNano(), entry.Mtime)
}

func TestE2E_WatcherRename(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "old-name.txt"), []byte("rename me"), 0644))
	})

	// Wait for seed
	env.waitConverge(5*time.Second, func() bool {
		return env.findEntryByName("old-name.txt") != nil
	})

	// Rename the file
	os.Rename(
		filepath.Join(env.archivesRoot, "old-name.txt"),
		filepath.Join(env.archivesRoot, "new-name.txt"),
	)

	// Wait for new-name.txt to appear and old-name.txt to disappear
	env.waitConverge(5*time.Second, func() bool {
		newEntry := env.findEntryByName("new-name.txt")
		oldEntry := env.findEntryByName("old-name.txt")
		return newEntry != nil && oldEntry == nil
	})

	newEntry := env.findEntryByName("new-name.txt")
	require.NotNil(t, newEntry)
	assert.Nil(t, env.findEntryByName("old-name.txt"))
}

func TestE2E_WatcherSpacesDelete(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "selfheal.txt"), []byte("self healing"), 0644))
		copyFilePreserveMtime(t,
			filepath.Join(archivesRoot, "selfheal.txt"),
			filepath.Join(spacesRoot, "selfheal.txt"))
	})

	// Wait for synced
	env.waitConverge(5*time.Second, func() bool {
		entry := env.findEntryByName("selfheal.txt")
		return entry != nil && entry.Status == "synced"
	})

	// Delete Spaces copy — watcher should detect and P3 should restore it
	os.Remove(filepath.Join(env.spacesRoot, "selfheal.txt"))

	// Wait for self-healing: Spaces file should be restored
	env.waitConverge(10*time.Second, func() bool {
		return env.fileExistsSpaces("selfheal.txt")
	})

	content := env.readSpaces("selfheal.txt")
	assert.Equal(t, []byte("self healing"), content)
}

// ============================================================================
// Group K: Full Lifecycle
// ============================================================================

func TestE2E_FullUserWorkflow(t *testing.T) {
	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "doc.txt"), []byte("document content"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "photo.jpg"), make([]byte, 500), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(archivesRoot, "video.mp4"), make([]byte, 1000), 0644))
	})

	// Step 1: seed → all archived
	env.waitConverge(5*time.Second, func() bool {
		return env.countAllEntries() >= 3
	})

	doc := env.findEntryByName("doc.txt")
	photo := env.findEntryByName("photo.jpg")
	require.NotNil(t, doc)
	require.NotNil(t, photo)

	assert.False(t, doc.Selected)
	assert.Equal(t, "archived", doc.Status)

	stats := env.getStats()
	assert.Equal(t, int64(0), stats.SpacesSize)

	// Step 2: select doc.txt
	resp := env.postSelect([]uint64{doc.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("doc.txt")
	})

	doc = env.findEntryByName("doc.txt")
	require.NotNil(t, doc)
	assert.True(t, doc.Selected)
	assert.Equal(t, "synced", doc.Status)

	// Step 3: Spoke edit — modify Spaces/doc.txt
	time.Sleep(10 * time.Millisecond)
	env.writeSpaces("doc.txt", []byte("spoke edited doc"))

	env.waitConverge(5*time.Second, func() bool {
		content, err := os.ReadFile(filepath.Join(env.archivesRoot, "doc.txt"))
		return err == nil && string(content) == "spoke edited doc"
	})

	// Step 4: select photo.jpg
	resp = env.postSelect([]uint64{photo.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return env.fileExistsSpaces("photo.jpg")
	})

	// Step 5: deselect doc.txt
	resp = env.postDeselect([]uint64{doc.Inode})
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	env.waitConverge(5*time.Second, func() bool {
		return !env.fileExistsSpaces("doc.txt")
	})

	// doc.txt: archived, Archives preserved
	assert.True(t, env.fileExistsArchive("doc.txt"))
	assert.True(t, env.fileExistsTrash("doc.txt"))

	doc = env.findEntryByName("doc.txt")
	require.NotNil(t, doc)
	assert.False(t, doc.Selected)
	assert.Equal(t, "archived", doc.Status)

	// Step 6: SSH edit — modify Archives/photo.jpg
	time.Sleep(10 * time.Millisecond)
	env.writeArchive("photo.jpg", make([]byte, 600))

	env.waitConverge(5*time.Second, func() bool {
		sInfo, err := os.Stat(filepath.Join(env.spacesRoot, "photo.jpg"))
		return err == nil && sInfo.Size() == 600
	})

	// Step 7: verify stats
	stats = env.getStats()
	assert.Equal(t, int64(600), stats.SpacesSize) // only photo.jpg (600 bytes)
}

func TestE2E_BulkOperationsLargeTree(t *testing.T) {
	const numDirs = 10
	const filesPerDir = 5

	env := setupE2E(t, func(archivesRoot, spacesRoot string) {
		for d := 0; d < numDirs; d++ {
			dirName := fmt.Sprintf("dir%02d", d)
			require.NoError(t, os.MkdirAll(filepath.Join(archivesRoot, dirName), 0755))
			for f := 0; f < filesPerDir; f++ {
				fileName := fmt.Sprintf("file%02d.txt", f)
				require.NoError(t, os.WriteFile(
					filepath.Join(archivesRoot, dirName, fileName),
					make([]byte, 100),
					0644,
				))
			}
		}
	})

	totalEntries := numDirs + numDirs*filesPerDir // 10 dirs + 50 files = 60

	// Wait for all entries
	env.waitConverge(10*time.Second, func() bool {
		return env.countAllEntries() >= totalEntries
	})

	// Collect all root dir inodes
	rootEntries := env.getEntries(nil)
	var rootInodes []uint64
	for _, e := range rootEntries {
		rootInodes = append(rootInodes, e.Inode)
	}

	// Select all root dirs (recursive)
	resp := env.postSelect(rootInodes)
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for all files to be copied
	env.waitConverge(30*time.Second, func() bool {
		return countFiles(env.spacesRoot) >= numDirs*filesPerDir
	})

	// Verify stats
	stats := env.getStats()
	assert.Equal(t, int64(numDirs*filesPerDir*100), stats.SpacesSize) // 50 * 100 = 5000

	// Deselect all
	resp = env.postDeselect(rootInodes)
	assert.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wait for all Spaces files to be removed
	env.waitConverge(30*time.Second, func() bool {
		return countFiles(env.spacesRoot) == 0
	})

	stats = env.getStats()
	assert.Equal(t, int64(0), stats.SpacesSize)
}

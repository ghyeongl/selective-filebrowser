package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSyncIgnore_DefaultsWithoutFile(t *testing.T) {
	// No .syncignore on disk — the built-in defaults must still apply.
	si := LoadSyncIgnore(filepath.Join(t.TempDir(), ".syncignore"))

	tests := []struct {
		name    string
		isDir   bool
		ignored bool
	}{
		{".syncthing.big.tmp", false, true},
		{"~syncthing~big.tmp", false, true},
		{".stfolder", true, true},
		{".stversions", true, true},
		{"report.txt.sync-conflict-20240101-123456-ABCDEFG.txt", false, true},
		{"report.txt.sync-tmp", false, true},
		{".sync-tmp-1a2b3c4d", false, true},
		{".trash", true, true},

		// Artifacts nested under a normal directory.
		{"docs/.syncthing.big.tmp", false, true},
		{"docs/.stversions", true, true},
		{"docs/.stversions/old.txt", false, true},

		// The trash default is anchored: only the app's own root trash.
		{".trash/2026-08-14/x.txt", false, true},
		{"project/.trash", true, false},
		{"project/.trash/keep.txt", false, false},

		// Not artifacts — must pass through.
		{"report.txt", false, false},
		{"notes.tmp", false, false},
		{"syncthing.txt", false, false},
		{".trashcan", true, false},
		{"docs/report.txt", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.ignored, si.IsIgnored(tt.name, tt.isDir))
		})
	}
}

func TestLoadSyncIgnore_UserPatternsUnionDefaults(t *testing.T) {
	cfgDir := t.TempDir()
	path := filepath.Join(cfgDir, ".syncignore")
	require.NoError(t, os.WriteFile(path, []byte("*.bak\nnode_modules/\n"), 0644))

	si := LoadSyncIgnore(path)

	assert.True(t, si.IsIgnored("foo.bak", false), "user pattern applies")
	assert.True(t, si.IsIgnored("src/foo.bak", false), "user pattern applies at any depth")
	assert.True(t, si.IsIgnored("node_modules", true), "dirOnly pattern matches a directory")
	assert.False(t, si.IsIgnored("node_modules", false), "dirOnly pattern does not match a file")
	assert.True(t, si.IsIgnored("node_modules/pkg/index.js", false), "everything under an ignored dir is ignored")
	assert.True(t, si.IsIgnored(".syncthing.big.tmp", false), "defaults survive a user file")
	assert.True(t, si.IsIgnored(".trash", true), "defaults survive a user file")
}

func TestLoadSyncIgnore_UserAnchoredPattern(t *testing.T) {
	cfgDir := t.TempDir()
	path := filepath.Join(cfgDir, ".syncignore")
	require.NoError(t, os.WriteFile(path, []byte("/build\n"), 0644))

	si := LoadSyncIgnore(path)

	assert.True(t, si.IsIgnored("build", true), "anchored pattern matches at the root")
	assert.True(t, si.IsIgnored("build/out.bin", false), "anchored pattern covers descendants")
	assert.False(t, si.IsIgnored("project/build", true), "anchored pattern does not match deeper")
}

func TestSyncIgnore_NilIgnoresNothing(t *testing.T) {
	var si *SyncIgnore
	assert.False(t, si.IsIgnored(".syncthing.big.tmp", false))
}

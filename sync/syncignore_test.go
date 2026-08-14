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
		{".trash", true, true},

		// Not artifacts — must pass through.
		{"report.txt", false, false},
		{"notes.tmp", false, false},
		{"syncthing.txt", false, false},
		{".trashcan", true, false},
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
	assert.True(t, si.IsIgnored("node_modules", true), "dirOnly pattern matches a directory")
	assert.False(t, si.IsIgnored("node_modules", false), "dirOnly pattern does not match a file")
	assert.True(t, si.IsIgnored(".syncthing.big.tmp", false), "defaults survive a user file")
	assert.True(t, si.IsIgnored(".trash", true), "defaults survive a user file")
}

func TestSyncIgnore_NilIgnoresNothing(t *testing.T) {
	var si *SyncIgnore
	assert.False(t, si.IsIgnored(".syncthing.big.tmp", false))
}

package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanDir_BasicTree(t *testing.T) {
	dir := t.TempDir()

	// Create structure:
	// dir/
	//   Documents/
	//     readme.txt
	//   photo.jpg
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "Documents"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Documents", "readme.txt"), []byte("hello"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "photo.jpg"), []byte("fake-jpg"), 0644))

	result, err := ScanDir(dir)
	require.NoError(t, err)

	assert.Len(t, result, 3) // Documents/, Documents/readme.txt, photo.jpg

	// Check directory
	docStat, ok := result["Documents"]
	require.True(t, ok)
	assert.True(t, docStat.IsDir)
	assert.Equal(t, "Documents", docStat.Name)
	assert.NotZero(t, docStat.Inode)

	// Check file
	readmeStat, ok := result[filepath.Join("Documents", "readme.txt")]
	require.True(t, ok)
	assert.False(t, readmeStat.IsDir)
	assert.Equal(t, "readme.txt", readmeStat.Name)
	assert.Equal(t, int64(5), readmeStat.Size) // "hello"

	// Check root-level file
	photoStat, ok := result["photo.jpg"]
	require.True(t, ok)
	assert.False(t, photoStat.IsDir)
}

func TestScanDir_SkipsSyncConflict(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "file.txt"), []byte("ok"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file.sync-conflict-20240101-123456.txt"), []byte("conflict"), 0644))

	result, err := ScanDir(dir)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	_, ok := result["file.txt"]
	assert.True(t, ok)
}

func TestScanDir_SkipsHiddenFiles(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "visible.txt"), []byte("ok"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden"), []byte("hidden"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".hiddendir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hiddendir", "inside.txt"), []byte("x"), 0644))

	result, err := ScanDir(dir)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	_, ok := result["visible.txt"]
	assert.True(t, ok)
}

func TestScanDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	result, err := ScanDir(dir)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestClassifyType(t *testing.T) {
	tests := []struct {
		name     string
		isDir    bool
		expected string
	}{
		{"folder", true, "dir"},
		{"movie.mp4", false, "video"},
		{"movie.mkv", false, "video"},
		{"movie.avi", false, "video"},
		{"song.mp3", false, "audio"},
		{"song.flac", false, "audio"},
		{"song.wav", false, "audio"},
		{"photo.jpg", false, "image"},
		{"photo.png", false, "image"},
		{"photo.gif", false, "image"},
		{"doc.pdf", false, "pdf"},
		{"readme.txt", false, "text"},
		{"code.go", false, "text"},
		{"data.json", false, "text"},
		{"page.html", false, "text"},
		{"style.css", false, "text"},
		{"unknown.xyz", false, "blob"},
		{"noext", false, "blob"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyType(tt.name, tt.isDir)
			assert.Equal(t, tt.expected, result)
		})
	}
}

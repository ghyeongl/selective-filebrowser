package sync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const copyChunkSize = 256 * 1024 // 256KB per chunk

// ErrSourceModified is returned when SafeCopy detects that the source
// file was modified during the copy.
var ErrSourceModified = fmt.Errorf("source modified during copy")

// SafeCopy copies src to dst atomically:
// 1. Record src mtime
// 2. Copy to dst.tmp in chunks (checking ctx and evalQueue between chunks)
// 3. Verify src mtime unchanged
// 4. MkdirAll + atomic rename tmp → dst
//
// hasQueued is called between chunks to check if this path has been
// re-queued (meaning a new event invalidated this copy). If nil, skipped.
func SafeCopy(ctx context.Context, src, dst string, hasQueued func() bool) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat src: %w", err)
	}
	mtime1 := srcInfo.ModTime().UnixNano()

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("mkdir dst parent: %w", err)
	}

	tmpPath := dst + ".sync-tmp"
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer srcFile.Close()

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	buf := make([]byte, copyChunkSize)
	var copyErr error
	for {
		// Check cancellation
		select {
		case <-ctx.Done():
			copyErr = ctx.Err()
			break
		default:
		}
		if copyErr != nil {
			break
		}

		// Check if path re-queued
		if hasQueued != nil && hasQueued() {
			copyErr = fmt.Errorf("path re-queued, aborting copy")
			break
		}

		n, readErr := srcFile.Read(buf)
		if n > 0 {
			if _, writeErr := tmpFile.Write(buf[:n]); writeErr != nil {
				copyErr = fmt.Errorf("write tmp: %w", writeErr)
				break
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			copyErr = fmt.Errorf("read src: %w", readErr)
			break
		}
	}

	tmpFile.Close()

	if copyErr != nil {
		os.Remove(tmpPath)
		return copyErr
	}

	// Verify source wasn't modified during copy
	srcInfo2, err := os.Stat(src)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("re-stat src: %w", err)
	}
	mtime2 := srcInfo2.ModTime().UnixNano()
	if mtime1 != mtime2 {
		os.Remove(tmpPath)
		return ErrSourceModified
	}

	// Preserve source mtime on destination
	if err := os.Chtimes(tmpPath, time.Now(), srcInfo.ModTime()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("chtimes tmp: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename tmp to dst: %w", err)
	}

	return nil
}

// SoftDelete moves a file to the trash directory (.trash/YYYY-MM-DD/).
// Returns the final trash path.
func SoftDelete(path, trashRoot string) (string, error) {
	dateDir := filepath.Join(trashRoot, time.Now().Format("2006-01-02"))
	if err := os.MkdirAll(dateDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir trash: %w", err)
	}

	base := filepath.Base(path)
	trashPath := filepath.Join(dateDir, base)

	// Handle name collision in trash
	if _, err := os.Stat(trashPath); err == nil {
		for i := 1; ; i++ {
			ext := filepath.Ext(base)
			name := base[:len(base)-len(ext)]
			trashPath = filepath.Join(dateDir, fmt.Sprintf("%s_%d%s", name, i, ext))
			if _, err := os.Stat(trashPath); os.IsNotExist(err) {
				break
			}
		}
	}

	if err := os.Rename(path, trashPath); err != nil {
		return "", fmt.Errorf("move to trash: %w", err)
	}

	return trashPath, nil
}

// RenameConflict renames a file by appending _conflict-N before the extension.
// Returns the new path.
func RenameConflict(path string) (string, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]

	var newPath string
	for i := 1; ; i++ {
		newPath = filepath.Join(dir, fmt.Sprintf("%s_conflict-%d%s", name, i, ext))
		if _, err := os.Stat(newPath); os.IsNotExist(err) {
			break
		}
	}

	if err := os.Rename(path, newPath); err != nil {
		return "", fmt.Errorf("rename conflict: %w", err)
	}

	return newPath, nil
}

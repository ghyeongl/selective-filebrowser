package sync

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const copyChunkSize = 256 * 1024 // 256KB per chunk

// slogDebug is a convenience alias for use in logEnabled() guards.
const slogDebug = slog.LevelDebug

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
	l := sub("fileops")

	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat src: %w", err)
	}
	mtime1 := srcInfo.ModTime().UnixNano()
	totalSize := srcInfo.Size()

	l.Debug("SafeCopy start", "src", src, "dst", dst, "size", totalSize)
	start := time.Now()

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
	var copied int64
	chunkCount := 0
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
			copied += int64(n)
			chunkCount++
			// Log progress every ~1MB (4 chunks of 256KB)
			if chunkCount%4 == 0 && logEnabled(slogDebug) {
				l.Debug("SafeCopy progress", "src", src, "bytesCopied", copied, "totalSize", totalSize)
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
		if ctx.Err() != nil {
			l.Warn("SafeCopy aborted", "src", src, "dst", dst, "reason", "ctx cancelled")
		} else {
			l.Warn("SafeCopy aborted", "src", src, "dst", dst, "reason", copyErr.Error())
		}
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
		l.Warn("SafeCopy source modified", "src", src, "mtime1", mtime1, "mtime2", mtime2)
		return ErrSourceModified
	}
	l.Debug("SafeCopy mtime verified", "src", src, "mtime", mtime1)

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

	l.Debug("SafeCopy complete", "src", src, "dst", dst, "size", totalSize, "durationMs", time.Since(start).Milliseconds())
	return nil
}

// SoftDelete moves a file to the trash directory (.trash/YYYY-MM-DD/).
// Returns the final trash path.
func SoftDelete(path, trashRoot string) (string, error) {
	l := sub("fileops")
	l.Debug("SoftDelete start", "path", path)

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
		l.Debug("SoftDelete collision", "base", base, "trashPath", trashPath)
	}

	if err := os.Rename(path, trashPath); err != nil {
		return "", fmt.Errorf("move to trash: %w", err)
	}

	l.Info("SoftDelete complete", "path", path, "trashPath", trashPath)
	return trashPath, nil
}

// ConflictName returns the conflict filename (e.g. "file_conflict-1.txt")
// without performing any disk rename. It checks the directory of originalPath
// to find the next available conflict number.
func ConflictName(originalPath string) string {
	dir := filepath.Dir(originalPath)
	base := filepath.Base(originalPath)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]

	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s_conflict-%d%s", name, i, ext)
		if _, err := os.Stat(filepath.Join(dir, candidate)); err != nil {
			return candidate
		}
	}
}

// RenameConflict renames a file by appending _conflict-N before the extension.
// Returns the new path.
func RenameConflict(path string) (string, error) {
	l := sub("fileops")
	l.Debug("RenameConflict start", "path", path)

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

	l.Info("RenameConflict complete", "oldPath", path, "newPath", newPath)
	return newPath, nil
}

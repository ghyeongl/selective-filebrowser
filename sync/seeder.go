package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Seed performs the initial database population by scanning both
// Archives and Spaces directories.
func Seed(store *Store, archivesPath, spacesPath string) error {
	l := sub("seeder")
	l.Info("seed starting", "archivesPath", archivesPath, "spacesPath", spacesPath)
	start := time.Now()

	scanStart := time.Now()
	archiveFiles, err := ScanDir(archivesPath)
	if err != nil {
		return fmt.Errorf("scan archives: %w", err)
	}
	l.Info("seed archives scanned", "entries", len(archiveFiles), "durationMs", time.Since(scanStart).Milliseconds())

	scanStart = time.Now()
	spacesFiles, err := ScanDir(spacesPath)
	if err != nil {
		return fmt.Errorf("scan spaces: %w", err)
	}
	l.Info("seed spaces scanned", "entries", len(spacesFiles), "durationMs", time.Since(scanStart).Milliseconds())

	// Build a set of Spaces relative paths for quick lookup
	spacesSet := make(map[string]bool, len(spacesFiles))
	for relPath := range spacesFiles {
		spacesSet[relPath] = true
	}

	// Phase 1: Process Archives entries — build the directory tree
	// We need to process directories before files to establish parent_ino.
	// Sort by depth (shallower first).
	var dirs, files []pathEntry
	for relPath, stat := range archiveFiles {
		if stat.IsDir {
			dirs = append(dirs, pathEntry{relPath, stat})
		} else {
			files = append(files, pathEntry{relPath, stat})
		}
	}

	l.Debug("seed phase 1: archives entries", "dirs", len(dirs), "files", len(files))

	// Sort dirs by depth
	sortByDepth(dirs)

	// Insert directories first
	for _, pe := range dirs {
		parentIno, err := resolveParentIno(store, archivesPath, pe.relPath, archiveFiles)
		if err != nil {
			return fmt.Errorf("resolve parent for %s: %w", pe.relPath, err)
		}
		inSpaces := spacesSet[pe.relPath]
		if err := store.UpsertEntry(Entry{
			Inode:     pe.stat.Inode,
			ParentIno: parentIno,
			Name:      pe.stat.Name,
			Type:      "dir",
			Mtime:     pe.stat.Mtime,
			Selected:  inSpaces,
		}); err != nil {
			return fmt.Errorf("insert dir %s: %w", pe.relPath, err)
		}
		l.Debug("seed insert dir", "path", pe.relPath, "inode", pe.stat.Inode, "selected", inSpaces)
	}

	// Insert files
	for _, pe := range files {
		parentIno, err := resolveParentIno(store, archivesPath, pe.relPath, archiveFiles)
		if err != nil {
			return fmt.Errorf("resolve parent for %s: %w", pe.relPath, err)
		}
		inSpaces := spacesSet[pe.relPath]
		size := pe.stat.Size
		fileType := ClassifyType(pe.stat.Name, false)
		if err := store.UpsertEntry(Entry{
			Inode:     pe.stat.Inode,
			ParentIno: parentIno,
			Name:      pe.stat.Name,
			Type:      fileType,
			Size:      &size,
			Mtime:     pe.stat.Mtime,
			Selected:  inSpaces,
		}); err != nil {
			return fmt.Errorf("insert file %s: %w", pe.relPath, err)
		}
		l.Debug("seed insert file", "path", pe.relPath, "inode", pe.stat.Inode, "type", fileType, "selected", inSpaces)
	}

	// Debug: log match/miss stats and sample misses to diagnose path format issues
	var matchCount, missCount int
	for relPath := range spacesFiles {
		if _, ok := archiveFiles[relPath]; ok {
			matchCount++
		} else {
			missCount++
			if missCount <= 5 {
				l.Warn("spaces-only sample path", "relPath", relPath, "len", len(relPath))
			}
		}
	}
	l.Info("seed phase 2 match stats", "match", matchCount, "miss", missCount, "archiveMapSize", len(archiveFiles), "spacesMapSize", len(spacesFiles))

	// Debug: log sample archive paths for comparison with spaces paths
	sampleCount := 0
	for relPath := range archiveFiles {
		if sampleCount >= 5 {
			break
		}
		l.Warn("archive sample path", "relPath", relPath, "len", len(relPath))
		sampleCount++
	}

	// Phase 2: Create spaces_view for entries that exist in BOTH Archives and Spaces.
	// This runs BEFORE Spaces-only processing so that already-synced files are
	// immediately visible as "synced" to the pipeline worker.
	l.Info("seed phase 2: spaces_view")
	now := time.Now().UnixNano()
	var svCount int
	for relPath, spStat := range spacesFiles {
		archStat, inArchive := archiveFiles[relPath]
		if !inArchive {
			continue
		}
		if err := store.UpsertSpacesView(SpacesView{
			EntryIno:    archStat.Inode,
			SyncedMtime: spStat.Mtime,
			CheckedAt:   now,
		}); err != nil {
			return fmt.Errorf("insert spaces_view for %s: %w", relPath, err)
		}
		svCount++
		l.Debug("seed spaces_view created", "path", relPath, "inode", archStat.Inode)
	}
	l.Info("seed phase 2 complete", "spacesViewCount", svCount)

	// Phase 3: Handle Spaces-only files (scenario #3) — SafeCopy S→A + INSERT + spaces_view
	var spacesOnlyDirs, spacesOnlyFiles []pathEntry
	for relPath, stat := range spacesFiles {
		if _, inArchive := archiveFiles[relPath]; !inArchive {
			if stat.IsDir {
				spacesOnlyDirs = append(spacesOnlyDirs, pathEntry{relPath, stat})
			} else {
				spacesOnlyFiles = append(spacesOnlyFiles, pathEntry{relPath, stat})
			}
		}
	}

	if len(spacesOnlyDirs)+len(spacesOnlyFiles) > 0 {
		l.Info("seed phase 3: spaces-only", "dirs", len(spacesOnlyDirs), "files", len(spacesOnlyFiles))

		// Dirs first (shallow → deep)
		sortByDepth(spacesOnlyDirs)
		for _, pe := range spacesOnlyDirs {
			archDir := filepath.Join(archivesPath, pe.relPath)
			if err := os.MkdirAll(archDir, 0755); err != nil {
				return fmt.Errorf("mkdir archives %s: %w", pe.relPath, err)
			}
			aMtime, _, aInode, _ := statFile(archDir)
			if aInode == nil {
				return fmt.Errorf("stat archives dir %s: inode unavailable", pe.relPath)
			}
			parentIno, err := resolveParentInoFromDB(store, pe.relPath)
			if err != nil {
				return fmt.Errorf("resolve parent for spaces-only dir %s: %w", pe.relPath, err)
			}
			if err := store.UpsertEntry(Entry{
				Inode:     *aInode,
				ParentIno: parentIno,
				Name:      pe.stat.Name,
				Type:      "dir",
				Mtime:     *aMtime,
				Selected:  true,
			}); err != nil {
				return fmt.Errorf("insert spaces-only dir %s: %w", pe.relPath, err)
			}
			if err := store.UpsertSpacesView(SpacesView{
				EntryIno:    *aInode,
				SyncedMtime: pe.stat.Mtime,
				CheckedAt:   now,
			}); err != nil {
				return fmt.Errorf("insert spaces_view for spaces-only dir %s: %w", pe.relPath, err)
			}
			l.Debug("seed spaces-only dir", "path", pe.relPath, "inode", *aInode)
		}

		// Files: SafeCopy S→A then INSERT entry + spaces_view
		for _, pe := range spacesOnlyFiles {
			src := filepath.Join(spacesPath, pe.relPath)
			dst := filepath.Join(archivesPath, pe.relPath)
			if err := SafeCopy(context.Background(), src, dst, nil); err != nil {
				return fmt.Errorf("seed copy S→A %s: %w", pe.relPath, err)
			}
			l.Debug("seed spaces-only file copied", "path", pe.relPath)

			aMtime, _, aInode, _ := statFile(dst)
			if aInode == nil {
				return fmt.Errorf("stat archives file %s after copy: inode unavailable", pe.relPath)
			}
			parentIno, err := resolveParentInoFromDB(store, pe.relPath)
			if err != nil {
				return fmt.Errorf("resolve parent for spaces-only file %s: %w", pe.relPath, err)
			}
			size := pe.stat.Size
			if err := store.UpsertEntry(Entry{
				Inode:     *aInode,
				ParentIno: parentIno,
				Name:      pe.stat.Name,
				Type:      ClassifyType(pe.stat.Name, false),
				Size:      &size,
				Mtime:     *aMtime,
				Selected:  true,
			}); err != nil {
				return fmt.Errorf("insert spaces-only file %s: %w", pe.relPath, err)
			}
			if err := store.UpsertSpacesView(SpacesView{
				EntryIno:    *aInode,
				SyncedMtime: *aMtime,
				CheckedAt:   now,
			}); err != nil {
				return fmt.Errorf("insert spaces_view for spaces-only file %s: %w", pe.relPath, err)
			}
			l.Debug("seed spaces-only file registered", "path", pe.relPath, "inode", *aInode)
		}
	}

	l.Info("seed complete", "archiveEntries", len(archiveFiles), "spacesEntries", len(spacesFiles), "durationMs", time.Since(start).Milliseconds())
	return nil
}

// resolveParentIno finds the parent inode for a given relative path.
// Returns 0 for root-level entries (virtual root).
func resolveParentIno(store *Store, root, relPath string, files map[string]FileStat) (uint64, error) {
	dir := filepath.Dir(relPath)
	if dir == "." {
		return 0, nil // root level → virtual root
	}

	parentStat, ok := files[dir]
	if !ok {
		return 0, fmt.Errorf("parent dir %s not found in scan results", dir)
	}
	return parentStat.Inode, nil
}

func sortByDepth(entries []pathEntry) {
	// Simple insertion sort — number of dirs is typically small
	for i := 1; i < len(entries); i++ {
		key := entries[i]
		j := i - 1
		for j >= 0 && depth(entries[j].relPath) > depth(key.relPath) {
			entries[j+1] = entries[j]
			j--
		}
		entries[j+1] = key
	}
}

func depth(path string) int {
	if path == "." || path == "" {
		return 0
	}
	n := 0
	for _, c := range path {
		if c == filepath.Separator {
			n++
		}
	}
	return n + 1
}

type pathEntry struct {
	relPath string
	stat    FileStat
}

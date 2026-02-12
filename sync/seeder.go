package sync

import (
	"fmt"
	"log"
	"path/filepath"
	"time"
)

// Seed performs the initial database population by scanning both
// Archives and Spaces directories.
func Seed(store *Store, archivesPath, spacesPath string) error {
	log.Println("[sync] Starting initial seed...")
	start := time.Now()

	archiveFiles, err := ScanDir(archivesPath)
	if err != nil {
		return fmt.Errorf("scan archives: %w", err)
	}
	log.Printf("[sync] Archives scan: %d entries", len(archiveFiles))

	spacesFiles, err := ScanDir(spacesPath)
	if err != nil {
		return fmt.Errorf("scan spaces: %w", err)
	}
	log.Printf("[sync] Spaces scan: %d entries", len(spacesFiles))

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
	}

	// Insert files
	for _, pe := range files {
		parentIno, err := resolveParentIno(store, archivesPath, pe.relPath, archiveFiles)
		if err != nil {
			return fmt.Errorf("resolve parent for %s: %w", pe.relPath, err)
		}
		inSpaces := spacesSet[pe.relPath]
		size := pe.stat.Size
		if err := store.UpsertEntry(Entry{
			Inode:     pe.stat.Inode,
			ParentIno: parentIno,
			Name:      pe.stat.Name,
			Type:      ClassifyType(pe.stat.Name, false),
			Size:      &size,
			Mtime:     pe.stat.Mtime,
			Selected:  inSpaces,
		}); err != nil {
			return fmt.Errorf("insert file %s: %w", pe.relPath, err)
		}
	}

	// Phase 2: Create spaces_view for entries that exist in Spaces
	now := time.Now().UnixNano()
	for relPath, spStat := range spacesFiles {
		if _, inArchive := archiveFiles[relPath]; inArchive {
			archStat := archiveFiles[relPath]
			if err := store.UpsertSpacesView(SpacesView{
				EntryIno:    archStat.Inode,
				SyncedMtime: spStat.Mtime,
				CheckedAt:   now,
			}); err != nil {
				return fmt.Errorf("insert spaces_view for %s: %w", relPath, err)
			}
		}
		// Spaces-only files (#3) will be handled by the pipeline later
	}

	log.Printf("[sync] Seed complete: %d archive entries, %d spaces entries (%s)",
		len(archiveFiles), len(spacesFiles), time.Since(start))

	return nil
}

// resolveParentIno finds the parent inode for a given relative path.
func resolveParentIno(store *Store, root, relPath string, files map[string]FileStat) (*uint64, error) {
	dir := filepath.Dir(relPath)
	if dir == "." {
		return nil, nil // root level
	}

	parentStat, ok := files[dir]
	if !ok {
		return nil, fmt.Errorf("parent dir %s not found in scan results", dir)
	}
	ino := parentStat.Inode
	return &ino, nil
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

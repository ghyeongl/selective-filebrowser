package sync

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// RunPipeline evaluates a single relative path through P0→P4.
// It gathers the 7 variables, determines the scenario, and executes
// the appropriate actions to converge toward the target state.
func RunPipeline(ctx context.Context, relPath string, store *Store, archivesRoot, spacesRoot, trashRoot string, hasQueued func() bool) error {
	archivePath := filepath.Join(archivesRoot, relPath)
	spacesPath := filepath.Join(spacesRoot, relPath)

	// Gather disk state
	archiveMtime, archiveIsDir, archiveInode, archiveSize := statFile(archivePath)
	spacesMtime, _, _, _ := statFile(spacesPath)

	// Gather DB state
	entry, sv, err := lookupDB(store, archivesRoot, relPath)
	if err != nil {
		return fmt.Errorf("db lookup: %w", err)
	}

	// Compute state
	state := ComputeState(entry, sv, archiveMtime, spacesMtime)
	scenario := state.Scenario()

	log.Printf("[pipeline] %s → scenario #%d (%s)", relPath, scenario, state.UIStatus())

	// P0: Archives disk recovery (A_disk=0)
	if !state.ADisk {
		if err := p0(ctx, store, entry, sv, relPath, archivePath, spacesPath, state, hasQueued); err != nil {
			return fmt.Errorf("P0: %w", err)
		}
		// Re-gather state after P0 actions
		archiveMtime, archiveIsDir, archiveInode, archiveSize = statFile(archivePath)
		spacesMtime, _, _, _ = statFile(spacesPath)
		entry, sv, err = lookupDB(store, archivesRoot, relPath)
		if err != nil {
			return fmt.Errorf("db lookup post-P0: %w", err)
		}
		state = ComputeState(entry, sv, archiveMtime, spacesMtime)
	}

	// P1: DB registration (A_db=0, A_disk=1 guaranteed after P0)
	if !state.ADb && state.ADisk {
		if err := p1(store, relPath, archivesRoot, archiveInode, archiveIsDir, archiveSize, archiveMtime, state); err != nil {
			return fmt.Errorf("P1: %w", err)
		}
		// Re-gather
		entry, sv, err = lookupDB(store, archivesRoot, relPath)
		if err != nil {
			return fmt.Errorf("db lookup post-P1: %w", err)
		}
		state = ComputeState(entry, sv, archiveMtime, spacesMtime)
	}

	// P2: Change sync (A_dirty or S_dirty)
	if state.ADirty || state.SDirty {
		if err := p2(ctx, store, entry, sv, relPath, archivePath, spacesPath, archivesRoot, state, hasQueued); err != nil {
			return fmt.Errorf("P2: %w", err)
		}
		// Re-gather
		archiveMtime, _, _, _ = statFile(archivePath)
		spacesMtime, _, _, _ = statFile(spacesPath)
		entry, sv, err = lookupDB(store, archivesRoot, relPath)
		if err != nil {
			return fmt.Errorf("db lookup post-P2: %w", err)
		}
		state = ComputeState(entry, sv, archiveMtime, spacesMtime)
	}

	// P3: Goal realization (selected ≠ S_disk)
	if entry != nil && entry.Selected != state.SDisk {
		if err := p3(ctx, store, entry, sv, relPath, archivePath, spacesPath, trashRoot, state, hasQueued); err != nil {
			return fmt.Errorf("P3: %w", err)
		}
		// Re-gather
		spacesMtime, _, _, _ = statFile(spacesPath)
		entry, sv, err = lookupDB(store, archivesRoot, relPath)
		if err != nil {
			return fmt.Errorf("db lookup post-P3: %w", err)
		}
		state = ComputeState(entry, sv, archiveMtime, spacesMtime)
	}

	// P4: DB consistency (S_db ≠ S_disk)
	if state.SDb != state.SDisk {
		if err := p4(store, entry, sv, relPath, spacesPath, state); err != nil {
			return fmt.Errorf("P4: %w", err)
		}
	}

	return nil
}

// p0 handles Archives disk recovery when A_disk=0.
func p0(ctx context.Context, store *Store, entry *Entry, sv *SpacesView, relPath, archivePath, spacesPath string, state State, hasQueued func() bool) error {
	if state.SDisk {
		// S_disk=1 → copy S→A to recover
		log.Printf("[P0] Recovering %s from Spaces → Archives", relPath)
		if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
			return err
		}
		// Update entries mtime/size if entry exists
		if entry != nil {
			info, err := os.Stat(archivePath)
			if err == nil {
				if err := store.UpdateEntryMtime(entry.Inode, info.ModTime().UnixNano(), ptrInt64(info.Size())); err != nil {
					return fmt.Errorf("update entry after recovery: %w", err)
				}
			}
		}
		return nil
	}

	// S_disk=0, A_disk=0 → both gone
	if entry != nil {
		// Clean up DB records
		if sv != nil {
			if err := store.DeleteSpacesView(sv.EntryIno); err != nil {
				return fmt.Errorf("delete spaces_view: %w", err)
			}
		}
		log.Printf("[P0] Deleting lost entry %s (inode %d)", relPath, entry.Inode)
		if err := store.DeleteEntry(entry.Inode); err != nil {
			return fmt.Errorf("delete entry: %w", err)
		}
	}
	return nil
}

// p1 handles DB registration when A_db=0 and A_disk=1.
func p1(store *Store, relPath, archivesRoot string, inode *uint64, isDir *bool, size *int64, mtime *int64, state State) error {
	if inode == nil || mtime == nil {
		return nil
	}

	// Resolve parent inode from DB
	parentIno, err := resolveParentInoFromDB(store, relPath)
	if err != nil {
		return fmt.Errorf("resolve parent ino: %w", err)
	}

	entryType := "blob"
	if isDir != nil && *isDir {
		entryType = "dir"
	} else {
		entryType = ClassifyType(filepath.Base(relPath), false)
	}

	sel := state.SDisk // S_disk=1 → sel=1, S_disk=0 → sel=0
	var sizePtr *int64
	if size != nil && !(isDir != nil && *isDir) {
		sizePtr = size
	}

	log.Printf("[P1] Registering %s (inode %d, sel=%v)", relPath, *inode, sel)
	return store.UpsertEntry(Entry{
		Inode:     *inode,
		ParentIno: parentIno,
		Name:      filepath.Base(relPath),
		Type:      entryType,
		Size:      sizePtr,
		Mtime:     *mtime,
		Selected:  sel,
	})
}

// p2 handles change synchronization when A_dirty or S_dirty.
func p2(ctx context.Context, store *Store, entry *Entry, sv *SpacesView, relPath, archivePath, spacesPath, archivesRoot string, state State, hasQueued func() bool) error {
	if state.ADirty && state.SDirty {
		// Both dirty → conflict
		log.Printf("[P2] Conflict on %s — renaming Archives copy", relPath)
		conflictPath, err := RenameConflict(archivePath)
		if err != nil {
			return fmt.Errorf("rename conflict: %w", err)
		}

		// Register the conflict file as a new entry
		conflictInfo, err := os.Stat(conflictPath)
		if err == nil {
			conflictRel, _ := filepath.Rel(filepath.Dir(archivePath), conflictPath)
			conflictRel = filepath.Join(filepath.Dir(relPath), conflictRel)
			stat, ok := conflictInfo.Sys().(*syscall.Stat_t)
			if ok {
				parentIno, _ := resolveParentInoFromDB(store, conflictRel)
				store.UpsertEntry(Entry{ //nolint:errcheck
					Inode:     stat.Ino,
					ParentIno: parentIno,
					Name:      filepath.Base(conflictPath),
					Type:      ClassifyType(filepath.Base(conflictPath), false),
					Size:      ptrInt64(conflictInfo.Size()),
					Mtime:     conflictInfo.ModTime().UnixNano(),
					Selected:  true,
				})
			}
		}

		// Copy Spaces → Archives (Spaces wins)
		if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
			return fmt.Errorf("copy S→A after conflict: %w", err)
		}
		// Update entry
		return updateEntryFromDisk(store, entry, archivePath, sv, spacesPath)
	}

	if state.ADirty {
		// Archives changed — update DB
		info, err := os.Stat(archivePath)
		if err != nil {
			return fmt.Errorf("stat archive: %w", err)
		}
		if err := store.UpdateEntryMtime(entry.Inode, info.ModTime().UnixNano(), ptrInt64(info.Size())); err != nil {
			return fmt.Errorf("update entry mtime: %w", err)
		}
		entry.Mtime = info.ModTime().UnixNano()
		entry.Size = ptrInt64(info.Size())

		// If selected and S_disk=1, propagate change to Spaces
		if entry.Selected && state.SDisk {
			log.Printf("[P2] Propagating A→S for %s", relPath)
			if err := SafeCopy(ctx, archivePath, spacesPath, hasQueued); err != nil {
				return fmt.Errorf("copy A→S: %w", err)
			}
			if sv != nil {
				spInfo, err := os.Stat(spacesPath)
				if err == nil {
					sv.SyncedMtime = spInfo.ModTime().UnixNano()
					sv.CheckedAt = nowNano()
					if err := store.UpsertSpacesView(*sv); err != nil {
						return fmt.Errorf("update spaces_view: %w", err)
					}
				}
			}
		}
		return nil
	}

	// S_dirty only — Spaces changed, propagate S→A
	log.Printf("[P2] Propagating S→A for %s", relPath)
	if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
		return fmt.Errorf("copy S→A: %w", err)
	}
	return updateEntryFromDisk(store, entry, archivePath, sv, spacesPath)
}

// p3 handles goal realization when selected ≠ S_disk.
func p3(ctx context.Context, store *Store, entry *Entry, sv *SpacesView, relPath, archivePath, spacesPath, trashRoot string, state State, hasQueued func() bool) error {
	if entry.Selected && !state.SDisk {
		// Need to copy A→S
		log.Printf("[P3] Syncing %s → Spaces", relPath)

		// Re-check selected before copy (UI race guard)
		freshEntry, err := store.GetEntry(entry.Inode)
		if err != nil {
			return fmt.Errorf("re-check entry: %w", err)
		}
		if freshEntry == nil || !freshEntry.Selected {
			log.Printf("[P3] Entry %s deselected before copy, skipping", relPath)
			return nil
		}

		if entry.Type == "dir" {
			// For directories, just create
			if err := os.MkdirAll(spacesPath, 0755); err != nil {
				return fmt.Errorf("mkdir spaces: %w", err)
			}
		} else {
			if err := SafeCopy(ctx, archivePath, spacesPath, hasQueued); err != nil {
				return fmt.Errorf("copy A→S: %w", err)
			}
		}

		// Update spaces_view
		spInfo, err := os.Stat(spacesPath)
		if err == nil {
			if err := store.UpsertSpacesView(SpacesView{
				EntryIno:    entry.Inode,
				SyncedMtime: spInfo.ModTime().UnixNano(),
				CheckedAt:   nowNano(),
			}); err != nil {
				return fmt.Errorf("upsert spaces_view: %w", err)
			}
		}
		return nil
	}

	if !entry.Selected && state.SDisk {
		// Need to remove from Spaces
		log.Printf("[P3] Removing %s from Spaces (soft delete)", relPath)
		if _, err := SoftDelete(spacesPath, trashRoot); err != nil {
			return fmt.Errorf("soft delete: %w", err)
		}
		return nil
	}

	return nil
}

// p4 handles DB consistency when S_db ≠ S_disk.
func p4(store *Store, entry *Entry, sv *SpacesView, relPath, spacesPath string, state State) error {
	if state.SDisk && !state.SDb {
		// S_disk=1 but S_db=0 → create spaces_view
		if entry == nil {
			return nil
		}
		spInfo, err := os.Stat(spacesPath)
		if err != nil {
			return fmt.Errorf("stat spaces: %w", err)
		}
		log.Printf("[P4] Creating spaces_view for %s", relPath)
		return store.UpsertSpacesView(SpacesView{
			EntryIno:    entry.Inode,
			SyncedMtime: spInfo.ModTime().UnixNano(),
			CheckedAt:   nowNano(),
		})
	}

	if !state.SDisk && state.SDb {
		// S_disk=0 but S_db=1 → delete spaces_view
		if sv == nil {
			return nil
		}
		log.Printf("[P4] Removing stale spaces_view for %s", relPath)
		return store.DeleteSpacesView(sv.EntryIno)
	}

	return nil
}

// --- helpers ---

// lookupDB finds the entry and spaces_view for a relative path.
// It walks the path components to find the entry by parent_ino+name.
func lookupDB(store *Store, archivesRoot, relPath string) (*Entry, *SpacesView, error) {
	parts := splitPath(relPath)
	var parentIno *uint64

	var entry *Entry
	for _, part := range parts {
		e, err := store.GetEntryByPath(parentIno, part)
		if err != nil {
			return nil, nil, err
		}
		if e == nil {
			return nil, nil, nil // not in DB
		}
		entry = e
		ino := e.Inode
		parentIno = &ino
	}

	if entry == nil {
		return nil, nil, nil
	}

	sv, err := store.GetSpacesView(entry.Inode)
	if err != nil {
		return nil, nil, err
	}

	return entry, sv, nil
}

// resolveParentInoFromDB walks the parent path components to find
// the parent's inode in the DB.
func resolveParentInoFromDB(store *Store, relPath string) (*uint64, error) {
	dir := filepath.Dir(relPath)
	if dir == "." || dir == "" {
		return nil, nil
	}

	parts := splitPath(dir)
	var parentIno *uint64
	for _, part := range parts {
		e, err := store.GetEntryByPath(parentIno, part)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return nil, fmt.Errorf("parent path component %q not found in DB", part)
		}
		ino := e.Inode
		parentIno = &ino
	}
	return parentIno, nil
}

// splitPath splits a relative path into its components.
func splitPath(relPath string) []string {
	var parts []string
	for relPath != "" && relPath != "." {
		dir, file := filepath.Split(relPath)
		if file != "" {
			parts = append([]string{file}, parts...)
		}
		relPath = filepath.Clean(dir)
	}
	return parts
}

// statFile returns mtime, isDir, inode, size for a path.
// All return nil if the file doesn't exist.
func statFile(path string) (mtime *int64, isDir *bool, inode *uint64, size *int64) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, nil, nil
	}

	m := info.ModTime().UnixNano()
	d := info.IsDir()
	s := info.Size()
	mtime = &m
	isDir = &d
	size = &s

	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		ino := stat.Ino
		inode = &ino
	}

	return
}

// updateEntryFromDisk refreshes entry mtime/size from Archives disk
// and spaces_view from Spaces disk.
func updateEntryFromDisk(store *Store, entry *Entry, archivePath string, sv *SpacesView, spacesPath string) error {
	aInfo, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("stat archive: %w", err)
	}
	if err := store.UpdateEntryMtime(entry.Inode, aInfo.ModTime().UnixNano(), ptrInt64(aInfo.Size())); err != nil {
		return fmt.Errorf("update entry: %w", err)
	}

	if sv != nil {
		sInfo, err := os.Stat(spacesPath)
		if err == nil {
			sv.SyncedMtime = sInfo.ModTime().UnixNano()
			sv.CheckedAt = nowNano()
			if err := store.UpsertSpacesView(*sv); err != nil {
				return fmt.Errorf("update spaces_view: %w", err)
			}
		}
	}
	return nil
}

func ptrInt64(v int64) *int64 { return &v }

func nowNano() int64 {
	return nowFunc().UnixNano()
}

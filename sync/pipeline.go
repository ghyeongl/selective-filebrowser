package sync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
)

// logState logs all 7 state variables at DEBUG level.
func logState(l *slog.Logger, msg, relPath string, state State) {
	l.Debug(msg,
		"path", relPath,
		"A_disk", state.ADisk,
		"A_db", state.ADb,
		"S_disk", state.SDisk,
		"S_db", state.SDb,
		"selected", state.Selected,
		"A_dirty", state.ADirty,
		"S_dirty", state.SDirty,
		"scenario", state.Scenario(),
		"status", state.UIStatus(),
	)
}

// RunPipeline evaluates a single relative path through P0→P4.
// It gathers the 7 variables, determines the scenario, and executes
// the appropriate actions to converge toward the target state.
func RunPipeline(ctx context.Context, relPath string, store *Store, archivesRoot, spacesRoot, trashRoot string, hasQueued func() bool) error {
	l := sub("pipeline")
	l.Debug("pipeline start", "path", relPath)

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

	if logEnabled(slog.LevelDebug) {
		logState(l, "state gathered", relPath, state)
	}
	l.Info("pipeline evaluated", "path", relPath, "scenario", scenario, "status", state.UIStatus())

	// P0: Archives disk recovery (A_disk=0)
	if !state.ADisk {
		l.Debug("P0 enter: archives recovery", "path", relPath, "S_disk", state.SDisk)
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
		if logEnabled(slog.LevelDebug) {
			logState(l, "P0 done, state re-gathered", relPath, state)
		}
	}

	// P1: DB registration (A_db=0, A_disk=1 guaranteed after P0)
	if !state.ADb && state.ADisk {
		l.Debug("P1 enter: DB registration", "path", relPath, "inode", archiveInode, "isDir", archiveIsDir)
		if err := p1(store, relPath, archivesRoot, archiveInode, archiveIsDir, archiveSize, archiveMtime, state); err != nil {
			return fmt.Errorf("P1: %w", err)
		}
		// Re-gather
		entry, sv, err = lookupDB(store, archivesRoot, relPath)
		if err != nil {
			return fmt.Errorf("db lookup post-P1: %w", err)
		}
		state = ComputeState(entry, sv, archiveMtime, spacesMtime)
		if logEnabled(slog.LevelDebug) {
			logState(l, "P1 done, state re-gathered", relPath, state)
		}
	}

	// P2: Change sync (A_dirty or S_dirty)
	if state.ADirty || state.SDirty {
		l.Debug("P2 enter: change sync", "path", relPath, "A_dirty", state.ADirty, "S_dirty", state.SDirty)
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
		if logEnabled(slog.LevelDebug) {
			logState(l, "P2 done, state re-gathered", relPath, state)
		}
	}

	// P3: Goal realization (selected ≠ S_disk)
	if entry != nil && entry.Selected != state.SDisk {
		l.Debug("P3 enter: goal realization", "path", relPath, "selected", entry.Selected, "S_disk", state.SDisk)
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
		if logEnabled(slog.LevelDebug) {
			logState(l, "P3 done, state re-gathered", relPath, state)
		}
	}

	// P4: DB consistency (S_db ≠ S_disk)
	if state.SDb != state.SDisk {
		l.Debug("P4 enter: DB consistency", "path", relPath, "S_db", state.SDb, "S_disk", state.SDisk)
		if err := p4(store, entry, sv, relPath, spacesPath, state); err != nil {
			return fmt.Errorf("P4: %w", err)
		}
		l.Debug("P4 done", "path", relPath)
	}

	l.Debug("pipeline complete", "path", relPath)
	return nil
}

// p0 handles Archives disk recovery when A_disk=0.
func p0(ctx context.Context, store *Store, entry *Entry, sv *SpacesView, relPath, archivePath, spacesPath string, state State, hasQueued func() bool) error {
	l := sub("P0")
	if state.SDisk {
		// S_disk=1 → copy S→A to recover
		l.Info("recovering from Spaces", "path", relPath)
		if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
			return err
		}
		l.Debug("SafeCopy S->A done", "path", relPath)
		// Update entries mtime/size if entry exists
		if entry != nil {
			info, err := os.Stat(archivePath)
			if err == nil {
				if err := store.UpdateEntryMtime(entry.Inode, info.ModTime().UnixNano(), ptrInt64(info.Size())); err != nil {
					return fmt.Errorf("update entry after recovery: %w", err)
				}
				l.Debug("updated mtime after recovery", "inode", entry.Inode)
			} else {
				l.Warn("stat failed after recovery", "path", archivePath, "err", err)
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
			l.Info("deleting lost entry and spaces_view", "path", relPath, "inode", entry.Inode)
		} else {
			l.Info("deleting lost entry", "path", relPath, "inode", entry.Inode)
		}
		if err := store.DeleteEntry(entry.Inode); err != nil {
			return fmt.Errorf("delete entry: %w", err)
		}
	} else {
		l.Debug("no-op: no DB records", "path", relPath)
	}
	return nil
}

// p1 handles DB registration when A_db=0 and A_disk=1.
func p1(store *Store, relPath, archivesRoot string, inode *uint64, isDir *bool, size *int64, mtime *int64, state State) error {
	l := sub("P1")
	if inode == nil || mtime == nil {
		l.Debug("skip: no inode/mtime", "path", relPath)
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

	l.Info("registering entry", "path", relPath, "inode", *inode, "type", entryType, "selected", sel, "parentIno", parentIno)
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
	l := sub("P2")
	if state.ADirty && state.SDirty {
		// Both dirty → conflict
		l.Warn("conflict: both dirty", "path", relPath)

		// 1) DB: update existing entry's name to conflict name
		conflictName := ConflictName(archivePath)
		if err := store.UpdateEntryName(entry.Inode, conflictName); err != nil {
			return fmt.Errorf("update entry name for conflict: %w", err)
		}
		l.Debug("renamed DB entry", "inode", entry.Inode, "newName", conflictName)

		// 2) Disk: rename archive to conflict name
		conflictPath := filepath.Join(filepath.Dir(archivePath), conflictName)
		if err := os.Rename(archivePath, conflictPath); err != nil {
			return fmt.Errorf("rename conflict: %w", err)
		}
		l.Debug("renamed archive file", "from", archivePath, "to", conflictPath)

		// 3) SafeCopy S→A (Spaces wins) → creates new file with new inode
		if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
			return fmt.Errorf("copy S→A after conflict: %w", err)
		}
		l.Debug("SafeCopy S->A after conflict", "path", relPath)

		// 4) Register the new archive file (new inode) in DB
		aInfo, err := os.Stat(archivePath)
		if err != nil {
			return fmt.Errorf("stat new archive: %w", err)
		}
		newStat, ok := aInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("failed to get inode for new archive")
		}
		if err := store.UpsertEntry(Entry{
			Inode:     newStat.Ino,
			ParentIno: entry.ParentIno,
			Name:      entry.Name,
			Type:      entry.Type,
			Size:      ptrInt64(aInfo.Size()),
			Mtime:     aInfo.ModTime().UnixNano(),
			Selected:  true,
		}); err != nil {
			return fmt.Errorf("register new archive entry: %w", err)
		}
		l.Info("conflict resolved", "path", relPath, "newInode", newStat.Ino, "oldInode", entry.Inode)

		// 5) Update spaces_view for the new entry
		if sv != nil {
			sInfo, err := os.Stat(spacesPath)
			if err == nil {
				sv.EntryIno = newStat.Ino
				sv.SyncedMtime = sInfo.ModTime().UnixNano()
				sv.CheckedAt = nowNano()
				if err := store.UpsertSpacesView(*sv); err != nil {
					return fmt.Errorf("update spaces_view: %w", err)
				}
				l.Debug("spaces_view updated for conflict winner", "inode", newStat.Ino)
			}
		}
		return nil
	}

	if state.ADirty {
		// Archives changed — update DB
		info, err := os.Stat(archivePath)
		if err != nil {
			return fmt.Errorf("stat archive: %w", err)
		}
		l.Debug("archive changed", "path", relPath, "oldMtime", entry.Mtime, "newMtime", info.ModTime().UnixNano(), "newSize", info.Size())
		if err := store.UpdateEntryMtime(entry.Inode, info.ModTime().UnixNano(), ptrInt64(info.Size())); err != nil {
			return fmt.Errorf("update entry mtime: %w", err)
		}
		entry.Mtime = info.ModTime().UnixNano()
		entry.Size = ptrInt64(info.Size())

		// If selected and S_disk=1, propagate change to Spaces
		if entry.Selected && state.SDisk {
			l.Info("propagating A->S", "path", relPath)
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
		} else {
			l.Debug("DB updated, no propagation", "path", relPath, "selected", entry.Selected, "S_disk", state.SDisk)
		}
		return nil
	}

	// S_dirty only — Spaces changed, propagate S→A
	l.Info("propagating S->A", "path", relPath)
	if err := SafeCopy(ctx, spacesPath, archivePath, hasQueued); err != nil {
		return fmt.Errorf("copy S→A: %w", err)
	}
	return updateEntryFromDisk(store, entry, archivePath, sv, spacesPath)
}

// p3 handles goal realization when selected ≠ S_disk.
func p3(ctx context.Context, store *Store, entry *Entry, sv *SpacesView, relPath, archivePath, spacesPath, trashRoot string, state State, hasQueued func() bool) error {
	l := sub("P3")
	if entry.Selected && !state.SDisk {
		// Need to copy A→S
		l.Info("syncing to Spaces", "path", relPath, "type", entry.Type)

		// Re-check selected before copy (UI race guard)
		freshEntry, err := store.GetEntry(entry.Inode)
		if err != nil {
			return fmt.Errorf("re-check entry: %w", err)
		}
		if freshEntry == nil || !freshEntry.Selected {
			l.Info("deselected before copy, skipping", "path", relPath, "inode", entry.Inode)
			return nil
		}

		if entry.Type == "dir" {
			// For directories, just create
			if err := os.MkdirAll(spacesPath, 0755); err != nil {
				return fmt.Errorf("mkdir spaces: %w", err)
			}
			l.Debug("mkdir Spaces", "path", spacesPath)
		} else {
			if err := SafeCopy(ctx, archivePath, spacesPath, hasQueued); err != nil {
				return fmt.Errorf("copy A→S: %w", err)
			}
			l.Debug("SafeCopy A->S done", "path", relPath)
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
			l.Debug("spaces_view upserted", "inode", entry.Inode)
		}
		return nil
	}

	if !entry.Selected && state.SDisk {
		// Need to remove from Spaces
		l.Info("removing from Spaces", "path", relPath)
		trashPath, err := SoftDelete(spacesPath, trashRoot)
		if err != nil {
			return fmt.Errorf("soft delete: %w", err)
		}
		l.Debug("soft-deleted", "path", relPath, "trashPath", trashPath)
		return nil
	}

	return nil
}

// p4 handles DB consistency when S_db ≠ S_disk.
func p4(store *Store, entry *Entry, sv *SpacesView, relPath, spacesPath string, state State) error {
	l := sub("P4")
	if state.SDisk && !state.SDb {
		// S_disk=1 but S_db=0 → create spaces_view
		if entry == nil {
			l.Debug("skip: no entry", "path", relPath)
			return nil
		}
		spInfo, err := os.Stat(spacesPath)
		if err != nil {
			return fmt.Errorf("stat spaces: %w", err)
		}
		l.Info("creating spaces_view", "path", relPath, "inode", entry.Inode)
		return store.UpsertSpacesView(SpacesView{
			EntryIno:    entry.Inode,
			SyncedMtime: spInfo.ModTime().UnixNano(),
			CheckedAt:   nowNano(),
		})
	}

	if !state.SDisk && state.SDb {
		// S_disk=0 but S_db=1 → delete spaces_view
		if sv == nil {
			l.Debug("skip: no spaces_view", "path", relPath)
			return nil
		}
		l.Info("removing stale spaces_view", "path", relPath, "inode", sv.EntryIno)
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
			if logEnabled(slog.LevelDebug) {
				sub("pipeline").Debug("lookupDB: not found", "path", relPath)
			}
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

	if logEnabled(slog.LevelDebug) {
		sub("pipeline").Debug("lookupDB: found", "path", relPath, "inode", entry.Inode, "hasSV", sv != nil)
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
	sub("pipeline").Debug("entry refreshed from disk", "path", archivePath, "inode", entry.Inode)
	return nil
}

func ptrInt64(v int64) *int64 { return &v }

func nowNano() int64 {
	return nowFunc().UnixNano()
}

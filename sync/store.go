package sync

import (
	"database/sql"
	"fmt"
	"sort"
)

// Store provides CRUD operations on the sync database.
type Store struct {
	db *sql.DB
}

// NewStore creates a Store backed by the given database.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// UpsertEntry inserts or updates an entry keyed by path (parent_ino + name).
// Handles rm+touch: same path, new inode → ON CONFLICT updates inode.
// Also handles inode reuse/move: if the same inode exists at a different path,
// the stale entry (and its subtree if directory) is removed first.
func (s *Store) UpsertEntry(e Entry) error {
	// Remove stale entry if this inode exists at a different path.
	// Recursive CTE handles directories: deletes the stale dir and all descendants.
	// spaces_view rows are cleaned up via ON DELETE CASCADE.
	_, err := s.db.Exec(`
		WITH RECURSIVE subtree(ino) AS (
			SELECT inode FROM entries
			WHERE inode = ? AND NOT (parent_ino = ? AND name = ?)
			UNION ALL
			SELECT e.inode FROM entries e JOIN subtree s ON e.parent_ino = s.ino
		)
		DELETE FROM entries WHERE inode IN (SELECT ino FROM subtree)
	`, e.Inode, e.ParentIno, e.Name)
	if err != nil {
		return fmt.Errorf("cleanup stale inode: %w", err)
	}

	// Type conflict: if an entry exists at this (parent_ino, name) with a different type,
	// rename the old entry to {name}-conflict and mark it selected for user visibility.
	var oldIno uint64
	var oldType string
	err = s.db.QueryRow(
		`SELECT inode, type FROM entries WHERE parent_ino = ? AND name = ?`,
		e.ParentIno, e.Name,
	).Scan(&oldIno, &oldType)
	if err == nil && oldType != e.Type {
		conflictName := e.Name + "-conflict"
		_, err = s.db.Exec(
			`UPDATE entries SET name = ?, selected = 1 WHERE inode = ?`,
			conflictName, oldIno,
		)
		if err != nil {
			return fmt.Errorf("rename type conflict: %w", err)
		}
		sub("store").Info("type conflict renamed", "oldIno", oldIno, "oldType", oldType, "newType", e.Type, "conflictName", conflictName)
	}

	_, err = s.db.Exec(`
		INSERT INTO entries (inode, parent_ino, name, type, size, mtime, selected)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(parent_ino, name) DO UPDATE SET
			type  = excluded.type,
			size  = excluded.size,
			mtime = excluded.mtime
	`, e.Inode, e.ParentIno, e.Name, e.Type, e.Size, e.Mtime, e.Selected)
	if err != nil {
		sub("store").Error("UpsertEntry failed", "inode", e.Inode, "name", e.Name, "err", err)
		return fmt.Errorf("upsert entry: %w", err)
	}
	return nil
}

// UpdateEntryName updates only the name of an existing entry.
func (s *Store) UpdateEntryName(inode uint64, newName string) error {
	_, err := s.db.Exec(`UPDATE entries SET name = ? WHERE inode = ?`, newName, inode)
	if err != nil {
		return fmt.Errorf("update entry name: %w", err)
	}
	return nil
}

// UpdateEntryMtime updates only the mtime and size of an existing entry.
func (s *Store) UpdateEntryMtime(inode uint64, mtime int64, size *int64) error {
	_, err := s.db.Exec(`
		UPDATE entries SET mtime = ?, size = ? WHERE inode = ?
	`, mtime, size, inode)
	if err != nil {
		return fmt.Errorf("update entry mtime: %w", err)
	}
	return nil
}

// GetEntry retrieves an entry by inode.
func (s *Store) GetEntry(inode uint64) (*Entry, error) {
	e := &Entry{}
	err := s.db.QueryRow(`
		SELECT inode, parent_ino, name, type, size, mtime, selected
		FROM entries WHERE inode = ?
	`, inode).Scan(&e.Inode, &e.ParentIno, &e.Name, &e.Type, &e.Size, &e.Mtime, &e.Selected)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}
	return e, nil
}

// GetEntryByPath retrieves an entry by parent inode and name.
// Use parentIno=0 for root-level entries.
func (s *Store) GetEntryByPath(parentIno uint64, name string) (*Entry, error) {
	e := &Entry{}
	err := s.db.QueryRow(`
		SELECT inode, parent_ino, name, type, size, mtime, selected
		FROM entries WHERE parent_ino = ? AND name = ?
	`, parentIno, name).Scan(&e.Inode, &e.ParentIno, &e.Name, &e.Type, &e.Size, &e.Mtime, &e.Selected)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get entry by path: %w", err)
	}
	return e, nil
}

// DeleteEntry removes an entry by inode.
func (s *Store) DeleteEntry(inode uint64) error {
	_, err := s.db.Exec("DELETE FROM entries WHERE inode = ?", inode)
	if err != nil {
		return fmt.Errorf("delete entry: %w", err)
	}
	return nil
}

// DeleteEntryRecursive removes an entry and all its descendants.
// spaces_view rows are cleaned up automatically via ON DELETE CASCADE.
func (s *Store) DeleteEntryRecursive(inode uint64) error {
	_, err := s.db.Exec(`
		WITH RECURSIVE subtree(ino) AS (
			SELECT ?
			UNION ALL
			SELECT e.inode FROM entries e JOIN subtree s ON e.parent_ino = s.ino
		)
		DELETE FROM entries WHERE inode IN (SELECT ino FROM subtree)
	`, inode)
	if err != nil {
		return fmt.Errorf("delete entry recursive: %w", err)
	}
	return nil
}

// ListChildren returns all direct children of the given parent inode.
// Use parentIno=0 for root-level entries.
func (s *Store) ListChildren(parentIno uint64) ([]Entry, error) {
	rows, err := s.db.Query(`
		SELECT inode, parent_ino, name, type, size, mtime, selected
		FROM entries WHERE parent_ino = ?
		ORDER BY type = 'dir' DESC, name ASC
	`, parentIno)
	if err != nil {
		return nil, fmt.Errorf("list children: %w", err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Inode, &e.ParentIno, &e.Name, &e.Type, &e.Size, &e.Mtime, &e.Selected); err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SetSelected updates the selected flag for the given inodes.
// If recursive is true, all descendants of directory entries are also updated.
// This is used for user intent, so the whole subtree is updated immediately.
func (s *Store) SetSelected(inodes []uint64, selected bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, ino := range inodes {
		// 0 is the virtual-root sentinel used by parent_ino, never a real
		// entry. Letting it through means setSelectedRecursive walks
		// "WHERE parent_ino = 0", which matches every top-level entry, so a
		// single {"inodes":[0]} would select or deselect the whole archive.
		if ino == 0 {
			continue
		}
		if _, err := tx.Exec("UPDATE entries SET selected = ? WHERE inode = ?", selected, ino); err != nil {
			return fmt.Errorf("update selected: %w", err)
		}
		// Recursively update children
		if err := setSelectedRecursive(tx, ino, selected); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func setSelectedRecursive(tx *sql.Tx, parentIno uint64, selected bool) error {
	rows, err := tx.Query("SELECT inode, type FROM entries WHERE parent_ino = ?", parentIno)
	if err != nil {
		return fmt.Errorf("query children: %w", err)
	}

	var children []struct {
		inode uint64
		typ   string
	}
	for rows.Next() {
		var c struct {
			inode uint64
			typ   string
		}
		if err := rows.Scan(&c.inode, &c.typ); err != nil {
			rows.Close()
			return fmt.Errorf("scan child: %w", err)
		}
		children = append(children, c)
	}
	rows.Close()

	for _, c := range children {
		if _, err := tx.Exec("UPDATE entries SET selected = ? WHERE inode = ?", selected, c.inode); err != nil {
			return fmt.Errorf("update child selected: %w", err)
		}
		if c.typ == "dir" {
			if err := setSelectedRecursive(tx, c.inode, selected); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetSelectedSingle updates the selected flag for a single entry (non-recursive).
// Use this instead of SetSelected when only the specific entry should be updated,
// not its descendants (e.g., pipeline auto-select from external Spaces introduction).
// Descendants must be re-evaluated separately instead of inheriting this bit eagerly.
func (s *Store) SetSelectedSingle(inode uint64, selected bool) error {
	_, err := s.db.Exec("UPDATE entries SET selected = ? WHERE inode = ?", selected, inode)
	if err != nil {
		return fmt.Errorf("set selected single: %w", err)
	}
	return nil
}

// UpdateEntryIdentity refreshes an entry after its Archives file was replaced
// on disk, including the inode.
//
// SafeCopy finishes with an atomic rename, so every S→A copy gives the
// destination a NEW inode. UpdateEntryMtime keys on the inode and cannot change
// it, so the row was left pointing at an inode that no longer exists. Once the
// filesystem recycled that number for some other file, UpsertEntry's
// stale-inode cleanup saw "this inode lives at a different path now" and
// deleted the still-valid row — the entry vanished from the catalog while its
// file sat on disk. spaces_view follows via ON UPDATE CASCADE.
func (s *Store) UpdateEntryIdentity(oldInode, newInode uint64, mtime int64, size *int64) error {
	_, err := s.db.Exec(`
		UPDATE entries SET inode = ?, mtime = ?, size = ? WHERE inode = ?
	`, newInode, mtime, size, oldInode)
	if err != nil {
		return fmt.Errorf("update entry identity: %w", err)
	}
	return nil
}

// UpsertSpacesView inserts or updates a spaces_view record.
func (s *Store) UpsertSpacesView(sv SpacesView) error {
	_, err := s.db.Exec(`
		INSERT INTO spaces_view (entry_ino, synced_mtime, checked_at)
		VALUES (?, ?, ?)
		ON CONFLICT(entry_ino) DO UPDATE SET
			synced_mtime = excluded.synced_mtime,
			checked_at   = excluded.checked_at
	`, sv.EntryIno, sv.SyncedMtime, sv.CheckedAt)
	if err != nil {
		return fmt.Errorf("upsert spaces view: %w", err)
	}
	return nil
}

// GetSpacesView retrieves the spaces_view for a given entry inode.
func (s *Store) GetSpacesView(entryIno uint64) (*SpacesView, error) {
	sv := &SpacesView{}
	err := s.db.QueryRow(`
		SELECT entry_ino, synced_mtime, checked_at
		FROM spaces_view WHERE entry_ino = ?
	`, entryIno).Scan(&sv.EntryIno, &sv.SyncedMtime, &sv.CheckedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get spaces view: %w", err)
	}
	return sv, nil
}

// DeleteSpacesView removes the spaces_view for a given entry inode.
func (s *Store) DeleteSpacesView(entryIno uint64) error {
	_, err := s.db.Exec("DELETE FROM spaces_view WHERE entry_ino = ?", entryIno)
	if err != nil {
		return fmt.Errorf("delete spaces view: %w", err)
	}
	return nil
}

// AggregateSyncedSize returns the total size of entries actually synced to Spaces.
func (s *Store) AggregateSyncedSize() (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRow(`
		SELECT COALESCE(SUM(e.size), 0)
		FROM entries e
		JOIN spaces_view sv ON e.inode = sv.entry_ino
		WHERE e.type != 'dir'
	`).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("aggregate synced size: %w", err)
	}
	if !total.Valid {
		return 0, nil
	}
	return total.Int64, nil
}

// AggregateTotalSize returns the total size of all file entries (excluding directories).
func (s *Store) AggregateTotalSize() (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRow(`
		SELECT SUM(size) FROM entries WHERE type != 'dir'
	`).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("aggregate total size: %w", err)
	}
	if !total.Valid {
		return 0, nil
	}
	return total.Int64, nil
}

// DirSize returns the total and synced recursive file sizes for a directory.
// syncedSize is based on spaces_view presence (files actually on Spaces disk).
// Both values exclude directories themselves.
func (s *Store) DirSize(inode uint64) (totalSize, syncedSize int64, err error) {
	err = s.db.QueryRow(`
		WITH RECURSIVE subtree(ino) AS (
			SELECT ?
			UNION ALL
			SELECT e.inode FROM entries e JOIN subtree s ON e.parent_ino = s.ino
		)
		SELECT
			COALESCE(SUM(e.size), 0),
			COALESCE(SUM(CASE WHEN sv.entry_ino IS NOT NULL THEN e.size ELSE 0 END), 0)
		FROM entries e
		LEFT JOIN spaces_view sv ON e.inode = sv.entry_ino
		WHERE e.inode IN (SELECT ino FROM subtree)
		  AND e.type != 'dir'
	`, inode).Scan(&totalSize, &syncedSize)
	if err != nil {
		return 0, 0, fmt.Errorf("dir size: %w", err)
	}
	return totalSize, syncedSize, nil
}

// ChildCounts returns the total count, selected count, and stable count of
// children for the given parent inode. A child is "stable" when its desired
// state matches reality: selected with spaces_view, or unselected without.
func (s *Store) ChildCounts(parentIno uint64) (total, selectedCount, stableCount int, err error) {
	err = s.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(e.selected), 0),
			COUNT(CASE
				WHEN e.selected = 1 AND sv.entry_ino IS NOT NULL THEN 1
				WHEN e.selected = 0 AND sv.entry_ino IS NULL THEN 1
			END)
		FROM entries e
		LEFT JOIN spaces_view sv ON e.inode = sv.entry_ino
		WHERE e.parent_ino = ?
	`, parentIno).Scan(&total, &selectedCount, &stableCount)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("child counts: %w", err)
	}
	return total, selectedCount, stableCount, nil
}

// StatusCounts returns aggregate file counts by DB-derived status.
// Directories are excluded. Status is approximated from selected flag + spaces_view presence.
func (s *Store) StatusCounts() (archived, synced, syncing, removing int, err error) {
	err = s.db.QueryRow(`
		SELECT
			COUNT(CASE WHEN e.selected = 0 AND sv.entry_ino IS NULL THEN 1 END),
			COUNT(CASE WHEN e.selected = 1 AND sv.entry_ino IS NOT NULL THEN 1 END),
			COUNT(CASE WHEN e.selected = 1 AND sv.entry_ino IS NULL THEN 1 END),
			COUNT(CASE WHEN e.selected = 0 AND sv.entry_ino IS NOT NULL THEN 1 END)
		FROM entries e
		LEFT JOIN spaces_view sv ON e.inode = sv.entry_ino
		WHERE e.type != 'dir'
	`).Scan(&archived, &synced, &syncing, &removing)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("status counts: %w", err)
	}
	return archived, synced, syncing, removing, nil
}

// DupGroup is a set of sibling directories under one parent whose subtrees
// have an identical (file count, total size) signature — the signal doc
// docs/issues/rename-collision-duplicates.md validated by hand with `du -sb`.
type DupGroup struct {
	ParentPath string   `json:"parentPath"`
	FileCount  int      `json:"fileCount"`
	TotalSize  int64    `json:"totalSize"`
	Names      []string `json:"names"`
	Inodes     []uint64 `json:"inodes"`
}

type dupNode struct {
	inode     uint64
	parentIno uint64
	name      string
	isDir     bool
	size      int64
	// filled in bottom-up
	fileCount int
	totalSize int64
}

// DuplicateSiblings reports sibling directories that hold byte-identical
// subtrees — the orphans left behind when a directory is renamed on Spaces
// (the old path is deselected but retained in Archives, and the new path is
// recovered S→A as a fresh tree).
//
// Report-only: it never mutates. Groups with fewer than minFiles files are
// skipped, because empty and near-empty directories collide constantly and
// name similarity alone yields false positives.
//
// ponytail: loads the whole entries table into memory (~150 MB at 1.5M rows).
// Fine for an on-demand maintenance pass; make it a streaming bottom-up walk
// if it ever needs to run on the hot path.
func (s *Store) DuplicateSiblings(minFiles int) ([]DupGroup, error) {
	rows, err := s.db.Query(`SELECT inode, parent_ino, name, type, COALESCE(size, 0) FROM entries`)
	if err != nil {
		return nil, fmt.Errorf("query entries for dup scan: %w", err)
	}
	defer rows.Close()

	nodes := make(map[uint64]*dupNode)
	children := make(map[uint64][]*dupNode)
	for rows.Next() {
		var n dupNode
		var typ string
		if err := rows.Scan(&n.inode, &n.parentIno, &n.name, &typ, &n.size); err != nil {
			return nil, fmt.Errorf("scan entry for dup scan: %w", err)
		}
		n.isDir = typ == "dir"
		nodes[n.inode] = &n
		children[n.parentIno] = append(children[n.parentIno], &n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entries for dup scan: %w", err)
	}

	// Non-nil so the JSON response is [] rather than null — a caller doing
	// groups.find(...) must not have to special-case an empty catalog.
	groups := []DupGroup{}
	// Recurse from the virtual root; children are aggregated before their parent
	// so each directory's signature covers its whole subtree.
	var visit func(parentIno uint64, parentPath string)
	visit = func(parentIno uint64, parentPath string) {
		kids := children[parentIno]
		for _, k := range kids {
			if !k.isDir {
				k.fileCount = 1
				k.totalSize = k.size
				continue
			}
			childPath := k.name
			if parentPath != "" {
				childPath = parentPath + "/" + k.name
			}
			visit(k.inode, childPath)
			for _, gk := range children[k.inode] {
				k.fileCount += gk.fileCount
				k.totalSize += gk.totalSize
			}
		}

		// Group sibling directories by subtree signature.
		bySig := make(map[[2]int64][]*dupNode)
		for _, k := range kids {
			if !k.isDir || k.fileCount < minFiles {
				continue
			}
			sig := [2]int64{int64(k.fileCount), k.totalSize}
			bySig[sig] = append(bySig[sig], k)
		}
		for sig, dupes := range bySig {
			if len(dupes) < 2 {
				continue
			}
			g := DupGroup{
				ParentPath: parentPath,
				FileCount:  int(sig[0]),
				TotalSize:  sig[1],
			}
			// Sort the nodes, not the two slices separately — Names[i] must keep
			// identifying Inodes[i], since a caller acting on the report deletes by inode.
			sort.Slice(dupes, func(i, j int) bool { return dupes[i].name < dupes[j].name })
			for _, d := range dupes {
				g.Names = append(g.Names, d.name)
				g.Inodes = append(g.Inodes, d.inode)
			}
			groups = append(groups, g)
		}
	}
	visit(0, "")

	sort.Slice(groups, func(i, j int) bool {
		if groups[i].TotalSize != groups[j].TotalSize {
			return groups[i].TotalSize > groups[j].TotalSize
		}
		return groups[i].ParentPath < groups[j].ParentPath
	})
	return groups, nil
}

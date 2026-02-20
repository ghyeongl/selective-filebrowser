package sync

import (
	"database/sql"
	"fmt"
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
func (s *Store) UpsertEntry(e Entry) error {
	_, err := s.db.Exec(`
		INSERT INTO entries (inode, parent_ino, name, type, size, mtime, selected)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(parent_ino, name) DO UPDATE SET
			inode = excluded.inode,
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
func (s *Store) SetSelected(inodes []uint64, selected bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, ino := range inodes {
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

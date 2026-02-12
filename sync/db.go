package sync

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

const schema = `
CREATE TABLE IF NOT EXISTS entries (
    inode      INTEGER PRIMARY KEY,
    parent_ino INTEGER REFERENCES entries(inode),
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,
    size       INTEGER,
    mtime      INTEGER NOT NULL,
    selected   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(parent_ino, name)
);

CREATE TABLE IF NOT EXISTS spaces_view (
    entry_ino    INTEGER PRIMARY KEY REFERENCES entries(inode),
    synced_mtime INTEGER NOT NULL,
    checked_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// OpenDB opens (or creates) the sync SQLite database next to the given
// filebrowser database path.
func OpenDB(filebrowserDBPath string) (*sql.DB, error) {
	dir := filepath.Dir(filebrowserDBPath)
	dbPath := filepath.Join(dir, "sync.db")
	return openDBAt(dbPath)
}

// openDBAt opens the database at the exact path. Useful for testing.
func openDBAt(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sync db: %w", err)
	}

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return db, nil
}

func migrate(db *sql.DB) error {
	var version int
	err := db.QueryRow("SELECT value FROM meta WHERE key = 'schema_version'").Scan(&version)
	if err != nil {
		// meta table doesn't exist or no row — fresh database
		if _, execErr := db.Exec(schema); execErr != nil {
			return fmt.Errorf("create schema: %w", execErr)
		}
		_, execErr := db.Exec("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", schemaVersion)
		if execErr != nil {
			return fmt.Errorf("set schema version: %w", execErr)
		}
		return nil
	}

	if version < schemaVersion {
		// Future migrations go here
	}

	return nil
}

package sync

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schemaVersion = 3

const schema = `
CREATE TABLE IF NOT EXISTS entries (
    inode      INTEGER PRIMARY KEY,
    parent_ino INTEGER NOT NULL DEFAULT 0,
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,
    size       INTEGER,
    mtime      INTEGER NOT NULL,
    selected   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(parent_ino, name)
);

CREATE TABLE IF NOT EXISTS spaces_view (
    entry_ino    INTEGER PRIMARY KEY REFERENCES entries(inode) ON UPDATE CASCADE ON DELETE CASCADE,
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
	l := sub("db")
	l.Info("opening sync database", "path", dbPath)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sync db: %w", err)
	}

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	l.Debug("PRAGMA foreign_keys=ON")

	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	l.Debug("PRAGMA journal_mode=WAL")

	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	l.Debug("PRAGMA busy_timeout=5000")

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return db, nil
}

func migrate(db *sql.DB) error {
	l := sub("db")
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
		l.Info("schema created", "version", schemaVersion)
		return nil
	}

	if version < schemaVersion {
		l.Info("schema upgrading", "from", version, "to", schemaVersion)
		if version < 2 {
			if err := migrateV1toV2(db); err != nil {
				return fmt.Errorf("migrate v1→v2: %w", err)
			}
			l.Info("migrated v1→v2")
		}
		if version < 3 {
			if err := migrateV2toV3(db); err != nil {
				return fmt.Errorf("migrate v2→v3: %w", err)
			}
			l.Info("migrated v2→v3")
		}
	} else {
		l.Debug("schema up to date", slog.Int("version", version))
	}

	return nil
}

func migrateV1toV2(db *sql.DB) error {
	// Temporarily disable FK checks for migration
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable FK: %w", err)
	}
	defer db.Exec("PRAGMA foreign_keys = ON") //nolint:errcheck

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmts := []string{
		// Dedup: previous runs may have duplicate inode rows due to NULL parent_ino bug
		`DELETE FROM entries WHERE rowid NOT IN (SELECT MIN(rowid) FROM entries GROUP BY inode)`,

		// Recreate entries table without FK on parent_ino, with NOT NULL DEFAULT 0
		`CREATE TABLE entries_new (
			inode      INTEGER PRIMARY KEY,
			parent_ino INTEGER NOT NULL DEFAULT 0,
			name       TEXT NOT NULL,
			type       TEXT NOT NULL,
			size       INTEGER,
			mtime      INTEGER NOT NULL,
			selected   INTEGER NOT NULL DEFAULT 0,
			UNIQUE(parent_ino, name)
		)`,

		`INSERT INTO entries_new (inode, parent_ino, name, type, size, mtime, selected)
		 SELECT inode, COALESCE(parent_ino, 0), name, type, size, mtime, selected FROM entries`,

		`DROP TABLE entries`,

		`ALTER TABLE entries_new RENAME TO entries`,

		// Recreate spaces_view to point to new entries table
		`CREATE TABLE spaces_view_new (
			entry_ino    INTEGER PRIMARY KEY REFERENCES entries(inode),
			synced_mtime INTEGER NOT NULL,
			checked_at   INTEGER NOT NULL
		)`,

		`INSERT INTO spaces_view_new SELECT * FROM spaces_view`,

		`DROP TABLE spaces_view`,

		`ALTER TABLE spaces_view_new RENAME TO spaces_view`,

		`UPDATE meta SET value = '2' WHERE key = 'schema_version'`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:40], err)
		}
	}

	return tx.Commit()
}

func migrateV2toV3(db *sql.DB) error {
	// Add ON UPDATE CASCADE ON DELETE CASCADE to spaces_view FK.
	// SQLite can't ALTER FK constraints, so we recreate the table.
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable FK: %w", err)
	}
	defer db.Exec("PRAGMA foreign_keys = ON") //nolint:errcheck

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmts := []string{
		`CREATE TABLE spaces_view_new (
			entry_ino    INTEGER PRIMARY KEY REFERENCES entries(inode) ON UPDATE CASCADE ON DELETE CASCADE,
			synced_mtime INTEGER NOT NULL,
			checked_at   INTEGER NOT NULL
		)`,
		`INSERT INTO spaces_view_new SELECT * FROM spaces_view`,
		`DROP TABLE spaces_view`,
		`ALTER TABLE spaces_view_new RENAME TO spaces_view`,
		`UPDATE meta SET value = '3' WHERE key = 'schema_version'`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:40], err)
		}
	}

	return tx.Commit()
}

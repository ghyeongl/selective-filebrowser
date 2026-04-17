package sync

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schemaVersion = 4

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

const ragflowSchema = `
CREATE TABLE IF NOT EXISTS ragflow_hash_cache (
    rel_path    TEXT NOT NULL,
    route_idx   INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    doc_id      TEXT,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (rel_path, route_idx)
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

	// DSN with _pragma params ensures every pooled connection gets these settings.
	// _txlock=immediate prevents deferred-lock-promotion deadlocks.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_txlock=immediate",
		dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sync db: %w", err)
	}

	// Worker goroutine + HTTP handler = 2 connections max.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

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
		if _, execErr := db.Exec(ragflowSchema); execErr != nil {
			return fmt.Errorf("create ragflow schema: %w", execErr)
		}
		_, execErr := db.Exec("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", schemaVersion)
		if execErr != nil {
			return fmt.Errorf("set schema version: %w", execErr)
		}
		l.Info("schema created", "version", schemaVersion)
		return nil
	}

	if version < 3 {
		return fmt.Errorf("unsupported schema version %d (expected %d), delete sync.db to recreate", version, schemaVersion)
	}

	if version == 3 {
		l.Info("migrating schema v3 → v4: adding RAGFlow tables")
		if _, err := db.Exec(ragflowSchema); err != nil {
			return fmt.Errorf("create ragflow tables: %w", err)
		}
		if _, err := db.Exec("UPDATE meta SET value = ? WHERE key = 'schema_version'", schemaVersion); err != nil {
			return fmt.Errorf("update schema version: %w", err)
		}
		l.Info("migration complete", "version", schemaVersion)
	}

	l.Debug("schema up to date", slog.Int("version", version))
	return nil
}

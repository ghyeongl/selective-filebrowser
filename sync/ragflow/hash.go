package ragflow

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"
)

// HashCache provides SHA256-based deduplication for RAGFlow uploads.
type HashCache struct {
	db *sql.DB
}

// NewHashCache creates a hash cache backed by the given database.
func NewHashCache(db *sql.DB) *HashCache {
	return &HashCache{db: db}
}

// Get returns the cached SHA256 and docID for a (relPath, routeIdx) pair.
func (hc *HashCache) Get(relPath string, routeIdx int) (sha256Hash string, docID string, ok bool) {
	err := hc.db.QueryRow(
		`SELECT sha256, COALESCE(doc_id,'') FROM ragflow_hash_cache WHERE rel_path = ? AND route_idx = ?`,
		relPath, routeIdx,
	).Scan(&sha256Hash, &docID)
	if err != nil {
		return "", "", false
	}
	return sha256Hash, docID, true
}

// Set stores or updates the hash and docID for a (relPath, routeIdx) pair.
func (hc *HashCache) Set(relPath string, routeIdx int, sha256Hash, docID string) error {
	_, err := hc.db.Exec(
		`INSERT INTO ragflow_hash_cache (rel_path, route_idx, sha256, doc_id, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (rel_path, route_idx) DO UPDATE SET sha256 = ?, doc_id = ?, updated_at = ?`,
		relPath, routeIdx, sha256Hash, docID, time.Now().UnixNano(),
		sha256Hash, docID, time.Now().UnixNano(),
	)
	return err
}

// Delete removes the cache entry for a (relPath, routeIdx) pair.
func (hc *HashCache) Delete(relPath string, routeIdx int) error {
	_, err := hc.db.Exec(
		`DELETE FROM ragflow_hash_cache WHERE rel_path = ? AND route_idx = ?`,
		relPath, routeIdx,
	)
	return err
}

// HasAny returns true if any cache entry exists for the given relPath (any route).
func (hc *HashCache) HasAny(relPath string) bool {
	var n int
	hc.db.QueryRow(`SELECT 1 FROM ragflow_hash_cache WHERE rel_path = ? LIMIT 1`, relPath).Scan(&n)
	return n == 1
}

// SHA256File computes the SHA256 hash of a file, returning the hex string.
func SHA256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open for hash: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash read: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

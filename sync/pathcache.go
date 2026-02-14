package sync

import (
	gosync "sync"
)

// PathCache maps inode → relative path for fast lookups.
// Parent directory paths are cached so that children under the same
// directory only require one DB walk to resolve the parent.
type PathCache struct {
	mu    gosync.RWMutex
	paths map[uint64]string // inode → relative path
}

// NewPathCache creates an empty path cache.
func NewPathCache() *PathCache {
	return &PathCache{
		paths: make(map[uint64]string),
	}
}

// Get returns the cached path for the given inode, or ("", false).
func (c *PathCache) Get(inode uint64) (string, bool) {
	c.mu.RLock()
	p, ok := c.paths[inode]
	c.mu.RUnlock()
	return p, ok
}

// Set stores a path for the given inode.
func (c *PathCache) Set(inode uint64, path string) {
	c.mu.Lock()
	c.paths[inode] = path
	c.mu.Unlock()
}

// Invalidate removes the cached path for the given inode.
func (c *PathCache) Invalidate(inode uint64) {
	c.mu.Lock()
	delete(c.paths, inode)
	c.mu.Unlock()
}

// Clear removes all cached paths.
func (c *PathCache) Clear() {
	c.mu.Lock()
	c.paths = make(map[uint64]string)
	c.mu.Unlock()
}

// Len returns the number of cached paths.
func (c *PathCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.paths)
}

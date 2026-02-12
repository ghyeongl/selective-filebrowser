package sync

import "sync"

// PathCache maps inode → relative path for fast lookups.
// Parent directory paths are cached so that children under the same
// directory only require one DB walk to resolve the parent.
type PathCache struct {
	mu    sync.RWMutex
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
	defer c.mu.RUnlock()
	p, ok := c.paths[inode]
	return p, ok
}

// Set stores a path for the given inode.
func (c *PathCache) Set(inode uint64, path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths[inode] = path
}

// Invalidate removes the cached path for the given inode.
func (c *PathCache) Invalidate(inode uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.paths, inode)
}

// Clear removes all cached paths.
func (c *PathCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths = make(map[uint64]string)
}

// Len returns the number of cached paths.
func (c *PathCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.paths)
}

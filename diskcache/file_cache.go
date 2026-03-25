package diskcache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type FileCache struct {
	fs      afero.Fs
	maxSize int64 // 0 = unlimited

	// granular locks
	scopedLocks struct {
		sync.Mutex
		sync.Once
		locks map[string]sync.Locker
	}
}

func New(fs afero.Fs, root string, maxSize ...int64) *FileCache {
	var ms int64
	if len(maxSize) > 0 {
		ms = maxSize[0]
	}
	return &FileCache{
		fs:      afero.NewBasePathFs(fs, root),
		maxSize: ms,
	}
}

func (f *FileCache) Store(_ context.Context, key string, value []byte) error {
	mu := f.getScopedLocks(key)
	mu.Lock()
	defer mu.Unlock()

	fileName := f.getFileName(key)
	if err := f.fs.MkdirAll(filepath.Dir(fileName), 0700); err != nil {
		return err
	}

	if err := afero.WriteFile(f.fs, fileName, value, 0700); err != nil {
		return err
	}

	if f.maxSize > 0 {
		f.evict()
	}

	return nil
}

func (f *FileCache) Load(_ context.Context, key string) (value []byte, exist bool, err error) {
	r, ok, err := f.open(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	defer r.Close()

	// Touch mtime to mark as recently used
	fileName := f.getFileName(key)
	_ = f.fs.Chtimes(fileName, time.Now(), time.Now())

	value, err = io.ReadAll(r)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (f *FileCache) Delete(_ context.Context, key string) error {
	mu := f.getScopedLocks(key)
	mu.Lock()
	defer mu.Unlock()

	fileName := f.getFileName(key)
	if err := f.fs.Remove(fileName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

type cacheEntry struct {
	path  string
	size  int64
	mtime time.Time
}

func (f *FileCache) evict() {
	var entries []cacheEntry
	var totalSize int64

	_ = afero.Walk(f.fs, ".", func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		entries = append(entries, cacheEntry{path: path, size: info.Size(), mtime: info.ModTime()})
		totalSize += info.Size()
		return nil
	})

	if totalSize <= f.maxSize {
		return
	}

	// Sort oldest first
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].mtime.Before(entries[j].mtime)
	})

	for _, e := range entries {
		if totalSize <= f.maxSize {
			break
		}
		_ = f.fs.Remove(e.path)
		totalSize -= e.size
	}
}

func (f *FileCache) open(key string) (afero.File, bool, error) {
	fileName := f.getFileName(key)
	file, err := f.fs.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return file, true, nil
}

// getScopedLocks pull lock from the map if found or create a new one
func (f *FileCache) getScopedLocks(key string) (lock sync.Locker) {
	f.scopedLocks.Do(func() { f.scopedLocks.locks = map[string]sync.Locker{} })

	f.scopedLocks.Lock()
	lock, ok := f.scopedLocks.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		f.scopedLocks.locks[key] = lock
	}
	f.scopedLocks.Unlock()

	return lock
}

func (f *FileCache) getFileName(key string) string {
	hasher := sha1.New()
	_, _ = hasher.Write([]byte(key))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return fmt.Sprintf("%s/%s/%s", hash[:1], hash[1:3], hash)
}

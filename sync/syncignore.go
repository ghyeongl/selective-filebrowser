package sync

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// SyncIgnore holds patterns loaded from a .syncignore file.
// Entries matching any pattern are excluded from scanning and watching.
type SyncIgnore struct {
	patterns []ignorePattern
}

type ignorePattern struct {
	pattern string
	dirOnly bool // trailing / in source line
}

// LoadSyncIgnore reads a .syncignore file and returns a SyncIgnore.
// If the file does not exist or cannot be read, returns an empty SyncIgnore
// (nothing is ignored).
func LoadSyncIgnore(path string) *SyncIgnore {
	si := &SyncIgnore{}

	f, err := os.Open(path)
	if err != nil {
		return si
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		p := ignorePattern{pattern: line}
		if strings.HasSuffix(line, "/") {
			p.pattern = strings.TrimSuffix(line, "/")
			p.dirOnly = true
		}
		si.patterns = append(si.patterns, p)
	}

	return si
}

// IsIgnored returns true if the given entry name matches any ignore pattern.
// For dirOnly patterns, isDir must be true for the pattern to match.
func (si *SyncIgnore) IsIgnored(name string, isDir bool) bool {
	if si == nil {
		return false
	}
	for _, p := range si.patterns {
		if p.dirOnly && !isDir {
			continue
		}
		if matched, _ := filepath.Match(p.pattern, name); matched {
			return true
		}
	}
	return false
}

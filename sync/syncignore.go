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

// defaultIgnorePatterns are transfer-tool and app artifacts that must never be
// promoted into Archives. They are always applied; the user's .syncignore adds
// to this list and cannot remove entries from it.
//
// Deliberately narrow — only tool-generated artifacts. A blanket `*.tmp` is not
// included: a user's own file may legitimately end in .tmp.
var defaultIgnorePatterns = []string{
	".syncthing.*.tmp",  // Syncthing in-flight transfer temp
	"~syncthing~*.tmp",  // legacy Syncthing temp
	".stfolder",         // Syncthing folder marker
	".stversions",       // Syncthing version history
	"*.sync-conflict-*", // Syncthing conflict copies
	".trash",            // this app's SoftDelete destination (see fileops.go)
}

// LoadSyncIgnore reads a .syncignore file and returns a SyncIgnore.
// It always starts from the built-in defaultIgnorePatterns; the user file only
// adds patterns and can neither replace nor disable the defaults. If the file
// does not exist or cannot be read, the defaults still apply.
func LoadSyncIgnore(path string) *SyncIgnore {
	si := &SyncIgnore{patterns: make([]ignorePattern, 0, len(defaultIgnorePatterns))}
	for _, p := range defaultIgnorePatterns {
		si.patterns = append(si.patterns, ignorePattern{pattern: p})
	}

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

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
	pattern  string
	dirOnly  bool // trailing / in source line
	anchored bool // leading / in source line — matches from the sync root only
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
	"*.sync-tmp",        // this app's SafeCopy temp (see safeTmpPath)
	".sync-tmp-*",       // ditto, hashed form for over-long names
	"/.trash",           // this app's SoftDelete destination — root only
}

// LoadSyncIgnore reads a .syncignore file and returns a SyncIgnore.
// It always starts from the built-in defaultIgnorePatterns; the user file only
// adds patterns and can neither replace nor disable the defaults. If the file
// does not exist or cannot be read, the defaults still apply.
func LoadSyncIgnore(path string) *SyncIgnore {
	si := &SyncIgnore{patterns: make([]ignorePattern, 0, len(defaultIgnorePatterns))}
	for _, p := range defaultIgnorePatterns {
		si.patterns = append(si.patterns, parseIgnorePattern(p))
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
		si.patterns = append(si.patterns, parseIgnorePattern(line))
	}

	return si
}

// parseIgnorePattern turns one source line into a pattern. A trailing "/" makes
// it directory-only; a leading "/" anchors it to the sync root.
func parseIgnorePattern(line string) ignorePattern {
	p := ignorePattern{pattern: line}
	if strings.HasSuffix(p.pattern, "/") {
		p.pattern = strings.TrimSuffix(p.pattern, "/")
		p.dirOnly = true
	}
	if strings.HasPrefix(p.pattern, "/") {
		p.pattern = strings.TrimPrefix(p.pattern, "/")
		p.anchored = true
	}
	return p
}

// IsIgnored reports whether relPath — a path relative to a sync root — is ignored.
//
// Unanchored patterns match any single path component, so everything beneath an
// ignored directory is ignored too. Anchored patterns ("/foo/bar") match only
// that path and its descendants, starting at the root. isDir describes relPath
// itself and is only consulted for dirOnly patterns matching the entry exactly;
// ancestors are directories by construction.
func (si *SyncIgnore) IsIgnored(relPath string, isDir bool) bool {
	if si == nil {
		return false
	}
	relPath = strings.Trim(filepath.ToSlash(relPath), "/")
	if relPath == "" || relPath == "." {
		return false
	}
	parts := strings.Split(relPath, "/")

	for _, p := range si.patterns {
		if p.anchored {
			if matchesAnchored(p, parts, isDir) {
				return true
			}
			continue
		}
		for i, part := range parts {
			exact := i == len(parts)-1
			if p.dirOnly && exact && !isDir {
				continue
			}
			if matched, _ := filepath.Match(p.pattern, part); matched {
				return true
			}
		}
	}
	return false
}

// matchesAnchored reports whether an anchored pattern matches parts from the
// root, either exactly or as a prefix (an ignored directory's descendants).
func matchesAnchored(p ignorePattern, parts []string, isDir bool) bool {
	pat := strings.Split(p.pattern, "/")
	if len(pat) > len(parts) {
		return false
	}
	exact := len(pat) == len(parts)
	if p.dirOnly && exact && !isDir {
		return false
	}
	for i, seg := range pat {
		if matched, _ := filepath.Match(seg, parts[i]); !matched {
			return false
		}
	}
	return true
}

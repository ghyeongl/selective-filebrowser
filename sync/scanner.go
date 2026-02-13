package sync

import (
	"mime"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// FileStat holds the stat information needed for sync operations.
type FileStat struct {
	Inode uint64
	Name  string
	Size  int64
	Mtime int64 // nanoseconds
	IsDir bool
}

// ScanDir walks a directory tree and returns FileStat for each entry.
// relativeTo is used for path-based matching (the returned paths are relative).
func ScanDir(root string) (map[string]FileStat, error) {
	l := sub("scanner")
	l.Debug("scan start", "root", root)
	result := make(map[string]FileStat)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			l.Warn("scan walk error", "path", path, "err", err)
			return err
		}

		// Skip the root itself
		if path == root {
			return nil
		}

		// Skip .sync-conflict files
		if strings.Contains(d.Name(), ".sync-conflict-") {
			return nil
		}

		// Skip hidden files/dirs
		if strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			l.Warn("scan stat error", "path", path, "err", err)
			return err
		}

		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil
		}

		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		result[relPath] = FileStat{
			Inode: stat.Ino,
			Name:  d.Name(),
			Size:  info.Size(),
			Mtime: info.ModTime().UnixNano(),
			IsDir: d.IsDir(),
		}

		return nil
	})

	l.Debug("scan complete", "root", root, "entries", len(result))
	return result, err
}

// ClassifyType determines the file type from its extension.
// Returns one of: "dir", "video", "audio", "image", "pdf", "text", "blob"
func ClassifyType(name string, isDir bool) string {
	if isDir {
		return "dir"
	}

	ext := strings.ToLower(filepath.Ext(name))
	if ext == "" {
		return "blob"
	}

	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		return classifyByExtension(ext)
	}

	parts := strings.SplitN(mimeType, "/", 2)
	switch parts[0] {
	case "video":
		return "video"
	case "audio":
		return "audio"
	case "image":
		return "image"
	case "text":
		return "text"
	case "application":
		return classifyApplication(parts[1], ext)
	}

	return "blob"
}

func classifyByExtension(ext string) string {
	switch ext {
	case ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v":
		return "video"
	case ".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".opus":
		return "audio"
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".tiff", ".ico":
		return "image"
	case ".pdf":
		return "pdf"
	case ".txt", ".md", ".json", ".xml", ".csv", ".yaml", ".yml", ".toml",
		".go", ".py", ".js", ".ts", ".html", ".css", ".sh", ".bash",
		".c", ".h", ".cpp", ".java", ".rs", ".rb", ".php", ".vue", ".sql":
		return "text"
	}
	return "blob"
}

func classifyApplication(subtype, ext string) string {
	if subtype == "pdf" || ext == ".pdf" {
		return "pdf"
	}
	if strings.Contains(subtype, "json") || strings.Contains(subtype, "xml") ||
		strings.Contains(subtype, "javascript") || strings.Contains(subtype, "typescript") {
		return "text"
	}
	return "blob"
}

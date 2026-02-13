package sync

import "time"

// nowFunc is the time source, replaceable in tests.
var nowFunc = time.Now

// Entry represents a file or directory in the Archives catalog.
// The inode is from the Archives filesystem only.
type Entry struct {
	Inode     uint64  `json:"inode"`
	ParentIno uint64  `json:"parentIno"`
	Name      string  `json:"name"`
	Type      string  `json:"type"` // "dir"|"video"|"audio"|"image"|"pdf"|"text"|"blob"
	Size      *int64  `json:"size"` // nil for directories
	Mtime     int64   `json:"mtime"` // nanoseconds
	Selected  bool    `json:"selected"`
}

// SpacesView tracks the Spaces copy metadata for a given entry.
type SpacesView struct {
	EntryIno    uint64 `json:"entryIno"`
	SyncedMtime int64  `json:"syncedMtime"` // nanoseconds
	CheckedAt   int64  `json:"checkedAt"`   // nanoseconds
}

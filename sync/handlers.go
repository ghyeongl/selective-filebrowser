package sync

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// SyncEntryResponse is a single entry in the API response.
type SyncEntryResponse struct {
	Inode              uint64 `json:"inode"`
	Name               string `json:"name"`
	Type               string `json:"type"`
	Size               *int64 `json:"size"`
	Mtime              int64  `json:"mtime"`
	Selected           bool   `json:"selected"`
	Status             string `json:"status"`
	ChildTotalCount    *int   `json:"childTotalCount,omitempty"`
	ChildSelectedCount *int   `json:"childSelectedCount,omitempty"`
}

// SyncStatsResponse holds aggregate sync statistics.
type SyncStatsResponse struct {
	DiskTotal    int64 `json:"diskTotal"`
	DiskFree     int64 `json:"diskFree"`
	ArchivesSize int64 `json:"archivesSize"`
	SpacesSize   int64 `json:"spacesSize"`
}

// Handlers holds the HTTP handlers for the sync API.
type Handlers struct {
	store        *Store
	daemon       *Daemon
	archivesRoot string
	spacesRoot   string
	trashRoot    string
}

// NewHandlers creates the sync HTTP handlers.
func NewHandlers(store *Store, daemon *Daemon, archivesRoot, spacesRoot string) *Handlers {
	trashRoot := filepath.Join(filepath.Dir(spacesRoot), ".trash")
	return &Handlers{
		store:        store,
		daemon:       daemon,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
		trashRoot:    trashRoot,
	}
}

// HandleListEntries handles GET /api/sync/entries?path=<path> or ?parent_ino=<ino>
func (h *Handlers) HandleListEntries(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	pathParam := r.URL.Query().Get("path")
	piParam := r.URL.Query().Get("parent_ino")
	l.Info("HTTP list entries", "method", r.Method, "path", pathParam, "parentIno", piParam)

	var parentIno *uint64
	if pathParam != "" {
		if pathParam != "/" {
			ino, err := h.resolvePathToIno(pathParam)
			if err != nil {
				l.Warn("list entries: path not found", "path", pathParam)
				http.Error(w, "path not found", http.StatusNotFound)
				return
			}
			parentIno = ino
		}
	} else if piParam != "" {
		pi, err := strconv.ParseUint(piParam, 10, 64)
		if err != nil {
			l.Warn("list entries: invalid parent_ino", "parentIno", piParam)
			http.Error(w, "invalid parent_ino", http.StatusBadRequest)
			return
		}
		parentIno = &pi
	}

	children, err := h.store.ListChildren(parentIno)
	if err != nil {
		l.Error("list entries failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Resolve the parent relative path once for statFile
	parentRelPath := h.resolveRelPathFromIno(parentIno)

	items := make([]SyncEntryResponse, 0, len(children))
	for _, child := range children {
		item := SyncEntryResponse{
			Inode:    child.Inode,
			Name:     child.Name,
			Type:     child.Type,
			Size:     child.Size,
			Mtime:    child.Mtime,
			Selected: child.Selected,
		}

		// Build full relative path for this child
		childRelPath := child.Name
		if parentRelPath != "" {
			childRelPath = parentRelPath + "/" + child.Name
		}

		// Compute status
		sv, _ := h.store.GetSpacesView(child.Inode)
		archiveMtime, _, _, _ := statFile(filepath.Join(h.archivesRoot, childRelPath))
		spacesMtime, _, _, _ := statFile(filepath.Join(h.spacesRoot, childRelPath))
		state := ComputeState(&child, sv, archiveMtime, spacesMtime)
		item.Status = state.UIStatus()

		// Add child counts for directories
		if child.Type == "dir" {
			total, sel, err := h.store.ChildCounts(child.Inode)
			if err == nil {
				item.ChildTotalCount = &total
				item.ChildSelectedCount = &sel
			}
		}

		items = append(items, item)
	}

	l.Debug("list entries response", "count", len(items), "parentIno", parentIno)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"items": items,
	})
}

// resolvePathToIno walks down the entries tree to find the inode for a given path.
func (h *Handlers) resolvePathToIno(path string) (*uint64, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var parentIno *uint64
	for _, part := range parts {
		if part == "" {
			continue
		}
		entry, err := h.store.GetEntryByPath(parentIno, part)
		if err != nil || entry == nil {
			return nil, err
		}
		ino := entry.Inode
		parentIno = &ino
	}
	return parentIno, nil
}

// resolveRelPathFromIno builds the relative path from root for a given inode.
func (h *Handlers) resolveRelPathFromIno(ino *uint64) string {
	if ino == nil {
		return ""
	}
	entry, err := h.store.GetEntry(*ino)
	if err != nil || entry == nil {
		return ""
	}
	return h.resolveRelPath(entry)
}

// HandleGetEntry handles GET /api/sync/entry/<inode>
func (h *Handlers) HandleGetEntry(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) == 0 {
		http.Error(w, "missing inode", http.StatusBadRequest)
		return
	}
	inoStr := parts[len(parts)-1]
	ino, err := strconv.ParseUint(inoStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid inode", http.StatusBadRequest)
		return
	}

	l.Debug("HTTP get entry", "inode", ino)

	entry, err := h.store.GetEntry(ino)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if entry == nil {
		l.Debug("get entry: not found", "inode", ino)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry) //nolint:errcheck
}

// SelectRequest is the request body for select/deselect.
type SelectRequest struct {
	Inodes []uint64 `json:"inodes"`
}

// HandleSelect handles POST /api/sync/select
func (h *Handlers) HandleSelect(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	var req SelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		l.Warn("select: bad body", "err", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	l.Info("HTTP select", "inodes", req.Inodes, "count", len(req.Inodes))

	if err := h.store.SetSelected(req.Inodes, true); err != nil {
		l.Error("select failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Run pipeline synchronously for each inode
	h.runPipelineForInodes(r.Context(), req.Inodes)

	l.Info("HTTP select complete", "count", len(req.Inodes))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// HandleDeselect handles POST /api/sync/deselect
func (h *Handlers) HandleDeselect(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	var req SelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		l.Warn("deselect: bad body", "err", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	l.Info("HTTP deselect", "inodes", req.Inodes, "count", len(req.Inodes))

	if err := h.store.SetSelected(req.Inodes, false); err != nil {
		l.Error("deselect failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Run pipeline synchronously for each inode
	h.runPipelineForInodes(r.Context(), req.Inodes)

	l.Info("HTTP deselect complete", "count", len(req.Inodes))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// HandleStats handles GET /api/sync/stats
func (h *Handlers) HandleStats(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	l.Debug("HTTP stats")

	archivesSize, err := h.store.AggregateTotalSize()
	if err != nil {
		l.Error("stats failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	spacesSize, err := h.store.AggregateSelectedSize()
	if err != nil {
		l.Error("stats failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var stat syscall.Statfs_t
	var diskTotal, diskFree int64
	if err := syscall.Statfs(h.archivesRoot, &stat); err == nil {
		diskTotal = int64(stat.Blocks) * int64(stat.Bsize)
		diskFree = int64(stat.Bavail) * int64(stat.Bsize)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SyncStatsResponse{ //nolint:errcheck
		DiskTotal:    diskTotal,
		DiskFree:     diskFree,
		ArchivesSize: archivesSize,
		SpacesSize:   spacesSize,
	})
}

// runPipelineForInodes resolves inodes to relative paths and runs the
// sync pipeline synchronously for each one.
func (h *Handlers) runPipelineForInodes(ctx context.Context, inodes []uint64) {
	l := sub("handlers")
	for _, ino := range inodes {
		entry, err := h.store.GetEntry(ino)
		if err != nil || entry == nil {
			continue
		}
		relPath := h.resolveRelPath(entry)
		if relPath == "" {
			continue
		}

		if err := RunPipeline(ctx, relPath, h.store, h.archivesRoot, h.spacesRoot, h.trashRoot, nil); err != nil {
			l.Error("pipeline error", "path", relPath, "err", err)
		}

		// For directories, also run pipeline on children
		if entry.Type == "dir" {
			h.runPipelineForChildren(ctx, ino, relPath)
		}
	}
}

func (h *Handlers) resolveRelPath(entry *Entry) string {
	if entry == nil {
		return ""
	}

	// Walk up parent chain
	var parts []string
	current := entry
	for current != nil {
		parts = append([]string{current.Name}, parts...)
		if current.ParentIno == nil {
			break
		}
		parent, err := h.store.GetEntry(*current.ParentIno)
		if err != nil || parent == nil {
			break
		}
		current = parent
	}

	return strings.Join(parts, "/")
}

func (h *Handlers) runPipelineForChildren(ctx context.Context, parentIno uint64, parentPath string) {
	l := sub("handlers")
	children, err := h.store.ListChildren(&parentIno)
	if err != nil {
		l.Error("list children error", "parentIno", parentIno, "err", err)
		return
	}
	for _, child := range children {
		childPath := parentPath + "/" + child.Name
		if err := RunPipeline(ctx, childPath, h.store, h.archivesRoot, h.spacesRoot, h.trashRoot, nil); err != nil {
			l.Error("pipeline error", "path", childPath, "err", err)
		}
		if child.Type == "dir" {
			h.runPipelineForChildren(ctx, child.Inode, childPath)
		}
	}
}


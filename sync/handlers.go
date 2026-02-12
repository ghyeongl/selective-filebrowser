package sync

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	SelectedSize int64 `json:"selectedSize"`
	SpacesTotal  int64 `json:"spacesTotal"`
	SpacesFree   int64 `json:"spacesFree"`
}

// Handlers holds the HTTP handlers for the sync API.
type Handlers struct {
	store        *Store
	daemon       *Daemon
	archivesRoot string
	spacesRoot   string
}

// NewHandlers creates the sync HTTP handlers.
func NewHandlers(store *Store, daemon *Daemon, archivesRoot, spacesRoot string) *Handlers {
	return &Handlers{
		store:        store,
		daemon:       daemon,
		archivesRoot: archivesRoot,
		spacesRoot:   spacesRoot,
	}
}

// HandleListEntries handles GET /api/sync/entries?parent_ino=<ino>
func (h *Handlers) HandleListEntries(w http.ResponseWriter, r *http.Request) {
	var parentIno *uint64
	if piStr := r.URL.Query().Get("parent_ino"); piStr != "" {
		pi, err := strconv.ParseUint(piStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid parent_ino", http.StatusBadRequest)
			return
		}
		parentIno = &pi
	}

	children, err := h.store.ListChildren(parentIno)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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

		// Compute status
		sv, _ := h.store.GetSpacesView(child.Inode)
		archiveMtime, _, _, _ := statFile(fmt.Sprintf("%s/%s", h.archivesRoot, child.Name))
		spacesMtime, _, _, _ := statFile(fmt.Sprintf("%s/%s", h.spacesRoot, child.Name))
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"items": items,
	})
}

// HandleGetEntry handles GET /api/sync/entry/<inode>
func (h *Handlers) HandleGetEntry(w http.ResponseWriter, r *http.Request) {
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

	entry, err := h.store.GetEntry(ino)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if entry == nil {
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
	var req SelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.store.SetSelected(req.Inodes, true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Push affected paths to eval queue
	h.pushInodesForEval(req.Inodes)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// HandleDeselect handles POST /api/sync/deselect
func (h *Handlers) HandleDeselect(w http.ResponseWriter, r *http.Request) {
	var req SelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.store.SetSelected(req.Inodes, false); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Push affected paths to eval queue
	h.pushInodesForEval(req.Inodes)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// HandleStats handles GET /api/sync/stats
func (h *Handlers) HandleStats(w http.ResponseWriter, r *http.Request) {
	selectedSize, err := h.store.AggregateSelectedSize()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var stat syscall.Statfs_t
	var spacesTotal, spacesFree int64
	if err := syscall.Statfs(h.spacesRoot, &stat); err == nil {
		spacesTotal = int64(stat.Blocks) * int64(stat.Bsize)
		spacesFree = int64(stat.Bavail) * int64(stat.Bsize)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SyncStatsResponse{ //nolint:errcheck
		SelectedSize: selectedSize,
		SpacesTotal:  spacesTotal,
		SpacesFree:   spacesFree,
	})
}

// pushInodesForEval resolves inodes to relative paths and pushes them
// to the daemon's eval queue.
func (h *Handlers) pushInodesForEval(inodes []uint64) {
	if h.daemon == nil {
		return
	}
	queue := h.daemon.Queue()
	for _, ino := range inodes {
		entry, err := h.store.GetEntry(ino)
		if err != nil || entry == nil {
			continue
		}
		// Build relative path by walking up parent chain
		relPath := h.resolveRelPath(entry)
		if relPath != "" {
			queue.Push(relPath)
			// For directories, also push children
			if entry.Type == "dir" {
				h.pushChildrenForEval(queue, ino, relPath)
			}
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

func (h *Handlers) pushChildrenForEval(queue *EvalQueue, parentIno uint64, parentPath string) {
	children, err := h.store.ListChildren(&parentIno)
	if err != nil {
		log.Printf("[handlers] list children error: %v", err)
		return
	}
	for _, child := range children {
		childPath := parentPath + "/" + child.Name
		queue.Push(childPath)
		if child.Type == "dir" {
			h.pushChildrenForEval(queue, child.Inode, childPath)
		}
	}
}

package sync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
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
	ChildStableCount   *int   `json:"childStableCount,omitempty"`
	DirTotalSize       *int64 `json:"dirTotalSize,omitempty"`
	DirSyncedSize      *int64 `json:"dirSyncedSize,omitempty"`
}

// SyncStatsResponse holds aggregate sync statistics.
type SyncStatsResponse struct {
	DiskTotal    int64          `json:"diskTotal"`
	DiskFree     int64          `json:"diskFree"`
	ArchivesSize int64          `json:"archivesSize"`
	SpacesSize   int64          `json:"spacesSize"`
	QueueLen     int            `json:"queueLen"`
	StatusCounts map[string]int `json:"statusCounts"`
	RecentErrors []LogEntry     `json:"recentErrors"`
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

// HandleListEntries handles GET /api/sync/entries?path=<path> or ?parent_ino=<ino>
func (h *Handlers) HandleListEntries(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	pathParam := r.URL.Query().Get("path")
	piParam := r.URL.Query().Get("parent_ino")
	l.Info("HTTP list entries", "method", r.Method, "path", pathParam, "parentIno", piParam)

	var parentIno uint64 // 0 = root
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
		parentIno = pi
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
			total, sel, stable, err := h.store.ChildCounts(child.Inode)
			if err == nil {
				item.ChildTotalCount = &total
				item.ChildSelectedCount = &sel
				item.ChildStableCount = &stable
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
// Returns 0 for root.
func (h *Handlers) resolvePathToIno(path string) (uint64, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var parentIno uint64
	for _, part := range parts {
		if part == "" {
			continue
		}
		entry, err := h.store.GetEntryByPath(parentIno, part)
		if err != nil || entry == nil {
			return 0, err
		}
		parentIno = entry.Inode
	}
	return parentIno, nil
}

// resolveRelPathFromIno builds the relative path from root for a given inode.
// Returns "" for root (parentIno=0).
func (h *Handlers) resolveRelPathFromIno(ino uint64) string {
	if ino == 0 {
		return ""
	}
	entry, err := h.store.GetEntry(ino)
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

// containsVirtualRoot reports whether the request names inode 0. That is the
// parent_ino sentinel for the virtual root, not a real entry; the store
// ignores it, and callers are told rather than left thinking it applied.
func containsVirtualRoot(inodes []uint64) bool {
	for _, ino := range inodes {
		if ino == 0 {
			return true
		}
	}
	return false
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

	if containsVirtualRoot(req.Inodes) {
		http.Error(w, "inode 0 is the virtual root, not a selectable entry", http.StatusBadRequest)
		return
	}

	if err := h.store.SetSelected(req.Inodes, true); err != nil {
		l.Error("select failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Push to eval queue — daemon worker will run pipeline
	h.pushInodesToQueue(req.Inodes)

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

	if containsVirtualRoot(req.Inodes) {
		http.Error(w, "inode 0 is the virtual root, not a selectable entry", http.StatusBadRequest)
		return
	}

	if err := h.store.SetSelected(req.Inodes, false); err != nil {
		l.Error("deselect failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Push to eval queue — daemon worker will run pipeline
	h.pushInodesToQueue(req.Inodes)

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

	spacesSize, err := h.store.AggregateSyncedSize()
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

	archived, synced, syncing, removing, _ := h.store.StatusCounts()
	queueLen := h.daemon.Queue().Len()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SyncStatsResponse{ //nolint:errcheck
		DiskTotal:    diskTotal,
		DiskFree:     diskFree,
		ArchivesSize: archivesSize,
		SpacesSize:   spacesSize,
		QueueLen:     queueLen,
		StatusCounts: map[string]int{
			"archived": archived,
			"synced":   synced,
			"syncing":  syncing,
			"removing": removing,
		},
		RecentErrors: RecentErrors(),
	})
}

// pushInodesToQueue resolves inodes to relative paths and pushes them
// to the eval queue for the daemon worker to process.
// User-driven select/deselect queues the whole known subtree because the intent is recursive.
func (h *Handlers) pushInodesToQueue(inodes []uint64) {
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

		h.daemon.Queue().PushPriority(relPath)
		l.Debug("queued for eval (priority)", "path", relPath, "inode", ino)

		if entry.Type == "dir" {
			h.pushChildrenToQueue(ino, relPath)
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
		if current.ParentIno == 0 {
			break
		}
		parent, err := h.store.GetEntry(current.ParentIno)
		if err != nil || parent == nil {
			break
		}
		current = parent
	}

	return strings.Join(parts, "/")
}

func (h *Handlers) pushChildrenToQueue(parentIno uint64, parentPath string) {
	// Queue each descendant so it can realize the new desired state via its own pipeline pass.
	children, err := h.store.ListChildren(parentIno)
	if err != nil {
		return
	}
	for _, child := range children {
		childPath := parentPath + "/" + child.Name
		h.daemon.Queue().PushPriority(childPath)
		if child.Type == "dir" {
			h.pushChildrenToQueue(child.Inode, childPath)
		}
	}
}

// HandleDirSize handles GET /api/sync/dirsize?inodes=123,456,789
// Returns an SSE stream with dir size results, one per inode.
// Cancellable via client disconnect (context cancellation).
func (h *Handlers) HandleDirSize(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	inodesParam := r.URL.Query().Get("inodes")
	if inodesParam == "" {
		http.Error(w, "missing inodes parameter", http.StatusBadRequest)
		return
	}

	parts := strings.Split(inodesParam, ",")
	inodes := make([]uint64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		ino, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			http.Error(w, "invalid inode: "+p, http.StatusBadRequest)
			return
		}
		inodes = append(inodes, ino)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	for _, ino := range inodes {
		select {
		case <-ctx.Done():
			return
		default:
		}

		totalSize, syncedSize, err := h.store.DirSize(ino)
		if err != nil {
			l.Warn("dirsize query failed", "inode", ino, "err", err)
			continue
		}

		event := struct {
			Inode         uint64 `json:"inode"`
			DirTotalSize  int64  `json:"dirTotalSize"`
			DirSyncedSize int64  `json:"dirSyncedSize"`
		}{ino, totalSize, syncedSize}

		data, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", data) //nolint:errcheck
		flusher.Flush()
	}
}

// HandleSSE handles GET /api/sync/events (Server-Sent Events stream).
func (h *Handlers) HandleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := h.daemon.Events().Subscribe()
	defer h.daemon.Events().Unsubscribe(ch)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", data) //nolint:errcheck
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n") //nolint:errcheck
			flusher.Flush()
		}
	}
}

// HandleDuplicates handles GET /api/sync/duplicates?min_files=<n>
//
// Reports sibling directories whose subtrees have an identical
// (file count, total size) signature — the orphan copies a directory rename
// on Spaces leaves behind in Archives. Report-only; it never mutates.
//
// Read the output with judgment, in both directions:
//
//   - False positives: legitimate peer directories (experiment runs, numbered
//     exports) can share a signature without being duplicates.
//   - False "identical": the signature is derived from the catalog, not from
//     Archives disk, and the two can diverge. Observed on
//     Learn/25_Fall/{창업실습,창업실습1}: disk holds 84 files/173.1 MB vs
//     79/170.6 MB, but entries holds 79/170.6 MB for both, so a genuinely
//     diverged pair reports as identical. Always confirm with `du -sb` on
//     Archives before deleting anything.
func (h *Handlers) HandleDuplicates(w http.ResponseWriter, r *http.Request) {
	l := sub("handlers")

	minFiles := 5
	if v := r.URL.Query().Get("min_files"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			http.Error(w, "min_files must be a positive integer", http.StatusBadRequest)
			return
		}
		minFiles = n
	}

	groups, err := h.store.DuplicateSiblings(minFiles)
	if err != nil {
		l.Error("duplicate scan failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Bytes recoverable by keeping one copy per group.
	var reclaimable int64
	for _, g := range groups {
		reclaimable += g.TotalSize * int64(len(g.Names)-1)
	}
	l.Info("duplicate scan complete", "groups", len(groups), "reclaimable", reclaimable)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
		"groups":      groups,
		"reclaimable": reclaimable,
	})
}

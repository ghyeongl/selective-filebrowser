import { defineStore } from "pinia";
import {
  listEntries,
  selectEntries,
  deselectEntries,
  getStats,
  connectSSE,
  fetchDirSizes,
  type SyncEntry,
  type SyncStats,
  type SyncEvent,
} from "@/api/sync";

interface SyncState {
  entries: SyncEntry[];
  currentPath: string | null;
  stats: SyncStats | null;
  loading: boolean;
  eventSource: EventSource | null;
  togglingInodes: Set<number>;
  statsTimer: ReturnType<typeof setInterval> | null;
  dirSizeSource: EventSource | null;
  dirSizeDebounce: ReturnType<typeof setTimeout> | null;
}

export const useSyncStore = defineStore("sync", {
  state: (): SyncState => ({
    entries: [],
    currentPath: null,
    stats: null,
    loading: false,
    eventSource: null,
    togglingInodes: new Set<number>(),
    statsTimer: null,
    dirSizeSource: null,
    dirSizeDebounce: null,
  }),
  actions: {
    async fetchEntries(path?: string) {
      this.loading = true;
      try {
        const resp = await listEntries(path);
        this.entries = resp.items;
        this.currentPath = path ?? null;
        this.loadDirSizes();
      } finally {
        this.loading = false;
      }
    },
    async select(inodes: number[]) {
      await selectEntries(inodes);
      await this.fetchEntries(this.currentPath ?? "/");
    },
    async deselect(inodes: number[]) {
      await deselectEntries(inodes);
      await this.fetchEntries(this.currentPath ?? "/");
    },
    async fetchStats() {
      this.stats = await getStats();
    },
    startStatsPolling() {
      this.stopStatsPolling();
      this.fetchStats();
      this.statsTimer = setInterval(() => this.fetchStats(), 10_000);
    },
    stopStatsPolling() {
      if (this.statsTimer) {
        clearInterval(this.statsTimer);
        this.statsTimer = null;
      }
    },
    async toggleEntry(entry: SyncEntry) {
      this.togglingInodes.add(entry.inode);
      try {
        if (entry.selected) {
          await this.deselect([entry.inode]);
        } else {
          await this.select([entry.inode]);
        }
      } finally {
        this.togglingInodes.delete(entry.inode);
      }
    },
    loadDirSizes() {
      if (this.dirSizeSource) {
        this.dirSizeSource.close();
        this.dirSizeSource = null;
      }
      const dirInodes = this.entries
        .filter((e) => e.type === "dir")
        .map((e) => e.inode);
      if (dirInodes.length === 0) return;

      let received = 0;
      this.dirSizeSource = fetchDirSizes(dirInodes, (event) => {
        const entry = this.entries.find((e) => e.inode === event.inode);
        if (entry) {
          entry.dirTotalSize = event.dirTotalSize;
          entry.dirSyncedSize = event.dirSyncedSize;
        }
        if (++received >= dirInodes.length) {
          this.dirSizeSource?.close();
          this.dirSizeSource = null;
        }
      });
    },
    debouncedLoadDirSizes() {
      if (this.dirSizeDebounce) clearTimeout(this.dirSizeDebounce);
      this.dirSizeDebounce = setTimeout(() => {
        this.dirSizeDebounce = null;
        this.loadDirSizes();
      }, 500);
    },
    connectEvents() {
      if (this.eventSource) return;
      this.eventSource = connectSSE((event: SyncEvent) => {
        if (event.type === "status") {
          this.applyStatusUpdate(event);
        }
      });
    },
    disconnectEvents() {
      if (this.eventSource) {
        this.eventSource.close();
        this.eventSource = null;
      }
      if (this.dirSizeSource) {
        this.dirSizeSource.close();
        this.dirSizeSource = null;
      }
      if (this.dirSizeDebounce) {
        clearTimeout(this.dirSizeDebounce);
        this.dirSizeDebounce = null;
      }
    },
    applyStatusUpdate(event: SyncEvent) {
      const entry = this.entries.find((e) => e.inode === event.inode);
      if (entry) {
        if (event.status) {
          entry.status = event.status;
        }
        if (event.childStableCount !== undefined) {
          entry.childStableCount = event.childStableCount;
        }
        if (event.childTotalCount !== undefined) {
          entry.childTotalCount = event.childTotalCount;
        }
        if (entry.type === "dir") {
          this.debouncedLoadDirSizes();
        }
      }
    },
  },
});

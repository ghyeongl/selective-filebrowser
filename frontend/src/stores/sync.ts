import { defineStore } from "pinia";
import {
  listEntries,
  selectEntries,
  deselectEntries,
  getStats,
  connectSSE,
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
}

export const useSyncStore = defineStore("sync", {
  state: (): SyncState => ({
    entries: [],
    currentPath: null,
    stats: null,
    loading: false,
    eventSource: null,
    togglingInodes: new Set<number>(),
  }),
  actions: {
    async fetchEntries(path?: string) {
      this.loading = true;
      try {
        const resp = await listEntries(path);
        this.entries = resp.items;
        this.currentPath = path ?? null;
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
    },
    applyStatusUpdate(event: SyncEvent) {
      const entry = this.entries.find((e) => e.inode === event.inode);
      if (entry) {
        entry.status = event.status;
      }
    },
  },
});

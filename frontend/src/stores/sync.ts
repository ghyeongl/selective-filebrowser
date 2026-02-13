import { defineStore } from "pinia";
import {
  listEntries,
  selectEntries,
  deselectEntries,
  getStats,
  type SyncEntry,
  type SyncStats,
} from "@/api/sync";

interface SyncState {
  entries: SyncEntry[];
  currentPath: string | null;
  stats: SyncStats | null;
  loading: boolean;
}

export const useSyncStore = defineStore("sync", {
  state: (): SyncState => ({
    entries: [],
    currentPath: null,
    stats: null,
    loading: false,
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
      // Re-fetch to get actual status after synchronous pipeline
      await this.fetchEntries(this.currentPath ?? "/");
    },
    async deselect(inodes: number[]) {
      await deselectEntries(inodes);
      // Re-fetch to get actual status after synchronous pipeline
      await this.fetchEntries(this.currentPath ?? "/");
    },
    async fetchStats() {
      this.stats = await getStats();
    },
    async toggleEntry(entry: SyncEntry) {
      if (entry.selected) {
        await this.deselect([entry.inode]);
      } else {
        await this.select([entry.inode]);
      }
    },
  },
});

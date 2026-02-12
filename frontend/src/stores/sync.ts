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
  parentIno: number | null;
  stats: SyncStats | null;
  loading: boolean;
}

export const useSyncStore = defineStore("sync", {
  state: (): SyncState => ({
    entries: [],
    parentIno: null,
    stats: null,
    loading: false,
  }),
  actions: {
    async fetchEntries(parentIno?: number) {
      this.loading = true;
      try {
        const resp = await listEntries(parentIno);
        this.entries = resp.items;
        this.parentIno = parentIno ?? null;
      } finally {
        this.loading = false;
      }
    },
    async select(inodes: number[]) {
      await selectEntries(inodes);
      // Optimistic update
      for (const entry of this.entries) {
        if (inodes.includes(entry.inode)) {
          entry.selected = true;
          entry.status = "syncing";
        }
      }
    },
    async deselect(inodes: number[]) {
      await deselectEntries(inodes);
      // Optimistic update
      for (const entry of this.entries) {
        if (inodes.includes(entry.inode)) {
          entry.selected = false;
          entry.status = "removing";
        }
      }
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

import { fetchJSON, fetchURL } from "./utils";
import { baseURL } from "@/utils/constants";

export interface SyncEntry {
  inode: number;
  name: string;
  type: string;
  size: number | null;
  mtime: number;
  selected: boolean;
  status: string;
  childTotalCount?: number;
  childSelectedCount?: number;
  childStableCount?: number;
}

export interface SyncListResponse {
  items: SyncEntry[];
}

export interface SyncStats {
  diskTotal: number;
  diskFree: number;
  archivesSize: number;
  spacesSize: number;
}

export async function listEntries(
  path?: string
): Promise<SyncListResponse> {
  const params = path != null ? `?path=${encodeURIComponent(path)}` : "";
  return fetchJSON<SyncListResponse>(`/api/sync/entries${params}`);
}

export async function getEntry(inode: number): Promise<SyncEntry> {
  return fetchJSON<SyncEntry>(`/api/sync/entry/${inode}`);
}

export async function selectEntries(inodes: number[]): Promise<void> {
  await fetchURL("/api/sync/select", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ inodes }),
  });
}

export async function deselectEntries(inodes: number[]): Promise<void> {
  await fetchURL("/api/sync/deselect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ inodes }),
  });
}

export async function getStats(): Promise<SyncStats> {
  return fetchJSON<SyncStats>("/api/sync/stats");
}

export interface SyncEvent {
  type: string;
  inode: number;
  name: string;
  status: string;
  childStableCount?: number;
  childTotalCount?: number;
}

export function connectSSE(
  onEvent: (event: SyncEvent) => void
): EventSource {
  const es = new EventSource(`${baseURL}/api/sync/events`);

  es.onmessage = (e) => {
    const event: SyncEvent = JSON.parse(e.data);
    onEvent(event);
  };

  return es;
}

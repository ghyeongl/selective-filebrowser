<template>
  <span v-if="progressLabel" class="sync-badge status-syncing">{{
    progressLabel
  }}</span>
  <span v-else-if="status" class="sync-badge" :class="statusClass">{{
    label
  }}</span>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  status: string;
  childStableCount?: number;
  childTotalCount?: number;
}>();

const progressLabel = computed(() => {
  if (
    props.childTotalCount != null &&
    props.childStableCount != null &&
    props.childTotalCount > 0 &&
    props.childStableCount < props.childTotalCount
  ) {
    return `${props.childStableCount}/${props.childTotalCount}`;
  }
  return null;
});

const statusClass = computed(() => `status-${props.status}`);

const label = computed(() => {
  const labels: Record<string, string> = {
    archived: "archived",
    synced: "synced",
    syncing: "syncing",
    removing: "removing",
    updating: "updating",
    conflict: "conflict",
    recovering: "recovering",
    lost: "lost",
    untracked: "untracked",
    repairing: "repairing",
    no_entry: "no entry",
  };
  return labels[props.status] || props.status;
});
</script>

<style scoped>
.sync-badge {
  display: inline-block;
  font-size: 0.7em;
  padding: 1px 6px;
  border-radius: 3px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  white-space: nowrap;
  vertical-align: middle;
  margin-left: 6px;
}

.status-archived {
  color: #868e96;
  background: #f1f3f5;
}
.status-synced {
  color: #2b8a3e;
  background: #d3f9d8;
}
.status-syncing {
  color: #1864ab;
  background: #d0ebff;
}
.status-removing {
  color: #e67700;
  background: #fff3bf;
}
.status-updating {
  color: #5c3d9e;
  background: #e5dbff;
}
.status-conflict {
  color: #c92a2a;
  background: #ffe3e3;
}
.status-recovering {
  color: #0b7285;
  background: #c5f6fa;
}
.status-lost {
  color: #c92a2a;
  background: #ffe3e3;
}
.status-untracked {
  color: #495057;
  background: #e9ecef;
}
.status-repairing {
  color: #e67700;
  background: #fff3bf;
}
.status-no_entry {
  color: #adb5bd;
  background: #f8f9fa;
}
</style>

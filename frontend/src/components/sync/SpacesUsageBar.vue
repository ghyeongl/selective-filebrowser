<template>
  <div v-if="stats" class="spaces-usage">
    <div class="usage-bar">
      <div class="usage-fill" :style="{ width: usedPercent + '%' }"></div>
      <div
        class="usage-selected"
        :style="{ width: selectedPercent + '%' }"
      ></div>
    </div>
    <div class="usage-text">
      <span>{{ selectedLabel }} selected</span>
      <span>{{ freeLabel }} free of {{ totalLabel }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from "vue";
import { useSyncStore } from "@/stores/sync";
import { storeToRefs } from "pinia";
import prettyBytes from "pretty-bytes";

const syncStore = useSyncStore();
const { stats } = storeToRefs(syncStore);

const usedPercent = computed(() => {
  if (!stats.value || stats.value.spacesTotal === 0) return 0;
  const used = stats.value.spacesTotal - stats.value.spacesFree;
  return Math.min(100, Math.round((used / stats.value.spacesTotal) * 100));
});

const selectedPercent = computed(() => {
  if (!stats.value || stats.value.spacesTotal === 0) return 0;
  return Math.min(
    100,
    Math.round((stats.value.selectedSize / stats.value.spacesTotal) * 100)
  );
});

const selectedLabel = computed(() =>
  prettyBytes(stats.value?.selectedSize ?? 0, { binary: true })
);

const freeLabel = computed(() =>
  prettyBytes(stats.value?.spacesFree ?? 0, { binary: true })
);

const totalLabel = computed(() =>
  prettyBytes(stats.value?.spacesTotal ?? 0, { binary: true })
);

onMounted(() => {
  syncStore.fetchStats();
});
</script>

<style scoped>
.spaces-usage {
  padding: 0 2.5em;
  margin-bottom: 1em;
}

.usage-bar {
  position: relative;
  height: 8px;
  background: #e9ecef;
  border-radius: 4px;
  overflow: hidden;
}

.usage-fill {
  position: absolute;
  top: 0;
  left: 0;
  height: 100%;
  background: #868e96;
  border-radius: 4px;
  transition: width 0.3s;
}

.usage-selected {
  position: absolute;
  top: 0;
  left: 0;
  height: 100%;
  background: #40c057;
  border-radius: 4px;
  transition: width 0.3s;
}

.usage-text {
  display: flex;
  justify-content: space-between;
  font-size: 0.75em;
  color: #868e96;
  margin-top: 4px;
}
</style>

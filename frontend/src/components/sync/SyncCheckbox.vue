<template>
  <label
    class="sync-checkbox"
    :class="{ indeterminate }"
    @click.stop="toggle"
    :title="tooltipText"
  >
    <input
      type="checkbox"
      :checked="selected"
      :indeterminate="indeterminate"
      @click.stop
      @change.stop="toggle"
    />
  </label>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  selected: boolean;
  childTotalCount?: number;
  childSelectedCount?: number;
}>();

const emit = defineEmits<{
  toggle: [];
}>();

const indeterminate = computed(() => {
  if (props.childTotalCount == null || props.childSelectedCount == null)
    return false;
  return (
    props.childSelectedCount > 0 &&
    props.childSelectedCount < props.childTotalCount
  );
});

const tooltipText = computed(() => {
  if (props.childTotalCount != null && props.childSelectedCount != null) {
    return `${props.childSelectedCount}/${props.childTotalCount} selected`;
  }
  return props.selected ? "Deselect" : "Select";
});

const toggle = () => {
  emit("toggle");
};
</script>

<style scoped>
.sync-checkbox {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  padding: 4px;
  margin-right: 4px;
  flex-shrink: 0;
}

.sync-checkbox input[type="checkbox"] {
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: #40c057;
}
</style>

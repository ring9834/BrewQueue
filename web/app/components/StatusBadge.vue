<script setup lang="ts">
import type { OrderStatus } from '~/types/api'

defineProps<{ status: OrderStatus }>()
</script>

<template>
  <span class="status" :class="status.toLowerCase()">{{ status }}</span>
</template>

<style scoped>
.status {
  --c: var(--status-received);
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 600;
  padding: 3px 10px;
  border-radius: 999px;
  color: var(--c);
  background: color-mix(in srgb, var(--c) 14%, transparent);
}

.status::before {
  content: '';
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: currentColor;
}

.preparing {
  --c: var(--status-preparing);
}
.ready {
  --c: var(--status-ready);
}
.collected {
  --c: var(--status-collected);
}
.cancelled {
  --c: var(--status-cancelled);
}
</style>

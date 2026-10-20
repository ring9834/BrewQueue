<script setup lang="ts">
const props = defineProps<{ id: string }>()
const emit = defineEmits<{ forget: [] }>()

// Each row owns its own query (and polling). Vue Query dedupes and caches by key,
// so opening the order page afterwards is instant.
const { data: order, error } = useOrderQuery(() => props.id)

// The server no longer knows this order (e.g. database reset): drop it from the list.
watch(error, (e) => {
  if (e && ApiError.from(e).status === 404) emit('forget')
})
</script>

<template>
  <NuxtLink :to="`/orders/${id}`" class="card row">
    <template v-if="order">
      <div>
        <strong>#{{ order.id.slice(-6).toUpperCase() }}</strong>
        <span class="muted"> · {{ order.lines.map((l) => `${l.quantity}× ${l.name}`).join(', ') }}</span>
      </div>
      <div class="right">
        <span class="muted">{{ formatTime(order.createdAt) }}</span>
        <StatusBadge :status="order.status" />
      </div>
    </template>
    <span v-else class="muted">Loading…</span>
  </NuxtLink>
</template>

<style scoped>
.row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  text-decoration: none;
  padding: 14px 18px;
  flex-wrap: wrap;
}

.right {
  display: flex;
  gap: 12px;
  align-items: center;
}
</style>

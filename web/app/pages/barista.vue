<script setup lang="ts">
import type { Order, OrderStatus } from '~/types/api'

useHead({ title: 'Barista board · BrewQueue' })

const board = useBoardStore()
const { data, isPending, error, dataUpdatedAt } = useBoardQuery()
const changeStatus = useChangeStatusMutation()

const columns: { status: OrderStatus; title: string; next?: OrderStatus; action?: string }[] = [
  { status: 'Received', title: 'New', next: 'Preparing', action: 'Start' },
  { status: 'Preparing', title: 'Making', next: 'Ready', action: 'Ready' },
  { status: 'Ready', title: 'Pick-up', next: 'Collected', action: 'Collected' },
]

const visible = computed(() => (data.value?.items ?? []).filter((o) => board.matches(o.customerName)))
const byStatus = (status: OrderStatus) => visible.value.filter((o) => o.status === status)

function move(order: Order, status: OrderStatus) {
  changeStatus.mutate({ id: order.id, status })
}

// Re-render "x min ago" labels every 30 s.
const now = ref(Date.now())
useIntervalFn(() => (now.value = Date.now()), 30_000)

// A tiny chime when a new order appears (UI preference lives in Pinia).
const seen = new Set<string>()
watch(
  () => data.value?.items,
  (items) => {
    if (!items) return
    const fresh = items.filter((o) => o.status === 'Received' && !seen.has(o.id))
    if (seen.size > 0 && fresh.length && board.soundOnNewOrder) chime()
    items.forEach((o) => seen.add(o.id))
  },
)

function chime() {
  try {
    const ctx = new AudioContext()
    const osc = ctx.createOscillator()
    const gain = ctx.createGain()
    osc.frequency.value = 880
    gain.gain.setValueAtTime(0.08, ctx.currentTime)
    gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.4)
    osc.connect(gain).connect(ctx.destination)
    osc.start()
    osc.stop(ctx.currentTime + 0.4)
  } catch {
    // autoplay policies may block audio until the user interacts; that's fine
  }
}

/** Minimal setInterval composable with cleanup. */
function useIntervalFn(fn: () => void, ms: number) {
  let handle: ReturnType<typeof setInterval> | undefined
  onMounted(() => (handle = setInterval(fn, ms)))
  onBeforeUnmount(() => clearInterval(handle))
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>Barista board</h1>
        <p class="muted">
          Refreshes every 3 s
          <template v-if="dataUpdatedAt"> · last update {{ new Date(dataUpdatedAt).toLocaleTimeString() }}</template>
        </p>
      </div>
      <div class="controls">
        <input v-model="board.search" type="search" placeholder="Find a name…" aria-label="Filter by name" />
        <label class="check"><input v-model="board.compact" type="checkbox" /> Compact</label>
        <label class="check"><input v-model="board.soundOnNewOrder" type="checkbox" /> Chime</label>
      </div>
    </div>

    <ProblemAlert v-if="error" :error="error" />

    <div class="columns" :class="{ compact: board.compact }">
      <section v-for="col in columns" :key="col.status" class="column">
        <header>
          <h2>{{ col.title }}</h2>
          <span class="count">{{ byStatus(col.status).length }}</span>
        </header>

        <div v-if="isPending" class="skeleton" />

        <TransitionGroup v-else name="card" tag="div" class="stack">
          <article v-for="order in byStatus(col.status)" :key="order.id" class="card ticket">
            <div class="ticket-head">
              <strong class="customer">{{ order.customerName }}</strong>
              <span class="muted age">{{ minutesAgo(order.createdAt, now) }}</span>
            </div>
            <ul v-if="!board.compact" class="items">
              <li v-for="l in order.lines" :key="l.menuItemId">
                <span class="qty">{{ l.quantity }}×</span> {{ l.name }}
              </li>
            </ul>
            <div class="ticket-actions">
              <button
                v-if="order.status === 'Received'"
                class="btn danger small"
                @click="move(order, 'Cancelled')"
              >
                Cancel
              </button>
              <button v-if="col.next" class="btn small" @click="move(order, col.next)">
                {{ col.action }} →
              </button>
            </div>
          </article>
          <p v-if="!byStatus(col.status).length" key="empty" class="muted none">Nothing here</p>
        </TransitionGroup>
      </section>
    </div>
  </div>
</template>

<style scoped>
.controls {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}

.controls input[type='search'] {
  width: 200px;
}

.check {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
  font-weight: 500;
}

.columns {
  display: grid;
  gap: 16px;
  grid-template-columns: 1fr;
}

@media (min-width: 860px) {
  .columns {
    grid-template-columns: repeat(3, 1fr);
  }
}

.column {
  background: var(--surface-2);
  border-radius: var(--radius);
  padding: 12px;
  min-height: 200px;
}

.column header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.column h2 {
  font-size: 1.1rem;
  margin: 0;
}

.count {
  font-weight: 700;
  color: var(--ink-soft);
}

.stack {
  display: grid;
  gap: 10px;
}

.ticket {
  padding: 14px;
}

.ticket-head {
  display: flex;
  justify-content: space-between;
  gap: 8px;
}

.customer {
  font: 700 1.1rem var(--font-display);
}

.age {
  font-size: 12px;
  white-space: nowrap;
}

.items {
  list-style: none;
  padding: 0;
  margin: 8px 0;
}

.qty {
  font-weight: 700;
  color: var(--accent);
}

.ticket-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 8px;
}

.compact .ticket {
  padding: 10px 12px;
}

.none {
  text-align: center;
  font-size: 13px;
  padding: 16px 0;
}

.card-enter-from,
.card-leave-to {
  opacity: 0;
  transform: scale(0.97);
}

.card-enter-active,
.card-leave-active {
  transition: all 0.2s ease;
}

.card-move {
  transition: transform 0.2s ease;
}
</style>

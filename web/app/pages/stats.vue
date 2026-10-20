<script setup lang="ts">
useHead({ title: 'Stats · BrewQueue' })

// Local UI state: no need for Pinia when only this page cares.
const hours = ref(12)
const { data: stats, isPending, error, isPlaceholderData } = useStatsQuery(hours)

const maxHourly = computed(() => Math.max(1, ...(stats.value?.hourly ?? []).map((h) => h.ordersPlaced)))
const maxItem = computed(() => Math.max(1, ...(stats.value?.topItems ?? []).map((i) => i.quantitySold)))
const live = computed(() => Object.entries(stats.value?.liveByStatus ?? {}))
const hourLabel = (iso: string) => new Date(iso).toLocaleTimeString('en-NZ', { hour: 'numeric' })
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>Stats</h1>
        <p class="muted">Built by the stats worker from Kafka events, so it trails orders by a moment.</p>
      </div>
      <div>
        <label for="hours">Range</label>
        <select id="hours" v-model.number="hours">
          <option :value="3">Last 3 hours</option>
          <option :value="12">Last 12 hours</option>
          <option :value="24">Last 24 hours</option>
          <option :value="168">Last 7 days</option>
        </select>
      </div>
    </div>

    <ProblemAlert v-if="error" :error="error" />
    <div v-if="isPending" class="skeleton" style="min-height: 320px" />

    <div v-else-if="stats" class="grid" :style="{ opacity: isPlaceholderData ? 0.6 : 1 }">
      <div class="tiles">
        <div class="card tile">
          <span class="muted">Orders (all time)</span>
          <strong>{{ stats.totalOrders }}</strong>
        </div>
        <div class="card tile">
          <span class="muted">Revenue (all time)</span>
          <strong>{{ formatMoney(stats.totalRevenue) }}</strong>
        </div>
        <div v-for="[status, count] in live" :key="status" class="card tile">
          <span class="muted">{{ status }} now</span>
          <strong>{{ count }}</strong>
        </div>
      </div>

      <div class="two">
        <section class="card">
          <h2>Orders per hour</h2>
          <p v-if="!stats.hourly.length" class="muted">No orders in this range yet.</p>
          <div v-else class="bars" role="img" aria-label="Orders placed per hour">
            <div v-for="h in stats.hourly" :key="h.hourStart" class="bar-col" :title="`${h.ordersPlaced} orders`">
              <span class="bar-value">{{ h.ordersPlaced }}</span>
              <div class="bar" :style="{ height: `${(h.ordersPlaced / maxHourly) * 100}%` }" />
              <span class="bar-label">{{ hourLabel(h.hourStart) }}</span>
            </div>
          </div>
        </section>

        <section class="card">
          <h2>Top items</h2>
          <p v-if="!stats.topItems.length" class="muted">Nothing sold yet.</p>
          <ul class="top">
            <li v-for="item in stats.topItems" :key="item.menuItemId">
              <div class="top-row">
                <span>{{ item.name }}</span>
                <strong>{{ item.quantitySold }}</strong>
              </div>
              <div class="meter"><div :style="{ width: `${(item.quantitySold / maxItem) * 100}%` }" /></div>
            </li>
          </ul>
        </section>
      </div>
    </div>
  </div>
</template>

<style scoped>
.tiles {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
}

.tile {
  display: grid;
  gap: 4px;
  padding: 16px;
}

.tile strong {
  font: 700 1.8rem var(--font-display);
}

.two {
  display: grid;
  gap: 16px;
}

@media (min-width: 860px) {
  .two {
    grid-template-columns: 3fr 2fr;
  }
}

h2 {
  font-size: 1.15rem;
}

.bars {
  display: flex;
  align-items: flex-end;
  gap: 6px;
  height: 220px;
  padding-top: 8px;
  overflow-x: auto;
}

.bar-col {
  flex: 1;
  min-width: 26px;
  height: 100%;
  display: grid;
  grid-template-rows: auto 1fr auto;
  align-items: end;
  justify-items: center;
  gap: 4px;
}

.bar {
  width: 100%;
  max-width: 36px;
  background: var(--accent);
  border-radius: 6px 6px 2px 2px;
  min-height: 2px;
  transition: height 0.3s ease;
}

.bar-value,
.bar-label {
  font-size: 11px;
  color: var(--ink-soft);
}

.top {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: 12px;
}

.top-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
}

.meter {
  height: 8px;
  border-radius: 999px;
  background: var(--surface-2);
  overflow: hidden;
}

.meter div {
  height: 100%;
  background: var(--brand);
  border-radius: 999px;
}
</style>

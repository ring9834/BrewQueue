<script setup lang="ts">
import type { OrderStatus } from '~/types/api'

const route = useRoute()
const id = computed(() => String(route.params.id))

const { data: order, isPending, error, isFetching } = useOrderQuery(id)

useHead({ title: () => (order.value ? `${order.value.status} · BrewQueue` : 'Your order · BrewQueue') })

const steps: OrderStatus[] = ['Received', 'Preparing', 'Ready', 'Collected']
const currentStep = computed(() => (order.value ? steps.indexOf(order.value.status) : -1))

const headline = computed(() => {
  switch (order.value?.status) {
    case 'Received':
      return 'We’ve got your order'
    case 'Preparing':
      return 'Your order is being made'
    case 'Ready':
      return `Ready for you, ${order.value.customerName}!`
    case 'Collected':
      return 'Enjoy!'
    case 'Cancelled':
      return 'This order was cancelled'
    default:
      return 'Your order'
  }
})
</script>

<template>
  <div class="page narrow">
    <div v-if="isPending" class="skeleton" style="min-height: 280px" />

    <ProblemAlert v-else-if="error" :error="error" />

    <article v-else-if="order" class="card">
      <div class="head">
        <div>
          <h1>{{ headline }}</h1>
          <p class="muted">
            Order #{{ order.id.slice(-6).toUpperCase() }} · placed {{ formatTime(order.createdAt) }}
          </p>
        </div>
        <StatusBadge :status="order.status" />
      </div>

      <ol v-if="order.status !== 'Cancelled'" class="steps">
        <li v-for="(step, i) in steps" :key="step" :class="{ done: i <= currentStep, current: i === currentStep }">
          <span class="dot" />
          {{ step }}
        </li>
      </ol>

      <ul class="lines">
        <li v-for="line in order.lines" :key="line.menuItemId">
          <span>{{ line.quantity }} × {{ line.name }}</span>
          <span>{{ formatMoney(line.lineTotal) }}</span>
        </li>
        <li class="total">
          <span>Total</span>
          <span>{{ formatMoney(order.total) }}</span>
        </li>
      </ul>

      <p class="muted live">
        <span class="pulse" :class="{ on: isFetching }" />
        {{
          ['Ready', 'Collected', 'Cancelled'].includes(order.status)
            ? 'Live updates stopped.'
            : 'Live: checking every 2 seconds.'
        }}
      </p>
    </article>
  </div>
</template>

<style scoped>
.narrow {
  max-width: 640px;
  margin: 0 auto;
}

.head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: start;
}

.steps {
  list-style: none;
  padding: 0;
  margin: 24px 0;
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}

.steps li {
  display: grid;
  gap: 6px;
  font-size: 13px;
  color: var(--ink-soft);
}

.dot {
  height: 6px;
  border-radius: 999px;
  background: var(--surface-2);
}

.steps li.done .dot {
  background: var(--accent);
}

.steps li.current {
  color: var(--ink);
  font-weight: 600;
}

.lines {
  list-style: none;
  padding: 0;
  margin: 0;
}

.lines li {
  display: flex;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px dashed var(--line);
}

.lines li.total {
  border-bottom: 0;
  font-weight: 700;
}

.live {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  margin: 16px 0 0;
}

.pulse {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--line);
}

.pulse.on {
  background: var(--ok);
}
</style>

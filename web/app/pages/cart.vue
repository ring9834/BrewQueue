<script setup lang="ts">
useHead({ title: 'Cart · BrewQueue' })

const cart = useCartStore()
const myOrders = useMyOrdersStore()
const toasts = useToastStore()
const placeOrder = usePlaceOrderMutation()

const name = computed({
  get: () => cart.customerName,
  set: (value: string) => cart.setCustomerName(value),
})

async function submit() {
  // beginCheckout() reuses the same Idempotency-Key until the cart changes or the order succeeds,
  // so clicking "Place order" again after a network error can never create a second order.
  const { body, idempotencyKey } = cart.beginCheckout()

  const { order, replayed } = await placeOrder.mutateAsync({ body, idempotencyKey }).catch(() => ({
    order: null,
    replayed: false,
  }))
  if (!order) return // error is shown by <ProblemAlert>

  cart.clear()
  myOrders.remember(order.id)
  toasts.push({
    kind: 'success',
    title: replayed ? 'Order already placed' : 'Order placed',
    detail: replayed
      ? 'We had already received this order, so we did not charge you twice.'
      : `Thanks ${order.customerName}! We'll call your name.`,
  })
  await navigateTo(`/orders/${order.id}`) // navigate to the order page after placing the order
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h1>Your cart</h1>
      <NuxtLink to="/" class="btn ghost">← Back to menu</NuxtLink>
    </div>

    <div v-if="cart.isEmpty" class="card empty">
      <p>Your cart is empty.</p>
      <NuxtLink to="/" class="btn">Browse the menu</NuxtLink>
    </div>

    <div v-else class="layout">
      <section class="card lines">
        <div v-for="line in cart.lines" :key="line.menuItemId" class="line">
          <div class="line-name">
            <strong>{{ line.name }}</strong>
            <span class="muted">{{ formatMoney(line.price) }} each</span>
          </div>
          <div class="stepper" role="group" :aria-label="`Quantity of ${line.name}`">
            <button class="btn ghost small" aria-label="Decrease" @click="cart.setQuantity(line.menuItemId, line.quantity - 1)">−</button>
            <span class="qty">{{ line.quantity }}</span>
            <button class="btn ghost small" aria-label="Increase" @click="cart.setQuantity(line.menuItemId, line.quantity + 1)">+</button>
          </div>
          <span class="line-total">{{ formatMoney(line.price * line.quantity) }}</span>
        </div>
      </section>

      <aside class="card checkout">
        <form class="grid" @submit.prevent="submit">
          <div>
            <label for="name">Name for the order</label>
            <input id="name" v-model="name" type="text" maxlength="50" autocomplete="given-name" placeholder="e.g. Aroha" />
          </div>

          <div class="total">
            <span>Total</span>
            <strong>{{ formatMoney(cart.total) }}</strong>
          </div>

          <ProblemAlert :error="placeOrder.error.value" />

          <button class="btn" type="submit" :disabled="!cart.canCheckout || placeOrder.isPending.value">
            {{ placeOrder.isPending.value ? 'Placing order…' : 'Place order' }}
          </button>

          <p class="muted key">
            Idempotency-Key:
            <code>{{ cart.checkoutKey ?? 'created when you place the order' }}</code>
          </p>
        </form>
      </aside>
    </div>
  </div>
</template>

<style scoped>
.layout {
  display: grid;
  gap: 16px;
  grid-template-columns: 1fr;
}

@media (min-width: 860px) {
  .layout {
    grid-template-columns: 1fr 340px;
    align-items: start;
  }
}

.line {
  display: grid;
  grid-template-columns: 1fr auto auto;
  align-items: center;
  gap: 12px;
  padding: 12px 0;
  border-bottom: 1px solid var(--line);
}

.line:last-child {
  border-bottom: 0;
}

.line-name {
  display: grid;
}

.stepper {
  display: flex;
  align-items: center;
  gap: 6px;
}

.qty {
  min-width: 24px;
  text-align: center;
  font-weight: 600;
}

.line-total {
  min-width: 72px;
  text-align: right;
  font-weight: 600;
}

.total {
  display: flex;
  justify-content: space-between;
  font-size: 18px;
}

.key {
  font-size: 12px;
  margin: 0;
  word-break: break-all;
}

code {
  font-size: 11px;
}
</style>

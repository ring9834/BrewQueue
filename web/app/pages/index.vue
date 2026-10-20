<script setup lang="ts">
import type { MenuItem } from '~/types/api'

useHead({ title: 'Menu · BrewQueue' })

const cart = useCartStore()
const { data: menu, isPending, error, refetch, suspense } = useMenuQuery()

// SSR: fetch the menu on the server; nuxt-query dehydrates it into the page and the
// browser's QueryClient starts with it already cached (no loading flash, no second request).
onServerPrefetch(suspense)

const categories = computed(() => {
  const groups = new Map<string, MenuItem[]>()
  for (const item of menu.value ?? []) {
    groups.set(item.category, [...(groups.get(item.category) ?? []), item])
  }
  return [...groups.entries()]
})
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>What are you having?</h1>
        <p class="muted">Order ahead and we'll call your name when it's ready.</p>
      </div>
      <NuxtLink v-if="cart.itemCount" to="/cart" class="btn">
        View cart · {{ formatMoney(cart.total) }}
      </NuxtLink>
    </div>

    <div v-if="isPending" class="grid menu-grid">
      <div v-for="n in 6" :key="n" class="skeleton" />
    </div>

    <div v-else-if="error" class="grid" style="max-width: 520px">
      <ProblemAlert :error="error" />
      <button class="btn ghost" @click="refetch()">Try again</button>
    </div>

    <section v-for="[category, items] in categories" v-else :key="category" class="category">
      <h2>{{ category }}</h2>
      <div class="grid menu-grid">
        <article v-for="item in items" :key="item.id" class="card item" :class="{ unavailable: !item.isAvailable }">
          <div class="item-top">
            <h3>{{ item.name }}</h3>
            <span class="price">{{ formatMoney(item.price) }}</span>
          </div>
          <p class="muted">{{ item.description }}</p>
          <div class="item-actions">
            <span v-if="cart.quantityOf(item.id)" class="in-cart">{{ cart.quantityOf(item.id) }} in cart</span>
            <button class="btn small" :disabled="!item.isAvailable" @click="cart.add(item)">
              {{ item.isAvailable ? 'Add' : 'Unavailable' }}
            </button>
          </div>
        </article>
      </div>
    </section>
  </div>
</template>

<style scoped>
.category {
  margin-bottom: 36px;
}

.category h2 {
  font-size: 1.3rem;
  color: var(--ink-soft);
  margin-bottom: 12px;
}

.menu-grid {
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
}

.item {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.item-top {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: baseline;
}

.item h3 {
  font-size: 1.15rem;
  margin: 0;
}

.price {
  font-weight: 600;
  white-space: nowrap;
}

.item p {
  margin: 0 0 12px;
  flex: 1;
}

.item-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
}

.in-cart {
  font-size: 13px;
  color: var(--accent);
  font-weight: 600;
}

.unavailable {
  opacity: 0.55;
}
</style>

<script setup lang="ts">
const cart = useCartStore()
const myOrders = useMyOrdersStore()
</script>

<template>
  <div class="shell">
    <header class="topbar">
      <div class="container topbar-inner">
        <NuxtLink to="/" class="brand">
          <span class="brand-mark" aria-hidden="true">◐</span>
          BrewQueue
        </NuxtLink>
        <nav class="nav">
          <NuxtLink to="/">Menu</NuxtLink>
          <NuxtLink v-if="myOrders.ids.length" to="/orders">My orders</NuxtLink>
          <NuxtLink to="/barista">Barista</NuxtLink>
          <NuxtLink to="/stats">Stats</NuxtLink>
          <NuxtLink to="/cart" class="cart-link">
            Cart
            <span v-if="cart.itemCount" class="badge">{{ cart.itemCount }}</span>
          </NuxtLink>
        </nav>
      </div>
    </header>

    <main class="container">
      <slot />
    </main>

    <ToastHost />
  </div>
</template>

<style scoped>
.topbar {
  position: sticky;
  top: 0;
  z-index: 10;
  background: color-mix(in srgb, var(--bg) 88%, transparent);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--line);
}

.topbar-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  height: 60px;
}

.brand {
  font: 700 20px var(--font-display);
  text-decoration: none;
  display: flex;
  align-items: center;
  gap: 8px;
  white-space: nowrap;
}

.brand-mark {
  color: var(--accent);
}

.nav {
  display: flex;
  gap: 4px;
  overflow-x: auto;
}

.nav a {
  text-decoration: none;
  padding: 6px 12px;
  border-radius: 999px;
  font-weight: 500;
  color: var(--ink-soft);
  white-space: nowrap;
}

.nav a.router-link-exact-active {
  background: var(--surface-2);
  color: var(--ink);
}

.cart-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.badge {
  background: var(--brand);
  color: var(--brand-ink);
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  min-width: 20px;
  height: 20px;
  display: inline-grid;
  place-items: center;
  padding: 0 6px;
}
</style>

<script setup lang="ts">
useHead({ title: 'My orders · BrewQueue' })
const myOrders = useMyOrdersStore()
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h1>My orders</h1>
    </div>

    <div v-if="!myOrders.ids.length" class="card empty">
      <p>No orders from this browser yet.</p>
      <NuxtLink to="/" class="btn">Order something</NuxtLink>
    </div>

    <div v-else class="grid">
      <MyOrderRow v-for="id in myOrders.ids" :key="id" :id="id" @forget="myOrders.forget(id)" />
    </div>
  </div>
</template>

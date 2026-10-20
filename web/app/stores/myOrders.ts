/**
 * Which orders this browser placed. Only ids live here; the orders themselves are server
 * state and are fetched with Vue Query (useOrderQuery).
 */
export const useMyOrdersStore = defineStore(
  'myOrders',
  () => {
    const ids = ref<string[]>([])

    function remember(id: string) {
      ids.value = [id, ...ids.value.filter((x) => x !== id)].slice(0, 10)
    }

    function forget(id: string) {
      ids.value = ids.value.filter((x) => x !== id)
    }

    return { ids, remember, forget }
  },
  { persist: true },
)

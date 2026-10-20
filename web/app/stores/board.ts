/**
 * Barista board UI preferences: purely client state, kept in localStorage
 * (the board page is client-rendered, so SSR consistency doesn't matter here).
 */
export const useBoardStore = defineStore(
  'board',
  () => {
    const search = ref('')
    const compact = ref(false)
    const soundOnNewOrder = ref(true)

    const matches = (customerName: string) =>
      customerName.toLowerCase().includes(search.value.trim().toLowerCase())

    return { search, compact, soundOnNewOrder, matches }
  },
  {
    persist: {
      storage: piniaPluginPersistedstate.localStorage(),
      pick: ['compact', 'soundOnNewOrder'],
    },
  },
)

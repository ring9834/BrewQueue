import type { MenuItem, PlaceOrderRequest } from '~/types/api'

export interface CartLine {
  menuItemId: number
  name: string
  /** Display only: the server recalculates prices, so a stale price here can't be exploited. */
  price: number
  quantity: number
}

const MAX_QUANTITY = 20

/**
 * Client state: only the browser knows what's in the cart, so Pinia owns it.
 * Persisted (cookie) so it survives reloads and is visible during SSR.
 * A Pinia store is owned by a user session, so each user has their own cart. The server never sees the cart until checkout.
 * The cart is cleared after a successful order, but the customer name is kept for next time.
 * The checkoutKey is used to avoid duplicate orders on network errors or retries.
 */
export const useCartStore = defineStore(
  'cart',
  () => {
    const lines = ref<CartLine[]>([])
    const customerName = ref('')

    /**
     * Idempotency key for the current checkout attempt. Created on first submit and kept
     * until the order succeeds, so a retry after a network error reuses the same key and the
     * server won't create a duplicate order. Any change to the cart starts a new attempt.
     */
    const checkoutKey = ref<string | null>(null)

    // ----- getters -----
    const itemCount = computed(() => lines.value.reduce((sum, l) => sum + l.quantity, 0))
    const total = computed(() => lines.value.reduce((sum, l) => sum + l.price * l.quantity, 0))
    const isEmpty = computed(() => lines.value.length === 0)
    const canCheckout = computed(() => !isEmpty.value && customerName.value.trim().length > 0)

    const quantityOf = (menuItemId: number) =>
      lines.value.find((l) => l.menuItemId === menuItemId)?.quantity ?? 0

    // ----- actions -----
    function add(item: MenuItem) {
      const line = lines.value.find((l) => l.menuItemId === item.id)
      if (line) line.quantity = Math.min(MAX_QUANTITY, line.quantity + 1)
      else lines.value.push({ menuItemId: item.id, name: item.name, price: item.price, quantity: 1 })
      checkoutKey.value = null
    }

    function setQuantity(menuItemId: number, quantity: number) {
      if (quantity <= 0) return remove(menuItemId)
      const line = lines.value.find((l) => l.menuItemId === menuItemId)
      if (line) line.quantity = Math.min(MAX_QUANTITY, quantity)
      checkoutKey.value = null
    }

    function remove(menuItemId: number) {
      lines.value = lines.value.filter((l) => l.menuItemId !== menuItemId)
      checkoutKey.value = null
    }

    function setCustomerName(name: string) {
      if (name !== customerName.value) checkoutKey.value = null
      customerName.value = name
    }

    function beginCheckout(): { body: PlaceOrderRequest; idempotencyKey: string } {
      checkoutKey.value ??= crypto.randomUUID()
      return {
        idempotencyKey: checkoutKey.value,
        body: {
          customerName: customerName.value.trim(),
          lines: lines.value.map(({ menuItemId, quantity }) => ({ menuItemId, quantity })),
        },
      }
    }

    /** After a successful order. The name is kept for next time. */
    function clear() {
      lines.value = []
      checkoutKey.value = null
    }

    return {
      lines,
      customerName,
      checkoutKey,
      itemCount,
      total,
      isEmpty,
      canCheckout,
      quantityOf,
      add,
      setQuantity,
      remove,
      setCustomerName,
      beginCheckout,
      clear,
    }
  },
  {
    persist: { pick: ['lines', 'customerName', 'checkoutKey'] }, // only persist the cart, not the computed totals
  },
)

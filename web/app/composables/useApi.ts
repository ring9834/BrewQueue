import type {
  MenuItem,
  Order,
  OrderStatus,
  PagedResult,
  PlaceOrderRequest,
  StatsSummary,
} from '../types/api'

export interface PlaceOrderResult {
  order: Order
  /** True when the server returned a stored response for a repeated Idempotency-Key. */
  replayed: boolean
}

/**
 * Typed API client. Every function throws ApiError (with the server's ProblemDetails) on failure,
 * which Vue Query surfaces as `error`.
 */
export function useApi() {
  const config = useRuntimeConfig()
  // During SSR the Nuxt server talks to the API over the Docker network; the browser uses localhost.
  const baseURL = import.meta.server ? config.apiBaseServer : config.public.apiBase

  const client = $fetch.create({ baseURL, retry: 0 })

  async function call<T>(promise: Promise<T>): Promise<T> {
    try {
      return await promise
    } catch (error) {
      throw ApiError.from(error)
    }
  }

  return {
    getMenu: () => call(client<MenuItem[]>('/api/menu')),

    getOrder: (id: string) => call(client<Order>(`/api/orders/${id}`)),

    listOrders: (statuses: OrderStatus[], pageSize = 100) =>
      call(client<PagedResult<Order>>('/api/orders', { query: { status: statuses, pageSize } })),

    async placeOrder(body: PlaceOrderRequest, idempotencyKey: string): Promise<PlaceOrderResult> {
      // $fetch.raw gives us the response headers as well as the parsed body.
      const response = await call(
        client.raw<Order>('/api/orders', {
          method: 'POST',
          body,
          headers: { 'Idempotency-Key': idempotencyKey },
        }),
      )
      return {
        order: response._data!,
        replayed: response.headers.get('Idempotent-Replayed') === 'true',
      }
    },

    changeStatus: (id: string, status: OrderStatus) =>
      call(client<Order>(`/api/orders/${id}/status`, { method: 'PATCH', body: { status } })),

    getStats: (hours: number) => call(client<StatsSummary>('/api/stats/summary', { query: { hours } })),
  }
}

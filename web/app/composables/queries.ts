import type { MaybeRefOrGetter } from 'vue'
import type { Order, OrderStatus, PagedResult, PlaceOrderRequest } from '~/types/api'

// ---------------------------------------------------------------------------
// Query keys in one place. Hierarchical keys let us invalidate whole groups,
// e.g. invalidateQueries({ queryKey: ['orders'] }) hits every order query.
// ---------------------------------------------------------------------------
export const queryKeys = {
  menu: ['menu'] as const,
  orders: ['orders'] as const,
  order: (id: string) => ['orders', 'detail', id] as const,
  board: (statuses: readonly OrderStatus[]) => ['orders', 'board', [...statuses]] as const,
  stats: (hours: number) => ['stats', hours] as const,
}

export const ACTIVE_STATUSES: readonly OrderStatus[] = ['Received', 'Preparing', 'Ready']

const isSettled = (status?: OrderStatus) =>
  status === 'Ready' || status === 'Collected' || status === 'Cancelled'

// ---------------------------------------------------------------------------
// Queries (server state: owned by Vue Query, never copied into Pinia)
// ---------------------------------------------------------------------------

export function useMenuQuery() {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.menu,
    queryFn: api.getMenu,
    staleTime: 5 * 60_000, // the menu rarely changes
  })
}

/** Polls every 2 s until the order is ready (or finished), then stops. */
export function useOrderQuery(id: MaybeRefOrGetter<string>) {
  const api = useApi()
  return useQuery({
    queryKey: computed(() => queryKeys.order(toValue(id))),
    queryFn: () => api.getOrder(toValue(id)),
    refetchInterval: (query) => (isSettled(query.state.data?.status) ? false : 2000),
  })
}

export function useBoardQuery() {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.board(ACTIVE_STATUSES),
    queryFn: () => api.listOrders([...ACTIVE_STATUSES]),
    refetchInterval: 3000,
    staleTime: 0,
  })
}

export function useStatsQuery(hours: MaybeRefOrGetter<number>) {
  const api = useApi()
  return useQuery({
    queryKey: computed(() => queryKeys.stats(toValue(hours))),
    queryFn: () => api.getStats(toValue(hours)),
    refetchInterval: 5000,
    placeholderData: (previous) => previous, // keep the old numbers while a new range loads
  })
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

export function usePlaceOrderMutation() {
  const api = useApi()
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ body, idempotencyKey }: { body: PlaceOrderRequest; idempotencyKey: string }) =>
      api.placeOrder(body, idempotencyKey),
    meta: { silent: true }, // the cart page shows its own error UI
    onSuccess: ({ order }) => {
      // Seed the detail cache so the tracking page renders instantly.
      queryClient.setQueryData(queryKeys.order(order.id), order)
      queryClient.invalidateQueries({ queryKey: queryKeys.orders })
    },
  })
}

/**
 * Optimistic update: the card moves column immediately; if the API rejects the change
 * (e.g. 409 because someone else moved it), we roll back to the snapshot.
 */
export function useChangeStatusMutation() {
  const api = useApi()
  const queryClient = useQueryClient()
  const boardKey = queryKeys.board(ACTIVE_STATUSES)

  return useMutation({
    mutationFn: ({ id, status }: { id: string; status: OrderStatus }) => api.changeStatus(id, status),

    onMutate: async ({ id, status }) => {
      await queryClient.cancelQueries({ queryKey: boardKey }) // don't let a poll overwrite us
      const previous = queryClient.getQueryData<PagedResult<Order>>(boardKey)

      queryClient.setQueryData<PagedResult<Order>>(boardKey, (old) =>
        old && {
          ...old,
          items: old.items
            .map((o) => (o.id === id ? { ...o, status } : o))
            .filter((o) => ACTIVE_STATUSES.includes(o.status)),
        },
      )
      return { previous }
    },

    onError: (_error, _vars, context) => {
      if (context?.previous) queryClient.setQueryData(boardKey, context.previous)
    },

    onSuccess: (order) => queryClient.setQueryData(queryKeys.order(order.id), order),

    onSettled: () => queryClient.invalidateQueries({ queryKey: queryKeys.orders }),
  })
}

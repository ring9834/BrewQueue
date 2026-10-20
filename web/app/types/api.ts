// Types mirroring the API's DTOs.
//
// Hand-written so the app builds without the API running. To generate them from the API's
// OpenAPI document instead, start the API and run `npm run gen:api`; that writes
// app/types/openapi.d.ts, whose `components['schemas']` you can alias here, e.g.
//   export type Order = components['schemas']['OrderDto']

export const ORDER_STATUSES = ['Received', 'Preparing', 'Ready', 'Collected', 'Cancelled'] as const
export type OrderStatus = (typeof ORDER_STATUSES)[number]

export interface MenuItem {
  id: number
  name: string
  category: string
  description: string
  price: number
  isAvailable: boolean
}

export interface OrderLine {
  menuItemId: number
  name: string
  unitPrice: number
  quantity: number
  lineTotal: number
}

export interface Order {
  id: string
  customerName: string
  status: OrderStatus
  total: number
  lines: OrderLine[]
  createdAt: string
  updatedAt: string
}

export interface PagedResult<T> {
  items: T[]
  page: number
  pageSize: number
  totalCount: number
}

export interface PlaceOrderRequest {
  customerName: string
  lines: { menuItemId: number; quantity: number }[]
}

export interface HourlyStat {
  hourStart: string
  ordersPlaced: number
  ordersCollected: number
  revenue: number
}

export interface TopItem {
  menuItemId: number
  name: string
  quantitySold: number
}

export interface StatsSummary {
  totalOrders: number
  totalRevenue: number
  hourly: HourlyStat[]
  topItems: TopItem[]
  liveByStatus: Record<string, number>
}

/** RFC 9457 ProblemDetails, plus the extensions our API adds. */
export interface ProblemDetails {
  type?: string
  title?: string
  status?: number
  detail?: string
  instance?: string
  traceId?: string
  correlationId?: string
  errorCode?: string
  /** Present on 400 validation problems: field name → messages. */
  errors?: Record<string, string[]>
}

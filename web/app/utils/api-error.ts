import type { ProblemDetails } from '~/types/api'

/**
 * One error type for every failed API call. It carries the server's ProblemDetails, so
 * components can show the right message, react to `errorCode`, and display the
 * `traceId`/`correlationId` a user can quote when reporting a problem.
 */
export class ApiError extends Error {
  readonly status: number
  readonly problem: ProblemDetails

  constructor(status: number, problem: ProblemDetails) {
    super(problem.detail ?? problem.title ?? `Request failed with status ${status}`)
    this.name = 'ApiError'
    this.status = status
    this.problem = problem
  }

  get errorCode(): string | undefined {
    return this.problem.errorCode
  }

  /** Flattened validation messages, e.g. ["CustomerName: The CustomerName field is required."] */
  get validationMessages(): string[] {
    return Object.entries(this.problem.errors ?? {}).flatMap(([field, messages]) =>
      messages.map((m) => (m.toLowerCase().includes(field.toLowerCase()) ? m : `${field}: ${m}`)),
    )
  }

  /** Converts whatever $fetch threw into an ApiError. */
  static from(error: unknown): ApiError {
    if (error instanceof ApiError) return error

    const e = error as { status?: number; statusCode?: number; data?: unknown; message?: string }
    const status = e?.status ?? e?.statusCode ?? 0
    const data = e?.data

    if (data && typeof data === 'object' && ('title' in data || 'detail' in data)) {
      return new ApiError(status, data as ProblemDetails)
    }

    // Network failure, CORS, API down: no ProblemDetails body.
    return new ApiError(status, {
      title: status === 0 ? 'Cannot reach the BrewQueue API' : 'Request failed',
      detail: status === 0 ? 'Check that the API is running and try again.' : e?.message,
      status,
    })
  }
}

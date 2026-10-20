import { MutationCache, QueryClient } from '@tanstack/vue-query'
import type { Pinia } from 'pinia'

/**
 * Customises the QueryClient that @peterbud/nuxt-query creates, via its `nuxt-query:configure`
 * hook. Anything that isn't JSON (functions, caches) has to be set up here rather than in
 * nuxt.config.ts. `enforce: 'pre'` makes sure our hook is registered before the module's plugin
 * calls it.
 */
export default defineNuxtPlugin({
  name: 'query-client',
  enforce: 'pre',
  setup(nuxtApp) {
    nuxtApp.hook('nuxt-query:configure', (getPluginOptions) => {
      const base = useRuntimeConfig().public.nuxtQuery?.queryClientOptions

      const queryClient = new QueryClient({
        ...base,
        defaultOptions: {
          ...base?.defaultOptions,
          queries: {
            ...base?.defaultOptions?.queries,
            // Retry network errors and 5xx; a 4xx won't fix itself.
            retry: (failureCount, error) => {
              const status = error instanceof ApiError ? error.status : 0
              return (status === 0 || status >= 500) && failureCount < 2
            },
          },
        },
        // One place to report mutation failures. Mutations with meta.silent handle their own UI.
        mutationCache: new MutationCache({
          onError: (error, _vars, _ctx, mutation) => {
            if (mutation.meta?.silent) return
            const apiError = ApiError.from(error)
            useToastStore(nuxtApp.$pinia as Pinia).push({
              kind: 'error',
              title: apiError.problem.title ?? 'Something went wrong',
              detail: apiError.message,
              reference: apiError.problem.traceId,
            })
          },
        }),
      })

      getPluginOptions(queryClient)
    })
  },
})

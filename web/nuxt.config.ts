// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2026-10-01',
  devtools: { enabled: true },

  modules: [
    '@pinia/nuxt',
    'pinia-plugin-persistedstate/nuxt',
    '@peterbud/nuxt-query',
  ],

  css: ['~/assets/css/main.css'],

  app: {
    head: {
      title: 'BrewQueue',
      htmlAttrs: { lang: 'en' },
      meta: [{ name: 'viewport', content: 'width=device-width, initial-scale=1' }],
      link: [
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,700&family=Inter:wght@400;500;600&display=swap',
        },
      ],
    },
  },

  runtimeConfig: {
    // Server-side only. During SSR the Nuxt server calls the API directly (in Docker: http://api:8080).
    // Override with NUXT_API_BASE_SERVER.
    apiBaseServer: 'http://localhost:5080',
    public: {
      // Used by the browser. Override with NUXT_PUBLIC_API_BASE.
      apiBase: 'http://localhost:5080',
    },
  },

  // Server-render the customer pages (menu is prefetched on the server and hydrated into
  // Vue Query); render the live dashboards only in the browser.
  routeRules: {
    '/barista': { ssr: false },
    '/stats': { ssr: false },
  },

  nuxtQuery: {
    // Auto-import these Vue Query composables, like Nuxt's own composables.
    autoImports: ['useQuery', 'useMutation', 'useQueryClient'],
    devtools: true,
    // These options travel through runtime config, so they must be JSON-serializable.
    // Function options (retry logic, global error handlers) live in app/plugins/query-client.ts.
    queryClientOptions: {
      defaultOptions: {
        queries: {
          staleTime: 10_000,
          refetchOnWindowFocus: true,
        },
      },
    },
  },

  piniaPluginPersistedstate: {
    // Cookies by default so SSR and the browser see the same persisted state (no hydration mismatch).
    storage: 'cookies',
    cookieOptions: { sameSite: 'lax', maxAge: 60 * 60 * 24 * 7 },
  },

  typescript: {
    strict: true,
  },
})

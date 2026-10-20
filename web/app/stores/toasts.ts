export interface Toast {
  id: number
  kind: 'error' | 'success' | 'info'
  title: string
  detail?: string
  /** Shown small, so users can quote it when reporting a problem. */
  reference?: string
}

/** Transient notifications. Not persisted. */
export const useToastStore = defineStore('toasts', () => {
  const toasts = ref<Toast[]>([])
  let nextId = 1

  function push(toast: Omit<Toast, 'id'>, timeoutMs = 6000) {
    const id = nextId++
    toasts.value.push({ ...toast, id })
    if (import.meta.client) setTimeout(() => dismiss(id), timeoutMs)
  }

  function dismiss(id: number) {
    toasts.value = toasts.value.filter((t) => t.id !== id)
  }

  return { toasts, push, dismiss }
})

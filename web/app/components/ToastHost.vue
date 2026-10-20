<script setup lang="ts">
const toastStore = useToastStore()
</script>

<template>
  <div class="toasts" aria-live="polite">
    <TransitionGroup name="toast">
      <div v-for="t in toastStore.toasts" :key="t.id" class="toast" :class="t.kind" role="status">
        <div>
          <strong>{{ t.title }}</strong>
          <p v-if="t.detail">{{ t.detail }}</p>
          <small v-if="t.reference" class="muted">Ref: {{ t.reference }}</small>
        </div>
        <button class="close" aria-label="Dismiss" @click="toastStore.dismiss(t.id)">×</button>
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.toasts {
  position: fixed;
  right: 16px;
  bottom: 16px;
  left: 16px;
  display: grid;
  gap: 8px;
  justify-items: end;
  pointer-events: none;
  z-index: 50;
}

.toast {
  pointer-events: auto;
  max-width: 380px;
  width: 100%;
  display: flex;
  gap: 12px;
  align-items: start;
  background: var(--surface);
  border: 1px solid var(--line);
  border-left: 4px solid var(--status-received);
  border-radius: 12px;
  box-shadow: var(--shadow);
  padding: 12px 14px;
}

.toast.error {
  border-left-color: var(--danger);
}

.toast.success {
  border-left-color: var(--ok);
}

.toast p {
  margin: 2px 0;
}

.close {
  margin-left: auto;
  background: none;
  border: 0;
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
  color: var(--ink-soft);
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
  transform: translateY(8px);
}

.toast-enter-active,
.toast-leave-active {
  transition: all 0.2s ease;
}
</style>

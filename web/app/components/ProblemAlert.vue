<script setup lang="ts">
/** Renders an API error (ProblemDetails) in a friendly way. */
const props = defineProps<{ error: unknown }>()

const apiError = computed(() => (props.error ? ApiError.from(props.error) : null))
</script>

<template>
  <div v-if="apiError" class="problem" role="alert">
    <strong>{{ apiError.problem.title ?? 'Something went wrong' }}</strong>
    <p v-if="apiError.validationMessages.length === 0 && apiError.problem.detail">
      {{ apiError.problem.detail }}
    </p>
    <ul v-else-if="apiError.validationMessages.length">
      <li v-for="m in apiError.validationMessages" :key="m">{{ m }}</li>
    </ul>
    <small v-if="apiError.problem.traceId" class="muted">
      {{ apiError.status || 'network' }} · {{ apiError.errorCode ?? 'error' }} · trace {{ apiError.problem.traceId }}
    </small>
  </div>
</template>

<style scoped>
.problem {
  border: 1px solid color-mix(in srgb, var(--danger) 35%, transparent);
  background: color-mix(in srgb, var(--danger) 8%, var(--surface));
  border-radius: 12px;
  padding: 12px 16px;
}

.problem p,
.problem ul {
  margin: 4px 0;
}

small {
  font-family: ui-monospace, monospace;
  font-size: 11px;
  word-break: break-all;
}
</style>

# BrewQueue ☕

A small café ordering system built to practise a modern full stack: customers order from a menu, baristas move orders across a live board, and a stats page is built from an event stream.

| Layer | Tech |
|---|---|
| API | ASP.NET Core 10 Web API (controllers), EF Core + PostgreSQL |
| Cross-cutting | RFC 9457 ProblemDetails, Serilog + Seq, correlation ids, health checks, built-in OpenAPI + Scalar |
| Idempotency | Redis (`Idempotency-Key` header on `POST /api/orders`) |
| Messaging | Transactional outbox → **RabbitMQ** (kitchen work queue, retries, DLQ) and **Kafka** (order event stream) |
| Workers | Kitchen worker (RabbitMQ consumer), Stats worker (Kafka consumer → read model) |
| Frontend | Nuxt 4, Vue 3, TypeScript, **Pinia** (client state), **Nuxt Query / TanStack Vue Query** (server state) |
| Tests | xUnit unit tests; integration tests with **Testcontainers** (Postgres, Redis, RabbitMQ, Kafka) |
| Local deployment | Docker Compose |

<center><img width="544" height="448" alt="brewqueue_architecture_overview" src="https://github.com/user-attachments/assets/b811de8c-febd-4cf6-a069-62a20f50381f" /></center>

---

## Prerequisites

- .NET 10 SDK
- Node.js 22+
- Docker Desktop (or Docker Engine) — also needed by the integration tests

## Run everything in Docker

```bash
docker compose up -d --build
```

| What | URL |
|---|---|
| Web app | http://localhost:3000 |
| API docs (Scalar) | http://localhost:5080/scalar/v1 |
| OpenAPI document | http://localhost:5080/openapi/v1.json |
| Health | http://localhost:5080/health/ready |
| RabbitMQ management | http://localhost:15672 (brewqueue / brewqueue) |
| Kafka UI | http://localhost:8081 |
| Seq (logs) | http://localhost:5341 |

## Run the apps locally (debugging), infrastructure in Docker

```bash
docker compose up -d postgres redis rabbitmq kafka seq kafka-ui

dotnet run --project src/BrewQueue.Api            # http://localhost:5080
dotnet run --project src/BrewQueue.KitchenWorker
dotnet run --project src/BrewQueue.StatsWorker

cd web && npm install && npm run dev             # http://localhost:3000
```

`appsettings.json` files already point at `localhost`, matching the ports Compose exposes.

## Tests

```bash
dotnet test tests/BrewQueue.UnitTests
dotnet test tests/BrewQueue.IntegrationTests   # Docker must be running; first run pulls images
```

---

## What to look at (the learning tour)

### 1. Pinia vs Vue Query: who owns which state
- **Server state** (menu, orders, stats) lives only in Vue Query: `web/app/composables/queries.ts`. It handles caching, polling (`refetchInterval`), invalidation and optimistic updates.
- **Client state** lives in Pinia: `web/app/stores/`. The cart and "my order ids" persist in cookies (so SSR and browser agree); board preferences persist in localStorage.
- Rule of thumb: never copy server data into a Pinia store.
- `web/app/pages/index.vue` prefetches the menu during SSR (`onServerPrefetch(suspense)`); `@peterbud/nuxt-query` dehydrates it into the page so the browser starts with a warm cache.
- `web/app/plugins/query-client.ts` uses the module's `nuxt-query:configure` hook for things JSON config can't express: retry rules and a global mutation error toast.
- The barista board (`pages/barista.vue`) moves cards **optimistically** and rolls back on failure.

### 2. Idempotency with Redis
- `[Idempotent]` on `OrdersController.PlaceOrder` → `CrossCutting/IdempotencyFilter.cs`.
- Redis `SET NX` claims the key atomically. Outcomes: first request → proceed; same key + same body → stored response replayed with `Idempotent-Replayed: true`; still running → 409; same key + different body → 422.
- The cart store (`stores/cart.ts`) keeps one key per checkout attempt, so retrying after a network error can't create a second order.
- Pure decision logic (`IdempotencyEvaluator`) is unit tested; the full behaviour (including 5 concurrent requests) is integration tested.

### 3. Transactional outbox
- `OrderService` saves the order **and** its outgoing messages in one transaction (`outbox_messages`).
- `Services/OutboxPublisher.cs` publishes them using `SELECT … FOR UPDATE SKIP LOCKED`, which is safe with several API instances.
- Delivery is at-least-once, so consumers are idempotent: setting a status the order already has is a no-op, and the stats projector dedupes by `EventId`.

### 4. RabbitMQ: commands, retries, dead letters
- Topology (`Infrastructure/Messaging/RabbitMq.cs`): direct exchange → quorum queue `kitchen.orders`, with a dead-letter exchange → `kitchen.orders.dlq`.
- `KitchenWorker/KitchenConsumer.cs`: manual acks, prefetch, transient failures re-published with an `x-retry-count` header and exponential backoff, permanent failures and poison messages nacked to the DLQ.
- Try `docker compose up -d --scale kitchen=3` and watch orders spread across workers.

### 5. Kafka: an event log
- Events are keyed by order id, so all events for one order land on one partition, in order.
- The producer has `EnableIdempotence = true`, so broker-side retries cannot create duplicates.
- `StatsWorker` commits offsets **after** the database update. Stop it, place orders, start it again: it catches up. Change `Kafka:ConsumerGroupId` to replay the topic from the start (the dedupe table then skips events it has already applied; clear the stats tables to rebuild from scratch).

### 6. Errors, logs, OpenAPI
- Every error is `application/problem+json` with `traceId`, `correlationId` and a machine-readable `errorCode` (`CrossCutting/ErrorHandling.cs`). Domain rule violations → 422, not found → 404, concurrency → 409, validation → 400 with field errors, unexpected → 500 with details hidden outside Development.
- Optimistic concurrency uses PostgreSQL's `xmin` (`Order.Version`).
- `X-Correlation-Id` is accepted or generated per request, stored on outbox rows, sent as a broker header and logged by both workers. In Seq, filter by `CorrelationId = '…'` to follow one order across all three services.
- OpenAPI comes from the built-in `Microsoft.AspNetCore.OpenApi` generator, enriched by XML doc comments and `[ProducesResponseType]`. With the API running, `cd web && npm run gen:api` generates TypeScript types from it.

---

## Database migrations

Out of the box, the app creates the schema with `EnsureCreated` so it runs immediately. To switch to migrations:

```bash
dotnet tool install --global dotnet-ef
dotnet ef migrations add InitialCreate \
  --project src/BrewQueue.Infrastructure --startup-project src/BrewQueue.Api
```

On the next start `DatabaseInitializer` sees the migration and applies migrations instead. (Drop the database first if `EnsureCreated` already made the tables: `docker compose down -v`.)

## Package versions

`Directory.Packages.props` centralises NuGet versions and uses floating versions within each major (`7.*`). After your first successful restore, consider pinning the exact versions it resolved.

## Project layout

```
src/
  BrewQueue.Contracts/       message contracts and queue/topic names (no dependencies)
  BrewQueue.Domain/          Order aggregate, status rules, domain exceptions
  BrewQueue.Infrastructure/  EF Core, Redis idempotency store, RabbitMQ, Kafka, stats projector
  BrewQueue.Api/             controllers, services, outbox publisher, cross-cutting concerns
  BrewQueue.KitchenWorker/   RabbitMQ consumer that "makes" orders via the API
  BrewQueue.StatsWorker/     Kafka consumer that builds the stats read model
tests/
  BrewQueue.UnitTests/
  BrewQueue.IntegrationTests/
web/                         Nuxt 4 app
docker-compose.yml
```

## Ideas to extend it

- Replace polling with Server-Sent Events or SignalR for the board and order page.
- Add OpenTelemetry tracing across API → outbox → RabbitMQ → worker → API.
- Run the kitchen consumer inside the integration tests and assert the full Received → Ready flow.
- Swap Vue Query for Pinia Colada on one page and compare.

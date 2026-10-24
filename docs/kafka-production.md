# Kafka in BrewQueue: production setup

This document explains how the Kafka part of BrewQueue is built for production, how to operate it,
and what is still left to do before a real enterprise rollout.

It covers five areas:

1. a 3-broker cluster with replication factor 3 and `min.insync.replicas = 2`
2. TLS, authentication and ACLs
3. outbox ordering across API instances
4. bounded retries, a dead-letter topic, and alerts for lag and dead letters
5. a Schema Registry with compatibility rules

---

## 1. Cluster durability

| Setting | Value | Why |
|---|---|---|
| Nodes | `kafka-1`, `kafka-2`, `kafka-3`, each broker + KRaft controller | A quorum of 3 survives losing 1 |
| `default.replication.factor` | 3 | Every partition has a copy on every node |
| `min.insync.replicas` | 2 (cluster default and on every topic) | With `acks=all`, a write is confirmed only when 2 copies have it |
| `unclean.leader.election.enable` | false | An out-of-date replica never becomes leader, so acknowledged writes are never silently dropped |
| `broker.rack` | `zone-1`/`zone-2`/`zone-3` | Replicas are spread across "zones"; in a real deployment, map these to availability zones |
| `auto.create.topics.enable` | false | Topics exist only if `setup-kafka.sh` declares them |
| Producers | `acks=all`, idempotent, `max.in.flight=5`, zstd | No loss, no duplicates or reordering from retries |

**Try it:** `docker compose stop kafka-2`. Orders keep flowing. `KafkaBrokerDown` fires within a
minute or two, and `KafkaUnderReplicatedPartitions` after 5 minutes. Stop a second node as well and
the KRaft quorum (2 of 3) is lost, so the cluster stops serving. Events wait safely in the outbox
and nothing is lost. Start the nodes again and everything drains.

**For a larger deployment:**
- Run dedicated controller nodes (`process.roles=controller`) so broker load can't disturb the
  metadata quorum.
- Size disks for volume × retention, and give brokers several GB of heap plus plenty of page cache.

## 2. Security

| Listener | Port | Protocol | Who uses it |
|---|---|---|---|
| `BROKER` | 9091 | SSL with required client certificates (mTLS) | Brokers replicating to each other |
| `CONTROLLER` | 9093 | SSL with required client certificates (mTLS) | KRaft quorum |
| `CLIENT` | 9092 | SASL_SSL, SCRAM-SHA-512 | Apps inside the Docker network |
| `EXTERNAL` | 19092–19094 | SASL_SSL, SCRAM-SHA-512 | Apps and tools on your machine |

- **Certificates** (`infra/kafka/generate-certs.sh`): a local CA signs one certificate per server.
  Each certificate's SAN covers its Docker hostname and `localhost`, and clients verify hostnames.
  The script only uses `keytool`, run inside the Kafka image.
- **Principals:** broker certificates map to `User:kafka-N` (via `ssl.principal.mapping.rules`).
  Apps are SCRAM users. `admin` is bootstrapped when the storage is formatted, because someone has
  to exist before users can be created.
- **ACLs:** deny by default (`allow.everyone.if.no.acl.found=false`), defined in `setup-kafka.sh`:

| Principal | Allowed |
|---|---|
| `api` | Write and Describe on `order-events` |
| `stats` | Read on `order-events`, its group `brewqueue-stats`; Write on `order-events.dlt` |
| `schema-registry` | All on `_schemas` and group `schema-registry` |
| `kafka-ui` | Read-only on every topic and group |
| `kafka-exporter` | Describe only (offsets and lag, never message contents) |

- **Secrets:** come from `.env` (git-ignored). The apps read passwords from environment variables,
  or from user-secrets when running on your machine. Nothing sensitive is in `appsettings.json`.
- **Schema Registry:** HTTPS with HTTP Basic auth.

## 3. Outbox ordering across API instances

The bug was that `FOR UPDATE SKIP LOCKED` let several API instances publish different batches in
parallel. Instance B could send `OrderStatusChanged` for an order before instance A had sent its
`OrderPlaced`.

The fix (`src/BrewQueue.Api/Services/OutboxPublisher.cs`) has four parts:

1. **One publisher at a time.** Each cycle runs in a transaction that starts with
   `pg_try_advisory_xact_lock(...)`. Only the instance holding the lock publishes; the others skip
   the cycle. The lock is released when the transaction ends, so if the publishing instance
   crashes, another takes over on its next cycle (within about 1 second).
2. **Database order, not clock order.** Rows are read by `Sequence`, an identity column that
   PostgreSQL assigns. `OccurredAt` comes from each instance's clock, and clocks drift.
3. **Pipelined but ordered.**
   - Each message is serialized first, then queued, so the queue order equals the send order.
   - The idempotent producer keeps that order on each partition.
   - A row is marked published only if it and every row before it succeeded. Anything after a
     failure is re-sent, and consumers deduplicate.
4. **No permanent blockage.**
   - Outages (broker down, timeouts) are retried forever with exponential backoff.
   - A message that fails *permanently* (unserializable payload, record too large) is abandoned
     after 3 attempts (`AbandonedAt`). `/health/ready` then reports Degraded.

**Try it:** `docker compose up -d --scale api=3`. The integration test
`OutboxPublisherTests.Only_the_instance_holding_the_lock_publishes` shows the lock behaviour.

**Remaining gap:** if the publishing instance loses its *database* connection mid-cycle while Kafka
is still reachable, a second instance can start before the first notices. Two ways to close it:
- Kafka transactions with a shared `transactional.id`, so the new producer fences off the old one.
- Change data capture (Debezium) reading the outbox from the write-ahead log.

## 4. Consumer: bounded retries, dead letters, alerts

`OrderEventProcessor` (unit-tested rule by rule) decides what happens to each message:

| Situation | Action |
|---|---|
| Can't decode (not Confluent-framed Protobuf), tombstone, or fails validation | Send to `order-events.dlt` immediately |
| Applying it throws | Retry in place: 3 attempts, 200 ms → 400 ms |
| Still failing, **transient** (database down, timeout, concurrency conflict) | `RetryLater`: the consumer seeks back to the same offset and backs off from 1 s up to 30 s. It never dead-letters because of an outage. |
| Still failing, **permanent** (a bug, a constraint violation) | Send to the DLT so it can't block the partition |
| DLT itself unavailable | `RetryLater`: a message is never skipped without being parked first |

The consumer itself (`OrderEventsConsumer`):
- Uses cooperative-sticky assignment, so adding or removing a worker only pauses the partitions
  that move.
- Supports optional static membership (`Kafka:GroupInstanceId`), so restarts don't rebalance.
- Stores the offset after processing; commits run in the background.
- Keeps calling `Consume()` while backing off, so it stays inside `max.poll.interval.ms`.
- Can scale to 6 instances (`--scale stats=6`).

Stats rows now have `xmin` concurrency tokens, so parallel workers can't overwrite each other's
increments. A conflict is classified as transient and simply retried.

**Dead-letter messages** keep the original key, value bytes and headers, plus:
`dlt-original-topic`, `dlt-original-partition`, `dlt-original-offset`, `dlt-reason`, `dlt-error`,
`dlt-attempts`, `dlt-failed-at`.

**Alerts** (`infra/monitoring/alerts.yml`, unit-tested in `alerts.test.yml`):

| Alert | Fires when |
|---|---|
| `OrderEventsDeadLettered` | Anything lands in the DLT (warning) |
| `StatsConsumerStuck` | Lag > 0 and no committed progress (critical) |
| `StatsConsumerLagHigh` | More than 100 events behind for 5 minutes (warning) |
| `KafkaBrokerDown` | Fewer than 3 brokers (critical) |
| `KafkaPartitionBelowMinIsr` | A partition has fewer than 2 in-sync replicas, so writes are rejected (critical) |
| `KafkaUnderReplicatedPartitions` | A replica has fallen behind for 5 minutes (warning) |
| `KafkaExporterDown` | The exporter is down, so the other alerts are blind (warning) |

Locally, alerts are visible at http://localhost:9090/alerts and http://localhost:9093. Point the
Alertmanager receiver at Slack, PagerDuty or email for real use (examples are in
`alertmanager.yml`).

### Runbook: replaying dead letters

1. Look at the messages: Kafka UI → Topics → `order-events.dlt`. Read the `dlt-reason` and
   `dlt-error` headers.
2. Fix the cause and deploy.
3. Replay:
   ```bash
   pip install confluent-kafka
   export KAFKA_ADMIN_PASSWORD=...           # from .env
   python3 infra/kafka/replay_dlt.py --dry-run
   python3 infra/kafka/replay_dlt.py --reason processing-failed
   ```

Replayed events are appended after everything already on their partition. That's safe for the
stats projection (counts are order-independent, and duplicates are skipped by event id). A consumer
that keeps "latest state per order" would need a per-order sequence check first.

## 5. Schema Registry and the event contract

- **Contract:** `src/BrewQueue.Contracts/Protos/order_event.proto`, compiled to C# at build time
  by Grpc.Tools. Money is sent as integer cents and timestamps as Unix milliseconds.
- **Wire format:** Confluent framing. Magic byte `0`, then the 4-byte schema id, then the message
  index, then the Protobuf payload.
- **Producer:** `auto.register.schemas=false` and `use.latest.version=true`. The API can't
  introduce a schema by accident.
- **Registration:** `infra/schema-registry/register_schemas.py` checks compatibility, then
  registers. The `schema-init` container runs it on startup.
- **Compatibility:** `BACKWARD_TRANSITIVE` (global and per subject). A new version must be able to
  read data written with every earlier version, which allows consumers to upgrade first.
- **Consumer:** decodes without contacting the registry, so a registry outage can't stop
  consumption.

### Runbook: changing the event schema

1. Edit `order_event.proto` following the rules in its header: add fields, never change a field's
   type or number, and `reserve` the numbers of removed fields.
2. Check it against the running registry (use this as a CI gate):
   ```bash
   SR_PASSWORD=... python3 infra/schema-registry/register_schemas.py --check-only
   ```
   It exits with code 2 and lists the problems if the change is incompatible. The integration
   tests in `SchemaRegistryTests` show what's rejected (type change, number reuse) and what's
   accepted (new field, rename).
3. Deploy consumers first, then register the new version (re-run `schema-init`, or run the script
   without `--check-only`), then deploy the producer.

With Protobuf, a field **rename** is wire-compatible: names aren't on the wire, only numbers. The
registry accepts it and existing consumers keep working. What it catches are the changes that
would actually break readers.

**Limitation:** open-source Schema Registry authenticates users but can't restrict who may
register a schema. For that you need Confluent RBAC, Apicurio with role-based access, or a gateway
in front of the registry. Here, the apps simply never register schemas.

---

## Other runbooks

**Rotate an app's Kafka password:** change it in `.env`, run `docker compose run --rm kafka-setup`
(which re-applies the SCRAM credential), then `docker compose up -d` to restart the app with it. To avoid downtime in production,
create a second user, switch the app over, then delete the old user.

**Rotate certificates:** delete `infra/kafka/secrets/*.p12`, `*.crt` and the `public/` copies, then
run `docker compose up -d --force-recreate`. In production, use your PKI or cert-manager with
automatic renewal. Brokers can reload keystores without a restart through dynamic configuration.

**Rebuild the stats from scratch:**
1. Stop `stats`.
2. Empty `stats_hourly_orders`, `stats_item_popularity` and `processed_events`.
3. Reset the group: `kafka-consumer-groups.sh --reset-offsets --to-earliest --group brewqueue-stats --topic order-events --execute`, run as admin.
4. Start `stats`.

Only events within the topic's retention (14 days) can be replayed.

## What's still not covered

These are left out on purpose; they're the next steps for a real enterprise deployment:

- Kafka on Kubernetes (Strimzi) or a managed service (Confluent Cloud, MSK, Event Hubs), with
  topics, users and ACLs managed through Terraform or GitOps instead of a shell script.
- Cross-region disaster recovery (MirrorMaker 2 or cluster linking), and tested failover.
- Fully fenced outbox publishing (Kafka transactions) or CDC with Debezium (see section 3).
- OpenTelemetry tracing propagated through the `traceparent` header, broker JMX metrics, and a
  Grafana dashboard.
- Client quotas, and keeping personal data out of events (or crypto-shredding it) for data-deletion
  requests. Today's events carry no customer name.
- EF Core migrations instead of `EnsureCreated`, so schema changes don't need a database reset.

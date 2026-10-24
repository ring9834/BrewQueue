#!/usr/bin/env bash
# "Topics, users and ACLs as code" for the local cluster. Runs once after the brokers are healthy
# and is safe to re-run: every step is idempotent.
#
# In production the same intent usually lives in Terraform (Confluent/MSK providers), Strimzi
# KafkaTopic/KafkaUser resources, or a GitOps tool, reviewed like any other code change.
set -euo pipefail

: "${CERT_PASSWORD:?}" "${KAFKA_ADMIN_PASSWORD:?}"
: "${KAFKA_API_PASSWORD:?}" "${KAFKA_STATS_PASSWORD:?}" "${KAFKA_SCHEMA_REGISTRY_PASSWORD:?}"
: "${KAFKA_UI_PASSWORD:?}" "${KAFKA_EXPORTER_PASSWORD:?}"

BIN=/opt/kafka/bin
BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
ADMIN=/tmp/admin-client.properties

cat > "$ADMIN" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=${CERT_PASSWORD}
EOF

kconfigs() { "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" "$@"; }
ktopics()  { "$BIN/kafka-topics.sh"  --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" "$@"; }
kacls()    { "$BIN/kafka-acls.sh"    --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" "$@"; }

# ------------------------------------------------------------------ users (SCRAM)
# One identity per service, so ACLs can follow least privilege and credentials rotate independently.
echo "==> Users"
create_user() {
  kconfigs --alter --entity-type users --entity-name "$1" --add-config "SCRAM-SHA-512=[password=$2]" > /dev/null
  echo "    $1"
}
create_user api             "$KAFKA_API_PASSWORD"
create_user stats           "$KAFKA_STATS_PASSWORD"
create_user schema-registry "$KAFKA_SCHEMA_REGISTRY_PASSWORD"
create_user kafka-ui        "$KAFKA_UI_PASSWORD"
create_user kafka-exporter  "$KAFKA_EXPORTER_PASSWORD"

# ------------------------------------------------------------------ topics
# create_topic NAME PARTITIONS CONFIG...  (creates if missing, then enforces the configs)
echo "==> Topics"
create_topic() {
  local name=$1 partitions=$2; shift 2
  local args=()
  for c in "$@"; do args+=(--config "$c"); done
  ktopics --create --if-not-exists --topic "$name" --partitions "$partitions" --replication-factor 3 "${args[@]}"
  # Re-apply configs so a manual change gets reverted to what's declared here.
  kconfigs --alter --entity-type topics --entity-name "$name" --add-config "$(IFS=,; echo "$*")" > /dev/null
  echo "    $name ($partitions partitions, RF 3)"
}

# Order events: keyed by order id. 6 partitions = up to 6 stats workers in parallel.
# Choose the partition count up front: adding partitions later changes which partition a key
# maps to, which temporarily breaks per-order ordering.
create_topic order-events 6 \
  min.insync.replicas=2 \
  retention.ms=1209600000 \
  cleanup.policy=delete

# Dead-letter topic: events the stats worker could not process. Kept longer for investigation.
create_topic order-events.dlt 6 \
  min.insync.replicas=2 \
  retention.ms=2592000000 \
  cleanup.policy=delete

# Schema Registry's storage. Must be a single, compacted partition.
create_topic _schemas 1 \
  min.insync.replicas=2 \
  cleanup.policy=compact

# ------------------------------------------------------------------ ACLs (least privilege)
echo "==> ACLs"
allow() { kacls --add --allow-principal "User:$1" "${@:2}" > /dev/null; }

# api: produce order events only.
allow api --operation Write --operation Describe --topic order-events

# stats: consume order events in its own group; write to the dead-letter topic.
allow stats --operation Read --operation Describe --topic order-events
allow stats --operation Read --operation Describe --group brewqueue-stats
allow stats --operation Write --operation Describe --topic order-events.dlt

# schema-registry: read/write its storage topic; coordinate instances via groups named schema-registry*
# (leader election uses "schema-registry", the store reader "schema-registry-<host>-<port>").
allow schema-registry --operation All --topic _schemas
allow schema-registry --operation All --resource-pattern-type prefixed --group schema-registry
allow schema-registry --operation Describe --operation DescribeConfigs --cluster

# kafka-ui: read-only browsing of every topic and group.
allow kafka-ui --operation Read --operation Describe --operation DescribeConfigs --topic '*'
allow kafka-ui --operation Describe --group '*'
allow kafka-ui --operation Describe --operation DescribeConfigs --cluster

# kafka-exporter: read offsets and lag only. It never reads message contents.
allow kafka-exporter --operation Describe --topic '*'
allow kafka-exporter --operation Describe --group '*'
allow kafka-exporter --operation Describe --cluster

kacls --list
echo "==> Kafka setup complete"

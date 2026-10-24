#!/usr/bin/env bash
# Entrypoint for each Kafka node. Writes server.properties from environment variables, formats
# the storage directory on first start (bootstrapping the `admin` SCRAM user), then starts Kafka.
#
# We don't use the image's built-in env-var configuration because it can't bootstrap SCRAM
# users at format time, and the first admin user has to exist before anyone can create others.
set -euo pipefail

: "${NODE_ID:?}" "${CLUSTER_ID:?}" "${EXTERNAL_PORT:?}" "${CERT_PASSWORD:?}" "${KAFKA_ADMIN_PASSWORD:?}"

HOST="kafka-${NODE_ID}"
SECRETS=/etc/kafka/secrets
CONFIG=/opt/kafka/config/brewqueue-server.properties
ADMIN_CONFIG=/opt/kafka/config/admin-client.properties

cat > "$CONFIG" <<EOF
############################## Roles and quorum ##############################
# Each node is both broker and KRaft controller. Large clusters usually run dedicated
# controller nodes (process.roles=controller) so broker load can't affect the metadata quorum.
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
controller.listener.names=CONTROLLER
broker.rack=zone-${NODE_ID}

############################## Listeners ##############################
#   BROKER     broker-to-broker replication   mTLS (certificates)       internal only
#   CONTROLLER KRaft quorum traffic           mTLS (certificates)       internal only
#   CLIENT     apps inside the Docker network SASL_SSL (SCRAM-SHA-512)  kafka-N:9092
#   EXTERNAL   apps on your machine           SASL_SSL (SCRAM-SHA-512)  localhost:${EXTERNAL_PORT}
listeners=BROKER://:9091,CLIENT://:9092,CONTROLLER://:9093,EXTERNAL://:19092
advertised.listeners=BROKER://${HOST}:9091,CLIENT://${HOST}:9092,EXTERNAL://localhost:${EXTERNAL_PORT}
listener.security.protocol.map=BROKER:SSL,CONTROLLER:SSL,CLIENT:SASL_SSL,EXTERNAL:SASL_SSL
inter.broker.listener.name=BROKER

############################## TLS ##############################
ssl.keystore.type=PKCS12
ssl.keystore.location=${SECRETS}/${HOST}.keystore.p12
ssl.keystore.password=${CERT_PASSWORD}
ssl.key.password=${CERT_PASSWORD}
ssl.truststore.type=PKCS12
ssl.truststore.location=${SECRETS}/truststore.p12
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=https
ssl.enabled.protocols=TLSv1.3,TLSv1.2
# Brokers and controllers must present a certificate signed by our CA.
listener.name.broker.ssl.client.auth=required
listener.name.controller.ssl.client.auth=required
# "CN=kafka-1,OU=brewqueue,O=BrewQueue" becomes the principal User:kafka-1.
ssl.principal.mapping.rules=RULE:^CN=([a-zA-Z0-9.-]+),.*$/\$1/,DEFAULT

############################## SASL ##############################
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;

############################## Authorization ##############################
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
# Deny by default: a principal can only do what an ACL explicitly allows.
allow.everyone.if.no.acl.found=false
# Brokers/controllers (via their certificates) and the bootstrap admin bypass ACLs.
super.users=User:admin;User:kafka-1;User:kafka-2;User:kafka-3

############################## Durability ##############################
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
# Never let an out-of-date replica become leader: that silently loses acknowledged writes.
unclean.leader.election.enable=false
# Topics are created deliberately by kafka-setup (topics as code), never on first use.
auto.create.topics.enable=false
num.partitions=6

############################## Storage and groups ##############################
log.dirs=/var/lib/kafka/data
log.retention.hours=168
group.initial.rebalance.delay.ms=3000
EOF

# Client config for the healthcheck and for running admin tools inside this container.
cat > "$ADMIN_CONFIG" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
ssl.truststore.type=PKCS12
ssl.truststore.location=${SECRETS}/truststore.p12
ssl.truststore.password=${CERT_PASSWORD}
EOF

# --ignore-formatted makes this a no-op after the first start. --add-scram only takes effect then too:
# later password changes go through kafka-configs.sh (see setup-kafka.sh).
/opt/kafka/bin/kafka-storage.sh format \
  --cluster-id "$CLUSTER_ID" \
  --config "$CONFIG" \
  --ignore-formatted \
  --add-scram "SCRAM-SHA-512=[name=admin,password=${KAFKA_ADMIN_PASSWORD}]"

exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"

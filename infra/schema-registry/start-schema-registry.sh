#!/usr/bin/env bash
# Entrypoint for Schema Registry: writes the HTTP Basic users file and JAAS config from
# environment variables, then hands over to the image's normal startup.
set -euo pipefail

: "${SR_ADMIN_PASSWORD:?}" "${SR_API_PASSWORD:?}" "${SR_UI_PASSWORD:?}"

# Format: "username: password,role". Roles are checked against SCHEMA_REGISTRY_AUTHENTICATION_ROLES.
# Open-source Schema Registry authenticates users but can't restrict *what* each one may do
# (for example "only CI may register schemas"); that needs Confluent RBAC or a gateway in front.
# Here the apps are configured never to register schemas themselves (auto.register.schemas=false).
# The stats worker has no user at all: decoding Protobuf doesn't need the registry.
cat > /tmp/sr-users.properties <<EOF
admin: ${SR_ADMIN_PASSWORD},admin
api: ${SR_API_PASSWORD},user
kafka-ui: ${SR_UI_PASSWORD},user
EOF
chmod 600 /tmp/sr-users.properties

# Jetty 9 class name, matching Confluent Platform 7.x. (Platform 8.x moved to Jetty 12:
# org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule.)
cat > /tmp/sr-jaas.conf <<'EOF'
SchemaRegistry {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/tmp/sr-users.properties"
  debug="false";
};
EOF

export SCHEMA_REGISTRY_OPTS="${SCHEMA_REGISTRY_OPTS:-} -Djava.security.auth.login.config=/tmp/sr-jaas.conf"
exec /etc/confluent/docker/run

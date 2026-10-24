#!/usr/bin/env bash
# Creates a private certificate authority (CA) and TLS keystores for the local Kafka cluster
# and Schema Registry. Uses only `keytool`, so it runs inside the apache/kafka image and needs
# nothing installed on your machine.
#
# Output (in $CERT_DIR):
#   ca.crt                         CA certificate (PEM): used by .NET/librdkafka, kafka-exporter, curl
#   public/                        copies of ca.crt and truststore.p12 in their own folder, so apps
#                                  can mount them without also seeing the private keys
#   truststore.p12                 CA certificate (PKCS12): used by Java clients (brokers, SR, Kafka UI)
#   <host>.keystore.p12            private key + certificate signed by the CA, one per server
#   ca.p12                         the CA's private key. In production this never leaves your PKI.
#
# Production note: use certificates from your organisation's PKI (or cert-manager on Kubernetes)
# with automated rotation. This script exists so the local cluster can run with real TLS.
set -euo pipefail

CERT_DIR=${CERT_DIR:-/secrets}
PASS=${CERT_PASSWORD:?CERT_PASSWORD must be set}
DAYS=${CERT_VALIDITY_DAYS:-825}
HOSTS=(kafka-1 kafka-2 kafka-3 schema-registry)

cd "$CERT_DIR"

all_present() {
  [[ -f ca.crt && -f truststore.p12 && -f public/ca.crt && -f public/truststore.p12 ]] || return 1
  for host in "${HOSTS[@]}"; do [[ -f "$host.keystore.p12" ]] || return 1; done
}

if all_present; then
  echo "Certificates already exist in $CERT_DIR; nothing to do. Delete the files to regenerate."
  exit 0
fi

echo "Generating CA and server certificates in $CERT_DIR"
rm -f ./*.p12 ./*.crt ./*.csr public/ca.crt public/truststore.p12

# 1. The certificate authority.
keytool -genkeypair -alias ca -dname "CN=BrewQueue Local CA,O=BrewQueue" \
  -keyalg RSA -keysize 3072 -validity "$DAYS" \
  -keystore ca.p12 -storetype PKCS12 -storepass "$PASS" \
  -ext bc:c=ca:true -ext ku:c=keyCertSign,cRLSign
keytool -exportcert -rfc -alias ca -keystore ca.p12 -storepass "$PASS" -file ca.crt

# 2. A truststore that contains only the CA certificate.
keytool -importcert -noprompt -alias ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$PASS"

# 3. One key pair per server, signed by the CA.
#    SAN = the Docker hostname (used inside the network) + localhost (used from your machine).
#    EKU includes clientAuth because brokers also present these certificates to each other (mTLS).
for host in "${HOSTS[@]}"; do
  keytool -genkeypair -alias "$host" -dname "CN=$host,OU=brewqueue,O=BrewQueue" \
    -keyalg RSA -keysize 2048 -validity "$DAYS" \
    -keystore "$host.keystore.p12" -storetype PKCS12 -storepass "$PASS" -keypass "$PASS"

  keytool -certreq -alias "$host" -keystore "$host.keystore.p12" -storepass "$PASS" -file "$host.csr"

  keytool -gencert -alias ca -keystore ca.p12 -storepass "$PASS" \
    -infile "$host.csr" -outfile "$host.crt" -rfc -validity "$DAYS" \
    -ext "san=dns:$host,dns:localhost,ip:127.0.0.1" \
    -ext ku:c=digitalSignature,keyEncipherment \
    -ext eku=serverAuth,clientAuth

  # Import the CA first so keytool can build the chain, then the signed certificate.
  keytool -importcert -noprompt -alias ca -file ca.crt -keystore "$host.keystore.p12" -storepass "$PASS"
  keytool -importcert -noprompt -alias "$host" -file "$host.crt" -keystore "$host.keystore.p12" -storepass "$PASS"

  rm -f "$host.csr"
done

mkdir -p public
cp ca.crt truststore.p12 public/

# Brokers and Schema Registry run as non-root users in their images, so they must be able to read these.
chmod 644 ./*.p12 ./*.crt public/ca.crt public/truststore.p12
# ...except the CA key, which nothing at runtime needs.
chmod 600 ca.p12
echo "Done."

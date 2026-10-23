#!/usr/bin/env bash
# Own one disposable broker, its certificates and ephemeral loopback ports per invocation.
set -euo pipefail
cd "$(dirname "$0")/.."
container="hashi-mqtt-fixture-$$-$(date +%s)"
certs="$(mktemp -d)"
cleanup() { docker rm -fv "$container" >/dev/null 2>&1 || true; rm -rf "$certs"; }
trap cleanup EXIT
image="${HASHI_MQTT_FIXTURE_IMAGE:-eclipse-mosquitto:2@sha256:38c0da4f2ef84284d47b3b3eeea1cb3bdeabe81ee10caf0cd5c5ff61ee3ea408}"
issue() { # name subject extensions
  openssl req -new -newkey rsa:2048 -nodes -keyout "$certs/$1.key" -subj "/CN=$2" -out "$certs/$1.csr" 2>/dev/null
  printf '%s\n' "$3" >"$certs/$1.ext"
  openssl x509 -req -in "$certs/$1.csr" -CA "$certs/ca.pem" -CAkey "$certs/ca.key" -CAcreateserial \
    -days 1 -sha256 -extfile "$certs/$1.ext" -out "$certs/$1.pem" 2>/dev/null
}
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=hashi fixture CA" \
  -keyout "$certs/ca.key" -out "$certs/ca.pem" 2>/dev/null
issue server localhost "subjectAltName=DNS:localhost,IP:127.0.0.1"
issue client hashi-client "extendedKeyUsage=clientAuth"
cat >"$certs/mosquitto.conf" <<'CONF'
allow_anonymous true
# Durable, so a soak's broker restart keeps acknowledged messages and sessions.
persistence true
persistence_location /mosquitto/data/
listener 1883
listener 8883
cafile /mosquitto/config/ca.pem
certfile /mosquitto/config/server.pem
keyfile /mosquitto/config/server.key
require_certificate true
CONF
chmod 644 "$certs"/*
docker create --name "$container" -p 127.0.0.1::1883 -p 127.0.0.1::8883 "$image" >/dev/null
docker cp "$certs/." "$container:/mosquitto/config/" >/dev/null
docker start "$container" >/dev/null
port() { docker port "$container" "$1/tcp" | head -n1 | sed 's/.*://'; }
for attempt in $(seq 1 30); do
  if docker logs "$container" 2>&1 | grep -q "Opening ipv4 listen socket on port 8883"; then break; fi
  if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ] || [ "$attempt" = 30 ]; then
    docker logs --tail 20 "$container" >&2
    echo "disposable MQTT broker did not become ready" >&2
    exit 1
  fi
  sleep 1
done
export HASHI_MQTT_DISPOSABLE=1 HASHI_MQTT_PORT="$(port 1883)" HASHI_MQTT_TLS_PORT="$(port 8883)" \
  HASHI_MQTT_CERTS="$certs" HASHI_MQTT_CONTAINER="$container" CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
# HASHI_SOAK_ONLY=1 (the nightly soak) skips the proofs; the soak pauses and
# restarts the broker, so it always runs alone, after them.
if [ "${HASHI_SOAK_ONLY:-}" != 1 ]; then
  cargo test --locked --features mqtt --test mqtt -- --ignored --nocapture --skip soak
fi
cargo test --locked --features mqtt --test mqtt -- --ignored --nocapture --exact mqtt_survives_a_soak_with_every_fault

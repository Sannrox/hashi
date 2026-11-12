#!/usr/bin/env bash
# Own one disposable database and ephemeral loopback port per invocation.
set -euo pipefail
cd "$(dirname "$0")/.."
container="hashi-postgres-fixture-$$-$(date +%s)"
cleanup() { docker rm -fv "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
image="${HASHI_POSTGRES_FIXTURE_IMAGE:-postgres:16-bookworm}"
docker run -d --name "$container" --tmpfs /var/lib/postgresql/data:rw,size=1073741824 -p 127.0.0.1::5432 \
  -e POSTGRES_PASSWORD=hashi-fixture-only "$image" \
  -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16 >/dev/null
for attempt in $(seq 1 90); do
  if docker exec -e PGPASSWORD=hashi-fixture-only "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then break; fi
  if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
    docker logs --tail 20 "$container" >&2
    exit 1
  fi
  if [ "$attempt" = 90 ]; then echo "disposable PostgreSQL did not become ready" >&2; exit 1; fi
  sleep 1
done
port="$(docker port "$container" 5432/tcp | sed 's/.*://')"
export HASHI_POSTGRES_DISPOSABLE=1 HASHI_POSTGRES_TEST_PORT="$port" HASHI_POSTGRES_CONTAINER="$container" \
  HASHI_POSTGRES_PASSWORD=hashi-fixture-only \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
cargo test --locked --features postgres --test postgres_cdc -- --ignored --nocapture --test-threads=1

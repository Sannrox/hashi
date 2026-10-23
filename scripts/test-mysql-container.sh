#!/usr/bin/env bash
# Own one disposable database and ephemeral loopback port per invocation.
set -euo pipefail
cd "$(dirname "$0")/.."
container="hashi-mysql-fixture-$$-$(date +%s)"
cleanup() { docker rm -fv "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
image="${HASHI_MYSQL_FIXTURE_IMAGE:-mysql:8.4@sha256:0f51e63e537a1f176423fa03ef559aabe6fa4372a53160a44bc7e75da7d15364}"
docker run -d --name "$container" --tmpfs /var/lib/mysql:rw,size=1073741824 -p 127.0.0.1::3306 \
  -e MYSQL_ROOT_PASSWORD=hashi-fixture-only -e MYSQL_ROOT_HOST=% "$image" \
  --server-id=1 --log-bin=mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON \
  --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL \
  --binlog-transaction-compression=OFF >/dev/null
for attempt in $(seq 1 90); do
  if docker exec -e MYSQL_PWD=hashi-fixture-only "$container" mysql --protocol=tcp -h127.0.0.1 -P3306 -uroot --connect-timeout=1 --batch -e "SELECT 1" >/dev/null 2>&1; then break; fi
  if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
    docker logs --tail 20 "$container" >&2
    exit 1
  fi
  if [ "$attempt" = 90 ]; then echo "disposable MySQL did not become ready" >&2; exit 1; fi
  sleep 1
done
port="$(docker port "$container" 3306/tcp | sed 's/.*://')"
export HASHI_MYSQL_DISPOSABLE=1 HASHI_MYSQL_TEST_PORT="$port" HASHI_MYSQL_CONTAINER="$container" \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
# HASHI_SOAK_ONLY=1 (the nightly soak) skips the proofs; the soak pauses the
# server and kills sessions, so it always runs alone, after them.
if [ "${HASHI_SOAK_ONLY:-}" != 1 ]; then
  cargo test --locked --features mysql --test mysql_cdc -- --ignored --nocapture --test-threads=1 --skip soak
fi
cargo test --locked --features mysql --test mysql_cdc -- --ignored --nocapture --exact mysql_survives_a_soak_with_every_fault

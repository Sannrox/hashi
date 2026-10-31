#!/usr/bin/env bash
# Own one disposable database and ephemeral loopback port per invocation.
set -euo pipefail
cd "$(dirname "$0")/.."
container="hashi-mssql-fixture-$$-$(date +%s)"
cleanup() { docker rm -fv "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
image="${HASHI_MSSQL_FIXTURE_IMAGE:-mcr.microsoft.com/mssql/server:2022-CU16-ubuntu-22.04}"
docker run -d --name "$container" -p 127.0.0.1::1433 \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD=Hashi_fixture1 \
  -e MSSQL_PID=Developer \
  -e MSSQL_AGENT_ENABLED=true \
  "$image" >/dev/null
for attempt in $(seq 1 90); do
  if docker exec "$container" /opt/mssql-tools18/bin/sqlcmd -C -S 127.0.0.1 -U sa -P Hashi_fixture1 -Q "SELECT 1" >/dev/null 2>&1; then break; fi
  if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
    docker logs --tail 40 "$container" >&2
    exit 1
  fi
  if [ "$attempt" = 90 ]; then echo "disposable SQL Server did not become ready" >&2; exit 1; fi
  sleep 2
done
port="$(docker port "$container" 1433/tcp | sed 's/.*://' | head -n 1)"
export HASHI_MSSQL_DISPOSABLE=1 HASHI_MSSQL_TEST_PORT="$port" HASHI_MSSQL_CONTAINER="$container" \
  HASHI_MSSQL_SA_PASSWORD=Hashi_fixture1 CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
cargo test --locked --features mssql --test mssql_cdc -- --ignored --nocapture --test-threads=1
cargo test --locked --features mssql --test mssql_historian -- --ignored --nocapture --test-threads=1

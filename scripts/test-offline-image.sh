#!/usr/bin/env bash
# Build the offline image and run a fixture config in it with no network at all.
# Each invocation owns its image tag, container and work directory.
set -euo pipefail
cd "$(dirname "$0")/.."
image="hashi-offline:$$-$(date +%s)"
container="hashi-offline-$$-$(date +%s)"
work="$(mktemp -d)"
cleanup() {
  docker rm -fv "$container" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
docker build -q -t "$image" . >/dev/null
mkdir -p "$work/secrets"
printf 'offline-fixture-credential' >"$work/secrets/line-1"
digest="$(printf 'offline-fixture-credential' | sha256sum 2>/dev/null || printf 'offline-fixture-credential' | shasum -a 256)"
cat >"$work/hashi.toml" <<CONF
checkpoint_dir = "state"

[[instance]]
name = "line-1"
source.fixture = { records = 3, credential_file = "secrets/line-1", accept_sha256 = ["${digest%% *}"] }
sink.jsonl = { dir = "out" }
CONF
# Run as the invoking user so the copied work directory stays writable.
docker create --name "$container" --network none --user "$(id -u):$(id -g)" "$image" \
  run --config /work/hashi.toml --once --listen 127.0.0.1:9464 >/dev/null
docker cp -a "$work/." "$container:/work"
logs="$(docker start -a "$container" 2>&1)"
echo "$logs"
grep -q "line-1: delivered 3 quarantined 0 checkpoint 3" <<<"$logs"
docker cp "$container:/work/out" "$work/result"
test "$(cat "$work"/result/*/part.jsonl | wc -l | tr -d ' ')" = 3
echo "offline image: fixture run delivered 3 records with --network none"

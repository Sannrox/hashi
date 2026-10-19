#!/usr/bin/env bash
# Disposable simulator proof. Each invocation owns its image tag and container.
set -euo pipefail
cd "$(dirname "$0")/.."
image="hashi-opcua-fixture:$$-$(date +%s)"
cleanup() { docker image rm "$image" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker build -f tests/opcua/Dockerfile -t "$image" .
docker run --rm --network none "$image"

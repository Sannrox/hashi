#!/usr/bin/env bash
# Disposable foreign-server proof. Each invocation owns its image tag.
# Starts the foreign OPC UA server and the Hashi client in one network
# namespace. No host ports, no state mounts; --network none stays.
set -euo pipefail
cd "$(dirname "$0")/.."
image="hashi-opcua-foreign-fixture:$$-$(date +%s)"
cleanup() { docker image rm "$image" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker build -f tests/opcua/foreign/Dockerfile -t "$image" .
docker run --rm --network none "$image"

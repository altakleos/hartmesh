#!/bin/sh
# Runs as uid 1000 (see entrypoint.sh). Prepares the tenant's data directory,
# renders config.yaml, seeds extensions_config.json once, then starts the
# Gateway with exactly one worker: DEER_FLOW_INTERNAL_AUTH_TOKEN is generated
# per process when unset and the login lockout counter is per worker, so a
# single worker is what keeps both coherent without a second configuration key.
set -eu

PROFILE=/opt/hartmesh

: "${DEER_FLOW_HOME:?}"
: "${DEER_FLOW_CONFIG_PATH:?}"
: "${DEER_FLOW_EXTENSIONS_CONFIG_PATH:?}"
: "${DEER_FLOW_SANDBOX_NETWORK:?}"

# uid 1000 has no passwd entry in the image; give tools that expect a home
# (docker CLI config, npx caches for MCP servers) a private, ephemeral one.
export HOME=/tmp/gateway-home
export DOCKER_CONFIG="$HOME/.docker"
mkdir -p "$HOME" "${UV_CACHE_DIR:-/tmp/uv-cache}"

mkdir -p "$DEER_FLOW_HOME" "$DEER_FLOW_HOME/skills"

# Compose does not create a network no service joins. Under SANDBOX_EGRESS=open
# every sandbox is started on this network, so it must exist before the first
# one; under allowlist it stays unused. Left unlabelled so `compose down`
# never has to remove a network that live sandboxes are attached to.
if ! docker network inspect "$DEER_FLOW_SANDBOX_NETWORK" >/dev/null 2>&1; then
  docker network create --driver bridge "$DEER_FLOW_SANDBOX_NETWORK" >/dev/null
fi

cd /app/backend

PYTHONPATH=. uv run --no-sync python "$PROFILE/gateway/render_config.py" \
  --template "$PROFILE/config.yaml" \
  --catalog "$PROFILE/providers" \
  --output "$DEER_FLOW_CONFIG_PATH"

if [ ! -f "$DEER_FLOW_EXTENSIONS_CONFIG_PATH" ]; then
  cp "$PROFILE/extensions_config.json" "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"
  chmod 0640 "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"
fi

exec env PYTHONPATH=. uv run --no-sync uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001 --workers 1

#!/bin/sh
# Runs as uid 1000 (see entrypoint.sh). Prepares the tenant's data directory,
# mirrors the image's public skill library onto it, renders config.yaml,
# seeds extensions_config.json once, then starts the
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

# The public skill library is release material: the image carries it and
# every start replaces public/ with it (README: "Public skills"); custom/ is
# the operator's and is never touched. Skills the image carries that the
# profile does not ship, by reason: chart-visualization and
# podcast-generation post tenant content to an external service (a chart's
# data, a script to narrate); web-design-guidelines fetches its own rules
# from a third-party URL through the Gateway's web_fetch, which the sandbox
# allowlist does not govern; find-skills and claude-to-deerflow describe
# flows that cannot work here (an install into a read-only mount the next
# start replaces, a DeerFlow at localhost:2026 a sandbox cannot reach); the
# other six the profile's own skill review refuses
# (tool_plane.validation_requires_skill_review), so a base holding any of
# them could never be promoted. backend/tests/test_compose_public_skills.py
# pins both lists and that every review exclusion is still needed.
EXCLUDED_PUBLIC_SKILLS="chart-visualization claude-to-deerflow find-skills github-deep-research image-generation music-generation podcast-generation skill-creator vercel-deploy-claimable video-generation web-design-guidelines"
# shellcheck disable=SC2086
sh "$PROFILE/gateway/seed_skills.sh" /app/skills/public "$DEER_FLOW_HOME/skills" $EXCLUDED_PUBLIC_SKILLS

# Compose does not create a network no service joins. Under SANDBOX_EGRESS=open
# every sandbox is started on this network, so it must exist before the first
# one; under allowlist it stays unused. Left unlabelled so `compose down`
# never has to remove a network that live sandboxes are attached to.
# Inter-container communication is off so a sandbox cannot reach a peer on
# its bridge address (the peer's published port on the host-gateway address
# remains; README: "SANDBOX_EGRESS=open"). Options apply at creation only: a
# network that already exists keeps the ones it was created with.
if ! docker network inspect "$DEER_FLOW_SANDBOX_NETWORK" >/dev/null 2>&1; then
  docker network create --driver bridge -o com.docker.network.bridge.enable_icc=false "$DEER_FLOW_SANDBOX_NETWORK" >/dev/null
fi

cd /app/backend

# Reads the environment as well as these two paths: the provider keys select
# catalog fragments, the optional HARTMESH_MODELS_FILE replaces the model
# list wholesale (README: "Operator-managed models"), and the sign-in keys
# select the mode (README: "Sign-in"). A refusal exits non-zero,
# which `set -e` turns into a container exit, leaving the last rendered
# config.yaml where it was.
PYTHONPATH=. uv run --no-sync python "$PROFILE/gateway/render_config.py" \
  --template "$PROFILE/config.yaml" \
  --catalog "$PROFILE/providers" \
  --output "$DEER_FLOW_CONFIG_PATH"

if [ ! -f "$DEER_FLOW_EXTENSIONS_CONFIG_PATH" ]; then
  cp "$PROFILE/extensions_config.json" "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"
  chmod 0640 "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"
fi

exec env PYTHONPATH=. uv run --no-sync uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001 --workers 1

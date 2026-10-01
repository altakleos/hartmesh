#!/bin/sh
# Starts SearXNG for the tenant profile. Always invoked as `sh run.sh` (the
# bundle carries no exec bits), as uid 1000 on a read-only root filesystem,
# with this directory as the instance's configuration directory.
#
# SearXNG refuses to start without a secret_key. That key signs the HTML
# interface's cookies and nothing else, and this instance has no HTML users:
# only the Gateway calls it, over the app network, for JSON. So the secret is
# minted here per start from the kernel's randomness and never written
# anywhere: no .env key to provision, nothing in git, nothing on the data
# disk, and a restart simply mints another.
#
# The image's own start script requires /etc/searxng/settings.yml and will
# try to create one there. The root filesystem is read-only, so compose.yaml
# gives /etc/searxng a small tmpfs and this script copies the bundle's
# settings into it; the bundle itself stays read-only.
set -eu

BUNDLE="${HARTMESH_SEARXNG_BUNDLE:-/opt/hartmesh/searxng}"
CONFIG_DIR="${SEARXNG_CONFIG_PATH:-/etc/searxng}"

if [ -z "${SEARXNG_SECRET:-}" ]; then
  SEARXNG_SECRET="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fi
export SEARXNG_SECRET

mkdir -p "$CONFIG_DIR"
cp "$BUNDLE/settings.yml" "$CONFIG_DIR/settings.yml"

exec /usr/local/searxng/entrypoint.sh "$@"

#!/bin/sh
# Gateway entrypoint for the tenant VM profile. Invoked as `sh entrypoint.sh`.
#
# The container starts as root (the image ships no USER directive, and the
# profile sets none) only to learn which group owns the host Docker socket,
# then drops to uid/gid 1000 plus that one supplementary group before any
# application code runs. The .env contract has no key for the socket's group
# id, and it differs between hosts, so it is read from the socket itself.
set -eu

SOCKET=/var/run/docker.sock
RUN=/opt/hartmesh/gateway/run.sh

if [ ! -S "$SOCKET" ]; then
  echo "entrypoint.sh: $SOCKET is not mounted; the Gateway cannot create sandboxes without it" >&2
  exit 1
fi

if [ "$(id -u)" != "0" ]; then
  # An operator override already runs the container unprivileged.
  exec sh "$RUN"
fi

docker_gid="$(stat -c %g "$SOCKET")"
exec setpriv --reuid=1000 --regid=1000 --groups="$docker_gid" --inh-caps=-all --no-new-privs sh "$RUN"

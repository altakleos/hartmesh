set -eu
SOCKET=/var/run/docker.sock
test -S "$SOCKET"
test "$(id -u)" = 0
docker_gid="$(stat -c %g "$SOCKET")"
test "$docker_gid" != 0
cd /app/backend
export HOME=/tmp/gateway-home
export DOCKER_CONFIG="$HOME/.docker"
export PYTHONPATH=.
exec setpriv --reuid=1000 --regid=1000 --groups="$docker_gid" --inh-caps=-all --no-new-privs sh -ec '
    mkdir -p -m 0700 "$HOME"
    exec "$@"
' gateway-user "$@"

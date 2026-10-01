#!/bin/sh
# Render the tenant's nginx.conf and start nginx.
#
# Always invoked as `sh render.sh` (the bundle carries no exec bits). Reads two
# contract keys from the environment, edits three anchored lines of the
# verbatim copy of docker/nginx/nginx.conf, and never runs envsubst over the
# file: the config carries 99 bare `$name` nginx variables and no `${...}`
# placeholders, so unconstrained substitution would blank every proxy header.
#
#   server_name _;            -> server_name ${HARTMESH_PUBLIC_HOST};
#                                set_real_ip_from <each HARTMESH_TRUSTED_PROXIES entry>;
#                                real_ip_header X-Forwarded-For;
#                                real_ip_recursive on;
#   listen [::]:2026 ...;     -> dropped when the container has no IPv6
#
# Writes /tmp/nginx.conf because uid 101 cannot write /etc/nginx.
set -eu

# Overridable only so the offline test suite can exercise this script.
SOURCE="${HARTMESH_NGINX_SOURCE:-/opt/hartmesh/nginx/nginx.conf}"
TARGET="${HARTMESH_NGINX_TARGET:-/tmp/nginx.conf}"

host="${HARTMESH_PUBLIC_HOST:?HARTMESH_PUBLIC_HOST is required}"
proxies="${HARTMESH_TRUSTED_PROXIES:?HARTMESH_TRUSTED_PROXIES is required}"

case "$host" in
  "" | *[!A-Za-z0-9.-]*)
    echo "render.sh: HARTMESH_PUBLIC_HOST must be a hostname (letters, digits, dots, hyphens)" >&2
    exit 1
    ;;
esac

real_ip=""
old_ifs="$IFS"
IFS=','
for entry in $proxies; do
  entry="$(printf '%s' "$entry" | tr -d ' \t')"
  [ -n "$entry" ] || continue
  case "$entry" in
    *[!0-9A-Fa-f.:/]*)
      echo "render.sh: HARTMESH_TRUSTED_PROXIES entry '$entry' is not an IP address or CIDR" >&2
      exit 1
      ;;
  esac
  real_ip="${real_ip}set_real_ip_from ${entry};|"
done
IFS="$old_ifs"
if [ -z "$real_ip" ]; then
  echo "render.sh: HARTMESH_TRUSTED_PROXIES must name at least one address" >&2
  exit 1
fi

awk -v host="$host" -v realip="$real_ip" '
  /^[[:space:]]*server_name _;[[:space:]]*$/ {
    match($0, /^[[:space:]]*/)
    indent = substr($0, 1, RLENGTH)
    print indent "server_name " host ";"
    n = split(realip, parts, "|")
    for (i = 1; i <= n; i++) if (parts[i] != "") print indent parts[i]
    print indent "real_ip_header X-Forwarded-For;"
    print indent "real_ip_recursive on;"
    replaced++
    next
  }
  { print }
  END {
    if (replaced != 1) {
      print "render.sh: expected exactly one `server_name _;` anchor, found " replaced + 0 > "/dev/stderr"
      exit 1
    }
  }
' "$SOURCE" > "$TARGET"

# Same guard as docker/docker-compose-dev.yaml and the chart: the IPv6 listen
# fails to bind in a container without IPv6.
if [ ! -e /proc/net/if_inet6 ]; then
  sed -i '/^[[:space:]]*listen[[:space:]]\+\[::\]:2026[[:space:]]/d' "$TARGET"
fi

if [ "${HARTMESH_RENDER_ONLY:-}" = "1" ]; then
  exit 0
fi
exec nginx -c "$TARGET" -g 'daemon off;'

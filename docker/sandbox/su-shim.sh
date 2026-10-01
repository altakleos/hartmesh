#!/bin/bash
# su(1) shim for the same-uid case.
# Rationale: gem.sh:572 runs `su - $USER -c ...` while ALREADY being $USER.
# Real su(1) calls initgroups()->setgroups(2) unconditionally, which requires
# CAP_SETGID even when the group list is unchanged. Under cap-drop=ALL that is
# EPERM. When the target uid equals our own uid, su is semantically a no-op, so
# we emulate its environment setup and exec the command directly.
# Any other target is delegated to the real su, which will fail as it should.
set -u
orig_args=("$@")
login=0; cmd=""; user=""; shell_opt=""
while [ $# -gt 0 ]; do
  case "$1" in
    -|-l|--login)                 login=1; shift ;;
    -c)                           cmd="${2:-}"; shift 2 ;;
    --command=*)                  cmd="${1#--command=}"; shift ;;
    -s|--shell)                   shell_opt="${2:-}"; shift 2 ;;
    --shell=*)                    shell_opt="${1#--shell=}"; shift ;;
    -m|-p|--preserve-environment) shift ;;
    --)                           shift; [ $# -gt 0 ] && { user="$1"; shift; }; break ;;
    -*)                           shift ;;
    *)                            user="$1"; shift ;;
  esac
done
[ -z "$user" ] && user="root"
ent="$(getent passwd "$user" 2>/dev/null || true)"
if [ -z "$ent" ]; then
  echo "su: user $user does not exist" >&2; exit 1
fi
t_uid="$(printf '%s' "$ent" | cut -d: -f3)"
t_name="$(printf '%s' "$ent" | cut -d: -f1)"
t_home="$(printf '%s' "$ent" | cut -d: -f6)"
t_shell="$(printf '%s' "$ent" | cut -d: -f7)"
if [ "$t_uid" != "$(id -u)" ]; then
  exec /usr/bin/su "${orig_args[@]}"          # different uid: real su, real failure
fi
[ -z "$t_shell" ] || [ "$t_shell" = "/usr/sbin/nologin" ] && t_shell="/bin/bash"
[ -n "$shell_opt" ] && t_shell="$shell_opt"
export HOME="$t_home" USER="$t_name" LOGNAME="$t_name" SHELL="$t_shell"
[ "$login" = 1 ] && cd "$t_home" 2>/dev/null
if [ -n "$cmd" ]; then exec "$t_shell" -c "$cmd"; else exec "$t_shell" -l; fi

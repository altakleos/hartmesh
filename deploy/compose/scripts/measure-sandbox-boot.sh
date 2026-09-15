#!/usr/bin/env bash
# Measure the sandbox image the way this profile runs it: time to a ready
# /v1/sandbox, processes and memory once idle, and optionally a command run
# inside every container once ready, for the full and the slim services
# profile, at one and two CPUs, one or several containers at once. One TSV row
# per container. Development-host figures are relative; the tenant VM class
# is what the readiness budget and the memory budget are set against
# (README: "Sandbox readiness budget", "Slim services profile").
#
# The bundle carries no exec bit, so invoke through bash (not sh: this is
# bash 4.4 or later) from this directory's parent, with knobs as environment
# variables:
#
#   bash scripts/measure-sandbox-boot.sh
#   PROFILES=slim CPUS=1 CONCURRENT=4 RUNS=2 bash scripts/measure-sandbox-boot.sh
#
#   IMAGE       sandbox image reference (default: sandbox.image in ./config.yaml)
#   RUNTIME     OCI runtime (default runsc; runc on a host without gVisor)
#   PROFILES    space-separated, from: full slim            (default "full slim")
#   CPUS        space-separated --cpus values                (default "1 2")
#   MEMORY      --memory and --memory-swap for every container. Default: the
#               profile's DEER_FLOW_SANDBOX_MEMORY in ./compose.yaml for slim
#               containers and 1024m for full ones, the figure the full
#               profile was released at (README: "Memory budget"); set MEMORY
#               to measure both at one limit.
#   PIDS        --pids-limit (default: DEER_FLOW_SANDBOX_PIDS_LIMIT in ./compose.yaml)
#   RUNS        repetitions of every profile x cpus cell      (default 1)
#   CONCURRENT  containers started at once in every run       (default 1)
#   EXEC        shell command run inside each container once ready, timed (default none)
#   MOUNTS      extra docker run arguments, e.g. "-v /srv/x:/mnt/x:ro" (default none);
#               placed before the hardening flags, so they cannot override it
#   TIMEOUT     seconds to wait for readiness                 (default 240)
#   SETTLE      seconds after readiness before the idle figures are read (default 5)
#   OUT         TSV path (default: a file under a private temporary directory,
#               named on exit; the bundle directory on a guest is root-owned)
#
# Containers get the profile's hardening (uid 1000, no capabilities,
# no-new-privileges, the built-in seccomp filter) and no network: the relay
# and the egress policy are not what is measured here. Idle memory and
# processes are read SETTLE seconds after readiness and before EXEC; memory
# and process peaks and OOM kills come from the host's cgroup for the
# container (cgroup v2, found through the container's init pid), which is
# where an OOM kill is decided, and are read after EXEC. Containers are kept
# until their row is written so a kill is recorded as one (status oom_killed
# or exited, never ok), then removed; an interrupted run removes its
# containers too. Every container's clock starts at its own `docker run`.
set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
profile_dir="$(dirname "$here")"
RUNTIME="${RUNTIME:-runsc}"
IMAGE="${IMAGE:-$(sed -n 's/^  image: \(ghcr\.io\/altakleos\/hartmesh-sandbox[^ ]*\)$/\1/p' "$profile_dir/config.yaml" | head -n 1)}"
SLIM_MEMORY="$(sed -n 's/^      DEER_FLOW_SANDBOX_MEMORY: \([0-9]*[mg]\)$/\1/p' "$profile_dir/compose.yaml" | head -n 1)"
FULL_MEMORY="1024m"
PIDS="${PIDS:-$(sed -n 's/^      DEER_FLOW_SANDBOX_PIDS_LIMIT: "\([0-9]*\)"$/\1/p' "$profile_dir/compose.yaml" | head -n 1)}"
PROFILES="${PROFILES:-full slim}"
CPUS="${CPUS:-1 2}"
RUNS="${RUNS:-1}"
CONCURRENT="${CONCURRENT:-1}"
EXEC="${EXEC:-}"
MOUNTS="${MOUNTS:-}"
TIMEOUT="${TIMEOUT:-240}"
SETTLE="${SETTLE:-5}"
if [ -z "$IMAGE" ] || [ -z "${MEMORY:-$SLIM_MEMORY}" ] || [ -z "$PIDS" ]; then
  echo "measure-sandbox-boot: could not read the image or the limits from $profile_dir; set IMAGE, MEMORY and PIDS" >&2
  exit 2
fi
for profile in $PROFILES; do
  case "$profile" in full|slim) ;; *) echo "measure-sandbox-boot: PROFILES accepts full and slim, not '$profile'" >&2; exit 2 ;; esac
done

work="$(mktemp -d)"
OUT="${OUT:-$work/measure-sandbox-boot.$RUNTIME.tsv}"
names=()
cleanup() {
  [ "${#names[@]}" -gt 0 ] && docker rm -f "${names[@]}" >/dev/null 2>&1
  case "$OUT" in "$work"/*) ;; *) rm -rf "$work" ;; esac
}
trap 'cleanup' EXIT
trap 'echo "measure-sandbox-boot: interrupted" >&2; exit 130' INT TERM

SLIM=(-e DISABLE_BROWSER=true -e DISABLE_JUPYTER=true -e DISABLE_CODE_SERVER=true -e DISABLE_VNC=true -e DISABLE_MCP_BROWSER=true -e DISABLE_NODEJS_REPL=true)
HARDENING=(--user 1000:1000 --cap-drop=ALL --security-opt no-new-privileges --security-opt seccomp=builtin --network none)
# MOUNTS is a deliberate word-split list of docker arguments.
# shellcheck disable=SC2206
EXTRA=($MOUNTS)

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1f", b - a }'; }
cgroup_dir() {
  local pid rel
  pid="$(docker inspect --format '{{.State.Pid}}' "$1" 2>/dev/null)" || return 1
  [ -n "$pid" ] && [ "$pid" != 0 ] || return 1
  rel="$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null)"
  [ -n "$rel" ] && [ -r "/sys/fs/cgroup$rel/memory.current" ] || return 1
  echo "/sys/fs/cgroup$rel"
}
mib() { [ -n "${1:-}" ] && awk -v m="$1" 'BEGIN { printf "%d", m / 1048576 }' || echo "-"; }
probe() { docker exec "$1" curl -s -o /dev/null -m 2 -w '%{http_code}' http://127.0.0.1:8080/v1/sandbox 2>/dev/null || echo 000; }

printf 'profile\tcpus\trun\tindex\tmemory\tready_s\tprocs\tmem_MiB\tpeak_MiB\tpids_peak\toom_kills\texec_s\texec_exit\tstatus\n' > "$OUT" || exit 2
for cpus in $CPUS; do
  for profile in $PROFILES; do
    envs=()
    memory="${MEMORY:-$FULL_MEMORY}"
    if [ "$profile" = slim ]; then envs=("${SLIM[@]}"); memory="${MEMORY:-$SLIM_MEMORY}"; fi
    for run in $(seq 1 "$RUNS"); do
      batch=(); began=(); ready=(); status=(); procs=(); mem=()
      for index in $(seq 1 "$CONCURRENT"); do
        name="hm-measure-$profile-$cpus-$run-$index-$$"
        batch+=("$name"); names+=("$name"); ready+=(""); status+=("timeout"); procs+=("-"); mem+=("-")
        began+=("$(now)")
        if ! docker run -d --name "$name" --runtime "$RUNTIME" --cpus "$cpus" \
            --memory "$memory" --memory-swap "$memory" --pids-limit "$PIDS" \
            ${EXTRA[@]+"${EXTRA[@]}"} "${HARDENING[@]}" ${envs[@]+"${envs[@]}"} "$IMAGE" >/dev/null 2>&1; then
          status[index-1]="run_failed"
        fi
      done
      start="$(now)"
      pending="$CONCURRENT"
      while [ "$pending" -gt 0 ] && awk -v a="$start" -v b="$(now)" -v t="$TIMEOUT" 'BEGIN { exit !(b - a < t) }'; do
        pending=0
        for i in $(seq 0 $((CONCURRENT - 1))); do
          [ -n "${ready[i]}" ] || [ "${status[i]}" = run_failed ] && continue
          if [ "$(probe "${batch[i]}")" = 200 ]; then
            ready[i]="$(elapsed "${began[i]}" "$(now)")"; status[i]=ok
          else
            pending=$((pending + 1))
          fi
        done
        [ "$pending" -gt 0 ] && sleep 1
      done
      sleep "$SETTLE"
      for i in $(seq 0 $((CONCURRENT - 1))); do
        [ "${status[i]}" = ok ] || continue
        procs[i]="$(docker exec "${batch[i]}" sh -c "ls /proc | grep -c '^[0-9]'" 2>/dev/null || echo -)"
        if dir="$(cgroup_dir "${batch[i]}")"; then mem[i]="$(mib "$(cat "$dir/memory.current" 2>/dev/null)")"; fi
      done
      if [ -n "$EXEC" ]; then
        pids=()
        for i in $(seq 0 $((CONCURRENT - 1))); do
          if [ "${status[i]}" = ok ]; then
            ( t0="$(now)"; docker exec "${batch[i]}" sh -c "$EXEC" >"$work/${batch[i]}.out" 2>&1; code=$?; printf '%s\t%s\n' "$(elapsed "$t0" "$(now)")" "$code" >"$work/${batch[i]}.exec" ) &
            pids+=($!)
          fi
        done
        for pid in ${pids[@]+"${pids[@]}"}; do wait "$pid"; done
      fi
      for i in $(seq 0 $((CONCURRENT - 1))); do
        name="${batch[i]}"
        peak="-"; pids_peak="-"; oom="-"; es="-"; ec="-"
        if [ "${status[i]}" != run_failed ]; then
          if dir="$(cgroup_dir "$name")"; then
            peak="$(mib "$(cat "$dir/memory.peak" 2>/dev/null)")"
            pids_peak="$(cat "$dir/pids.peak" 2>/dev/null || echo -)"
            oom="$(awk '$1 == "oom_kill" { print $2 }' "$dir/memory.events" 2>/dev/null || echo -)"
          else
            echo "--- $name: host cgroup not readable; peaks and OOM kills not recorded ---" >&2
          fi
          if [ -f "$work/$name.exec" ]; then
            IFS=$'\t' read -r es ec <"$work/$name.exec"
            [ "$ec" = 0 ] || { echo "--- $name: exec exited $ec ---" >&2; tail -n 20 "$work/$name.out" >&2; }
          fi
          state="$(docker inspect --format '{{.State.OOMKilled}} {{.State.Running}}' "$name" 2>/dev/null || echo "- -")"
          case "$state" in
            "true "*) status[i]="oom_killed" ;;
            "false false") status[i]="exited" ;;
            "- -") status[i]="unknown" ;;
          esac
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$profile" "$cpus" "$run" "$i" "$memory" "${ready[i]:--}" "${procs[i]}" "${mem[i]}" "$peak" "$pids_peak" "$oom" "$es" "$ec" "${status[i]}" >> "$OUT"
        docker rm -f "$name" >/dev/null 2>&1
      done
    done
  done
done
names=()
column -t -s $'\t' "$OUT" 2>/dev/null || cat "$OUT"
echo "written: $OUT"

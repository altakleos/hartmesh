#!/usr/bin/env bash
# Measure the sandbox image the way this profile runs it: time to a ready
# /v1/sandbox, processes and memory once idle, and optionally a command run
# inside every container once ready, for the full and the slim services
# profile, at one and two CPUs, one or several containers at once. One TSV row
# per container. Development-host figures are relative; the tenant VM class
# is what the readiness budget and the memory budget are set against
# (README: "Sandbox readiness budget", "Slim services profile").
#
# The bundle carries no exec bit, so invoke through bash from this directory's
# parent, with knobs as environment variables:
#
#   bash scripts/measure-sandbox-boot.sh
#   PROFILES=slim CPUS=1 CONCURRENT=4 RUNS=2 bash scripts/measure-sandbox-boot.sh
#
#   IMAGE       sandbox image reference (default: sandbox.image in ./config.yaml)
#   RUNTIME     OCI runtime (default runsc; runc on a host without gVisor)
#   PROFILES    space-separated, from: full slim            (default "full slim")
#   CPUS        space-separated --cpus values                (default "1 2")
#   MEMORY      --memory and --memory-swap (default: DEER_FLOW_SANDBOX_MEMORY in ./compose.yaml)
#   PIDS        --pids-limit (default: DEER_FLOW_SANDBOX_PIDS_LIMIT in ./compose.yaml)
#   RUNS        repetitions of every profile x cpus cell      (default 1)
#   CONCURRENT  containers started at once in every run       (default 1)
#   EXEC        shell command run inside each container once ready, timed (default none)
#   MOUNTS      extra docker run arguments, e.g. "-v /srv/x:/mnt/x:ro" (default none)
#   TIMEOUT     seconds to wait for readiness                 (default 240)
#   SETTLE      seconds after readiness before the idle figures are read (default 5)
#   OUT         TSV path (default $TMPDIR/measure-sandbox-boot.<RUNTIME>.tsv; the
#               bundle directory on a guest is root-owned, so not there)
#
# Containers get the profile's hardening (uid 1000, no capabilities,
# no-new-privileges, the built-in seccomp filter) and no network: the relay
# and the egress policy are not what is measured here. Memory and process
# peaks come from the host's cgroup for the container when it can be read
# (cgroup v2, systemd or cgroupfs driver), which is where an OOM kill is
# decided; the in-container figure is used for idle memory otherwise.
set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
profile_dir="$(dirname "$here")"
RUNTIME="${RUNTIME:-runsc}"
IMAGE="${IMAGE:-$(sed -n 's/^  image: \(ghcr\.io\/altakleos\/hartmesh-sandbox[^ ]*\)$/\1/p' "$profile_dir/config.yaml" | head -n 1)}"
MEMORY="${MEMORY:-$(sed -n 's/^      DEER_FLOW_SANDBOX_MEMORY: \([0-9]*[mg]\)$/\1/p' "$profile_dir/compose.yaml" | head -n 1)}"
PIDS="${PIDS:-$(sed -n 's/^      DEER_FLOW_SANDBOX_PIDS_LIMIT: "\([0-9]*\)"$/\1/p' "$profile_dir/compose.yaml" | head -n 1)}"
PROFILES="${PROFILES:-full slim}"
CPUS="${CPUS:-1 2}"
RUNS="${RUNS:-1}"
CONCURRENT="${CONCURRENT:-1}"
EXEC="${EXEC:-}"
MOUNTS="${MOUNTS:-}"
TIMEOUT="${TIMEOUT:-240}"
SETTLE="${SETTLE:-5}"
OUT="${OUT:-${TMPDIR:-/tmp}/measure-sandbox-boot.$RUNTIME.tsv}"
if [ -z "$IMAGE" ] || [ -z "$MEMORY" ] || [ -z "$PIDS" ]; then
  echo "measure-sandbox-boot: could not read the image or the limits from $profile_dir; set IMAGE, MEMORY and PIDS" >&2
  exit 2
fi

SLIM=(-e DISABLE_BROWSER=true -e DISABLE_JUPYTER=true -e DISABLE_CODE_SERVER=true -e DISABLE_VNC=true -e DISABLE_MCP_BROWSER=true -e DISABLE_NODEJS_REPL=true)
HARDENING=(--user 1000:1000 --cap-drop=ALL --security-opt no-new-privileges --security-opt seccomp=builtin --network none)
# MOUNTS is a deliberate word-split list of docker arguments.
# shellcheck disable=SC2206
EXTRA=($MOUNTS)

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1f", b - a }'; }
cgroup_dir() {
  local id
  id="$(docker inspect --format '{{.Id}}' "$1" 2>/dev/null)" || return 1
  for dir in "/sys/fs/cgroup/system.slice/docker-$id.scope" "/sys/fs/cgroup/docker/$id"; do
    [ -r "$dir/memory.current" ] && { echo "$dir"; return 0; }
  done
  return 1
}
mib() { [ -n "${1:-}" ] && awk -v m="$1" 'BEGIN { printf "%d", m / 1048576 }' || echo "-"; }
probe() { docker exec "$1" curl -s -o /dev/null -m 2 -w '%{http_code}' http://127.0.0.1:8080/v1/sandbox 2>/dev/null || echo 000; }

printf 'profile\tcpus\trun\tindex\tready_s\tprocs\tmem_MiB\tpeak_MiB\tpids_peak\toom_kills\texec_s\texec_exit\tstatus\n' > "$OUT"
for cpus in $CPUS; do
  for profile in $PROFILES; do
    envs=()
    [ "$profile" = slim ] && envs=("${SLIM[@]}")
    for run in $(seq 1 "$RUNS"); do
      names=(); ready=(); status=()
      for index in $(seq 1 "$CONCURRENT"); do
        name="hm-measure-$profile-$cpus-$run-$index-$$"
        names+=("$name"); ready+=(""); status+=("timeout")
        if ! docker run -d --rm --name "$name" --runtime "$RUNTIME" --cpus "$cpus" \
            --memory "$MEMORY" --memory-swap "$MEMORY" --pids-limit "$PIDS" \
            "${HARDENING[@]}" "${envs[@]}" "${EXTRA[@]}" "$IMAGE" >/dev/null 2>&1; then
          status[index-1]="run_failed"
        fi
      done
      start="$(now)"
      pending="$CONCURRENT"
      while [ "$pending" -gt 0 ] && awk -v a="$start" -v b="$(now)" -v t="$TIMEOUT" 'BEGIN { exit !(b - a < t) }'; do
        pending=0
        for i in $(seq 0 $((CONCURRENT - 1))); do
          [ -n "${ready[i]}" ] || [ "${status[i]}" = run_failed ] && continue
          if [ "$(probe "${names[i]}")" = 200 ]; then
            ready[i]="$(elapsed "$start" "$(now)")"; status[i]=ok
          else
            pending=$((pending + 1))
          fi
        done
        [ "$pending" -gt 0 ] && sleep 1
      done
      sleep "$SETTLE"
      if [ -n "$EXEC" ]; then
        pids=()
        for i in $(seq 0 $((CONCURRENT - 1))); do
          if [ "${status[i]}" = ok ]; then
            ( began="$(now)"; docker exec "${names[i]}" sh -c "$EXEC" >"/tmp/${names[i]}.out" 2>&1; code=$?; printf '%s\t%s\n' "$(elapsed "$began" "$(now)")" "$code" >"/tmp/${names[i]}.exec" ) &
            pids+=($!)
          fi
        done
        for pid in "${pids[@]}"; do wait "$pid"; done
      fi
      for i in $(seq 0 $((CONCURRENT - 1))); do
        name="${names[i]}"
        procs="-"; mem="-"; peak="-"; pids_peak="-"; oom="-"; es="-"; ec="-"
        if [ "${status[i]}" != run_failed ]; then
          if dir="$(cgroup_dir "$name")"; then
            mem="$(mib "$(cat "$dir/memory.current" 2>/dev/null)")"
            peak="$(mib "$(cat "$dir/memory.peak" 2>/dev/null)")"
            pids_peak="$(cat "$dir/pids.peak" 2>/dev/null || echo -)"
            oom="$(awk '$1 == "oom_kill" { print $2 }' "$dir/memory.events" 2>/dev/null || echo -)"
          else
            mem="$(mib "$(docker exec "$name" sh -c 'cat /sys/fs/cgroup/memory.current 2>/dev/null' 2>/dev/null)")"
          fi
          procs="$(docker exec "$name" sh -c "ls /proc | grep -c '^[0-9]'" 2>/dev/null || echo -)"
          if [ -f "/tmp/$name.exec" ]; then
            IFS=$'\t' read -r es ec <"/tmp/$name.exec"
            [ "$ec" = 0 ] || { echo "--- $name: exec exited $ec ---" >&2; tail -n 20 "/tmp/$name.out" >&2; }
          fi
          [ "$(docker inspect --format '{{.State.OOMKilled}}' "$name" 2>/dev/null)" = true ] && status[i]="oom_killed"
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$profile" "$cpus" "$run" "$i" "${ready[i]:--}" "$procs" "$mem" "$peak" "$pids_peak" "$oom" "$es" "$ec" "${status[i]}" >> "$OUT"
        docker rm -f "$name" >/dev/null 2>&1
        rm -f "/tmp/$name.exec" "/tmp/$name.out"
      done
    done
  done
done
column -t -s $'\t' "$OUT" 2>/dev/null || cat "$OUT"
echo "written: $OUT"

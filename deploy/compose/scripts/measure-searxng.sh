#!/usr/bin/env bash
# Measure the search service the way this profile runs it, under the workload
# a tenant actually produced: the container's own limits, tmpfs mounts, pids
# bound and read-only root from compose.yaml, replaying real queries at the
# Gateway's concurrency, then reading the cgroup's own counters.
#
# Why it exists: the released 192 MiB was taken from what the Gateway could
# spare and checked against one near-idle sample (92 MiB resident after a
# turn). Under two ordinary tenant turns the cgroup reached its ceiling 170
# times (`memory.peak` 201,330,688 bytes, no OOM kill), which is the workload
# saying the figure was chosen the wrong way round (README: "Web search",
# "Memory budget").
#
# Run it uncapped first to find the natural peak, then at each candidate
# limit, and take the smallest limit that records zero `memory.events max`
# at this workload with the margin the README states. A run sends real
# queries to real engines from this host's address: Google's element gated
# the measured address after about 54 queries, so keep total queries per
# afternoon well under that and prefer few runs over many.
#
# The bundle carries no exec bit, so invoke through bash from this
# directory's parent, with knobs as environment variables:
#
#   bash scripts/measure-searxng.sh
#   MEMORY=320m QUERIES=queries.txt bash scripts/measure-searxng.sh
#
#   IMAGE       searxng image reference (default: the searxng service pin in ./compose.yaml)
#   MEMORY      --memory and --memory-swap (default: the compose mem_limit;
#               "none" leaves the container uncapped, which is how the
#               natural peak is measured)
#   PIDS        --pids-limit (default: the compose pids_limit)
#   CONCURRENT  queries in flight at once (default 4, the Gateway's own
#               CONCURRENT_SEARCHES in community/searxng/tools.py)
#   QUERIES     file of one query per line (default: the built-in set below);
#               a line may be prefixed "images:" to send it as an image query,
#               which reaches the separate image engines image_search uses
#   ROUNDS      times the query list is replayed                 (default 1)
#   SETTLE      seconds after the last query before the counters are read (default 5)
#   CGROUP_DIR  directory holding this container's memory.* counters, for a
#               host whose cgroup layout is neither of the two tried below
#   OUT         TSV path (default: a file under a private temporary directory)
#
# Output is one TSV row per run: the limit, the workload, the counters, and
# the verdict. Nothing is written to the bundle directory, which is
# root-owned on a guest.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$HERE/compose.yaml"
BUNDLE="$HERE/searxng"

_compose_value() {
  # The searxng service block's value for key $1, without a YAML parser:
  # from the "  searxng:" line to the next service at the same indent.
  awk -v key="$1" '
    /^  [a-z]/ { inblock = ($0 ~ /^  searxng:/) }
    inblock && $1 == key":" { print $2; exit }
  ' "$COMPOSE"
}

IMAGE="${IMAGE:-$(_compose_value image)}"
MEMORY="${MEMORY:-$(_compose_value mem_limit)}"
PIDS="${PIDS:-$(_compose_value pids_limit)}"
CONCURRENT="${CONCURRENT:-4}"
ROUNDS="${ROUNDS:-1}"
SETTLE="${SETTLE:-5}"
OUT="${OUT:-$(mktemp -d)/searxng-memory.tsv}"

# The exact queries two tenant-class turns produced (Great Lakes research and
# the Muse report), in the order the model asked them. Real questions of the
# shape the service is sized for, not synthetic load.
read -r -d '' _DEFAULT_QUERIES <<'EOF' || true
Great Lakes overview geography facts
Great Lakes ecology environment issues 2025
Great Lakes economy shipping tourism
Great Lakes surface area volume depth statistics freshwater
Great Lakes invasive species pollution climate change challenges
Great Lakes population recreation tourism activities
"Great Lakes" Wikipedia comprehensive overview
Great Lakes drinking water supply 30 million people
Great Lakes shipping cargo tonnage economy industry
Great Lakes native species biodiversity fish wildlife
muse agent AI
Muse agent AI Meta personal agent features capabilities 2026
Muse personal AI agent Meta features pricing tiers how it works 2026
Muse AI agent deep research report analysis
"Meta Muse" personal AI agent Secure VM pricing features WhatsApp iOS Android
Muse Spark Meta agent architecture tools browser automation
Muse agent PDF report create document
Muse personal AI agent subscription pricing $20 $100 Meta September 2026
Muse agent Meta WhatsApp iOS Android access rollout
Muse agent secure VM sentinel safety system permissions
Muse agent comparison ChatGPT Atlas Gemini agent
Muse agent Meta key features tasks email travel shopping health finances
EOF

if [ -n "${QUERIES:-}" ]; then
  mapfile -t QUERY_LIST < "$QUERIES"
else
  mapfile -t QUERY_LIST <<< "$_DEFAULT_QUERIES"
fi

NAME="searxng-measure-$$"
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

memory_args=()
if [ "$MEMORY" != "none" ]; then
  memory_args=(--memory "$MEMORY" --memory-swap "$MEMORY")
fi

echo "image=$IMAGE memory=$MEMORY pids=$PIDS concurrent=$CONCURRENT rounds=$ROUNDS queries=${#QUERY_LIST[@]}" >&2

# Exactly the profile's runtime shape: the bundle read-only, the three tmpfs
# mounts compose.yaml declares, uid 1000, read-only root, the pids bound.
docker run -d --name "$NAME" \
  --user 1000:1000 \
  --read-only \
  --pids-limit "$PIDS" \
  "${memory_args[@]}" \
  -v "$BUNDLE:/opt/hartmesh/searxng:ro" \
  --tmpfs /etc/searxng:size=1048576,mode=1777 \
  --tmpfs /tmp:size=16777216,mode=1777 \
  --tmpfs /var/cache/searxng:size=4194304,mode=1777 \
  -p 127.0.0.1:0:8080 \
  --entrypoint sh \
  "$IMAGE" /opt/hartmesh/searxng/run.sh >/dev/null

PORT="$(docker port "$NAME" 8080/tcp | head -1 | awk -F: "{print \$NF}")"
BASE="http://127.0.0.1:$PORT"

echo "waiting for readiness at $BASE" >&2
for _i in $(seq 1 60); do
  if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "$BASE/healthz" >/dev/null || { echo "searxng never became ready" >&2; docker logs "$NAME" >&2; exit 1; }

cgroup_value() {
  # The container's own cgroup counters, read from inside the host's
  # cgroup tree by container id. CGROUP_DIR names the directory outright for
  # a host whose layout is neither of the two below.
  local id file
  id="$(docker inspect -f '{{.Id}}' "$NAME")"
  for file in \
    "${CGROUP_DIR:+$CGROUP_DIR/$1}" \
    "/sys/fs/cgroup/system.slice/docker-$id.scope/$1" \
    "/sys/fs/cgroup/docker/$id/$1"; do
    if [ -n "$file" ] && [ -r "$file" ]; then cat "$file"; return 0; fi
  done
  return 1
}

# Without the counters there is no measurement, and a row of zeros would read
# like one. Refuse here, naming the directory to pass as CGROUP_DIR, rather
# than let `set -e` end the run with no reason after readiness.
if ! cgroup_value memory.peak >/dev/null 2>&1; then
  echo "cannot read this container's cgroup counters (tried ${CGROUP_DIR:-the docker and system.slice layouts}); set CGROUP_DIR to the directory holding memory.peak" >&2
  exit 3
fi

# `memory.peak` is deliberately not reset: the limit has to hold the start as
# well as the workload, so the figure covers both. `memory.events max` is
# read before and after instead, so the reported count is this workload's.
started_events="$(cgroup_value memory.events 2>/dev/null | awk '$1=="max"{print $2}' || true)"
started_events="${started_events:-0}"

query_one() {
  # The two shapes the Gateway sends (community/searxng/tools.py): a web
  # query, and an image query carrying categories=images.
  local query="$1" category=()
  if [ "${query#images:}" != "$query" ]; then
    query="${query#images:}"
    category=(--data "categories=images")
  fi
  curl -fsS --max-time 30 -X POST "$BASE/search" \
    --data-urlencode "q=$query" --data "format=json" --data "language=auto" --data "pageno=1" \
    "${category[@]}" \
    -o /dev/null -w '%{http_code}\n' 2>/dev/null || echo "000"
}
export -f query_one 2>/dev/null || true

t0=$(date +%s.%N)
codes_file="$(mktemp)"
for _round in $(seq 1 "$ROUNDS"); do
  inflight=0
  for query in "${QUERY_LIST[@]}"; do
    [ -z "$query" ] && continue
    ( BASE="$BASE" query_one "$query" >> "$codes_file" ) &
    inflight=$((inflight + 1))
    if [ "$inflight" -ge "$CONCURRENT" ]; then wait -n; inflight=$((inflight - 1)); fi
  done
  wait
done
t1=$(date +%s.%N)

sleep "$SETTLE"

peak="$(cgroup_value memory.peak 2>/dev/null || echo 0)"
current="$(cgroup_value memory.current 2>/dev/null || echo 0)"
events="$(cgroup_value memory.events 2>/dev/null | awk '$1=="max"{print $2}' || true)"
events="${events:-0}"
oom="$(cgroup_value memory.events 2>/dev/null | awk '$1=="oom_kill"{print $2}' || true)"
oom="${oom:-0}"
hit=$((events - started_events))
ok="$(grep -c '^200$' "$codes_file" || true)"
total="$(wc -l < "$codes_file")"
elapsed="$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')"
rm -f "$codes_file"

if [ ! -s "$OUT" ]; then
  printf 'limit\tpids\tconcurrent\tqueries\tanswered\tseconds\tmemory_peak_bytes\tmemory_peak_mib\tmemory_current_mib\tmemory_max_events\toom_kills\n' > "$OUT"
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.1f\t%.1f\t%s\t%s\n' \
  "$MEMORY" "$PIDS" "$CONCURRENT" "$total" "$ok" "$elapsed" \
  "$peak" "$(awk -v p="$peak" 'BEGIN{print p/1048576}')" \
  "$(awk -v c="$current" 'BEGIN{print c/1048576}')" "$hit" "$oom" >> "$OUT"

column -t -s $'\t' "$OUT"
echo "rows: $OUT" >&2

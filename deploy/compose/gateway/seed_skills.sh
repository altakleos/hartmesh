#!/bin/sh
# Mirrors the public skill library the Gateway image carries onto the tenant
# data disk. Invoked by gateway/run.sh at every start, before the Gateway
# reads anything, as:
#
#   sh seed_skills.sh <image library> <skills root> [excluded skill ...]
#
# <skills root>/public is release material and is replaced whole: a skill an
# earlier image shipped, a stray file, or an edit made in place is gone after
# the next start. Skills the operator adds belong in custom/, which this never
# touches, and the sandbox-visible projection (skills_view/) is the Gateway's
# own to rebuild. An excluded skill is one the image carries but the profile
# does not ship; run.sh names them and says why. The copy is staged beside
# public/ and swapped in, so a copy that fails leaves the previous set in
# place, and `set -e` stops the start before uvicorn runs.
set -eu

if [ "$#" -lt 2 ]; then
  echo "usage: seed_skills.sh <image library> <skills root> [excluded skill ...]" >&2
  exit 2
fi
source=$1
root=$2
shift 2

if [ ! -d "$source" ]; then
  echo "seed_skills.sh: $source is not a directory; this Gateway image carries no public skill library" >&2
  exit 1
fi
if [ -z "$(find "$source" -mindepth 2 -name SKILL.md -type f -print -quit)" ]; then
  echo "seed_skills.sh: $source holds no skill; refusing to seed from the wrong image" >&2
  exit 1
fi

for name in "$@"; do
  case $name in
    '' | . | .. | .* | */*)
      echo "seed_skills.sh: '$name' is not a skill directory name" >&2
      exit 2
      ;;
  esac
done

staging="$root/public.seed"
rm -rf "$staging"
mkdir -p "$root"
cp -R "$source" "$staging"
for name in "$@"; do
  rm -rf "${staging:?}/${name:?}"
done
rm -rf "$root/public"
mv "$staging" "$root/public"
count=$(find "$root/public" -mindepth 2 -maxdepth 2 -name SKILL.md -type f | wc -l | tr -d ' ')
echo "seed_skills.sh: $count public skills seeded into $root/public from $source${1+ (excluded: $*)}"

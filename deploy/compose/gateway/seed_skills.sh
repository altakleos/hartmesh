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
# public/ and the swap is two renames, so a copy that fails leaves the
# previous set in place, and `set -e` stops the start before uvicorn runs.
# An image that carries no library at all is an older release: the seed says
# so and leaves public/ alone, so a profile checked out ahead of its pinned
# image still starts. A library directory without a single skill, or one that
# carries a symlink, is the wrong image and is refused.
set -eu
# cp -R applies the umask; pin it so the seeded modes are the script's, not
# the host's (files 0644, directories 0755, readable by the sandbox user).
umask 022

if [ "$#" -lt 2 ]; then
  echo "usage: seed_skills.sh <image library> <skills root> [excluded skill ...]" >&2
  exit 2
fi
source=$1
root=$2
shift 2

for path in "$source" "$root"; do
  case $path in
    /?*) ;;
    *)
      echo "seed_skills.sh: '$path' is not an absolute path" >&2
      exit 2
      ;;
  esac
done
for name in "$@"; do
  case $name in
    '' | . | .. | .* | */*)
      echo "seed_skills.sh: '$name' is not a skill directory name" >&2
      exit 2
      ;;
  esac
done

if [ ! -d "$source" ]; then
  echo "seed_skills.sh: $source is absent; this Gateway image predates the public skill library. Leaving $root/public as it is." >&2
  exit 0
fi
if [ -z "$(find "$source" -mindepth 2 -name SKILL.md -type f -print -quit)" ]; then
  echo "seed_skills.sh: $source holds no skill; refusing to seed from the wrong image" >&2
  exit 1
fi
if [ -n "$(find "$source" -type l -print -quit)" ]; then
  echo "seed_skills.sh: $source carries a symlink; refusing to seed from it" >&2
  exit 1
fi

staging="$root/public.seed"
previous="$root/public.old"
rm -rf "$staging" "$previous"
mkdir -p "$root"
cp -R "$source" "$staging"
for name in "$@"; do
  rm -rf "${staging:?}/${name:?}"
done
if [ -e "$root/public" ]; then
  mv "$root/public" "$previous"
fi
mv "$staging" "$root/public"
rm -rf "$previous"
count=$(find "$root/public" -mindepth 2 -maxdepth 2 -name SKILL.md -type f | wc -l | tr -d ' ')
echo "seed_skills.sh: $count public skills seeded into $root/public from $source${1+ (excluded: $*)}"

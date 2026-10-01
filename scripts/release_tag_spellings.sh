#!/usr/bin/env bash
# Print the OCI image and Helm chart tag spellings for one release version.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <version>" >&2
  exit 1
fi

VERSION="$1"
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([0-9A-Za-z.+-]+)?$ ]]; then
  echo "error: '$VERSION' is not a supported release version" >&2
  exit 1
fi

release_tag_spellings() {
  printf 'image_tag=v%s\n' "${VERSION//+/-}"
  printf 'chart_oci_tag=%s\n' "${VERSION//+/_}"
}

release_tag_spellings

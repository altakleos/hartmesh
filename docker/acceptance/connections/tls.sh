#!/usr/bin/env bash
# Trust one disposable fixture CA inside a hosted release's Gateway.
set -euo pipefail
: "${RUNNER_TEMP:?}"
: "${GITHUB_ACTIONS:?}"
[[ "$GITHUB_ACTIONS" == true && $# -eq 2 ]]
image=$1
bundle=$2
if [[ -n "${SOURCE_JOB_REVISION:-}" ]]; then
  # Only the source job installs a checkout-built image in the primary fixture.
  [[ "$bundle" == /opt/hartmesh && "$image" == "${BACKEND_REF:?}" ]]
  [[ "$image" =~ ^localhost:80/hartmesh-backend@sha256:[a-f0-9]{64}$ ]]
  [[ "$SOURCE_JOB_REVISION" =~ ^[a-f0-9]{40}$ && "$SOURCE_JOB_REVISION" == "${GITHUB_SHA:?}" ]]
  grep -Fxq -- "$image" "$bundle/images.txt"
  labels=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}} {{index .Config.Labels "org.opencontainers.image.source"}}' "$image")
  [[ "$labels" == "$SOURCE_JOB_REVISION https://github.com/${GITHUB_REPOSITORY:?}" ]]
else
  [[ "$image" =~ ^ghcr\.io/altakleos/hartmesh-backend@sha256:[a-f0-9]{64}$ ]]
fi
[[ "$bundle" == /opt/hartmesh || "$bundle" =~ ^/opt/hartmesh-rollback-(60|61|66)$ ]]
tls_dir="$RUNNER_TEMP/connection-fixture-tls"
if [[ ! -f "$tls_dir/ca.pem" ]]; then
  mkdir -m 0700 "$tls_dir"
  openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj '/CN=Disposable acceptance root' \
    -keyout "$tls_dir/ca.key" -out "$tls_dir/ca.pem"
  openssl req -new -newkey rsa:2048 -noenc -subj '/CN=notices-a.example.test' \
    -keyout "$tls_dir/notice.key" -out "$tls_dir/notice.csr"
  printf '%s\n' 'basicConstraints=critical,CA:FALSE' 'extendedKeyUsage=serverAuth' \
    'subjectAltName=DNS:notices-a.example.test,DNS:notices-b.example.test,DNS:notices-unlisted.example.test' > "$tls_dir/extensions"
  openssl x509 -req -days 1 -in "$tls_dir/notice.csr" -CA "$tls_dir/ca.pem" -CAkey "$tls_dir/ca.key" \
    -CAcreateserial -extfile "$tls_dir/extensions" -out "$tls_dir/notice.pem"
  sudo install -m 0644 "$tls_dir/ca.pem" /opt/hartmesh/notice-ca.pem
  sudo install -m 0644 "$tls_dir/notice.pem" /opt/hartmesh/notice.pem
  sudo install -m 0400 -o 65534 "$tls_dir/notice.key" /opt/hartmesh/notice.key
fi
python=/app/backend/.venv/bin/python
cafile=$(docker run --rm --entrypoint "$python" "$image" -c 'import certifi; print(certifi.where())')
[[ "$cafile" == /etc/ssl/certs/ca-certificates.crt || "$cafile" =~ ^/app/backend/\.venv/lib/python3\.[0-9]+/site-packages/certifi/cacert.pem$ ]]
docker run --rm --entrypoint "$python" "$image" -c 'import certifi,sys;from pathlib import Path;sys.stdout.buffer.write(Path(certifi.where()).read_bytes())' > "$tls_dir/bundle.pem"
cat "$tls_dir/ca.pem" >> "$tls_dir/bundle.pem"
sudo install -m 0644 "$tls_dir/bundle.pem" "$bundle/notice-trust.pem"
printf 'services:\n  gateway:\n    volumes:\n      - ./notice-trust.pem:%s:ro\n' "$cafile" | sudo tee "$bundle/compose.override.yaml" >/dev/null

# HartMesh tenant VM compose profile

One KVM guest per customer. Inside it the whole stack runs under Docker
Compose: gateway, frontend, nginx, PostgreSQL 16, Redis 7, with sandboxes
created by the Gateway's local Docker backend as containers under gVisor
(`runsc`). This directory is the profile that guest boots from; it is a
released deployment path beside the Helm chart, and the two consume the same
images. The Kubernetes provisioner and the chart are not present in the guest.

The reason for a VM at all is blast radius: the Gateway needs the host Docker
socket to create sandboxes, and a socket that grants root-equivalent control
of one customer's own guest is a trade the operator accepts, where the same
socket on a shared cluster would not be.

## What the estate does with this directory

The golden VM image copies this directory to `/opt/hartmesh` (root-owned,
`0755` directories, every file `0644`, every exec bit stripped) and pre-pulls
every line of `images.txt` by digest. Each tenant clone gets one data disk
mounted at `/srv/hartmesh` with `postgres/ redis/ home/ uploads/ artifacts/`
pre-created as uid/gid 1000, mode `0750`, plus a single `.env` written by the
estate's onboarding verb. The stack is started with:

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env up -d
```

Nothing in the bundle relies on being executable: every script is invoked as
`sh /opt/hartmesh/...` from `compose.yaml`.

## The `.env` contract

The estate writes exactly these keys; the profile consumes them and needs no
other. Documentation values are in `.env.example`, which renders under
`docker compose config` on its own.

| Key | Consumed by |
| --- | --- |
| `HARTMESH_TENANT` | `DEER_FLOW_TENANT_ID` on the Gateway (a DNS label). |
| `HARTMESH_PUBLIC_HOST` | nginx `server_name` only. Nothing in the application consumes it: the frontend derives its API origin from the page and its `NEXT_PUBLIC_*` values are baked at build time. |
| `HARTMESH_TRUSTED_PROXIES` | nginx `set_real_ip_from`, one per comma-separated address or CIDR. The front-door proxies whose `X-Forwarded-For` is trusted as the client address. |
| `HARTMESH_LISTEN` | nginx's published port, `<bind address>:<port>`; the only published port in the stack. |
| `HARTMESH_DATA_DIR` | Every bind mount, `DEER_FLOW_HOME`, `DEER_FLOW_HOST_BASE_DIR`, the rendered `config.yaml`, and the service-level `env_file` (`${HARTMESH_DATA_DIR}/.env`). Fixed at `/srv/hartmesh` by `config.yaml`, see below. |
| `SANDBOX_RUNTIME` | `DEER_FLOW_SANDBOX_RUNTIME`: the OCI runtime name each sandbox container runs under (`runsc` on the VM). |
| `SANDBOX_EGRESS` | `allowlist` (also when absent) or `open`; selects the `sandbox.network` block of the rendered `config.yaml`. Any other value refuses to render and the Gateway does not start. |
| `POSTGRES_PASSWORD` | `initdb` on the first start and `DATABASE_URL` on every start. Stable for the tenant's lifetime. |
| `REDIS_PASSWORD` | `redis-server --requirepass` and `DEER_FLOW_STREAM_BRIDGE_REDIS_URL`. Stable for the tenant's lifetime. |
| `AUTH_JWT_SECRET` | The Gateway's session-signing secret, so sessions survive a `home/` restored from backup. |

Provider keys follow verbatim, any subset of the `*_API_KEY` names
`config.example.yaml` references; nothing guarantees any particular one is
present. Both datastore passwords are embedded in DSNs as-is, so they must be
URL-safe (the estate generates them that way).

Only the Gateway receives the whole `.env` (`env_file`). The frontend and nginx
get explicit `environment:` entries and never see a provider key.

## Mount points

Two directories cross the container boundary:

- `/srv/hartmesh` (`HARTMESH_DATA_DIR`), created by the estate: `postgres/`
  is PGDATA, `redis/` holds the AOF, `home/` is the Gateway's home
  (`DEER_FLOW_HOME`) and is mounted into the Gateway **at the same path it has
  on the host**. Every sandbox bind-mount source the Gateway hands the host
  daemon resolves through `DEER_FLOW_HOST_BASE_DIR`; if the two paths
  differed, Docker would silently create the in-container path on the root
  disk and the sandbox would get an empty workspace while everything reported
  healthy. `config.yaml` cannot interpolate paths, so its literal
  `/srv/hartmesh/home/skills` fixes `HARTMESH_DATA_DIR` to `/srv/hartmesh`:
  changing it is a profile change, not a tenant setting. The Gateway keeps
  uploads and artifacts under `home/threads/<thread>/user-data/`, so the
  pre-created `uploads/` and `artifacts/` directories are unused by this
  profile and stay empty.
- `/opt/hartmesh`, this directory, mounted read-only into the Gateway
  (`gateway/`, `config.yaml`, `providers/`, `extensions_config.json`) and into
  nginx (`nginx/`). Two files the Gateway needs writable are seeded into
  `home/` at start: the rendered `config.yaml` (every start) and
  `extensions_config.json` (once, then the Gateway edits it at runtime).

Named volumes are deliberately absent: they would live on the root disk,
which is a linked clone of the golden image and is not backed up as tenant
data.

### Directory ownership

`postgres/` and `redis/` are pre-created as `1000:1000 0750` while the
official `postgres:16` and `redis:7-alpine` images run as uid 999, which
cannot write there. Both services run as `user: "1000:1000"`: the postgres
entrypoint supports an arbitrary uid through nss_wrapper given a writable data
directory, and redis needs only a writable `/data`. No root init step chowns
anything, and no estate-side change is needed.

### Gateway user

The Gateway image ships no `USER` directive. The container starts as root
only long enough for `gateway/entrypoint.sh` to read the group id of
`/var/run/docker.sock` (it differs between hosts and is not a contract key),
then `setpriv` drops to uid/gid 1000 plus that one supplementary group with
`--no-new-privs` before any application code runs. uid 1000 is the sandbox
container's user and the data directory's owner, so a directory the Gateway
creates is writable by the sandbox and a file the sandbox writes is readable
by the Gateway, with no chown in either direction.

## Ports

nginx publishes `${HARTMESH_LISTEN}:2026`, which renders as `0.0.0.0:2026:2026`
under the contract because the value carries its own bind address. Upstream
pins its own compose publishes to `${BIND_HOST:-127.0.0.1}`; this profile
differs on purpose because the VM's host firewall is the front door and admits
only the platform's front-door proxies. No other compose service publishes a
port (`docker compose ps` shows one `PORTS` entry).

Each sandbox does publish one host port by design: its proxy's under
`allowlist`, its own under `open`. Those bind the daemon's host-gateway
address (`host.docker.internal` from the Gateway's side; resolved by
`_resolve_docker_bind_host()`), never `0.0.0.0`, so `ss -ltnp` on the guest
shows more listeners than nginx while sandboxes run, all on the bridge address.

## Network model

Two compose networks:

- `app` (`172.30.10.0/24`, pinned): the five services. PostgreSQL and Redis
  are reachable only here and are never published. The pinned subnet is also
  `AUTH_TRUSTED_PROXIES` on the Gateway, whose login path honours `X-Real-IP`
  only from a TCP peer in that list and ignores `X-Forwarded-For` entirely;
  without it every login attempt would carry nginx's container address and
  the per-IP lockout would be shared by the whole tenant.
- `sandbox` (`hartmesh_sandbox`): joined by **no** compose service. Compose
  only creates networks a service uses, so `gateway/run.sh` creates it
  idempotently before the first sandbox can exist, unlabelled so `compose
  down` never has to remove a network live sandboxes are attached to.

### `SANDBOX_EGRESS=allowlist` (the default)

The backend stops being a plain `docker run` wrapper and builds a small
topology per sandbox, verified on `main`:

- a per-sandbox `--internal` bridge (`deer-flow-sandbox-net-<digest>`, both
  IPv4 and IPv6 gateway modes `isolated`) holding the sandbox and its proxy
  and nothing else;
- a per-sandbox egress bridge (`deer-flow-sandbox-egress-<digest>`) with
  inter-container communication disabled, holding only the proxy;
- a network-policy sidecar (`deer-flow-netproxy-<digest>`) created
  `--cap-drop=ALL --security-opt no-new-privileges --read-only --user
  65532:65532 --cpus 1 --pids-limit 128` with a 16 MiB `/tmp`, into which the
  Gateway installs `network_proxy.py` over `docker exec` (so the Gateway needs
  `docker exec`, not only `docker run`);
- the sandbox publishes **no** host port. The proxy publishes it, relays to
  `<sandbox>:8080`, and the Gateway authenticates with a per-sandbox relay
  token. The sandbox's own HTTP API has no authentication, so this is what
  closes the sandbox-to-sandbox reach: each sandbox sits alone on an internal
  network, reachable only through a relay that holds a token.

`DEER_FLOW_SANDBOX_NETWORK` is inert here: the backend passes its own
per-sandbox network as an override and never reads it. The proxy allows HTTP on
port 80 and HTTPS `CONNECT` on 443 to the standing `allow_domains` (the
package-installation set: `pypi.org`, `files.pythonhosted.org`,
`registry.npmjs.org`, `github.com`) plus per-session grants from the approval
card (`approval: prompt`, temporary grants 300 s); private, loopback,
link-local, multicast and cloud-metadata destinations are denied, and name
resolution is the proxy's, so a bare `dig` inside the sandbox failing is by
design. `web_search` and `web_fetch` run from the Gateway, not the sandbox, so
the list governs only what the model's own shell and code reach.

Restricted modes hard-require **Docker Engine 28 or newer**, checked when the
backend is constructed with a `RuntimeError`: a too-old daemon is a Gateway
that will not start, not a sandbox that degrades. The estate asserts no Docker
version, so this is the only guard.

The sidecar runs under the daemon's default runtime (runc), not `runsc`, by
decision: it is the trusted policy enforcement point, runs no model-authored
code, is already `--cap-drop=ALL --read-only` as uid 65532, and gVisor's
netstack under a policy proxy is unproven here.

### `SANDBOX_EGRESS=open`

All of the above is replaced by the shared `hartmesh_sandbox` bridge: every
sandbox is started on it (which keeps sandboxes off the daemon's default
bridge and away from `app`), egress is direct, there is no proxy, and each
sandbox publishes its own port on the host-gateway address. Consequences the
operator accepts for a tenant that chooses `open`:

- a sandbox **can reach a peer sandbox** (shared bridge, and published ports
  on a host address). Both belong to the same customer inside one VM, which
  is why it is that tenant's accepted residual and not the profile's, and why
  it would not be acceptable on a shared cluster;
- nothing in the profile denies `169.254.169.254` or other private ranges;
  the VM's firewall is the only guard.

PostgreSQL and Redis remain unreachable from the `sandbox` network in both
modes: they live only on `app`, and Docker isolates user-defined bridges from
each other.

## Sandbox hardening

Per-sandbox flags are configuration on the Gateway (`compose.yaml`) plus two
seams added to the backend for this profile:

| Setting | Value | Why |
| --- | --- | --- |
| `DEER_FLOW_SANDBOX_RUNTIME` | `${SANDBOX_RUNTIME}` | `--runtime runsc` on the sandbox's `docker run`; the daemon sets no `default-runtime`, so anything under gVisor must say so per container. |
| `DEER_FLOW_SANDBOX_CONTAINER_USER` | `1000:1000` | The fork's sandbox image ends in `USER 1000:1000`. |
| `DEER_FLOW_SANDBOX_IMAGE_STARTUP_CAPS` | `0` | `--cap-drop=ALL --security-opt no-new-privileges` with no compatibility capabilities: the image is pre-initialised non-root, so it needs neither `FOWNER` nor `DAC_OVERRIDE`. |
| `DEER_FLOW_SANDBOX_SECCOMP_UNCONFINED` | `0` | Emits `--security-opt seccomp=builtin` explicitly (omitting the option would inherit the daemon default). Under gVisor the host filter applies to the Sentry's own syscalls; the sandbox and its browser were proved to start with it (see below). |
| `DEER_FLOW_SANDBOX_MEMORY` | `640m` | The design's figure, with `--memory-swap` pinned equal (the guest has no swap, so this is explicitness, not a measurable change). |
| `DEER_FLOW_SANDBOX_CPUS` | `1` | Two sandboxes plus two proxies at `--cpus 1` sum to the guest's four vCPUs. |
| `DEER_FLOW_SANDBOX_PIDS_LIMIT` | `384` | The sandbox image idles at 198 processes (Chromium, Jupyter, node, supervisord) and peaked at 232 during a measured bash-plus-browser turn; 384 leaves 1.6x headroom over that peak while still bounding a fork bomb. |
| `DEER_FLOW_SANDBOX_PROXY_MEMORY` | `96m` | The sidecar's cgroup peaked at 48 MiB (process high-water mark 33 MiB) during the same turn; 96 MiB is twice that peak, with `--memory-swap` equal. |

## Memory budget

The guest has 6 GiB and no swap. In-VM limits sum to at most 5.0 GiB so it
keeps at least 512 MiB `MemAvailable` under a burst of Chromium sandboxes plus
a 1 GiB file operation.

| Service | `mem_limit` = `memswap_limit` |
| --- | --- |
| gateway | 1536 MiB |
| frontend | 384 MiB |
| nginx | 128 MiB |
| postgres | 768 MiB |
| redis | 256 MiB (`maxmemory 200mb`, `allkeys-lru`) |
| **services** | **3072 MiB** |

Equal `memswap_limit` is an assertion of intent: with no swap device it
changes nothing measurable.

Under `allowlist` each concurrent sandbox costs its 640 MiB plus its proxy's
limit (96 MiB, from the measurement above; the backend's own default is
256 MiB). The concurrent-sandbox ceiling is therefore **2**
(`sandbox.replicas: 2` in both modes, one budget, one gate, one behaviour):
3072 + 2 × (640 + 96) = 4544 MiB, against 5280 MiB for three. Three would need
a proxy under 42 MiB, which is not a realistic Python process, so the smaller
proxy buys headroom under the 512 MiB floor, not a third slot. `open` mode
carries no proxy and would fit three (3072 + 3 × 640 = 4992 MiB) if the
operator ever splits the ceiling by mode. A third concurrent sandbox is the
8 GiB VM class: an estate change, not a profile change.

640 MiB is the design's number, not the backend's default (`2g`), and it is
tight for the fork's sandbox image: under `runc` the idle container sat at
about 600 MiB of its 640 MiB cgroup limit with reclaim active
(`memory.events` `max` counting up) and no OOM kill through the measured
turn. Every per-sandbox limit is therefore set explicitly; inheriting the
backend defaults would give a tenant 2 GiB sandboxes and an OOM-killed guest.

`replicas` is a maximum with **LRU eviction**: a third acquisition does not
fail, it evicts the least-recently-used sandbox, which is what keeps the count
at two and the budget true. It is customer-visible: a thread whose sandbox was
evicted gets a fresh one on its next turn (its files persist under `home/`).
`idle_timeout: 1800` keeps an idle sandbox warm for thirty minutes: the budget
reserves both slots whether or not they are used, and a cold start of the
sandbox image under gVisor takes tens of seconds, so idle slots are kept
rather than freed; eviction still reclaims one when a third thread needs it.

## Durability

Deliberately relaxed, because the guest's disks sit on a replicated tier that
acknowledges every write it has journaled and the recovery point the product
quotes is one hour: PostgreSQL runs `synchronous_commit=off` with
`wal_writer_delay=200ms` (it still fsyncs on its own cadence; a guest crash
loses at most the last 200 ms of commits) and Redis uses `appendfsync
everysec`. This keeps the tenant's fsync rate near one per second, and the
storage tier's write budget is the density ceiling, so do not "fix" it upward.

## Deployment profile

The rendered `config.yaml` selects `deployment.profile: local_development`,
one of the two profiles that migrate the database themselves on start (no
migration job is needed; do not copy the chart's). `durable_production`, the
fail-closed one, also self-migrates but hard-requires the
`EXECUTION_POLICY_HMAC_KEYS` and `EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID`
credentials at start, which are not in the `.env` contract; adopting it is a
two-key contract change the estate must make, after which it is a one-line
change here. Everything the durable profile would otherwise check is already
in place: PostgreSQL for every store, `run_events.backend: db`,
`dedupe_storage: auto`, and an explicit `DEER_FLOW_TENANT_ID`.

The Gateway runs exactly one worker. `DEER_FLOW_INTERNAL_AUTH_TOKEN` is
generated per process when unset and the login lockout counter is per worker,
so a single worker is what keeps both coherent without a second configuration
key.

## Models and tools

`config.yaml` in this directory is a template; the Gateway never reads it
directly. At every start `gateway/run.sh` renders the effective file into
`home/config.yaml` (`gateway/render_config.py`) and points
`DEER_FLOW_CONFIG_PATH` at it; a set-but-missing path fails loudly, and the
Gateway image ships no `config.yaml` that could win silently.

The catalog under `providers/` holds one fragment per provider key, derived
from `config.example.yaml`: `models/` carries the models upstream's example
lists for that key (entries needing a second key, a regional endpoint or a
local endpoint the contract cannot supply are omitted), `tools/` carries the
key-bearing `web_search` / `web_fetch` / `image_search` backends. A fragment is
included only when its variable is present and non-empty, and it writes
`api_key: $NAME` (the reference, never the value), so the Gateway still expands
the secret itself and no secret lands on disk. Fragment tools replace the
template's keyless defaults (DuckDuckGo search, Jina fetch, DuckDuckGo image
search) by name; when several present keys provide the same tool, the first
fragment in file order wins, which is why the files are numbered.

A tenant with **no** model key starts, logs `provider keys found: none`, and
serves a frontend that reports no model configured. That is the correct
failure for the profile; refusing such a tenant belongs in the estate's
onboarding verb.

## Applying a `.env` re-render

Rotating a provider key is a re-render of `.env` followed by the same `up -d`.
Compose recreates only the services whose configuration changed: the Gateway
(which re-renders `config.yaml` on start), never PostgreSQL unless its
password changed, and it must not: `POSTGRES_PASSWORD` is used by `initdb`
once and authenticates every later connection.

## Logging

No service carries a `logging:` block. The guest's daemon sets
`log-driver: journald`, so every container's stdout already lands in the
journal, which the VM caps and ships; a `logging:` block would override that
with `json-file` on the root disk.

## Release pinning

At release the `image:` references in `compose.yaml`, `sandbox.image` and
`network.proxy_image` in `config.yaml`, and the lines of `images.txt` are the
same digest-pinned strings, written by `scripts/pin_compose_images.py` before
the tag (see `RELEASING.md`, "Compose profile pins"). Between releases the tree
carries the previous release's tag-form references as development
placeholders; the estate's grammar refuses such a bundle by design. Seven
images are pinned: gateway, frontend, sandbox, the network proxy (built under
the fork's own name, `<repo>-sandbox-network-proxy`), `postgres`, `redis` and
`nginx`.

## Not here

TLS, the front door (Traefik on the platform cluster, forwarding plain HTTP
with `X-Forwarded-For` / `X-Forwarded-Proto`), backups, and the host firewall
are all outside the VM and outside this profile.

## Proof record

Evidence gathered on a development host with Docker Engine 28.4.0, Compose
v2.39.4, and no `runsc` registered, so every live line below ran under `runc`;
the two gVisor-specific claims are recorded as unproved with the command that
proves them. The data directory, ownership and `.env` were built exactly as the
golden image lays them out (subdirectories `1000:1000 0750`), and the bundle
files carried no exec bit.

- `docker compose --project-directory deploy/compose --env-file .env.example
  config` renders with the contract keys alone: nginx `host_ip: 0.0.0.0`,
  `published: "2026"`, five services with `mem_limit == memswap_limit`.
- `docker compose ps` after `up -d`: one `PORTS` entry, nginx's; postgres and
  redis healthy as uid 1000 on the pre-created directories; the gateway
  healthy on `/health/ready`.
- `docker network ls` after `up`: `hartmesh_app` and `hartmesh_sandbox`
  present before the first sandbox (Compose created only `app`; `run.sh`
  created `sandbox`).
- Gateway process (`/proc/1/status` in the container): `Uid 1000`, `Gid
  1000`, `Groups <docker socket gid>`, `NoNewPrivs 1`; the docker CLI works
  from that identity and `pid 1` is uvicorn with `--workers 1`.
- `nginx -T -c /tmp/nginx.conf` in the running container: syntax ok,
  `server_name <HARTMESH_PUBLIC_HOST>;`, one `set_real_ip_from` per contract
  address, `real_ip_header X-Forwarded-For;`, `real_ip_recursive on;`; the
  bare-`$name` variable count is identical before and after the render (99
  across the eleven names). `GET /` through nginx returns the frontend
  (`<title>DeerFlow</title>`), `GET /health` the Gateway's health document.
- `allowlist`, from inside a live sandbox created through the real provider
  (two sandboxes acquired, the ceiling): `id` is `uid=1000(gem)`; the only
  route is the internal network; `postgres`/`redis` do not resolve and their
  `app` addresses are unreachable; the peer sandbox is unreachable both on its
  internal address and on its published host-gateway port; `https://pypi.org/
  simple/` answers `200` through the proxy; `https://example.com/` is refused
  (`403 from proxy after CONNECT`); `http://neverssl.com/` `403`;
  `http://169.254.169.254/` answers `IP-literal destinations are not allowed
  by sandbox network policy`; direct DNS is unavailable by design. `docker
  inspect` of the sandbox: `User=1000:1000`, `SecurityOpt=[no-new-privileges,
  seccomp=builtin]`, `CapDrop=[ALL]`, no `CapAdd`, `Memory=MemorySwap=640 MiB`,
  `NanoCpus=1`, no published port, one network `deer-flow-sandbox-net-*`
  (`internal=true`, both gateway modes `isolated`); every bind mount source
  under `home/`. The proxy: `User=65532`, read-only, `Memory=MemorySwap`,
  published on `172.17.0.1` (the host-gateway address), on the egress and the
  internal network only. `ss -ltn` showed the two sandbox ports on
  `172.17.0.1` and nothing of ours on `0.0.0.0`.
- Workspace both ways: the sandbox wrote `/mnt/user-data/outputs/proof.txt`
  (`1000:1000`) into a directory the Gateway had created, and the Gateway read
  it back through `read_file`.
- Browser under `seccomp=builtin`, no capabilities, uid 1000: `GET
  /v1/browser/info` reports Chromium 146 with its CDP endpoint and `GET
  /v1/browser/screenshot` returns a PNG; no OOM kill (`memory.events`).
- Measured turn (`pip download requests` through the proxy plus Chromium page
  loads and screenshots): sandbox `pids.peak` 232 (idle 198), proxy
  `memory.peak` 48 MiB (idle 19 MiB, process HWM 33 MiB), `pids.peak` 9.
- `open`, after re-rendering `.env` with `SANDBOX_EGRESS=open` and `up -d`
  (only the Gateway was recreated): the sandbox sits on `hartmesh_sandbox`
  with a default route, publishes its own port on `172.17.0.1`, no proxy
  exists; `postgres`/`redis` still do not resolve and are unreachable from the
  sandbox network; `https://example.com/` and `http://neverssl.com/` answer
  `200` directly, direct DNS works; the **peer sandbox is reachable** on its
  bridge address and on its published port (the accepted residual); the
  metadata address merely timed out here because this host has no route to it,
  nothing in the profile denied it.
- After release and process exit, no sandbox containers or per-sandbox
  networks remained.

Unproved here, because the host has no `runsc`:

- `docker inspect <sandbox> --format '{{.HostConfig.Runtime}}'` printing
  `runsc` after `SANDBOX_RUNTIME=runsc` (the argument builder is unit-tested to
  emit `--runtime runsc`; the flag arrived as `--runtime runc` live).
- The sandbox and its browser starting under `seccomp=builtin` **with gVisor**:
  the same `GET /v1/browser/info` and `/v1/browser/screenshot` checks against a
  sandbox created with `SANDBOX_RUNTIME=runsc`.


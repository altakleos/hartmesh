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

## What the operator does with this directory

The golden VM image copies this directory to `/opt/hartmesh` (root-owned,
`0755` directories, every file `0644`, every exec bit stripped) and pre-pulls
every line of `images.txt` by digest. Each tenant clone gets one data disk
mounted at `/srv/hartmesh` with `postgres/ redis/ home/ uploads/ artifacts/`
pre-created as uid/gid 1000, mode `0750`, plus a single `.env` written by the
operator's onboarding verb. The stack is started with:

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env up -d
```

Nothing in the bundle relies on being executable: every script is invoked as
`sh /opt/hartmesh/...` from `compose.yaml`.

## The `.env` contract

The operator writes exactly these keys; the profile consumes them and needs no
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
URL-safe (the operator generates them that way).

Values pass through Compose's dotenv parser twice (the `--env-file` and the
Gateway's `env_file` are the same file): a bare `$NAME` or `${NAME}` inside a
value is interpolated, ` #` after whitespace starts a comment, and surrounding
quotes are stripped. A secret carrying a `$` would therefore be silently
rewritten before the Gateway saw it. The operator single-quotes every value it
does not fix itself (the bao secrets and every provider key), which disables
interpolation entirely, and refuses a value containing a single quote or a
newline, which the format cannot carry. The fixed keys are quote-free by
construction.

Only the Gateway receives the whole `.env` (`env_file`). The frontend and nginx
get explicit `environment:` entries and never see a provider key.

### One optional key

| Key | Consumed by |
| --- | --- |
| `HARTMESH_APP_SUBNET` | The `app` bridge's IPAM subnet **and** the Gateway's `AUTH_TRUSTED_PROXIES`, which are the same reference. Absent -- which is what every existing tenant `.env` is -- both take the shipped default, `10.201.26.0/24`. Set it only when that range collides with something the guest must still reach (§ "Network model"). |

It is deliberately not in `.env.example`: the fixed keys are what onboarding
writes for every tenant, and this one is an escape hatch for a guest whose
surroundings the shipped default does not suit. Because both uses are the same
`${HARTMESH_APP_SUBNET:-...}` reference, an override cannot move the network
without moving the Gateway's trust with it.

## Mount points

Two directories cross the container boundary:

- `/srv/hartmesh` (`HARTMESH_DATA_DIR`), created by the operator: `postgres/`
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
anything, and no operator-side change is needed.

### Gateway user

The Gateway image ships no `USER` directive. The container starts as root
only long enough for `gateway/entrypoint.sh` to read the group id of
`/var/run/docker.sock` (it differs between hosts and is not a contract key),
then `setpriv` drops to uid/gid 1000 plus that one supplementary group with
`--no-new-privs` before any application code runs. uid 1000 is the sandbox
container's user and the data directory's owner, so a directory the Gateway
creates is writable by the sandbox and a file the sandbox writes is readable
by the Gateway, with no chown in either direction.

The service starts with `cap_drop: [ALL]` and `cap_add: [SETUID, SETGID]`,
the two capabilities the drop itself needs, so the root window can do nothing
else; the entrypoint refuses a socket owned by gid 0 rather than grant the
Gateway group root. Docker runs the healthcheck as the container's user, root
here, so the probe performs the same `setpriv` drop before its Python runs.
Proved on the published image: with those two capabilities the drop succeeds,
the process reports `CapEff 0` and `NoNewPrivs 1`, and the docker CLI reaches
the daemon from uid 1000.

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

- `app` (`${HARTMESH_APP_SUBNET:-10.201.26.0/24}`, pinned): the five services.
  PostgreSQL and Redis are reachable only here and are never published. The
  pinned subnet is also `AUTH_TRUSTED_PROXIES` on the Gateway, whose login path
  honours `X-Real-IP` only from a TCP peer in that list and ignores
  `X-Forwarded-For` entirely; without it every login attempt would carry
  nginx's container address, and the spray guard would then see the whole world
  as one source. It no longer decides whether the tenant can log in: the
  lockout is keyed on the account (§ "Login lockout"), so one office behind one
  address cannot lock itself out. Both places are the same interpolation, so
  the network and the Gateway's trust of it move together or not at all --
  including under an override, which is what makes the override safe to offer.
- `sandbox` (`hartmesh_sandbox`): joined by **no** compose service. Compose
  only creates networks a service uses, so `gateway/run.sh` creates it
  idempotently before the first sandbox can exist, unlabelled so `compose
  down` never has to remove a network live sandboxes are attached to.

### Choosing the `app` subnet

A bridge is a **connected route inside the guest**. Every address in `app`'s
range stops being reachable through the VM's real default gateway, because the
kernel prefers the on-link route. The range therefore has to clear everything
the guest must still reach, and that is a property of the surroundings, not of
this profile.

The profile shipped `172.30.10.0/24` until 2026-09-09. On the operator whose
tenants run this profile, kosmos allocates pod addresses from `172.30.0.0/16`
and service addresses from `172.31.0.0/16`, and on that date a production
control-plane node's assigned pod subnet was exactly `172.30.10.0/24`. Public
ingress to the tenant kept working throughout, which proves nothing: the
Kubernetes worker source-NATs the request onto its own `10.17.72.0/24` leg, so
the reply never needed the tenant's view of `172.30.10.0/24` to be correct.
The overlap was real regardless, and would have surfaced the first time
anything in the guest addressed a pod directly.

The default is now `10.201.26.0/24`, chosen to clear:

| Range | What it is |
| --- | --- |
| `172.30.0.0/16`, `172.31.0.0/16` | kosmos pods and services |
| `10.17.0.0/16`, `10.18.10.0/24`, `10.199.199.248/29` | the operator's own networks |
| `172.16.220.0/22`, `172.16.224.0/24` | the operator's own networks |
| `192.168.1.0/24`, `192.168.200.0/24` | the operator's own networks |
| `100.64.0.0/10` | carrier-grade NAT, also the overlay range |
| `172.17.0.0/16` | the `docker0` bridge inside the guest |
| `172.17.0.0/12` in `/16`s, `192.168.0.0/16` in `/20`s | Docker's built-in default address pools |

The last row matters twice over. Every network the profile does **not** pin is
allocated from those pools: the per-sandbox internal and egress bridges under
`allowlist`, and `hartmesh_sandbox` under `open`. Keeping `app` outside them
means it can never collide with a sandbox network, and never costs the daemon a
whole `/16` of pool it would otherwise skip as overlapping.

But it also means the pools themselves still reach `172.30.0.0/16` and
`172.31.0.0/16`, on a guest that has allocated enough networks to get there.
The profile cannot pin those subnets -- the backend and the daemon allocate
them -- so this is the golden image's job, in `/etc/docker/daemon.json`:

```json
{ "default-address-pool": [ { "base": "10.202.0.0/16", "size": 24 } ] }
```

with a `base` chosen the same way as `app`'s. `backend/tests/test_compose_profile.py`
pins the shipped default against every range in the table above; nothing in the
profile can check the daemon's pools, so that setting is verified by looking.

**No private range is universally free.** Before onboarding a tenant, check the
guest's own view rather than trusting this default:

```bash
# 1. Everything the guest already routes. An `app` range that appears here,
#    or inside one of these prefixes, is the defect this section describes.
ip -4 route show
ip -4 route get 10.201.26.1        # must say "via <default gateway>", not "dev br-*"

# 2. Everything Docker has already allocated on this guest.
docker network inspect -f '{{.Name}} {{range .IPAM.Config}}{{.Subnet}} {{end}}' $(docker network ls -q)

# 3. Whether the guest narrows the pool everything unpinned comes from.
#    `null` means it does not, and the built-in defaults in the table apply.
docker info --format '{{json .DefaultAddressPools}}'
```

If `app`'s range collides, put a free one in that tenant's `.env` as
`HARTMESH_APP_SUBNET=` and follow § "Moving the `app` subnet on a running
tenant". A `/24` is the shape the profile assumes; five services and the bridge
address need six.

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
  network, reachable only through a relay that holds a token;
- the sandbox is told to use the proxy by container name, and that name is
  also pinned in its `/etc/hosts` with the address Docker assigned on the
  internal bridge. Docker's embedded DNS at `127.0.0.11` is a NAT rule in the
  host network namespace; a sandbox under gVisor runs its own network stack
  and never sees it, so without the hosts entry every proxied request fails
  with "could not resolve proxy" (observed live, fixed in the backend).

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

Considered for the standing list and left out, each reachable through the
approval card when a session needs it: `raw.githubusercontent.com` and
`objects.githubusercontent.com` (raw files and release assets; `github.com`
already covers `git clone` and `pip install git+https`); `api.github.com`
(token-bearing calls belong in a per-session grant); `huggingface.co` (model
weights are gigabytes into a 1 GiB sandbox, a per-session decision);
`deb.debian.org` and `security.debian.org` (the sandbox runs as uid 1000 and
cannot `apt install`); `crates.io`, `rubygems.org` and `proxy.golang.org`
(no toolchain in the image); `registry.yarnpkg.com` (a mirror of the npm
registry already listed). Widening the standing list is the operator's
decision, not a session's.

Restricted modes hard-require **Docker Engine 28 or newer**, checked when the
backend is constructed with a `RuntimeError`: a too-old daemon is a Gateway
that will not start, not a sandbox that degrades. The operator asserts no Docker
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

- a sandbox **can reach a peer sandbox's published port** on the host-gateway
  address, and the sandbox API behind it has no authentication. The bridge
  path is closed: `run.sh` creates `hartmesh_sandbox` with inter-container
  communication disabled, so peers cannot reach each other on their bridge
  addresses; the published-port path goes through the daemon's proxy on the
  host, which no network option closes. Both sandboxes belong to the same
  customer inside one VM, which is why it is that tenant's accepted residual
  and not the profile's, and why it would not be acceptable on a shared
  cluster. The operator writes `SANDBOX_EGRESS` for every tenant from a
  per-tenant field of its own, validated against the same two values before
  any VM exists, so the profile's refusal of any other value is that
  operator's second guard and every other consumer's only one; no tenant is
  in this mode today because there are no tenants yet;
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
| `DEER_FLOW_SANDBOX_MEMORY` | `1024m` | The only value that held in every provider-driven run under gVisor (see "Memory budget"): the design's 640 MiB and the standalone-measured 768 MiB were both OOM-killed through the real `create` path. The 192 MiB two such sandboxes cost over 768 MiB is paid for by the Gateway's limit. `--memory-swap` is pinned equal (the guest has no swap, so this is explicitness, not a measurable change). |
| `DEER_FLOW_SANDBOX_CPUS` | `1` | Two sandboxes plus two proxies at `--cpus 1` sum to the guest's four vCPUs. |
| `DEER_FLOW_SANDBOX_PIDS_LIMIT` | `384` | The sandbox image idles at 198 processes (Chromium, Jupyter, node, supervisord) and peaked at 232 during a measured bash-plus-browser turn; 384 leaves 1.6x headroom over that peak while still bounding a fork bomb. |
| `DEER_FLOW_SANDBOX_PROXY_MEMORY` | `96m` | The sidecar's cgroup peaked at 48 MiB (process high-water mark 33 MiB) during the same turn; 96 MiB is twice that peak, with `--memory-swap` equal. |

## Memory budget

The guest has 6 GiB and no swap. In-VM limits sum to at most 5.0 GiB so it
keeps at least 512 MiB `MemAvailable` under a burst of Chromium sandboxes plus
a 1 GiB file operation.

| Service | `mem_limit` = `memswap_limit` |
| --- | --- |
| gateway | 1344 MiB |
| frontend | 384 MiB |
| nginx | 128 MiB |
| postgres | 768 MiB |
| redis | 256 MiB (`maxmemory 128mb`, `volatile-lru`) |
| **services** | **2880 MiB** |

Equal `memswap_limit` is an assertion of intent: with no swap device it
changes nothing measurable. The services were 3072 MiB (exactly 3.0 GiB) with
the Gateway at 1536 MiB until 2026-09-06; the Gateway gave up 192 MiB so the
sandbox could go from 768 MiB to 1 GiB without moving the 5.0 GiB line (the
measurement that chose the Gateway is under "Settling the sandbox figure").

Redis's `maxmemory` is half its cgroup on purpose: a background AOF rewrite
forks, and the parent plus the copy-on-write child must fit under the limit,
or the cgroup OOM-kills Redis (it restarts and replays the AOF, but every SSE
stream in flight drops). `volatile-lru` evicts only keys that carry a TTL; the
stream bridge's keys all do, so a key written without one is never evicted
to make room.

### Process and CPU limits

| Service | `pids_limit` | `cpus` |
| --- | --- | --- |
| gateway | 2048 | 2 |
| frontend | 512 | 2 |
| nginx | 256 | scheduler |
| postgres | 512 | scheduler |
| redis | 128 | scheduler |

Memory is what the budget bounds; these bound availability. Without them a
runaway MCP server the Gateway spawns in its own container (`npx`, `uvx`) or
a Next.js fault could take every pid and every core of the four-vCPU guest
from the sandboxes, which are the only other bounded processes. The Gateway
and frontend caps of two vCPUs keep the two sandboxes' `--cpus 1` shares
under contention; nginx and the datastores are small enough to leave to the
scheduler.

Under `allowlist` each concurrent sandbox costs its 1 GiB plus its proxy's
limit (96 MiB, from the measurement above; the backend's own default is
256 MiB). The concurrent-sandbox ceiling is therefore **2**
(`sandbox.replicas: 2` in both modes, one budget, one gate, one behaviour):
2880 + 2 × (1024 + 96) = 5120 MiB, **exactly** the 5.0 GiB line with nothing
to spare, against 6240 MiB for three. `open` mode carries no proxy and still
does not fit three (2880 + 3 × 1024 = 5952 MiB > 5120), so the ceiling is 2
in both modes. A third concurrent sandbox is the 8 GiB VM class: an operator
change, not a profile change. Because the total sits on the line, the next
increase to any limit in `compose.yaml` has to be paid for by a decrease
somewhere else in it; `backend/tests/test_compose_profile.py` asserts the
equality, not just the bound.

The design's 640 MiB was already tight for the fork's sandbox image under
`runc`, where the idle container sat at about 600 MiB of its 640 MiB cgroup
limit with reclaim active (`memory.events` `max` counting up) and no OOM kill
through the measured turn. Neither figure is the backend's default (`2g`), so
every per-sandbox limit is set explicitly; inheriting the backend defaults
would give a tenant 2 GiB sandboxes and an OOM-killed guest.

**Under gVisor the design's 640 MiB does not hold.** The Sentry keeps the
guest's file cache in its own memory, which the host cannot reclaim, so the
cgroup fills to whatever limit it is given and the excess is an OOM kill of the
whole sandbox rather than a slow page cache. Measured with the profile's exact
flags on `runsc` (systrap): at 640 MiB the sandbox reached its API and a
working browser but was OOM-killed (`exit 137`, `OOMKilled=true`) during its
first load round (a shell session plus one package download); at 768 MiB and
at 1 GiB the same load ran twice with no OOM kill. The operator therefore
chose 768 MiB for `compose.yaml`: the smallest measured value that holds, and
the largest the budget allows, since 1 GiB with two sandboxes and two proxies
would be 3072 + 2 × (1024 + 96) = 5312 MiB, over the 5.0 GiB line. gVisor
start-up is also slower: the API answered after 53 to 59
seconds and the image's browser supervisor, which restarts Chromium when its
CDP port is not up within 30 seconds, needed one or two restarts before
reporting `Chromium ready`; expect 75 to 80 seconds to a working browser.

**Re-measured through the provider path on 2026-09-06, the 768 MiB figure does
not hold.** The standalone runs above created the sandbox with a bare `docker
run`; the re-measurement used the real `create` (per-sandbox networks, proxy
sidecar, relay, hosts entry) with the `2.1.0+hartmesh.6` images and the
profile's exact flags, the same two load rounds (a package download through
the proxy, allowed and denied fetches, a browser screenshot), sampling the
cgroup every three seconds:

| `--memory` | idle after readiness | outcome |
| --- | --- | --- |
| 768 MiB | 762 MiB (at the ceiling) | OOM-killed within 30 s of the first package download, **2 of 2 runs** |
| 896 MiB | 829 to 839 MiB | survived both rounds once; OOM-killed in the second round once (**1 of 2**) |
| 1 GiB | 859 MiB | survived both rounds (1 of 1 here, plus the earlier run; then 20 of 20 created by the Gateway itself under "Settling the sandbox figure") |

The Sentry fills whatever limit it is given (`memory.peak` equals the limit at
every size) and reclaims under pressure (`memory.events max` counted 2433
reclaims in the 896 MiB run that survived, 293 at 1 GiB); below about 1 GiB
that reclaim loses to a package download often enough to kill the sandbox.
The only value that held in every provider-driven run is 1 GiB, which the
budget does not fit at two sandboxes: 3072 + 2 × (1024 + 96) = 5312 MiB,
192 MiB over the 5.0 GiB line. The choices were the operator's: trim 192 MiB
from the five services, run one sandbox at 1 GiB, or move the class to
8 GiB. The operator chose the trim, keeping the ceiling at 2 and the class at
6 GiB; which service pays is settled by the measurement that follows.

### Settling the sandbox figure (2026-09-06, P-s)

`compose.yaml` now says 1 GiB for the sandbox and **1344 MiB for the Gateway**
(1536 less the 192 MiB). Which limit gave up the memory was decided by
measuring the trimmed services the way the sandbox should have been measured
the first time: the whole profile running under Compose on a host with
`runsc` (release-20260817.0, systrap; Docker Engine 28.4.0, Compose v2.39.4),
the `2.1.0+hartmesh.6` images, a tenant `.env` and data disk laid out as the
golden image does, and agent turns driven through nginx exactly as a browser
drives them. The model was a stub OpenAI-compatible server reached through
`OPENAI_BASE_URL` in the tenant `.env` (a provider variable the Gateway
already passes through; nothing in the profile changed for it), scripted so
that every turn makes two `bash` calls in the sandbox (a `pip download`
through the proxy, then a CPU-bound Python one-liner) and a `present_files`
call before its answer, so each run passes the fork's delivery verification.
Each load run was 3 or 4 rounds of 2 concurrent turns on fresh threads (with
`replicas: 2` every round after the first evicts both sandboxes and creates
two more), two 16 MiB uploads between rounds, and 12 or 24 web workers
fetching the frontend's pages and the Gateway's thread search and message
endpoints without pause. Every service cgroup was sampled every three
seconds; the figures below are `memory.peak`, and "reclaims" is the `max`
counter of `memory.events`.

| trim tried | service | idle | peak | reclaims / OOM kills | outcome |
| --- | --- | --- | --- | --- | --- |
| gateway 1408, frontend 320 (the aside above) | gateway | 509 MiB | 532 MiB | 0 / 0 | held, 2.6× headroom |
| | frontend | 202 MiB | 272 MiB | 0 / 0 | held, but 1.18× headroom: **put back** |
| gateway 1344, frontend 384, run 1 (fresh containers, 3 rounds, 12 workers) | gateway | 256 MiB at start | 416 MiB | 0 / 0 | held |
| | frontend | 85 MiB at start | 156 MiB | 0 / 0 | held |
| gateway 1344, frontend 384, run 2 (same containers warm, 4 rounds, 24 workers) | gateway | | 586 MiB | 0 / 0 | held, 2.3× headroom |
| | frontend | | 178 MiB | 0 / 0 | held, 2.2× headroom |
| | nginx | 11 MiB | 19 MiB | 0 / 0 | untouched |
| | postgres | 195 MiB | 257 MiB | 0 / 0 | untouched |
| | redis | 34 MiB | 39 MiB | 0 / 0 | untouched |

The frontend trim was rejected by its own measurement, not by the arithmetic:
under 12 concurrent page loads its cgroup reached 272 MiB, and Node 22 in the
image sizes its default V8 heap limit at 259 MiB whether the cgroup is 320 or
384 MiB, so a 320 MiB limit would leave about 60 MiB for everything the
process holds outside that heap. At 384 MiB the same load peaked at 178 MiB.
The Gateway carries the whole 192 MiB because it is the only service that
still has more than twice its measured peak after the trim: 586 MiB at the
end of the second, heavier run (14 of 14 turns succeeded, 14 uploads, 11.5
thousand page loads, 23 thousand API reads; one upload during the final
eviction answered `504` from nginx and the upload after it succeeded, a
latency observation, not a memory one). Its earlier 1536 MiB was headroom for MCP
servers the Gateway spawns in its own container (`npx`, `uvx`); none is
configured by default, and 758 MiB of spare remains for those a tenant adds.
The datastores were not touched: the brief's own reasoning, that a Redis or
PostgreSQL OOM loses work in flight and drags derived figures with it, holds,
and neither was near its limit. The proxy sidecar keeps 96 MiB: one of the 20
sidecars in these runs peaked at 57 MiB, above the 48 MiB the earlier turn
measured, so 96 is now 1.7× rather than 2×.

The sandbox figure is confirmed from the Gateway's side as well: the same runs
created 20 sandboxes at 1 GiB through the real acquisition path (`docker
inspect`: `Runtime=runsc`, `Memory=MemorySwap=1 GiB`, `NanoCpus=1`,
`PidsLimit=384`, `User=1000:1000`, `CapDrop=[ALL]`), each ran the package
download and the Python step, and **20 of 20 survived**: `memory.peak` between
907 MiB and 1024 MiB, reclaims from 0 to 9134, no OOM kill in any of them
(`memory.events` `oom_kill 0`). The allowlist proxy also refused one
`CONNECT redirector.gvt1.com:443` per sandbox, Chromium's component updater
inside the image reaching for Google, recorded on each run as an
`egress.blocked` diagnostic; that is the standing list doing its job.

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
two-key contract change the operator must make, after which it is a one-line
change here. Everything the durable profile would otherwise check is already
in place: PostgreSQL for every store, `run_events.backend: db`,
`dedupe_storage: auto`, and an explicit `DEER_FLOW_TENANT_ID`.

The Gateway runs exactly one worker. `DEER_FLOW_INTERNAL_AUTH_TOKEN` is
generated per process when unset, so a single worker is what keeps it coherent
without a second configuration key. The login lockout no longer depends on
that: its counters are in Redis (§ "Login lockout").

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
failure for the profile; refusing such a tenant belongs in the operator's
onboarding verb.

### Login lockout

The rendered `config.yaml` sets `auth.local.lockout_store: redis`; every other
login-throttle value is the Gateway default. Two facts about this profile make
that the right setting and the defaults the right numbers.

**One tenant is one company.** Five to twenty staff, all behind one office NAT
address. A lockout keyed on the client address therefore locks the company, not
the guesser: five failures by anyone lock everyone, remote staff included, and
five failures in a five-minute window is a median Monday morning with a tool
nobody's password manager has learned yet. The lock is keyed on the **account**
instead — five failures against one email address lock that address for five
minutes, from any address — so the tenant's shared egress address is irrelevant
to it. A much looser per-source guard (50 distinct accounts or 300 failures in
15 minutes) still catches spraying; a twenty-person office cannot reach either
number, because it does not have fifty accounts.

**Clearing a lock must not be an outage.** The stack is one replica with a
recreate-style rollout, so restarting the Gateway to drop an in-process counter
interrupts everyone still working. Redis is already running here for the stream
bridge, so the counters live there and an administrator clears one account with
`POST /api/v1/auth/lockouts/clear` (admin role, interactive session, not a PAT);
`GET /api/v1/auth/lockouts` lists what is currently locked. A Redis outage fails
the login path closed with a 503 — on this profile Redis is already load-bearing
for every SSE stream, so it is not a state the tenant is working in anyway.

A locked account returns exactly what a wrong password returns, including the
cost of one password verification, so the lockout cannot be used to discover
which addresses have accounts. Only the source guard answers 429, and it says
nothing about any account.

### Differences from the chart's rendered `config.yaml`

The same render with three keys present (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`,
`TAVILY_API_KEY`), compared key by key with `helm template` of the chart's
config ConfigMap under the chart README's recommended values, at
`2.1.0+hartmesh.6`. Identical: `checkpointer`, `config_version`, `database`,
`dedupe_storage`, `deployment` (profile, readiness and shutdown budgets),
`log_level`, `memory`, `stream_bridge`, `tool_groups`, `tool_plane`,
`verification`. Every difference and its reason:

| Key | Chart | Profile | Why |
| --- | --- | --- | --- |
| `models` | `[]` | the catalog entries for the keys present | the chart leaves models to the operator's values; the profile renders them from the tenant's keys |
| `tools[web_search]` | DuckDuckGo | DuckDuckGo without a search key, the keyed provider when one is present (Tavily here) | same ten tools; only the search backend follows the tenant |
| `sandbox.image` | upstream `latest` | the fork's digest pin | release pinning |
| `sandbox.replicas` | 3 | 2 | the memory budget |
| `sandbox.idle_timeout` | absent | 1800 | recorded above |
| `sandbox.network` | absent | `allowlist` block | the chart's sandboxes are fenced by CiliumNetworkPolicy; the VM has no such fence, so the backend's own mode is the fence |
| `sandbox.provisioner_url`, `provisioner_service_account_token_file`, `accepted_skill_projection_profile` | set | absent | the Kubernetes provisioner path; the local Docker backend has no provisioner and mounts skills directly |
| `skills` | absent (PVC mounts) | `path` under `home/`, `container_path: /mnt/skills` | the local backend's skills mount |
| `run_events.backend` | upstream default (`memory`) | `db` | run events survive a Gateway restart on a single-Gateway VM |
| `auth.local.lockout_store` | absent (`memory`) | `redis` | § "Login lockout": one replica with a recreate rollout, so clearing a lockout must not need a restart |

## Moving the `app` subnet on a running tenant

A tenant already running carries a `hartmesh_app` network the daemon created
from the bundle it started with. **Editing the profile does not move it.**
The bridge, its subnet and its route belong to the daemon, not to the file; the
file is only what the next `docker network create` reads.

Nor is an in-place `up -d` enough. On Compose v2.39.4 / Engine 28.4.0 it
recreated the network with the new subnet **and left the stack broken**:
Compose reattached the containers it merely restarted without their compose
service aliases, so `postgres` and `redis` stopped resolving inside the guest's
network (`gateway`, which Compose did recreate, still did), the Gateway
crash-looped on `socket.gaierror` from `asyncpg`, nginx exited, and the command
returned `dependency failed to start`. The stack does not recover on its own.

The move is therefore a **short full-stack outage**, on the order of a minute:
every service sits on the one network, so there is no rolling variant.

```bash
cd /opt/hartmesh
ENV=/srv/hartmesh/.env

# 0. Record what you are changing away from.
docker network inspect hartmesh_app --format '{{(index .IPAM.Config 0).Subnet}}'
ip -4 route show | grep br-

# 1. Stop the stack and let the daemon drop the old bridge. `down` removes the
#    containers and the networks Compose created -- nothing else. The profile
#    declares no named volumes, so there is no volume for it to touch even in
#    principle; PostgreSQL, Redis and home/ are bind mounts on the data disk
#    and are not part of what `down` removes. Never `down -v`, and never
#    `docker network prune`: the sandbox networks are not Compose's, and a
#    prune would take live ones with it.
docker compose --project-directory /opt/hartmesh --env-file "$ENV" down

# 2. Bring it back up on the new bundle.
docker compose --project-directory /opt/hartmesh --env-file "$ENV" up -d --wait

# 3. Verify. The first two must agree -- that is the whole point of the single
#    interpolation -- and the third must show the route on the new bridge.
docker network inspect hartmesh_app --format '{{(index .IPAM.Config 0).Subnet}}'
docker inspect hartmesh-gateway-1 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep AUTH_TRUSTED_PROXIES
ip -4 route show | grep 10.201.26
docker compose --project-directory /opt/hartmesh --env-file "$ENV" ps
```

Then two functional checks. A real login through nginx must return `200`. And
the trust must actually have taken, which the account lockout says out loud:
fail a throwaway account past `auth.local.account_max_attempts` through nginx
and read the line the Gateway logs.

```bash
docker compose --project-directory /opt/hartmesh --env-file "$ENV" logs gateway | grep 'Login lockout'
# ... Login lockout: account <email> locked after 5 failed attempts for 300s (last source <client>)
```

`last source` must name the client address the front door forwarded. If it
names nginx's own address on the app network, the Gateway is counting the proxy
rather than the client -- that is what a subnet and an `AUTH_TRUSTED_PROXIES`
that disagree look like from outside, and it collapses the per-source spray
guard onto one address for the whole world. Clear the lockout afterwards with
`POST /api/v1/auth/lockouts/clear` (§ "Login lockout").

**Rollback** is the same two commands with the old value restored, and needs no
edit to the bundle: append `HARTMESH_APP_SUBNET=<the old subnet>` to the
tenant's `.env` and run `down` then `up -d --wait`. Because the network and the
Gateway's trust are the same interpolation, one line moves both back together.

**Active sandboxes.** No sandbox is ever on `app`, so none can hold
`hartmesh_app` open or be stranded by its removal, and `down` removes only
`hartmesh_app`: `hartmesh_sandbox` is unlabelled and survives, along with
anything attached to it (verified with a stand-in container across a `down`).
What does depend on the sandboxes is the Gateway's own shutdown: it releases
them during its 60 s `stop_grace_period`, which `down` honours. A `down -t 0`,
or a guest that loses power mid-move, leaves sandbox containers and per-sandbox
networks with no owner; remove those by name afterwards
(`deer-flow-sandbox-*`, `deer-flow-netproxy-*`) rather than with a prune. Do
the move when the tenant is idle: any turn in flight dies with the stack.

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
the tag (see `RELEASING.md`, "Compose profile pins"). Between cuts the tree
carries the **previous release's digest pins**: the pin commit is the last
thing a release changes and nothing restores placeholders, so a bundle built
from `main` is grammatical and boots the previous release's images. A cut
re-points every fork line with `--release`; a third-party image is bumped by
putting its new tag form in place of the old digest string in all three files
and running the pin script. Seven images are pinned: gateway, frontend,
sandbox, the network proxy (built under the fork's own name,
`<repo>-sandbox-network-proxy`), `postgres`, `redis` and `nginx`.

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

Then with `runsc` release-20260817.0 registered on the systrap platform
(`SANDBOX_RUNTIME=runsc`, the same Docker Engine 28.4.0):

- Full provider-driven proof under `allowlist`, memory raised to 1 GiB by a
  scratch override for the reason recorded under "Memory budget": `docker
  inspect` shows `Runtime=runsc`, `ExtraHosts=[<proxy name>:<internal
  address>]`, `SecurityOpt=[no-new-privileges, seccomp=builtin]`,
  `CapDrop=[ALL]`, `User=1000:1000`, no published port; inside the sandbox
  `uname -r` is `4.19.0-gvisor`; `GET /v1/browser/info` and
  `/v1/browser/screenshot` answer `200` (the browser starts under Docker's
  built-in seccomp profile with gVisor); the isolation transcript is identical
  to the `runc` one (datastores and peer unreachable, pypi `200` through the
  proxy, example.com refused after CONNECT, `neverssl.com` `403`, the metadata
  address refused as an IP literal, direct DNS unavailable); the workspace
  round-trips; the proxy sidecar runs under `runc` at its 96 MiB limit
  (`memory.peak` 45 MiB); nothing remained after release.
- The first gVisor attempt, before the hosts entry existed, failed every
  proxied request with `Could not resolve proxy`, which is what the backend
  change fixes; the standalone reproduction is a plain `docker run --runtime
  runsc` on a user-defined network, where `getent hosts <peer>` fails and
  `--add-host` succeeds.
- At the design's 640 MiB, the profile's value at the time, the gVisor sandbox
  was OOM-killed under load (see "Memory budget"); the figures there are from
  three standalone runs at 640 MiB, 768 MiB and 1 GiB with the profile's other
  flags unchanged.

Then on 2026-09-06, through the provider path with the `2.1.0+hartmesh.6`
sandbox and proxy images and the backend that refuses a proxy without an
address (`runsc` release-20260817.0, systrap, Docker Engine 28.4.0):

- `create` → readiness in 53 to 59 s at every size; `docker inspect`:
  `Runtime=runsc`, `Memory=MemorySwap` at the requested size, `NanoCpus=1`,
  `PidsLimit=384`, `User=1000:1000`, `SecurityOpt=[no-new-privileges,
  seccomp=builtin]`, `CapDrop=[ALL]`, no `CapAdd`, `ExtraHosts=[<proxy
  name>:<internal address>]`, no published port; inside, `uname -r` is
  `4.19.0-gvisor` and `id` is `uid=1000(gem)`.
- Each load round: `pip download requests` through the proxy (five files),
  `https://pypi.org/simple/` `200`, `https://example.com/` refused with `403
  from proxy after CONNECT`, `http://169.254.169.254/` refused as an IP
  literal, `GET /v1/browser/info` `200` and `/v1/browser/screenshot` a 41 KiB
  PNG. The proxy: `runc`, 96 MiB, uid 65532, read-only, published on
  `172.17.0.1`. The workspace round-trips. After `destroy`, no container and
  no per-sandbox network remained, at every size, including the OOM-killed
  runs.
- The memory outcome per size is the table under "Memory budget": 768 MiB
  OOM-killed in both runs, 896 MiB in one of two, 1 GiB in neither.

Then on 2026-09-06 for the trim (P-s), the profile itself under Compose on the
same host (`runsc` release-20260817.0, Docker Engine 28.4.0, Compose v2.39.4,
the `2.1.0+hartmesh.6` images), a scratch copy of this directory with only
`skills.path` pointed at the scratch data disk (the template fixes it at
`/srv/hartmesh`, which this host does not have), the data disk laid out as the
golden image does and the tenant `.env` on it:

- `up -d --wait`: all five services healthy; `docker inspect` of the Gateway
  `Memory=MemorySwap=1344 MiB`, of the frontend `384 MiB`, and of every
  sandbox the Gateway created `Runtime=runsc`, `Memory=MemorySwap=1 GiB`,
  `NanoCpus=1`, `PidsLimit=384`, `User=1000:1000`, `CapDrop=[ALL]`,
  `SecurityOpt=[no-new-privileges, seccomp=builtin]`.
- Three load runs through nginx (`/api/v1/auth/initialize`, then per turn
  `POST /api/threads` and `POST /api/threads/<id>/runs/stream` read to
  `event: end`, exactly the frontend's calls): 20 turns in all, 14 of 14 at
  the shipped limits with `status=success`, each having made two `bash` calls
  in its sandbox (`pip download requests` through the proxy: five wheels
  saved) and one `present_files` call; 20 uploads of 16 MiB (one `504` from
  nginx during the last eviction, the next upload succeeded); 19.6 thousand
  frontend page loads and 39 thousand Gateway reads across the runs. The per-service `memory.peak` and `memory.events` figures
  are the table under "Settling the sandbox figure"; no service and no sandbox
  recorded an OOM kill.
- After each Gateway recreate, no sandbox or per-sandbox network remained.

Then on 2026-09-09 for the network move (P-v), on the same host (Docker Engine
28.4.0, Compose v2.39.4) with a disposable stack: a scratch copy of this
directory, a scratch data disk laid out as the golden image does, fixture
`.env` values, no provider key, `SANDBOX_RUNTIME=runc`, and nginx published on
`127.0.0.1:20260`. Two deliberate deviations, both noted where they matter: the
stand-in "old" subnet was `10.203.10.0/24`, not `172.30.10.0/24`, because this
development host is itself inside the operator's `10.17.0.0/16` and pinning a
bridge on a live pod range to prove a point is the defect, not a test of it;
and `HARTMESH_TRUSTED_PROXIES` was widened to `10.0.0.0/8` so the host could
act as the front-door proxy and forge distinct client addresses.

- The mechanism, on this host: `ip -4 route get 203.0.113.5` answers `via
  10.17.100.1 dev eth0`; with a bridge pinned on `203.0.113.0/24` it answers
  `dev br-e4b410c39c00 src 203.0.113.1`; with the bridge removed it answers
  `via 10.17.100.1` again. A pinned bridge replaces the route to its whole
  range, which is what `172.30.10.0/24` was doing to kosmos pod addresses.
- Trust follows the network, observed through the running stack. The images
  pinned here are `2.1.0+hartmesh.6`, which predate the account-keyed lockout,
  so the probe below exercises that release's per-address counter rather than
  the account lockout the § "Moving the `app` subnet" check describes. It
  answers the same question -- which address the Gateway resolves for a request
  arriving through nginx -- and answers it more sharply, because the older
  counter locks per address. Six failed
  logins through nginx forwarding `198.51.100.10` answer `401 401 401 401 401
  429`, and `198.51.100.11` and `.12` are still served `401` -- each forwarded
  client address is counted on its own. The negative control, the same stack
  with `AUTH_TRUSTED_PROXIES` pointed at `192.0.2.0/24` instead of the app
  network: `.20` locks at the sixth attempt exactly as before, and then `.21`
  and `.22` are refused `429` without a single attempt of their own. That is
  the whole guard collapsed onto nginx's own address, and it is what changing
  IPAM alone would have shipped.
- An in-place `up -d` onto the new bundle is not the procedure. It did recreate
  the network on the new subnet, and it left the stack broken: `getent hosts
  postgres` and `redis` failed from inside `hartmesh_app` while `gateway`
  resolved, the Gateway crash-looped on `socket.gaierror` out of `asyncpg`,
  nginx exited `0`, and the command returned `dependency failed to start:
  container hartmesh-gateway-1 is unhealthy`. It did not recover on its own.
- `down` then `up -d --wait` is. After it: `hartmesh_app` is `10.201.26.0/24`,
  nginx holds `10.201.26.6`, the Gateway's `AUTH_TRUSTED_PROXIES` is
  `10.201.26.0/24`, the guest routes `10.201.26.0/24 dev br-60c3849500a6`, all
  five services are healthy and `ps` shows one published port. The seeded
  PostgreSQL row, the `users` row, the Redis key and the `home/` file all read
  back unchanged; a real login through nginx answers `200`; and the probe
  behaves as it did before the move (`.30` locks, `.31` served).
- `down` removed `hartmesh_app` and nothing else: `hartmesh_sandbox` and a
  stand-in container attached to it both survived it, and `down` did not
  complain about either. No live sandbox existed during this run, so the
  Gateway's release of real sandboxes during its 60 s grace period is the
  earlier proof above, not this one.
- Rollback: appending `HARTMESH_APP_SUBNET=10.203.10.0/24` to the tenant `.env`
  and repeating `down` / `up -d --wait` put the network, the route and the
  Gateway's trust all back on the old subnet together, with the data markers
  still intact. The bundle was not edited.
- The pool arithmetic, on the same host: with `172.22.5.0/24` pinned on a
  scratch network, two unpinned `docker network create` calls were allocated
  `172.21.0.0/16` and then `172.23.0.0/16` -- the daemon skips a whole `/16` it
  cannot use, which is the cost of pinning `app` inside the pool and the reason
  the default sits outside it. `docker info --format '{{json
  .DefaultAddressPools}}'` answered `null` here, which is what an unnarrowed
  daemon looks like.
- Not covered here: `runsc`, a real provider-driven sandbox across the move,
  and the daemon-level `default-address-pool` setting, which is the golden
  image's and cannot be checked from inside this profile.


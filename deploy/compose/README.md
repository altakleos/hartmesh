# HartMesh tenant VM compose profile

The `frontend` image is built from `frontend-hm/Dockerfile`, with the app still
installed at `/app/frontend`. Service names and cache mounts remain stable.
The sibling `frontend/` source is an upstream reference. Source changes take
effect on tenants only after the normal candidate publish and digest pinning;
the checked-in image pins continue to identify their existing release.

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

Nothing in the bundle relies on being executable: every script `compose.yaml`
runs is invoked as `sh /opt/hartmesh/...`, and the operator tools under
`scripts/` are invoked as `bash scripts/...` (their headers say so).

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

### Optional keys

| Key | Consumed by |
| --- | --- |
| `HARTMESH_APP_SUBNET` | The `app` bridge's IPAM subnet **and** the Gateway's `AUTH_TRUSTED_PROXIES`, which are the same reference. Absent -- which is what every existing tenant `.env` is -- both take the shipped default, `10.201.26.0/24`. Set it only when that range collides with something the guest must still reach (§ "Network model"). |
| `HARTMESH_MODELS_FILE` | The path of the operator's own model file, read by `gateway/render_config.py` at every Gateway start. Absent -- which is what every existing tenant `.env` is -- the rendered `models:` section comes from the bundled provider catalog exactly as before. Set, that one file is the whole model list (§ "Operator-managed models"). |
| `SANDBOX_READY_TIMEOUT` | The cold-start readiness budget, `sandbox.ready_timeout` in the rendered `config.yaml`: whole seconds from 60 to 600. Absent -- which is what every existing tenant `.env` is -- the template's 120 applies. Anything else (zero, a negative or fractional number, text, a value outside the range) refuses to render and the Gateway does not start, so no value can turn the deadline off (§ "Sandbox readiness budget"). |
| `HARTMESH_SANDBOX_RESOLV_CONF` | The Docker host's upstream DNS file, default `/run/systemd/resolve/resolv.conf` on the Debian tenant VM. The Gateway receives a read-only view; open-mode runsc sandboxes bind the validated file at `/etc/resolv.conf`. On hosts without systemd-resolved, select an existing resolver file containing reachable upstream IP addresses. A loopback stub file is refused (§ "DNS under gVisor"). |

These are absent from `.env.example`: the fixed keys are what onboarding
writes for every tenant. Optional keys adapt the profile when its defaults
do not suit the host or tenant. The resolver source must exist even when
allowlist mode is selected; Compose refuses a missing file instead of
creating an empty directory in its place.

They reach the stack by different routes, on purpose. Both `HARTMESH_APP_SUBNET`
uses are the same `${HARTMESH_APP_SUBNET:-...}` reference, so an override
cannot move the network without moving the Gateway's trust with it.
`HARTMESH_MODELS_FILE` and `SANDBOX_READY_TIMEOUT` are not interpolated by
`compose.yaml` at all: they reach the Gateway through `env_file` and are read
inside the container by `gateway/render_config.py`, so leaving either unset is
simply an unset variable rather than a hole in the rendered Compose document.
The resolver source and its Gateway environment value share the same
interpolation, so validation reads the file Docker will bind into the sandbox.

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
  changing it is a profile change, not a tenant setting. Its `public/` is
  mirrored from the Gateway image at every start and its `custom/` is the
  operator's (§ "Public skills"). The library must not run ahead of the
  pinned sandbox image: `data-analysis` imports `duckdb` and
  `business-report` imports `python-docx` from the image (both shipped from
  the first release with the skill-library layer, see RELEASING.md) and each
  exits with a message naming the image rather than installing anything.
  The profile mounts the tenant bundle (§ "Tenant bundle") read-only at
  `/mnt/tenant` in every sandbox, which is where `business-report` reads the
  company name, logo, colours and report profiles; without one, reports carry
  no company branding. The Gateway keeps
  uploads and artifacts under `home/users/<user>/threads/<thread>/user-data/`
  and each person's kept files under `home/users/<user>/files/` (mounted into
  every sandbox of theirs at `/mnt/user-data/files`; no quota, nothing reaps
  it, and it outlives the conversations it came from), so the pre-created
  `uploads/` and `artifacts/` directories are unused by this profile and stay
  empty. `home/runtime/` is the other persistent consumer of this disk: the
  accepted skill snapshots and thread views a warm turn reuses instead of
  staging again, sized and bounded in § "Public skills", **Per-turn
  material**.
- `/srv/hartmesh/operator`, mounted **read-only** into the Gateway at the same
  path: operator-owned deployment material that is not release content: the
  optional model file `HARTMESH_MODELS_FILE` names (§ "Operator-managed
  models") and the tenant bundle at `operator/tenant/` (§ "Tenant bundle").
  It sits beside `home/` rather than inside it
  on purpose -- `home/` is `DEER_FLOW_HOME`, whose `threads/` and `skills/`
  subtrees are what sandboxes bind-mount -- so nothing a chat user or an agent
  can write chooses a model client class or a provider endpoint, and the
  Gateway itself has no write path to it either. An existing tenant's data disk
  has no such directory; create it with
  `install -d -o 1000 -g 1000 -m 0750 /srv/hartmesh/operator`, or let Docker
  create it (root-owned, `0755`) on the next `up`.
- `/opt/hartmesh`, this directory, mounted read-only into the Gateway
  (`gateway/`, `config.yaml`, `providers/`, `extensions_config.json`), into
  nginx (`nginx/`) and into the search service (`searxng/`, which its
  `run.sh` reads as uid 1000, so the files must stay world-readable). Two files the Gateway needs writable are seeded into
  `home/` at start: the rendered `config.yaml` (every start) and
  `extensions_config.json` (once, then the Gateway edits it at runtime).

Named volumes are deliberately absent: they would live on the root disk,
which is a linked clone of the golden image and is not backed up as tenant
data. Anonymous ones count: a service whose image declares a `VOLUME` gets
one at every `up` unless the profile mounts that path itself, which is why
the search service has a tmpfs on `/var/cache/searxng`.

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

### Tenant bundle

`/srv/hartmesh/operator/tenant/` is where the operator writes what the
workspace says about the company it serves. Two readers, one directory, no
copy: every sandbox mounts it read-only at `/mnt/tenant` (a `sandbox.mounts`
entry in `config.yaml`), which is where the `business-report` skill picks up
the company name, logo and colours by itself, and the Gateway reads the same
path (`tenant_bundle.path`) for the workspace header, the About page and
Home's starter grid. A person sees the company after signing in; the login
page stays the product's own.

The layout is the skill's contract, and every file is optional:

```
/srv/hartmesh/operator/tenant/
├── brand.json          {"company_name": "Example Services Co.",
│                        "logo": "logo.png",
│                        "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}
├── logo.png            PNG or JPEG, next to brand.json (an SVG is refused:
│                       it can carry a stylesheet or an image reference that
│                       reaches out)
├── starters.json       [{"id": "business-review", "title": "...", "prompt": "..."}]
│                       -- the same shape and rules as `ui.starters`: at most
│                       six, distinct ids, plain text; present, it *is* the grid
└── report-profiles/    <name>.json, loaded by the skill by name ahead of its
                        own `profiles/` (a `services-generic.json` here replaces
                        the built-in one)
```

Create it before the first `up` that carries this profile, owned by the
Gateway and sandbox user:
`install -d -o 1000 -g 1000 -m 0750 /srv/hartmesh/operator/tenant`.
Left absent, Compose creates it root-owned (`0755`) at `up`, before the
Gateway starts, and the profile still works; the operator then needs root
to write into it. That Compose entry is what makes the directory safe to
bind: every sandbox mounts it with `--mount type=bind`, which refuses a
missing source rather than creating one. Every file in it must be
readable by uid 1000 (`install -o 1000 -g 1000 -m 0640 brand.json …`, or `chmod 0640`
after a `sudo cp`). A directory uid 1000 cannot traverse, or a picture it
cannot open, is one named problem: the workspace shows the product's name,
and the Gateway starts regardless. An SVG logo is refused (it can carry a
stylesheet or an image reference that reaches out); export it as PNG. Edits
are live: the Gateway reads the files on each request and the sandbox mount
is a bind, so a new `brand.json` reaches the next page load and the next
report, no restart.

A missing file is not a problem. A malformed one -- a logo that is not a PNG
or JPEG inside the directory, a colour that is not `#rrggbb`, a company name
that is blank, longer than 80 characters or spans lines, a starter list that
breaks the `ui.starters` rules -- degrades that field alone (the name still
shows without its picture; Home keeps the grid `config.yaml` would have
shown) and is named in the Gateway log as `tenant bundle: <file>: <rule>`,
never with the value. `gateway/render_config.py --check` (§ "Operator-managed
models" gives the invocation: an `exec` into the running Gateway, which reads
the bundle through its own read-only mount, so what it sees is what the
workspace sees) prints the same problems and one summary line -- whether a
name and a logo are set, how many starters and report profiles there are --
and still renders. Report profiles are listed, not validated: the skill
validates a profile when it loads one and says what is missing.

The profile runs the business profile (`ui.profile: business` in
`config.yaml`): the screens for building the deployment -- skills, tools,
subagents, integrations, and the scheduled-task recipes -- are offered to
administrators only. It is presentation, not authorization; `system_role` is
what limits a person.

### Public skills

The Gateway image carries the repository's public skill library at
`/app/skills/public` (`backend/Dockerfile`, its last layer). At every start
`gateway/run.sh` runs `gateway/seed_skills.sh`, which replaces
`home/skills/public` on the data disk with that library: the copy is staged
beside `public/` and swapped in by two renames, so a copy that fails leaves
the previous set in place and stops the start before uvicorn runs. `public/`
is therefore release material. A skill an earlier image shipped, a file
dropped in by hand or an edit made in place is gone after the next start;
until that start an edited public skill is live, and the next turn admitted
after the edit snapshots it, so the seed is a restore, not a tamper guard. A
skill the operator adds goes in `home/skills/custom/`, which the seed never
touches. A chat's sandbox never mounts `public/` itself: every turn is
admitted with an immutable snapshot of the skills enabled for that user,
projected read-only at `/mnt/skills/.accepted/<snapshot digest>/public/<name>`
and re-verified by digest before that turn uses it (**Per-turn material**
below has what the material costs and how long it is kept), and the sandbox
tools refuse skill paths outside it (`backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`, "Which
population a deployment profile runs"). The Gateway's own projection,
`home/skills_view/`, is rebuilt from `public/` at startup; the measurement
script below mounts `home/skills` directly, so its paths carry `public/`.
Because the library travels in the image, a change to a public skill reaches
a tenant only through a release (RELEASING.md). A Gateway image older than this
feature carries no library; the seed says so in the log and leaves `public/`
as it is, so a profile checked out ahead of its pinned image still starts.
A library directory that holds no skill, or that carries a symlink, is the
wrong image and is refused.

The seeded set is the repository's public library minus the exclusions
below, not a set chosen for business use; it includes authoring- and
developer-facing skills. `run.sh` names the exclusions in
`EXCLUDED_PUBLIC_SKILLS`, 11 at this release, by reason:

- `chart-visualization` posts the data it charts to an external service, and
  `podcast-generation` posts the script it narrates to a third-party
  text-to-speech service; a tenant's content does not leave the VM for
  that. The sandbox network is allowlisted, but `approval: prompt` lets a
  user grant a host for a session, so the fence alone is not the guarantee.
- `web-design-guidelines` fetches its own rules from a third-party URL and
  tells the agent that the fetched text carries the instructions to follow.
  `web_fetch` runs from the Gateway, so the sandbox allowlist does not govern
  that fetch; the profile does not ship a skill whose instructions come from
  outside the release.
- `find-skills` and `claude-to-deerflow` describe flows that cannot work
  here: a skill install into a read-only mount that the next start replaces,
  and a DeerFlow at `localhost:2026` that a sandbox cannot reach.
- `github-deep-research`, `image-generation`, `music-generation` and
  `video-generation` assign a credential read from the environment to a
  variable the review's `secret-env-assignment` rule treats as blocking;
  `skill-creator` uses `subprocess`; and `vercel-deploy-claimable` declares a
  sensitive capability. The profile's own skill review refuses each, and
  `tool_plane.validation_requires_skill_review: true` makes that review a
  condition of promotion: a governed base holding any one of them could
  never be promoted, so the profile does not ship them.
  `backend/tests/test_compose_public_skills.py` pins that each is still
  refused and that the policy exclusions are not review refusals; an
  exclusion the review no longer requires fails the suite, and the skill goes
  to tenants at the next release.

The list can only subtract: it names skills the image carries, and there is
no entry that adds one. It lives in `run.sh`, so changing it is a profile
change, not a tenant setting. A skill the operator wants goes in
`home/skills/custom/`, with the same caveat: a package the review refuses
blocks promotion of a governed base from `custom/` exactly as it would from
`public/`. 13 skills are seeded at this release; the seed's line in the
Gateway log says how many and which names were excluded.

**Per-turn material.** A chat never mounts this library directly: each
admission snapshots the effective skills into a content-addressed, read-only
tree under `home/runtime/skill-snapshots/<subject>/<digest>/` and binds it
into the thread's view at
`home/runtime/skill-snapshot-active-views/<subject>/<thread>/`, which is what
the sandbox sees at `/mnt/skills/.accepted/<digest>`. Until 2026-09-17 both
were deleted when the run ended and staged again, with a `fsync` per file, on
the next turn. Measured on a development host, not the tenant class, with the
13 seeded packages (43 files, 0.40 MB): staging the snapshot 2.0 to 2.4 s and
45 `fsync`s against 23 ms to verify a retained one, and staging the view 3.2 s
and 43 `fsync`s against 13 ms to verify a retained one. Both are now retained
and re-verified by digest before any use — the bytes authorize the reuse, the
identity never does — so a warm turn pays the verification, not the staging.
The snapshot is the user's *effective* skills, so a user's custom and
integration skills ride in it and their trees are larger than the seeded
library.

Retention is bounded by what removes it, and nothing here expires on a timer:

- **Per user, two snapshot digests.** The third publication evicts the
  oldest, so toggling one skill keeps both sets warm. A digest a run still
  holds is never evicted, and the bound is re-applied when that user next
  publishes — a scope transiently holds two plus its concurrent runs.
- **Per parked thread, one view.** It goes when a different digest replaces
  it, or with the container: `destroy`, an idle reap at `idle_timeout`
  (1800 s here), or a replica eviction, which on two slots is routine.
- **Per tenant, `2 × snapshot × users admitted since the last Gateway
  start`.** That is the number to size for: nothing reclaims a user's trees
  while the process lives, not even deleting the thread. A Gateway restart is
  the reclaim — startup removes every snapshot and view no live lease holds,
  and the first turn of each user after it pays one staging.

At this release that is about 1.3 MB per user of seeded library (0.61 MB
allocated per tree on a 4 KiB-block filesystem) on the tenant data disk, plus
whatever their own skills add, against the 32 MiB per-snapshot ceiling
(`max_total_bytes`). See `backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`,
"Material is retained across turns".

**Governance.** The profile runs the governed tool plane
(`tool_plane.enabled: true`) under the `local_development` deployment
profile, so governance state never fails readiness: the seeded library is
usable at once, and adoption is optional. Until an administrator adopts it
the tool-plane status reads `unmanaged`: the settings notice says no active
revision is available and that direct skill and MCP changes are disabled,
which is the governed tool plane, not the seed; in every state the direct
skill and MCP controls stay read-only. To adopt it, `POST
/api/tool-plane/bootstrap/stage-current`, then validate and promote the
returned base through `/api/tool-plane/admin/revisions/{revision_id}/validate`
and `.../promote` (docs/GOVERNED_TOOL_PLANE.md, "Upgrade bootstrap");
stage-current also returns an overlay revision for every user with custom
skills, and those must be promoted too before bootstrap clears, while a
tenant whose users have added no custom skill gets a base and nothing else.
The capture is exactly the seeded bytes, so a restart, which seeds the same
bytes again, is not drift. A release whose library differs is: after the
upgrade the base stays `governed` and reports `drift: true`, the skills keep
working, and the same stage-current, validate, promote sequence adopts the
new library. Once adopted, `public/` has two writers and the seed is the last
one: promoting a base that adds or drops a *public* skill changes `public/`
immediately, and the next start puts the image's library back. `public/` is
image-owned on this profile; operator material belongs in `custom/`, which
neither the seed nor a base projection replaces. The upstream library
carries review warnings (referenced files that do not exist, unreferenced
resources, plain-HTTP links); none blocks promotion. All of this is pinned
offline by `backend/tests/test_compose_public_skills.py` against the real
tree and the profile's own policy values.

**A tenant that predates this release** needs nothing: its first start on
this release seeds the library into the empty `home/skills/` earlier releases
created, and an `extensions_config.json` seeded earlier is kept as it is (no
public skill is disabled by default, so it needs no entry).

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

- `app` (`${HARTMESH_APP_SUBNET:-10.201.26.0/24}`, pinned): the six services.
  PostgreSQL, Redis and SearXNG are reachable only here and are never
  published. The
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

### DNS under gVisor

Docker places its custom-bridge DNS service on loopback. gVisor's isolated
netstack cannot reach that host loopback service, so an open sandbox can
pass API readiness and still fail every public hostname lookup. See
[Docker DNS services](https://docs.docker.com/engine/network/#dns-services)
and [gVisor's explanation](https://gvisor.dev/docs/user_guide/faq/#my-container-cannot-resolve-another-containers-name-when-using-docker-user-defined-bridge).

For `SANDBOX_EGRESS=open` with runtime `runsc`, the profile validates a
read-only view of the VM's upstream resolver file and adds it to the
sandbox's configured mounts. The default is systemd-resolved's uplink file,
`/run/systemd/resolve/resolv.conf`; `/etc/resolv.conf` on the Debian VM points
to the loopback stub and is unsuitable. Missing files, missing nameservers,
invalid or loopback/link-local/multicast nameservers and conflicting resolver
mounts refuse the render before a new sandbox can be created. This is a
configuration check; the generation gate must still prove DNS reachability.

The bridge, disabled peer communication and runsc's `--network=sandbox`
remain in force. This gives public DNS through the VM's configured upstreams;
it does not provide Docker container-name discovery. Allowlist sandboxes
continue using their policy proxy and its pinned hosts entry. Recreate the
Gateway and its owned sandboxes after changing the host resolver: a running
bind mount can retain the old file when the host replaces it atomically.

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
tenant". A `/24` is the shape the profile assumes; six services and the bridge
address need seven.

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
design. `web_search` (through the profile's own SearXNG, § "Web search") and
`web_fetch` run from the Gateway, not the sandbox, so the list governs only
what the model's own shell and code reach.

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
| `DEER_FLOW_SANDBOX_MEMORY` | `1024m` | The limit the slim profile is measured to need on the real chat path (§ "Two 1 GiB slots"): a slim sandbox holds 436 to 484 MiB right after boot before any command runs, and a 5,000-row report peaks at 713 MiB inside it; at the 512 MiB of 2026-09-15 that report took 289 s on the tenant class with 32.9 million page refaults, at 1 GiB 86 s with none. Root-filesystem writes still stay in gVisor's memory (about 280 MiB of them fit beside the runtime at 512 MiB; the ceiling scales with the limit). `--memory-swap` is pinned equal (the guest has no swap, so this is explicitness, not a measurable change). |
| `DEER_FLOW_SANDBOX_CPUS` | `2` | A report render wants about 1.6 CPUs: at one CPU the sandbox spent 6.05 of its 10.3 s build throttled (§ "Two 1 GiB slots"), and the slim cold boot halves at two (10.2 to 5.7 s on the development host). Two sandboxes at two plus two relays at one are six quotas on the guest's four vCPUs, against the eight the four-slot profile had; oversubscription while both render at once is what the Gateway's and frontend's two-CPU caps leave room for (§ "Process and CPU limits"). |
| `DEER_FLOW_SANDBOX_PIDS_LIMIT` | `256` | The limit bounds the host-side task count, gVisor's own threads included, not the eleven processes the slim profile shows inside. Measured (§ "Slim services profile"): 54 to 58 host tasks idle, 58 to 62 through a report render, 134 under a forty-way process fan-out inside the sandbox, which a 128 limit (the first candidate) capped exactly. 256 leaves that fan-out room while still bounding a fork bomb; the full profile idled at 198 processes inside (Chromium, Jupyter, node, supervisord), peaked at 232 in a bash-plus-browser turn and needed 384. |
| `DEER_FLOW_SANDBOX_PROXY_MEMORY` | `96m` | The sidecar's cgroup peaked at 48 MiB (process high-water mark 33 MiB) during the same turn; 96 MiB is twice that peak, with `--memory-swap` equal. |

### Slim services profile

Every sandbox this profile creates runs the image's own service switches
off, set as `sandbox.environment` in `config.yaml` and passed to `docker run`
as `-e` by the local backend (the render copies the mapping through
unchanged):

```yaml
sandbox:
  environment:
    DISABLE_BROWSER: "true"
    DISABLE_JUPYTER: "true"
    DISABLE_CODE_SERVER: "true"
    DISABLE_VNC: "true"
    DISABLE_MCP_BROWSER: "true"
    DISABLE_NODEJS_REPL: "true"
```

The memory figure that goes with this profile, `DEER_FLOW_SANDBOX_MEMORY:
1024m` since 2026-09-17 (512 MiB from 2026-09-15 to then; § "Two 1 GiB
slots" for why), is deliberately not a `.env` key: it is one term of the
budget equality, so an operator who meets OOM kills in a sandbox does not
edit the bundle, which the next upgrade replaces; the remedies are the 8 GiB
VM class or the full-profile set below, both operator decisions recorded
here.

Each value is the quoted string `"true"` on purpose: the image's entrypoint
compares strings, and the harness types the mapping as text, so a bare YAML
boolean refuses to load. The mapping is injected verbatim into a container
that runs model-authored code, and a value of the form `$NAME` is resolved
from the Gateway's own environment, which carries the tenant `.env`; never
put a credential reference here. Off: Chromium and its MCP server, VNC (with the
window manager and the CJK input method that depend on it), Jupyter,
code-server and the Node REPL, whose switch also stops its on-demand start.
Left running in the container: the sandbox API server and its nginx; the
relay is unchanged and stays where it always was, in its own sidecar. The
switches stop services, not runtimes: `node`, `npx` and `python3` with the document
libraries, which `bash` and the public skills invoke, were checked in a slim
container (`node --version && npx --version && python3 -c "import weasyprint,
matplotlib, pandas, docx, duckdb"` exits 0), and the shipped tool list is
`bash` and the file tools. A tenant that needs the browser removes the six
lines and raises `DEER_FLOW_SANDBOX_PIDS_LIMIT` to the full profile's `"384"`;
the slot count, memory and Gateway limit are the full profile's already
(§ "Memory budget" keeps that history).

**Measured on a development host, not the tenant class.** Proxmox host,
Intel Xeon Gold 6138 at 2.0 GHz, eight vCPUs, `runsc` (systrap), the pinned
tenant image, the profile's hardening, no network, readiness polled from
inside the container against `/v1/sandbox`, memory and processes read five
seconds after readiness, every container at a 1 GiB limit and 384 pids (the
full profile's released figures; the memory a gVisor sandbox shows depends on
the limit it is given, § "Memory budget"). `scripts/measure-sandbox-boot.sh`,
below, is the script: the second slim one-CPU figure in each cell is its
first run, the rest are the same measurement by its predecessor on the same
day, 2026-09-15:

| profile | cpus | `--memory` | ready | processes in the sandbox | idle memory |
| --- | --- | --- | --- | --- | --- |
| full | 1 | 1 GiB | 20.2 s | 31 at +5 s (198 once every service is up) | 1,209 MiB |
| slim | 1 | 1 GiB | 10.2 s, 10.8 s | 11 | 238 MiB, 226 MiB |
| full | 2 | 1 GiB | 10.8 s | 34 at +5 s | 1,363 MiB |
| slim | 2 | 1 GiB | 5.7 s | 11 | 239 MiB |

Slim at its own 512 MiB limit, same script after its clock moved to each
container's own `docker run`: ready in 12.1 and 12.5 s, 212 and 264 MiB idle,
54 host-side tasks. This host is roughly four times faster than the tenant
class at the same settings (the full profile at one CPU took 80 to 91 s
there, § "Sandbox readiness budget", on an earlier image, `runsc` release and
Docker); the slim boot on the tenant class measured about 15 s at one CPU
in the `.18` run (create 4.2 s, readiness 10.8 s) and should halve at the two
CPUs the profile gives since 2026-09-17 (§ "Two 1 GiB slots"); the readiness
budget stays at 120 as the conservative bound. The measurement to run there, from the bundle directory on
the guest, ten serial starts per cell (the script measures full containers
at 1 GiB and slim ones at the profile's limit unless `MEMORY` says
otherwise):

```sh
RUNS=10 bash scripts/measure-sandbox-boot.sh          # full and slim, one and two CPUs
PROFILES=slim CPUS=1 CONCURRENT=4 RUNS=10 bash scripts/measure-sandbox-boot.sh
```

Run it with the stack down (`docker compose --project-directory /opt/hartmesh
--env-file /srv/hartmesh/.env down`) or on a scratch guest: four measurement
containers beside a live stack and its own warm sandboxes do not fit the
6 GiB, and the figures are only comparable to the ones here without the
stack's own load.

The script reads the image and the limits from this directory's
`config.yaml` and `compose.yaml`; its knobs are `IMAGE`, `RUNTIME`,
`PROFILES`, `CPUS`, `MEMORY`, `PIDS`, `RUNS`, `CONCURRENT`, `EXEC`, `MOUNTS`,
`TIMEOUT`, `SETTLE` and `OUT` (its header documents each). It starts every
container with the profile's hardening after any `MOUNTS`, so those cannot
undo it, refuses a profile name it does not know, keeps each container until
its row is written so an OOM kill is recorded as `oom_killed` rather than as
blank figures, removes its containers on interrupt, and writes one TSV row
per container: the memory limit, time to ready from that container's own
`docker run`, processes and idle memory read `SETTLE` seconds after
readiness and before `EXEC`, the host cgroup's memory and pid peaks and OOM
kills read after `EXEC`, and `EXEC`'s duration and exit code. The bundle's
compose-run scripts are invoked with `sh`; this operator tool needs `bash`.

**Four sandboxes rendering at once, on the same development host
(2026-09-15).** History: the four-slot budget of 2026-09-15 to 2026-09-17
was only true if four slim sandboxes doing real work stayed under 512 MiB
each, and on the direct-execution path measured here they did; what the
measurement missed is under "Two 1 GiB slots". Measured with the script above at
the profile's limits (`--cpus 1`, 512 MiB, 128 pids, `runsc`), the sandbox
image built from `docker/sandbox/` at the current tree (the pinned tenant
image predates the document libraries; the next release cut moves the pin),
`CONCURRENT=4 RUNS=2`, and `EXEC` running the public `business-report` skill
end to end in every container: build from the skill's 5,000-row test workbook
(`backend/tests/skills/business_report/fixtures/example_services_export.xlsx`,
generated by the `make_example_services_export.py` beside it), then render
PDF, Word and Excel. Host cgroup figures, so gVisor's own memory and threads
are counted. The ready times in this table were taken from a clock started
after the fourth `docker run` returned (the script has since moved to one
clock per container), so they understate readiness by up to the
`docker run` latency and the spread across indices is partly probe order:

| run | ready (four at once) | render, build + three formats | `memory.peak` | `pids.peak` | OOM kills |
| --- | --- | --- | --- | --- | --- |
| 1 | 7.5, 8.1, 8.8, 10.7 s | 13.2, 13.4, 13.4, 13.4 s | 334 to 338 MiB | 58 to 62 | 0 |
| 2 | 7.6, 8.2, 10.1, 10.7 s | 12.2, 12.4, 12.6, 12.9 s | 335 to 355 MiB | 58 to 60 | 0 |

Memory after the renders was 221 to 242 MiB. The peak leaves 157 MiB of the
512 under a render heavier than the skill's small fixture (300 rows), and the
pid peak sits at a quarter of the 256 limit. This host gave each of the four
sandboxes two vCPUs where the tenant class gives one, so the concurrency
figures are the least transferable in this section. The invocation, from
the bundle directory on a guest whose skills directory carries the skill and
a workbook to build from (the recorded figures predate the `public/` seed
and were taken with `/mnt/skills/business-report/...`):

```sh
PROFILES=slim CPUS=1 CONCURRENT=4 RUNS=2 \
  MOUNTS="-v /srv/hartmesh/home/skills:/mnt/skills:ro -v /srv/hartmesh/operator/measure:/mnt/measure:ro" \
  EXEC='set -e; R=/mnt/skills/public/business-report/scripts/report.py; python3 $R build /mnt/measure/export.xlsx --period 2026-08 --out /tmp/r >/dev/null; for f in pdf docx xlsx; do python3 $R render /tmp/r/2026-08-business-review.report.json --to $f >/dev/null; done' \
  bash scripts/measure-sandbox-boot.sh
```

Three more single-container probes at 512 MiB, slim, one CPU, same host and
script, for the two ways a sandbox has died before (§ "Memory budget"):

| probe (`EXEC`) | `memory.peak` | `pids.peak` | outcome |
| --- | --- | --- | --- |
| forty processes at once (`seq 1 40 \| xargs -P 40 -I{} python3 -c "import time; time.sleep(5)"`) | 412 MiB | 134 (at a 384 limit); at the first candidate of 128 the peak was 128, the limit itself | held; the limit is now 256 |
| a 600 MiB file written with `dd` then read, on a bind mount (`/mnt/user-data/outputs`) | 512 MiB, the limit | 54 | held, no OOM kill: file pages on a bind mount are reclaimed under pressure |
| a 200 MiB file written then read on the container's own root filesystem (`/tmp`) | 479 MiB | 50 | held; the same at 600 MiB was OOM-killed (`exit 128`), because root-filesystem writes stay in gVisor's memory. About 280 MiB is the ceiling for files a sandbox keeps outside its bind mounts |

Package installation through the proxy, the load that killed the full profile
below 1 GiB, was not re-run here (the script runs without a network); with
the slim profile idling at 226 MiB rather than the full profile's 760 MiB it is
a different case, and it is the first item for the tenant-class gate. This is
the development-host stand-in for that gate, not the gate itself: the same
invocations on the tenant VM class, with the skill at its data-disk path and
the pinned image once the next cut carries the document libraries, are what
close it.

### Two 1 GiB slots at two CPUs (2026-09-17)

The four 512 MiB slots of 2026-09-15 were sized from direct-execution
measurements (the table above: a report render peaks at 334 to 355 MiB in a
fresh container). The tenant-class `.18` run measured the same report on the
real chat path and it did not fit: **288.8 s** at 512 MiB against **86.3 s**
at 1 GiB, with 32,856,411 file-page refaults and 125.4 GiB of block reads
in the 512 MiB run (182.7 sandbox CPU-seconds, 93 of them throttled, no OOM
kill) against 20,634 refaults and 81 MiB at 1 GiB, where the cgroup peaked at
713 MiB with no reclaim event. Standalone trials on the same guest still
rendered in 6.4 to 9.5 s at either limit, so the difference was looked for in
the chat path and found in two places, the shell session the Gateway opens
and the sandbox's own idle footprint, on the development host with this image, `runsc`, one CPU and the six switches
(`scratchpad/memx` in the session that made this change; three runs):

| measurement | value |
| --- | --- |
| `memory.current` right after readiness, before any command | 436 to 484 MiB (317 MiB when an earlier container's page cache was still charged elsewhere) |
| of which gVisor's memfd (`shmem`) | 162 MiB |
| of which the image's page cache (`active_file` + `inactive_file`) | 200 to 260 MiB |
| of which host anon (the Sentry and gofer) | 41 to 45 MiB |
| processes inside, by RSS (worst of three runs unless a range is given) | `python-server` 167 MiB, `ipykernel_launcher` 85 to 95 MiB (started by the API server regardless of `DISABLE_JUPYTER`; no sandbox provider in this harness calls it), supervisord 35, nginx 20, tmux 11, bash 12 |
| first `/v1/shell/exec` call | +100 to 114 MiB host anon over 2 to 2.7 s wall (2.0 to 2.2 CPU-seconds), never paid by `docker exec` |
| `build --render pdf,docx,xlsx` via the API at 512 MiB | 13.7 s, 25,692 refaults, 273 MB re-read, cap reached 1,717 times |
| the same at 1 GiB | 10.3 s, 928 refaults, 3.8 MB, no cap event; 6.05 of the 10.3 s throttled at one CPU |

So at 512 MiB roughly 330 to 340 MiB is unreclaimable once the shell session
is open, and the report's mapped libraries stream through what is left. On
this host that cost 1.33× against the same container at 1 GiB (13.7 s against
10.3 s) and 1.46× against `docker exec` at the same limit (13.7 s against
9.4 s). On the tenant class the same 512-against-1 GiB comparison was 3.3×
(288.8 s against 86.3 s), far more reclaim than anything reproduced here; why
it degrades that much further is not measured, and a slower disk and a guest
already near its own memory line are the untested hypotheses. The budget
follows the measured peak: 1 GiB holds 713 MiB with room; 768 MiB might once
the idle kernel is gone, and is unqualified. Two slots at 1 GiB with their
relays put the profile back on the 5.0 GiB line exactly (§ "Memory budget"),
so **concurrency halves**: two warm sandboxes, a third acquisition evicting
the idler of the two, and with both active a third created beyond the cap
(§ "Memory budget" says what that costs). `DEER_FLOW_SANDBOX_CPUS` goes to 2
because the render is throttled at one and the slim cold boot halves.

One of the two slots is spent ahead of demand on purpose. A new chat's
sandbox is built the moment the chat opens, before the first message
(`sandbox.prewarm_claim_timeout` in `config.yaml`; § "Reading a turn's timing"
says what a prewarmed first turn looks like), because the tenant-class runs
measured the container -- 4 to 19 s to create and 5 to 10 s to answer its
readiness probe -- as the whole of the first turn's pre-model wait, paid while
the person watched "workspace starting". The prewarm takes a free slot or
nothing: it never evicts a parked sandbox some thread will reclaim, and one
nobody sends a message to is stopped about 300 s later rather than holding the
slot for the 1800 s idle timeout. So with one thread active and one chat freshly
opened, both slots are in use, and a third thread pays the same eviction it
paid before; what changes is who pays the cold start -- nobody, when the
prewarm lands -- not how many containers fit.

Two follow-ups this licenses, neither done here: stopping the API server's
idle Jupyter kernel in the slim profile (an image change; 85 to 95 MiB of
RSS nothing uses), and, if density matters, re-measuring the line with that
footprint. A third 1 GiB slot does not fit this class, and three 768 MiB
slots do not fit the 5.0 GiB line either (2880 + 3 × 864 = 5472 MiB) without
352 MiB paid elsewhere in `compose.yaml`. Both belong to the tenant-class
gate, not to a compose edit.

### Sandbox readiness budget

A cold start is `docker run` (the two networks, the relay sidecar, then the
sandbox container) followed by the Gateway polling the new sandbox's
`/v1/sandbox` through the authenticated relay. The polling has a budget,
`sandbox.ready_timeout`, and a sandbox that has not answered `200` by the end
of it is destroyed under the ownership fences and the acquisition fails: the
turn gets an error, never a hang. Both acquisition paths (the synchronous one
tools use and the asynchronous one the run middleware uses) enforce the same
value. The harness default is 60 seconds and, until 2026-09-12, it was a
constant. This profile sets **120** in `config.yaml`.

**Why.** On the four-vCPU tenant VM class, with the released image
(`sha256:60c696...`), `runsc` release-20260831.0 (systrap) and Docker 29.8.0,
the sandbox at the released limits (`--cpus 1`, 1 GiB, 384 pids, uid 1000,
`--cap-drop=ALL`, no-new-privileges, built-in seccomp) answered `200` after
80.119 s and 91.139 s, measured from the moment `create` returned with a
three-second request timeout; changing only the CPU quota to two brought that
to 33.631 s and 37.324 s. One-CPU runs showed CPU throttling in 98 to 99 % of
sampled periods and no memory pressure, memory-limit, OOM or OOM-kill event:
the service simply progresses through its imports and session initialisation
at one CPU's pace, and the inner nginx serves once it is ready. So at the
fixed 60 every one-CPU cold start on that class was destroyed at 60 s, by
design, while a two-CPU sandbox created for the test and returned to one CPU
at readiness completed a public hello (streamed output, one model call, a
stored answer) in 46.963 s. Nothing about the model, keys, egress, runtime or
Gateway configuration was involved. These figures compared CPU quotas on one
installed runtime; they say nothing about any particular `runsc` release.
Since the seeded library makes every turn's skill snapshot nonempty, the
sandbox is bound before the model is called, so a new chat's first text waits
out the whole cold start (80 to 91 s measured on one CPU, 9.0 to 11.7 s on the
slim profile) and a chat whose sandbox was evicted pays it again; a reused
sandbox does not (§ "Public skills").

**The setting.** `SANDBOX_READY_TIMEOUT` in the tenant `.env` overrides the
template: whole seconds, 60 to 600 inclusive, absent means 120. The floor is
the harness default (a smaller budget has never been useful on any host); the
ceiling keeps one cold start inside a single nginx `proxy_read_timeout` window
(600 s in the shipped `nginx.conf`) so the front door cannot cut off a turn
that is still waiting for its sandbox. Any other value, including `0`, refuses
to render: the Gateway does not start, the last rendered `config.yaml` stays,
and the refusal names the key and the rule. The backend validates the rendered
value again (`sandbox.ready_timeout`: finite, greater than 0, at most 3600;
zero, negative, `.inf`, `.nan`, booleans and text refuse to load). There is no
value on either side that disables the deadline. `render_config.py --check`
prints the effective value (`sandbox ready_timeout=...s`).

**What the deadline means.** It runs on a monotonic clock, so a wall-clock
step or a suspended guest neither extends nor shortens it. Every probe and
every sleep is clamped to what is left of it; on the asynchronous path each
probe is additionally bounded as a whole (httpx timeouts are per phase, and a
reply that dripped a byte at a time would never trip them). A `200` that lands
after the deadline has passed is not a success: the container is destroyed as
if it had never answered. A cancelled asynchronous acquisition (the run
middleware's lease manager shields the acquire from client disconnects and
drains it, so this is the direct-provider path) tears its container down under
the same fences before the cancellation propagates.

**Ownership during startup.** The provider used to publish ownership only
after readiness, so a starting container ran unowned for the whole budget.
On this profile ownership is in Redis (inferred from the stream bridge), and
a peer's or this instance's own reconciliation adopts an unowned container
once it has stayed unowned for one lease TTL: 30 s renewal × 4 = **120 s**,
the same as the new budget, so a longer wait would have made a starting
container adoptable mid-start (a dead warm entry when the wait then timed
out, or a container stopped underneath its own registration). Now the lease
is taken before the wait starts and renewed during it, and the container is
marked as starting in-process before `docker run`, so reconciliation defers
it and the lease renewal thread keeps it. A creator that dies mid-wait stops
renewing, its lease lapses, and after the grace a peer adopts the container
exactly as before. Every teardown on the timeout, cancellation and
registration-failure paths still claims the teardown lease first and fails
closed when a peer owns the container or the store cannot answer; where the
fences refuse, the container is left for reconciliation and the refusal is
logged (`Not destroying unready sandbox ...`), never reported as a clean stop.

**Cleanup allowance.** After the budget the fenced teardown stops the sandbox
and the sidecar (Docker's 10 s SIGKILL escalation each), removes the sidecar
and both networks; the documented allowance is **60 s** on top of the budget,
and the never-ready control in the live regression asserts it. The hard
bounds behind it are the backend's per-stop timeout (120 s, for a wedged
daemon) and 15 s per removal.

**Related deadlines, checked.** nginx proxies `/api/*` with 600 s connect,
send and read timeouts; the chart's provisioner startup probe (200 s) is a
different backend and unchanged; the adoption probe `discover()` runs on a
warm container and keeps its 5 s; `idle_timeout`, the tool command timeouts
and the shutdown phases are unaffected.

**What 120 is and is not.** It is the initial candidate the estate evaluates,
chosen as roughly a third above the slower of the two observed one-CPU starts.
One 91-second success on one VM does not establish a fleet-wide bound.
Acceptance needs repeated cold starts on representative Intel and AMD VM
hosts, serially and with a full ceiling of sandboxes (two since 2026-09-17)
starting concurrently, each at the profile's CPU quota (two since the same
date; the one-CPU figures above are the history the budget was set against
and the budget stays until two-CPU starts are measured on the class),
recording the `create` duration
separately from the readiness poll, the sample count, every observed latency
and the margin against the effective
deadline; if the margin is inadequate the evidence is what to report, and the
budget (or the fleet's CPU allocation, a separate capacity decision) is what
changes. The live regression prints exactly those records:

```sh
cd backend && PYTHONPATH=. HARTMESH_READINESS_SAMPLES=5 \
  uv run pytest -m live tests/test_restricted_runsc_readiness_live.py -s -v
```

It needs a Docker 28+ daemon with a registered `runsc` runtime and skips
anywhere else (a skip is an unpassed gate, not a pass). It builds the released
topology from this directory's `config.yaml` and `compose.yaml` (image and
proxy digests, allowlist, limits, hardening, the slim service switches)
through the real provider, on both acquisition paths and with as many
concurrent starts as `sandbox.replicas`; it requires readiness
within the profile's own budget, resolved by the same function the Gateway
uses; on failure it prints the inner listener state and the python-server and
nginx program logs with the relay token redacted; it checks that a missing
and a wrong relay token are refused; and a never-ready control (the image with
its service port set to 1, which uid 1000 cannot bind) must fail within the
budget plus the cleanup allowance with the sandbox, sidecar and both networks
gone. A warm diagnostic sandbox or a temporary CPU increase satisfies none of
this. Changing the fleet's CPU allocation or adding a startup burst is a
separate capacity decision. Slimming is the tool-surface decision the profile
has since taken (§ "Slim services profile"), and the 80 to 91 s that set this
budget were the full profile's; the slim boot on the tenant class is the
measurement that decides whether 120 can come down.

## Memory budget

The guest has 6 GiB and no swap. In-VM limits sum to at most 5.0 GiB so it
keeps at least 512 MiB `MemAvailable` under a burst of sandbox starts plus a
1 GiB file operation (the line was drawn when every sandbox ran Chromium; the
slim profile has not moved it).

| Service | `mem_limit` = `memswap_limit` |
| --- | --- |
| gateway | 1088 MiB |
| frontend | 384 MiB |
| nginx | 128 MiB |
| postgres | 768 MiB |
| redis | 256 MiB (`maxmemory 128mb`, `volatile-lru`) |
| searxng | 256 MiB |
| **services** | **2880 MiB** |

Equal `memswap_limit` is an assertion of intent: with no swap device it
changes nothing measurable. The services were 3072 MiB (exactly 3.0 GiB) with
the Gateway at 1536 MiB until 2026-09-06; the Gateway gave up 192 MiB so the
sandbox could go from 768 MiB to 1 GiB without moving the 5.0 GiB line (the
measurement that chose the Gateway is under "Settling the sandbox figure").
On 2026-09-15 the sandbox went from two 1 GiB full-profile slots to four
512 MiB slim ones and the Gateway paid the two extra 96 MiB relays, 1344 to
1152 MiB. On 2026-09-17 the profile returned to two slots at 1 GiB, slim
(§ "Two 1 GiB slots"), and the Gateway took those 192 MiB back. Later that
day it gave them up again, to the search service (§ "Web search"), and on
2026-09-18 a further 64 MiB to the same service after the tenant class found
it at its ceiling: 1088 MiB is 1.86 times the 586 MiB the Gateway peaked at
under the 2026-09-06 two-turn tenant-load runs and 1.90 times the 572 MiB it
peaked at on the tenant class, and leaves 502 MiB for MCP servers a tenant
adds; the datastores were left alone for the reason recorded below. The
Gateway is the donor each time because it is the one service whose limit is
set as a multiple of a measured peak rather than against an observed failure,
so what it gives up is stated headroom rather than margin of unknown size.
Moving the 5.0 GiB line instead is the operator's call, not this profile's.

Every term of that line, so a change to one has to name the other it took
from: services 2880 MiB (the table above), two 1024 MiB sandbox slots, and
their two 96 MiB relays is 5120 MiB, exactly 5.0 GiB.
`backend/tests/test_compose_profile.py` adds them up, so a limit cannot move
without a counterpart moving with it.

**Before snapshotting the data disk.** A turn's memory extraction runs behind
the turn: the conversation is handed to a buffer, a worker picks it up later,
calls a model and writes the document. So "nobody is using it" does not mean
the files have stopped moving, and a snapshot or byte-exact comparison taken
in that window catches a document mid-write. Ask first, from the guest:

```sh
curl -fsS http://127.0.0.1:2026/api/memory/writers
```

Take the snapshot when `idle` is `true`. Anything else — buffered work, a
worker still running, or a backend that cannot account for its own writers —
answers `false`, and the honest reading of `false` is "not yet", not "never".
Stopping the stack settles it too: the Gateway drains those writers inside its
shutdown budget, which is why a baseline taken *before* `docker compose down`
and compared *after* it can differ on `memory.json` with nobody having touched
it by hand.

**Upgrading a guest that already runs sandboxes.** `docker compose up -d`
stops the Gateway with SIGTERM and its shutdown destroys every sandbox it
owns, so the new limits and switches apply from the next acquisition. A
Gateway that was killed rather than stopped leaves its sandboxes running, and
the provider adopts survivors on restart by their labels and networks alone,
with whatever environment and cgroup they were created with: four adopted
512 MiB sandboxes from the 2026-09-15 profile, of which the two-slot ceiling
evicts one per new acquisition, leave two of them and their relays beside
the two 1 GiB slots this profile creates, **6336 MiB** with the services on
this 6 GiB guest. Before bringing
up a new bundle on a guest whose Gateway did not stop cleanly, list and
remove them:

```sh
docker ps --filter name=deer-flow-sandbox- --filter name=deer-flow-netproxy-
docker rm -f $(docker ps -q --filter name=deer-flow-sandbox- --filter name=deer-flow-netproxy-)
```

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
| searxng | 128 | scheduler |

Memory is what the budget bounds; these bound availability. Without them a
runaway MCP server the Gateway spawns in its own container (`npx`, `uvx`) or
a Next.js fault could take every pid and every core of the four-vCPU guest
from the sandboxes, which are the only other bounded processes. The Gateway
and frontend caps of two vCPUs keep the sandboxes' `--cpus 2` shares under
contention; nginx and the datastores are small enough to leave to the
scheduler. Two sandboxes at two CPUs and two relays at one are six quotas on
four vCPUs (the four-slot profile had eight), with the Gateway's and the
frontend's two each on top: while both sandboxes render or boot at once the
guest is oversubscribed for seconds, which is what the readiness budget is
for; at steady state a sandbox that is rendering wants about 1.6 CPUs for
seconds (§ "Two 1 GiB slots") and an idle one needs none.

Under `allowlist` each concurrent sandbox costs its 1 GiB plus its proxy's
limit (96 MiB, from the measurement above; the backend's own default is
256 MiB). The concurrent-sandbox ceiling is therefore **2**
(`sandbox.replicas: 2` in both modes, one budget, one gate, one behaviour):
2880 + 2 × (1024 + 96) = 5120 MiB, **exactly** the 5.0 GiB line with nothing
to spare, against 6240 MiB for three. `open` mode carries no proxy and still
does not fit three (2880 + 3 × 1024 = 5952 MiB > 5120), so the ceiling is 2
in both modes. A third concurrent sandbox is the 8 GiB VM class, or the
3 × 768 MiB candidate named under "Two 1 GiB slots" once it is qualified: an
operator change, not a profile change. Because the total sits on the line,
the next increase to any limit in `compose.yaml` has to be paid for by a
decrease somewhere else in it; `backend/tests/test_compose_profile.py`
asserts the equality, not just the bound.

`replicas` is a soft maximum with **LRU eviction of warm sandboxes**: a third
acquisition does not fail, it evicts the least-recently-used sandbox that no
thread is using, which is what keeps the count at two and the budget true
while at least one slot is idle. With both in active use the provider logs a
soft-cap breach and creates a third anyway, and neither the provider nor the
kernel refuses a fourth: the limits are ceilings, not reservations. That
third one (1024 + 96 MiB of limit) already exceeds the 1024 MiB the line
leaves unallocated, so three concurrently active people put 6240 MiB of
limits against 6144 MiB of RAM (the four-slot profile reached that point at
five to six), and what bounds them from there is the guest's own memory, with
the victim of an OOM kill whatever the kernel scores highest rather than the
newest sandbox. Bounding the breach in the provider is open. Eviction is
customer-visible: a thread whose sandbox was evicted
gets a fresh one on its next turn (its files persist under `home/`, and that
turn waits a cold start before its first text).
`idle_timeout: 1800` keeps an idle sandbox warm for thirty minutes: the budget
reserves every slot whether or not it is used, and a cold start of the
sandbox image under gVisor takes tens of seconds, so idle slots are kept
rather than freed; eviction still reclaims one when a third thread needs it.

**History: the full services profile.** Everything from here to the end of
"Settling the sandbox figure" was measured with every service in the image
running (Chromium, Jupyter, VNC, code-server, the Node REPL), which the
profile no longer does by default (§ "Slim services profile"). The figures
stay because they are the ones a tenant that turns the browser back on
returns to, and because the Gateway and frontend measurements below are
still the basis of those two limits.

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

`compose.yaml` then said 1 GiB for the sandbox and **1344 MiB for the Gateway**
(1536 less the 192 MiB; from 2026-09-15 it said 512 MiB and 1152 MiB, and since
2026-09-17 it says 1024 MiB and, with the search service added the same day,
1152 MiB, § "Memory budget"). Which limit gave up the memory was decided by
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
The Gateway carried the whole 192 MiB because it was then the only service
with more than twice its measured peak after the trim: 586 MiB at the
end of the second, heavier run (14 of 14 turns succeeded, 14 uploads, 11.5
thousand page loads, 23 thousand API reads; one upload during the final
eviction answered `504` from nginx and the upload after it succeeded, a
latency observation, not a memory one). Its earlier 1536 MiB was headroom for MCP
servers the Gateway spawns in its own container (`npx`, `uvx`); none is
configured by default, and 758 MiB of spare remained at the time for those a
tenant adds (566 MiB since 2026-09-15, briefly 758 MiB on 2026-09-17 before
the search service took the difference, § "Memory budget").
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
`dedupe_storage: auto`, and an explicit `DEER_FLOW_TENANT_ID`. One thing is
not: under `durable_production` a turn whose skill snapshot is nonempty, which
every tenant's is since `public/` is seeded, admits only a qualified durable
materializer, and the local container backend offers none, so every chat turn
would fail with `AcceptedSkillSandboxBindingError` before a sandbox existed
(`backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`, "Which population a deployment
profile runs"). Adopting the durable profile is blocked on a qualified
materializer, not only on the two keys.

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
template's keyless defaults (the profile's own SearXNG for search and image
search, § "Web search"; the Gateway's own fetch, § "Web fetch") by name; when several present
keys provide the same tool, the first fragment in file order wins, which is
why the files are numbered.

**The keyless search default is best effort, and usually will not work.**
DuckDuckGo answers an automated search from a server address with a
human-verification challenge rather than results — measured on a development
host on 2026-09-17, and a tenant VM is the same class of address. HartMesh
treats that as what it is: an access control, which it does not solve, evade
or route around. A declined search fails the turn with one sentence naming the
missing configuration, so the model stops instead of retrying a tool that
cannot work and the person is not told the internet is broken. **A tenant that
needs web search needs a search-provider key**, which replaces the keyless
default by name and is the supported path. Everything else in the profile —
uploaded documents, the sandbox, reports — works without one.

A tenant with **no** model key starts, logs `provider keys found: none`, and
serves a frontend that reports no model configured. That is the correct
failure for the profile; refusing such a tenant belongs in the operator's
onboarding verb.

### Web search

`web_search` and `image_search` without a search-provider key are the
profile's own SearXNG: the `searxng` service, one instance per tenant,
reachable only on `app`, called by the Gateway alone and only for JSON. Both
replaced DuckDuckGo on 2026-09-17. Its HTML endpoint answers a server
address with an anomaly challenge on every query (HTTP 202 and an image
puzzle) and its image endpoint refuses this address outright, so a tenant
whose model reached for either got an error where an answer should have
been; this profile does not solve, evade, or endpoint-shop around such
challenges, so both providers had to change.

`searxng/settings.yml` names exactly three engines and `keep_only` makes
them the whole registry: a query goes to Google's search element, Bing and
Yahoo, from the tenant's own address, and nowhere else. They were chosen by measurement rather than
reputation. Each candidate was isolated behind SearXNG's own parser (the
engine name stamped on every result checked, since an unregistered engine
silently falls back to the whole set) and asked six questions of the kind a
small business asks: a plain fact, a VAT rate, an accounting-software how-to,
a central-bank rate, a library API, a public grant. A question counts as
answered when results came back, and as authoritative when the institution,
the vendor's own help centre or the standards body was in the top three.
Measured 2026-09-17 from a server-class host, a few dozen queries per engine
across one afternoon:

| Engine | Answered | Authoritative in top 3 | Median | Why it is or is not here |
| --- | --- | --- | --- | --- |
| Google, search element | 18/18, then gated | 18/18 | 0.26 s | best ranking of any keyless engine; leads at weight 2 |
| Yahoo | 18/18 | 15/18 | 0.6 s | never gated; the ranking fallback, full weight |
| Bing | 18/18 | 9/18 | 0.15 s | never gated; the availability floor at half weight, see below |
| Google, results page | 17/18 | 6/18 | 0.33 s | answers a server only with the degraded no-JavaScript page |
| Yandex | 18/18 | 12/18 | 0.6 s | excluded; a tenant's questions do not leave for it |
| Qwant | 12/18 | 10/12 while answering | 0.66 s | a challenge on every query after about 24 |
| Mojeek | 6/6 | 4/6 | 1.4 s | SearXNG ships it inactive; slowest and weakest |
| Brave | 6/24 | 5/6 while answering | 0.6 s | rate-limited after about 6, still closed 30 min later |
| Startpage | 6/6 | 6/6 | 0.4 s | answers only by solving its proof-of-work challenge, which SearXNG marks inactive for that reason; excluded on principle |
| DuckDuckGo, html and lite | 0/12 | none | none | a challenge every time |

Merged as configured: 12/12 answered, 12/12 authoritative while Google
answers and 10/12 once it gates, seven results per query at 0.7 s.

Google leads and is the first to go. Its ordinary results page answers a
server only with the degraded no-JavaScript page, so SearXNG's `google cse`
engine instead calls Google's embedded-element endpoint using a publisher
engine id that ships in SearXNG's own source rather than any key of ours.
That is the engine most likely to stop working without warning, either
because the address gates (about 54 queries here, not lifted by a pause) or
because Google changes the scheme. Bing and Yahoo are what the tool keeps
answering with when it does, which is why all three are configured rather
than the best one alone.

Bing carries half weight because it is the one engine measured returning
results unrelated to the question: a tree-service directory for the
central-bank rate, a game forum for the VAT rate. It never gates and it is
the fastest, so it stays as the floor that keeps search answering, but at
half weight it cannot outrank Yahoo. With that weighting and Google gated,
the same twelve questions answer 12/12 with 10/12 authoritative in the top
three.

`image_search` reaches a separate set of engines in the same instance, and
the two groups do not mix: a web query reaches only the general engines, an
image query only the image ones. The image engines were measured the same
way on six reference-image queries, counting a query as usable when a result
in the top five carried a direct image address a generator could be handed:

| Engine | Answered | Usable address | Results | Median | Why it is or is not here |
| --- | --- | --- | --- | --- | --- |
| Google, element images | 6/6 | 6/6 | 20 | 0.34 s | leads at weight 2, as on the web side |
| Unsplash | 6/6 | 6/6 | 20 | 0.16 s | a photo library that publishes for this use |
| Openverse | 5/6 | 5/6 | 17 | 0.11 s | openly licensed images, fastest of the set |
| Wikimedia Commons | 5/6 | 5/6 | 5 | 0.38 s | fewer and smaller, but covers public-domain subjects the others miss |
| DuckDuckGo images | 0/6 | none | none | none | refused this address on every query; the endpoint being replaced |
| Flickr | 6/6 | 0/6 | 25 | 1.16 s | answers, but its results carry no direct image address |
| Pixabay | 4/6 | 0/6 | 29 | 0.18 s | parsing errors, and no usable address when it answers |
| Startpage images | 6/6 | 6/6 | 50 | 0.62 s | answers only through a challenge and ships inactive upstream |

Several image engines share an outgoing network with their general-search
namesake, so naming one in `keep_only` without the other refuses to start:
Qwant, Mojeek, Brave and Startpage images all failed that way while the set
was being chosen.

Nothing keyless is both best and reliable. Gating is by address reputation,
so a tenant may meet different thresholds, and every engine here reads a
page or endpoint meant for a browser rather than an API with terms. A search
key (`providers/tools/`: Tavily, Serper, Brave, Exa, Firecrawl, Serply,
GroundRoute, Tencent WSA, FastCRW) is the supported upgrade; when one is
present its fragment replaces this tool by name.

The engine set is release content, not a tenant setting: `settings.yml` is
mounted read-only from the bundle and the next upgrade restores it, and no
`.env` key changes it. An operator who wants different search buys a key.

When SearXNG is down or answers with an error the tool returns one sentence
the model can act on (answer from what it already knows, tell the person the
web was not checked, do not retry this turn) rather than an exception. The
query stays out of the log at every level: the search is a POST, so the
question never appears in a request line httpx logs at INFO or in any
access log, and the status-error path logs the status rather than the
exception text, which would carry the URL. Four searches
per Gateway process run at once: a research fan-out that asked dozens of
questions in a minute is what gets an address gated. Results are bounded
before the model sees them: web addresses only, and a title, address and
snippet each capped, because a result page is text somebody else wrote.

Neither tool is evidence-bearing: they have no durable evidence adapter, so
their turns carry tool receipts but no `retrieval.observation.v1` row
(`backend/docs/EVIDENCE_BEARING_RETRIEVAL.md`). The keyed durable providers
keep theirs, and with them the domain allowlists and byte ceilings their
policy carries. `image_search` returns only results carrying a direct image
address, bounded the same way, and says so plainly when the service is down
rather than when a query simply found nothing.

The service: `searxng/searxng`, pinned by digest in `images.txt`; uid 1000; a
read-only root with `/etc/searxng` a 1 MiB tmpfs, `/tmp` a 16 MiB one and
`/var/cache/searxng` a 4 MiB one (the image declares that path a volume;
without the mount, Docker would put an anonymous volume on the root disk at
every `up`). The image's start script insists on `/etc/searxng/settings.yml`
and ignores its arguments, so `searxng/run.sh` replaces the entrypoint: it
copies the bundle's settings into that tmpfs, mints the instance secret from
`/dev/urandom` (it signs HTML cookies nothing sets, and lands nowhere: no
`.env` key, and the contract is unchanged) and execs the image's script.
256 MiB, taken from the Gateway (§ "Memory budget"); `pids_limit` 128. That
figure was 192 MiB until 2026-09-18, sized against one near-idle sample
(92 MiB resident after a turn), and the tenant class then found the cgroup at
exactly that ceiling: `memory.peak` 201,330,688 bytes, `memory.events max`
170, no OOM kill, search still answering with thirteen linked sources.
`scripts/measure-searxng.sh` replays those two turns' own 22 queries through
this container at the Gateway's concurrency of four, with the same image,
limits, mounts, read-only root, `pids_limit` and CPU count; across 90 queries
in three shapes (uncapped, capped, and with image queries mixed in) it peaks
at 145 MiB, holds an anonymous working set near 110 MiB with 9 MiB of file
cache, answers every query, and never reaches the ceiling. So the tenant's
pressure is real and unreproduced here, and the limit is set against what the
tenant was observed to need rather than against that replay: 256 MiB clears
the observed ceiling by 64 MiB and the measured peak by 111 MiB. Reclaim with
no OOM kill is the cgroup working, not a failure, but a service sitting on
its ceiling has no room for the next engine change, and the figure it had was
chosen from what another service could spare rather than from this one's
demand. SearXNG's own limiter is off (it needs a
Redis this instance does not have and guards an HTML surface nothing
reaches), as are metrics and the image proxy; `safe_search` is 1. The
healthcheck is the instance's `/healthz`, which reports the process, not
whether an engine still answers: a tenant whose engines all gate looks
healthy everywhere, and the signal is the Gateway's `web_search (SearXNG)
failed` line plus SearXNG's own per-query engine errors. Its start prints an
ownership warning for the root-owned tmpfs and a missing-`limiter.toml`
notice, both expected.

### Web fetch

`web_fetch` without a fetch-provider key is
`deerflow.community.direct_fetch.tools:web_fetch_tool` since 2026-09-17: the
Gateway reads the page itself. It replaced the hosted reader (`r.jina.ai`),
which answers a tenant's server address with HTTP 401
`AuthenticationRequiredError` for every page unless a key is sent; on the
tenant class one research turn made three such calls and a report turn
thirteen, each to a different address, before answering from search snippets
alone. Probed from a development host on 2026-09-17, the seventeen exact
addresses those turns asked for: fourteen answer a plain `GET` with
`200 text/html`, two refuse with 403 (a reference site and a blog platform
that gate automated readers, which the profile does not solve, evade or shop
around), one timed out. The hosted reader answered none without a key.

What the fetch does, in order: the address must be `http` or `https` with a
host; every address the host resolves to must be public (the same never-allowed
set as the sandbox egress policy: private, loopback, link-local, carrier NAT,
multicast, documentation and cloud-metadata ranges); the connection is made to
the checked address with the name on `Host` and on TLS SNI, so the certificate
is still verified against the name and a resolver that answers differently the
second time gains nothing; redirects are followed by hand, each hop checked and
pinned again, eight at most; only HTML, XHTML and plain text are read, to
2 MiB; one 10 s budget (`timeout` on the tool entry) covers the chain. No
cookies, no credentials, no retries. The page is reduced to its article and
handed to the model as Markdown, 4,096 characters at most, as before.

A refusal is typed by who refused. A page's own 401, 403, 404, 429 or 5xx is
the *origin's*: the model is told to use another source and nothing else
changes. A refusal by the fetch path itself, which the direct fetch has no
way to produce but a keyed provider fragment does (a bad key, a spent quota,
a rate-limited deployment), is the *provider's*: it holds for every address,
so the Gateway withdraws `web_fetch` from the model's tools for the rest of
that turn and tells it once to answer from search results and say that
sources could not be fetched. The next turn tries once more. This is the
typed `error_scope` on the tool result, never the wording of it.

Four pages are read at once, no more. The work now lands on the Gateway
rather than a hosted reader: up to 2 MiB buffered per fetch and an article
extraction that spawns a Node subprocess, inside the same memory, CPU and pid
budget this profile gives the Gateway. Search has carried the same bound for
the same reason; a model that issues several fetch calls in one step would
otherwise have no ceiling at all.

`web_fetch` runs from the Gateway's own address, like search: the sandbox
allowlist (§ "`SANDBOX_EGRESS=allowlist` (the default)") does not govern it, and a site that gates
that address gates it for every tenant behind it.

### Operator-managed models

The catalog above is release content: it is mounted read-only from the bundle,
and adding a model to it means editing fork source and cutting a release. That
is the wrong shape for a decision that belongs to whoever runs the tenant --
which model a provider has just published, which of them this customer is
allowed to use, what they now cost.

So one optional `.env` key, `HARTMESH_MODELS_FILE`, may name a YAML file on the
tenant's own data disk. When it does, that file's `models:` list is the whole
rendered `models:` section, and the bundled catalog contributes none. Nothing
else about the profile changes: tool providers are still selected by key the
way they always were, and every other setting still comes from the template.

```bash
install -d -o 1000 -g 1000 -m 0750 /srv/hartmesh/operator
$EDITOR /srv/hartmesh/operator/models.yaml
```

```yaml
# /srv/hartmesh/operator/models.yaml -- operator-owned, not release content.
models:
  - name: novita-kimi-k2                     # stable identity; threads keep it
    display_name: Kimi K2
    description: Long-context general model
    use: langchain_openai:ChatOpenAI         # a client class this release installs
    model: moonshotai/kimi-k2-instruct       # the provider's own id
    base_url: https://api.novita.ai/openai   # the provider's endpoint
    api_key: $NOVITA_API_KEY                 # a reference, never a value
    context_window: 131072
    max_tokens: 8192
    pricing:                                 # read only by the cost display
      currency: USD
      input_per_million: 0.57
      output_per_million: 2.30
  - name: claude-sonnet-4
    display_name: Claude Sonnet 4
    use: langchain_anthropic:ChatAnthropic
    model: claude-sonnet-4-20250514
    api_key: $ANTHROPIC_API_KEY
    context_window: 200000
    max_tokens: 8192
    supports_thinking: true
    thinking: { type: enabled, budget_tokens: 2048 }
```

Then, in `/srv/hartmesh/.env`:

```
HARTMESH_MODELS_FILE=/srv/hartmesh/operator/models.yaml
```

The first entry is the tenant's default model. Several providers and several
models may share one credential, and nothing requires a `$NAME` to be a key the
bundled catalog knows about.

**Credential references.** A credential is written as the reference the Gateway
expands itself, exactly as the bundled fragments do, and the render enforces
it rather than trusting it:

- The only accepted form is a **whole-string** `$NAME`: `api_key: $NOVITA_API_KEY`.
  `${NOVITA_API_KEY}`, `$NOVITA_API_KEY-v2` and `sk-...` are all literals, and
  a literal is refused. The Gateway's own expansion is whole-string too, so a
  form it would not expand is one that would reach the provider verbatim.
- `NAME` must be a variable the tenant `.env` actually carries. An unset one is
  refused by name -- never by value.
- A field is treated as credential-bearing when the last `_`/`-` segment of its
  name is `key`, `apikey`, `token`, `secret`, `password`, `credential`,
  `credentials` or `authorization`. That catches `api_key`, `gemini_api_key`,
  `azure_ad_token` and an `Authorization` header nested in `default_headers`,
  and leaves `max_tokens` and `budget_tokens` alone.
- **Omitting one is fine.** A client that needs no credential simply carries no
  such field; the rule is about the form of a credential that is configured,
  not about requiring one. A provider that wants a placeholder (some local
  OpenAI-compatible servers want a literal `EMPTY`) gets one through a variable
  in `.env` like any other value.

So no secret is in this file, in the rendered `config.yaml`, in a refusal, in a
log line or in `GET /api/models` -- and the render refuses instead of writing
one there.

**What a model entry may contain** is the Gateway's own `models[*]` schema,
unchanged and undocumented here on purpose: `config.example.yaml` at the repo
root is the worked reference, and `ModelConfig`
(`backend/packages/harness/deerflow/config/model_config.py`) is the contract.
`name`, `use` and `model` are required; `display_name`, `description`,
`context_window`, `supports_thinking` / `thinking` /
`when_thinking_enabled` / `when_thinking_disabled`, `supports_vision`,
`supports_reasoning_effort`, `use_responses_api`, `output_version`,
`stream_chunk_timeout` and `pricing` are understood; anything else is passed to
the client constructor, which is how endpoints (`base_url`) and ordinary
request settings (`temperature`, `max_tokens`, `extra_body`, …) are set.

**The four answers.** These are distinct, and the render says which it took in
the Gateway's first log line (`models from bundled catalog` /
`models from operator file /srv/hartmesh/operator/models.yaml`):

| The key is | The file | Result |
| --- | --- | --- |
| unset or empty | -- | The bundled catalog, by provider key. Unchanged behaviour. |
| set | `models:` with a list | Exactly that list, in that order, whatever keys the tenant carries. |
| set | `models: []` | This tenant has no models, key or no key. A deliberate choice, not an absent source. |
| set | empty, unreadable, missing, or refused below | The Gateway refuses to start. It never falls back to the bundled catalog. |

An empty file is a refusal precisely because it is ambiguous: an operator who
means "no models" writes `models: []`, and one whose editor truncated the file
gets told so. `models:` with nothing after it is YAML null and is refused the
same way.

**What is refused:** a path that does not exist or that uid 1000 cannot read;
a document that is not a mapping; any top-level key but `models:` (this is a
model list, not a second copy of `config.yaml` -- `auth:`, `sandbox:`,
`database:` and the rest stay with the profile and cannot be reached from
here); **any entry the Gateway's own `ModelConfig` would reject**, which is
where a missing `name`/`use`/`model`, a `context_window: 0`, a
`supports_vision: banana` and every other schema rule land; a `use` this
release cannot import or that is not a `BaseChatModel`; two entries with the
same `name`; a credential field that is not a `$NAME` reference; a `$NAME` the
tenant `.env` does not carry.

The schema check is the backend's own `ModelConfig`, called on each rendered
entry -- not a second schema kept in the profile, which would drift from it.
It runs on the bundled catalog as well as the operator file, so a fragment that
ever drifts out of the schema is caught here rather than at Gateway load. It is
deliberately **not** a full `AppConfig.from_file`: that applies a dozen
unrelated process-wide singletons, and validating a model list has no business
doing that. What it cannot tell you is whether the provider will accept the id
or the settings -- see the boundary below.

**What a refusal says.** The source, the entry's **position** (`models[0]`),
the field, and the rule that was broken (`context_window: greater_than`). It
does not say what the value was, and it does not say what the *key* was
either: a paste lands in a mapping key as readily as in a value, and a refusal
is written to the journal, where it is kept and shipped. So, uniformly:

- **A location is structure, never text.** Locations are built from path
  components -- a top-level key of the template, then list indices this
  renderer generated -- and stop at the first operator-typed key. So
  `models[0].api_key` and a credential nested three levels down under keys the
  operator invented both locate `models[0]`, a bracket typed into a key cannot
  be mistaken for an index, and an index below an operator key is dropped with
  it, because it means nothing without the key above it.
- A **YAML syntax error** reports where the parser stopped and withholds the
  parser's own message, which quotes the source line.
- A **schema rejection** reports the field and the rule, and drops any message
  that quotes what it rejected. Only field names the schema itself declares are
  printed -- they are its vocabulary, not the operator's. Anything else, such
  as the rejected key an `invalid_key` error carries, prints as `(key)`:
  `models[0] is not a model the Gateway will load: (key): invalid_key (Keys
  should be strings)`.
- A **client-class refusal** reports the field and the *category* of failure --
  not a path, not a class name, and never the resolver's own message, which
  quotes the value it was handed: `models[0] field `use` names a module this
  release does not install`. The categories are a malformed path, a module the
  release does not install, a module that failed to import, a module with no
  such attribute, and an attribute that is not a `BaseChatModel` subclass.
- A **duplicate name** reports the colliding positions, never the shared name:
  `these entries share one: operator model file … models[0], models[1]`.
- A **credential-form refusal** counts the offending fields per entry
  (`models[0]: 1`) rather than naming them, because a field *name* is
  operator-typed content as much as a value is. Likewise, top-level keys other
  than `models:` are counted, not listed.
- **Entries are named by index**, never by their `name`.

Nothing an operator typed is echoed back, and the exception chain is broken at
each of these so no traceback can restore it. `--check` obeys the same policy
as the start-time render, because it is the same code path. The one operator
value that is deliberately printed is the *path* of the model file itself: it
is the locator, and the successful-render log line prints it too.

**Who can change it.** Only whoever has write access to the tenant's data
disk -- the operator, root on the guest. The Gateway mounts the directory
read-only and exposes no route that writes it: `GET /api/models` is read-only
and this profile mounts no configuration-writing endpoint, so a tenant
administrator signed into the application cannot add a model and neither can an
agent, whose reach is `home/` and the sandbox. Whatever is edited into the
rendered `home/config.yaml` by hand is discarded at the next start, as it
always was -- that file is generated output, not a source.

The **client-class** check, unlike the schema check, is the operator file's
alone: import-checking the bundled catalog would turn a provider package the
image happens not to carry into a new start requirement for a tenant who never
selected that model. It is a check that the class exists and is a chat model,
not that the provider will accept the id: a syntactically valid `model` the
provider rejects is a provider error on the first message, visible as itself,
with no substitution of some other model.

**Applying a change.** The render happens once, at Gateway start; there is no
hot reload. Validate first, while the Gateway is still serving the previous
list:

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env \
  exec --user 1000 gateway sh -c 'cd /app/backend && PYTHONPATH=. uv run --no-sync \
  python /opt/hartmesh/gateway/render_config.py --template /opt/hartmesh/config.yaml \
  --catalog /opt/hartmesh/providers --check'
```

`--check` runs the whole render, including every refusal above, and writes
nothing. `--user 1000` is not optional: the Gateway container drops every
capability, so its root has no `CAP_DAC_OVERRIDE` and cannot read the
uid-1000-owned data directory the render reads and writes. The check sees the
*running* container's environment, so the first time the key is turned on --
before the restart that gives the container the new `.env` -- pass it
explicitly with `exec --user 1000 -e HARTMESH_MODELS_FILE=/srv/hartmesh/operator/models.yaml ...`.

Then restart the one service. Which command depends on what changed, and the
difference matters:

```bash
# the file changed, .env did not -- Compose sees no configuration change, so
# `up -d` would do nothing at all and report the container as already running
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env restart gateway

# .env changed (turning the key on, off, or moving it) -- the container's
# environment must be replaced, which is a recreate
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env up -d gateway

docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env logs gateway | grep render_config
curl -s localhost:2026/api/models | jq '.models[].name'
```

Restarting the Gateway is a visible interruption: it is one replica, so
in-flight streams end and users reconnect. Nothing else is touched --
PostgreSQL, Redis, `home/` and running sandboxes are not part of this.

If a change is applied anyway and turns out to be invalid, the previous
rendered `home/config.yaml` is still intact: `render_config.py` writes to a
temporary file and renames it into place, so a refusal never truncates what
last worked. What does happen is that the Gateway exits, and `restart:
unless-stopped` retries it; `docker compose ps` shows it restarting and
`logs gateway` shows one line beginning `render_config: refusing to render:`.

**Rolling back** is the same restart with the previous input: restore the
previous `models.yaml` (keep a copy before editing), or comment
`HARTMESH_MODELS_FILE` out of `.env` to return to the bundled catalog.

**What survives.** Both inputs live on the tenant's data disk, which is what a
release replacement keeps: `docker compose down`, swap `/opt/hartmesh` for the
new bundle, `up -d`, and the same `.env` and the same `models.yaml` render the
same list against the new release's template and catalog. Container recreation
is the same story -- the file is a bind mount, not image content.

**Prices are configuration too.** `pricing` is read by the console's cost
display (`currency`, `input_per_million`, `output_per_million`, optional
`input_cache_hit_per_million`) and by nothing else: the model factory excludes
it from what it hands the client, so it never reaches an outbound request. The
profile makes no price lookups -- there is no price feed here, at boot or ever;
the numbers are whatever the operator wrote. They are an **estimate**, computed
at display time from recorded token counts and today's configured prices, so
repricing a model changes the figure shown for runs that already happened.
Nothing here is a billing ledger, and it should not be read as one. One
currency per tenant: if two priced models disagree, the console logs that and
shows no cost at all. A model with no `pricing` simply has no estimate.

**Identities are yours to keep.** `name` is the identity threads, agents and
subagent references store. Removing or renaming a model does not remap anything
onto a survivor -- a thread that asks for a name the file no longer carries
gets an error naming it. Rename only when you are ready to update the
references too; to retire a model, prefer leaving the entry in place until the
threads that used it are done with it.

**Where the boundary is.** A new model id, a compatible provider endpoint, a
supported request parameter, a capability flag and a price are all
configuration: this file, a restart, done. A provider that speaks a wire
protocol none of the installed clients implement, one that needs an SDK this
release does not ship, or a feature the client class does not expose, is a
software change -- an adapter or a release -- and no amount of configuration
substitutes for it. Accepting an entry here means the shape is valid and the
client class exists, not that the provider will honour the id or the setting;
that answer comes from the provider, on the first message.

### Login lockout

The rendered `config.yaml` departs from the Gateway defaults in exactly two
places: `auth.local.lockout_store: redis` and
`auth.local.source_max_failures: 600`. Three facts about this profile explain
both, and the rest of the numbers being left alone.

**One tenant is one company.** Five to twenty staff, all behind one office NAT
address. A lockout keyed on the client address therefore locks the company, not
the guesser: five failures by anyone lock everyone, remote staff included, and
five failures in a five-minute window is a median Monday morning with a tool
nobody's password manager has learned yet. The lock is keyed on the **account**
instead — five failures against one email address lock that address for five
minutes, from any address — so the tenant's shared egress address is irrelevant
to it. A much looser per-source guard still catches spraying: 50 distinct
accounts, or `source_max_failures` in total, within a 15-minute window.

**The office's own retry budget reaches the generic volume limit exactly.**
The account lock is silent by design — a locked account answers exactly what a
wrong password answers, so nothing tells a person to stop retrying. Allow
twenty staff five wrong attempts each to reach their own lock, then ten further
retries each during it, and the arithmetic is 20 × 15 = **300 failures**, which
is the standalone default for `source_max_failures` to the failure. One such
morning would land on the limit and lock the office's shared address for 15
minutes — the whole failure this profile's account-keyed lock exists to avoid,
re-entering by the other door. The profile therefore sets **600**: that
allowance doubled, leaving 299 further failures before the limit trips. These
are design allowances, not measured customer behaviour.

The tradeoff is real and is not hidden here. This source may now cause twice as
many counted failures before the volume block, and every one of them still
costs the Gateway one password-equivalent verification. Neither this limit nor
the 50-account guard makes an office immune to a malicious user who shares its
address — the account lock is what bounds guessing at any one account, and the
source guard only bounds volume and breadth. Submitted addresses are what the
50-account guard counts, so mistyped ones count as distinct accounts too. A
legitimate office **can** reach either threshold; 600 is headroom, not a
promise.

The window these counts live in is **fixed, not sliding**: it opens on that
address's first counted failure and closes 900 seconds later, after which the
next failure opens a new one. Nothing decays inside an open window (in Redis
the counter simply carries that TTL from its first increment). And raising
`source_max_failures` does not release an address that is already locked —
unlike the per-account lock, which is re-derived against the live threshold on
every check, a source lock is a written sentence. It runs out after
`source_lockout_seconds`, or an administrator clears it.

**Clearing a lock must not be an outage.** The stack is one replica with a
recreate-style rollout, so restarting the Gateway to drop an in-process counter
interrupts everyone still working. Redis is already running here for the stream
bridge, so the counters live there and an administrator clears one account with
`POST /api/v1/auth/lockouts/clear` (admin role, interactive session, not a PAT);
`GET /api/v1/auth/lockouts` lists what is currently locked. The two clears are
separate on purpose: clearing the office's **source** lock releases the address
and nothing else, so every account that locked itself behind it stays locked
until it is cleared or expires. A Redis outage fails the login path closed with
a 503 — on this profile Redis is already load-bearing for every SSE stream, so
it is not a state the tenant is working in anyway.

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
| `models` | `[]` | the catalog entries for the keys present, or the operator's own list when `HARTMESH_MODELS_FILE` is set | the chart leaves models to the operator's values; the profile renders them from the tenant's keys, or from the file § "Operator-managed models" describes |
| `tools[web_search]` | DuckDuckGo | the profile's own SearXNG without a search key (§ "Web search"), the keyed provider when one is present (Tavily here) | same ten tools; only the search backend follows the tenant |
| `sandbox.image` | upstream `latest` | the fork's digest pin | release pinning |
| `sandbox.replicas` | 3 | 2 | the memory budget |
| `sandbox.idle_timeout` | absent | 1800 | recorded above |
| `sandbox.environment` | absent | the six `DISABLE_*` switches, each `"true"` | § "Slim services profile": the slim sandbox is what the two 1 GiB slots are measured for |
| `sandbox.network` | absent | `allowlist` block | the chart's sandboxes are fenced by CiliumNetworkPolicy; the VM has no such fence, so the backend's own mode is the fence |
| `sandbox.provisioner_url`, `provisioner_service_account_token_file`, `accepted_skill_projection_profile` | set | absent | the Kubernetes provisioner path; the local Docker backend has no provisioner and mounts skills directly |
| `skills` | absent (PVC mounts) | `path` under `home/`, `container_path: /mnt/skills` | the local backend's skills mount; `public/` is seeded from the Gateway image at every start (§ "Public skills") |
| `run_events.backend` | upstream default (`memory`) | `db` | run events survive a Gateway restart on a single-Gateway VM |
| `auth.local.lockout_store` | absent (`memory`) | `redis` | § "Login lockout": one replica with a recreate rollout, so clearing a lockout must not need a restart |
| `auth.local.source_max_failures` | absent (`300`) | `600` | § "Login lockout": twenty staff reaching their own account lock and retrying past it is 300 failures exactly, so the generic limit leaves the office no margin |

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
Setting or unsetting `HARTMESH_MODELS_FILE` is the same operation (the file it
names is not `.env` and needs only the Gateway restart § "Operator-managed
models" describes). Compose recreates only the services whose configuration
changed: the Gateway
(which re-renders `config.yaml` on start), never PostgreSQL unless its
password changed, and it must not: `POSTGRES_PASSWORD` is used by `initdb`
once and authenticates every later connection.

## Logging

No service carries a `logging:` block. The guest's daemon sets
`log-driver: journald`, so every container's stdout already lands in the
journal, which the VM caps and ships; a `logging:` block would override that
with `json-file` on the root disk.

### Reading a turn's timing

Every turn ends with one `turn phase timings` line from
`deerflow.runtime.turn_phases`, which carries the reading an operator needs
(the structured record behind it is the complete one). One turn, one line —
wrapped here only to fit the page:

```text
turn phase timings run=<run id> correlation=<id> total=9261ms outcome=success kind=accepted \
acquisition=accepted_warm_reclaim reused=accepted_active acquire_reason=accepted_binding \
snapshot=present/13pkg/mandatory queue=0ms phases=admission@0ms assembly@6ms sandbox_lookup@22ms+143ms \
skill_materialization@21ms+5825ms agent_build@5857ms+232ms checkpoint_preflight@6109ms graph_start@6115ms \
sandbox_binding@6131ms+2677ms sandbox_acquire@6131ms+2677ms model_request@8821ms first_provider_text@9022ms \
first_stream_text@9025ms model_completion@9156ms terminal@9261ms \
unobservable=browser_first_text(requires_a_browser_measurement_through_public_ingress)
```

`@` is an offset from the turn's start and `+` the phase's own measured
duration, both in milliseconds, so a phase carrying both **ends** at
`@ + duration` and the questions are arithmetic on one line. A span is printed
when it *ends*, so an enclosing span appears after the spans it contains and
its `@` can be earlier than the entry printed before it: `sandbox_lookup@22ms`
prints ahead of the `skill_materialization@21ms` that encloses it, and
`sandbox_binding` ahead of the `sandbox_acquire` that encloses it. Sum only
spans that enclose nothing. The sandbox is in
hand at the end of `sandbox_acquire` (`6131 + 2677 = 8808ms`), so
`first_stream_text@9025ms` leaves 217 ms between the sandbox being in hand and
the first assistant text leaving the Gateway. `first_stream_text -
first_provider_text` is what the Gateway added to the provider's own first
token (both are instants, so that one is a plain subtraction). The line above
is a *warm* turn that still took 9.3 s, and it says where: 5.8 s projecting the
accepted skill snapshot and 2.7 s binding it, against 143 ms to find the
container. That line was measured before 2026-09-17, when both were staged
again every turn; a warm turn on this release verifies the retained material
instead (§ "Public skills", **Per-turn material**), so the same shape today
reads in milliseconds. It stays here because reading the line is the point.

A *first* turn on a new chat should read the same way. The web client asks
the Gateway to build the thread's sandbox the moment the chat opens (`POST
/api/threads/{id}/workspace/prewarm`), seconds before the first message, so
the turn's `sandbox_lookup` finds it parked: `acquisition=accepted_warm_reclaim`
with no `sandbox_create` or `sandbox_readiness` span at all, and the Gateway
log carries `Accepted sandbox <id> was built <n>s ahead of this turn and
reclaimed warm`. A first turn that still shows `acquisition=created` with
`sandbox_create@…+4000ms sandbox_readiness@…+6000ms` says the prewarm did not
happen or was not claimed in time: both slots were taken (a prewarm never evicts;
`Not prewarming a sandbox … every slot is taken`), or the chat sat idle past
`sandbox.prewarm_claim_timeout` and the container was stopped
(`Prewarmed sandbox <id> was not claimed within 300s`; the reaper looks every 30 s, so an abandoned slot comes back at up to 330 s). A person who sends
within a few seconds of opening the chat, while the build is still running,
waits for it on that same turn -- the acquisition serialises on the thread --
and then reclaims it. That wait is the line's top-level
`queue=<n>ms` field, which is printed only when it is not zero, and it falls
inside the `skill_projection` phase, not a `sandbox_create` span: the progress
label stays at "preparing" rather than "workspace starting", and the phase
reads long even though its own work took milliseconds. So read `queue=`
before concluding that projection is slow. Until `v2.1.0+hartmesh.27` the
prewarm also published the thread's skill view under that same lock, which
added its staging to the wait: the released `.26` tenant-class run measured
`queue=3687ms` on a warm host and `queue=16263ms` on the first chat after a
boot, against 129 ms of projection work and a 15 ms reclaim. The publication
now runs after the lock is released, so what `queue=` still shows on a first
turn is the container build itself. Total wait is what the cold start would have
been, less the seconds the build had already run.

`acquisition=` says where this turn's sandbox came from. `created`,
`rediscovered`, `discovered`, `warm_reclaim`, `accepted_warm_reclaim` and
`unknown_provenance` are **origins** — they name how the container came to be;
`accepted_active` and `in_process` are **observations** that it was already in
hand, which any turn can make. A turn acquires in stages (the worker projects
the accepted skills before the graph runs; the sandbox middleware binds later
against whatever is now active), so `acquisition=` is the turn's origin
whenever a stage reported one, and a later stage that merely found the
container active is reported beside it as `reused=`. `acquisition=created
reused=accepted_active` is one turn that created its sandbox and bound to it
again — one container, not two — so a turn carrying `creates=1` no longer
reports `acquisition=accepted_active`. A bare `acquisition=accepted_active` or
`acquisition=in_process` with no `reused=` is a turn that found its container
already there and no stage said how it got there. (A counter prints only when
non-zero, so the warm turn above carries no `creates=`.)

Four phases name the work that used to sit unattributed between the sandbox
lookup and the binding, which on a warm turn is most of the wait.
`skill_materialization` is the accepted skill snapshot being projected into
the sandbox — authorization, the snapshot's manifest and verification, the
container acquisition and the projection itself — and the sandbox phases *it
records* nest inside it (`sandbox_lookup@22ms+143ms` sits within
`skill_materialization@21ms+5825ms`, so the projection cost beyond finding the
container is the difference; on a cold turn `sandbox_create` and
`sandbox_readiness` nest there too). The middleware's later `sandbox_binding`
and `sandbox_acquire` do not: they run after the graph starts and are counted
separately. Then `agent_build` (building the graph; with
`skill_materialization` one of the two here carrying a measured duration),
`checkpoint_preflight` (the thread's stored state being loaded) and
`graph_start` (the worker entering the stream attempt, ahead of the per-thread
checkpoint lock — so a wait behind a concurrent turn on the same thread falls
between it and `sandbox_binding`). `graph_start` is marked per attempt, so a
resumed or retried stream shows two. `skill_materialization` is recorded only
on `kind=accepted` turns; an ordinary turn has the other three.

`launch=` is what happened *before* the journal opened, and it is **outside**
`total=` and every `@` offset: the journal's zero is the worker's admission,
so `total=` and the phases start there, and `launch=` is the interval from the
request reaching the application to that zero. It is the server's half of the
person's wait for a first word — read acknowledgement as `launch=` plus
`first_stream_text@`, never as the offset alone — and until this field
existed it was invisible except as the gap between the access log and "Run
created". Every entry point stamps the intent when it builds it: the HTTP
routes at `start_run`, the scheduler when it dispatches an occurrence, an IM
channel when it turns a message into a run, the embedded runtime API at its
call. The steps in brackets are consecutive from that stamp, so they account
for the whole interval up to the persisted row: `identify` (the idempotency
lookup, when the entry point supplied a key), `permit` (the admission fence),
`seal` (the accepted invocation: config, agent revision and skill snapshot),
`authorize`, `constrain`, `prepare` (the projection reservation and the
checkpoint seed check) and `persist` (the run row). `handoff` is the rest:
the worker being attached, its task being scheduled, and the thread-metadata
setup it runs before opening the journal. The example line above predates
the field and is left as it was measured. Measured on the development host on
2026-09-17 (the Gateway stream suite's ordinary turn against the probe model:
no skill snapshot, the in-process stores, so `seal` is the cheap case):

```text
launch=405ms(identify=0ms,permit=16ms,seal=96ms,authorize=0ms,constrain=0ms,prepare=268ms,persist=20ms,handoff=4ms)
```

Tenant-class `.19`, before this field existed, showed the same interval as
1.4 to 2.2 s on warm turns and 3.4 s on the session's first, with `seal`
staging the skill snapshot each time; that is the figure the next
qualification reads from `launch=` directly. A launch that replays an
already-admitted run (an idempotent resubmission) starts no worker and prints
no new line; a run recovered by execution takeover prints a line with no
`launch=`, because no request in that process launched it.

What the server cannot see it declares instead of inferring:
`browser_first_text` is always `unobservable` here, because only a browser
measuring through the front door can time what the person actually waited
for.

```sh
docker compose --project-directory /opt/hartmesh --env-file "$ENV" \
  logs gateway | grep 'turn phase timings'
```

is therefore a complete per-turn latency record. The same fields also ride the
log record as a structured `turn_phases` field for deployments that enable
`logging.enhance.format: json`; this profile logs text. That record stamps
`version: 5` (`launch` was added in 5), and fields are added rather than repurposed, so a reader that
tolerates unknown keys needs no change. The line reports
confirmed resource counts (`creates=`, `teardowns=`), not attempts: the
structured record keeps `create_attempts` and `unknown_create_results`
separately, and one confirmed create can stand for several attempts.

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
  (two sandboxes acquired, the ceiling at the time): `id` is `uid=1000(gem)`; the only
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

Operator-managed models (2026-09-09, P-y), on the same host and engine, with a
disposable `/srv/hartmesh` laid out as the golden image lays it out, the bundle
copied to a scratch directory with every exec bit stripped, `SANDBOX_RUNTIME`
set to `runc`, and fixture provider keys (`fixture-openai-key-not-real`,
`fixture-acme-key-not-real`). No provider was ever contacted with an intent to
succeed. The two model identities used, `acme-lightning-1` /
`acme/lightning-1-2099` and `acme-anvil-9` / `acme-anvil-9-20991231`, are
fictitious, so nothing here can pass by having been added to the bundle.

- Baseline, no `HARTMESH_MODELS_FILE`, `OPENAI_API_KEY` present: five services
  healthy, `render_config: wrote /srv/hartmesh/home/config.yaml (models from
  bundled catalog; egress=allowlist; provider keys found: OPENAI_API_KEY)`, and
  `GET /api/models` answers `gpt-4`, `gpt-5-responses`. Inside the Gateway,
  `/srv/hartmesh/operator` is mounted and `touch` there answers `Read-only file
  system`.
- With the key set and a two-model file, the same `OPENAI_API_KEY` still
  present: the log line becomes `models from operator file
  /srv/hartmesh/operator/models.yaml` and `/api/models` answers exactly
  `acme-lightning-1`, `acme-anvil-9`. `gpt-4` is gone -- a provider key buys
  tools, not models. The rendered `config.yaml` carries `api_key:
  $ACME_API_KEY` verbatim and no fixture value appears anywhere in it.
- One edit doing all three things -- `max_tokens` 4096 to 16384, output price
  5.00 to 4.00, a new `acme-vision-3`, `acme-anvil-9` removed -- then
  `restart gateway`: `/api/models` answers `acme-lightning-1`,
  `acme-vision-3`, and the rendered file carries `max_tokens: 16384`,
  `output_per_million: 4.0` and `supports_vision: true`. `up -d gateway` alone
  did **not** apply it: Compose saw no configuration change and left the
  container running, which is why the procedure above names `restart`.
- `--check` refused each of these with one line naming the cause and no value:
  a duplicate `name`; a top-level `auth:` key; `$NOVITA_API_KEY` when the
  tenant carries no such variable; an empty file; `models:` with nothing after
  it; `langchain_nonesuch:ChatNonesuch`; and a `chmod 0000` file
  (`Permission denied`). It wrote nothing -- the rendered `config.yaml` kept
  its checksum through all seven.
- Applying an invalid file anyway: the Gateway exits and `restart:
  unless-stopped` retries it, `logs gateway` repeats `render_config: refusing
  to render: rendered models carry a duplicate name: ['acme-lightning-1']`, and
  `/srv/hartmesh/home/config.yaml` is byte-identical to before the attempt with
  no `.config.yaml.*` temporary left beside it. Restoring the previous file and
  restarting brought the list back.
- Persistence, twice. `down` then `up -d --wait` on the same bundle: the list is
  still `acme-lightning-1`, `acme-vision-3`. Then a *replaced* bundle -- a copy
  whose `providers/models/10-openai.yaml` carries an extra `gpt-6-imaginary`,
  standing in for a later release -- `down`, `up -d --wait` from the new
  directory: the tenant's list is unchanged and the new bundled model does not
  appear. The `.env` and the model file were never inside the bundle. The
  bundle-A tree hashed the same before the first edit and after the last
  (`cc6f7099e3a8be5000f6a630997ded4b0636720475c3bd984d700746c6aa0758`).
- Both client families, built from the effective config inside the running
  Gateway: `ChatOpenAI acme/lightning-1-2099 https://api.acme.invalid/openai`
  and `ChatAnthropic acme-anvil-9-20991231 https://api.acme.invalid/anthropic`
  with `{'type': 'enabled', 'budget_tokens': 2048}`; neither client's dump
  carries `pricing` or `context_window`. The console's map reads the operator's
  prices back (`USD 1.25 4.0`, 1M in + 1M out = `5.25`). One bounded call to
  `acme-lightning-1` answers `APIConnectionError: Connection error.` -- the
  configured endpoint, reached and failed at, with no substitution. Asking for
  the removed `acme-anvil-9` answers `ValueError: Model acme-anvil-9 not found
  in config`.
- Rollback by unsetting: commenting `HARTMESH_MODELS_FILE` out of `.env` and
  `up -d gateway` returns the log line to `models from bundled catalog` and the
  list to `gpt-4`, `gpt-5-responses`, `gpt-6-imaginary` -- the replaced
  bundle's catalog, which had been there and unused throughout.
- Not covered here: any real provider. Every id above is fictitious and every
  key a fixture, so nothing in this run says a real provider will accept a
  configured id, a parameter or an endpoint -- that answer arrives on the first
  message, as the `APIConnectionError` line stands in for. A streamed
  completion and a tool-call round trip against a live model, and any advertised
  thinking or vision behaviour, need an authorized key on a real endpoint and
  remain unqualified. `runsc` and sandbox behaviour were not exercised: no
  sandbox is created by a model-configuration change.

Validation and diagnostics (2026-09-10, P-y follow-up), same host, the profile
at `v2.1.0+hartmesh.9`. Two defects the consuming deployment reproduced: the
render accepted models the backend rejects, and a credential could reach the
generated file or a refusal. The fake credential below is the string
`FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY`, which exists only to be searched for.

- Offline, through both CLI modes. A valid fixture rendered and its bytes
  recorded; then `context_window: 0`, an empty `name`, and
  `supports_vision: banana` each in turn. Every one exits 1 from `--check`
  *and* from the ordinary `--output` invocation, naming the entry by position
  and the rule that was broken -- `models[0] is not a model the Gateway will
  load: context_window: greater_than (Input should be greater than 0)`. The
  previously rendered file kept its checksum through all six invocations, and
  `home/` held nothing but `config.yaml`. A corrected file then rendered.
- Credentials. A literal `api_key` is refused (`credential fields must be
  environment references of the whole-string form $NAME ...
  ['models[0].api_key']` -- the wording at that commit; it now counts fields
  per entry instead), as is `${NAME}`, `$NAME-suffix` and a bare `$`. An
  unset reference is refused by variable name. None of the refusals contains
  the sentinel. `Authorization` nested in `default_headers` is covered by the
  same rule; `max_tokens` and `budget_tokens` are not.
- Malformed YAML holding the sentinel -- `api_key: [FAKE-CREDENTIAL-...` --
  answers `is not valid YAML at line 3, column 14 (the parser gave up at line
  4); the parser's message is withheld because it quotes the source`. The
  sentinel appears in neither stdout nor stderr.
- Live, on a disposable stack at the pinned images. Baseline: healthy, one
  operator model, rendered checksum `0b70eb...`. A valid-to-invalid update
  (`context_window: 0` *and* a literal credential) through the documented
  preflight: `exec --user 1000 ... --check` exits 1 with the schema refusal.
  Applied anyway, the Gateway restart-loops, repeating that line once per
  attempt; the rendered `config.yaml` still hashes `0b70eb...`, no
  `.config.yaml.*` sits beside it, and `logs gateway` contains no sentinel.
  Fixing only the schema error surfaces the credential refusal next -- the
  first fix cannot reintroduce the second defect -- with the checksum still
  unchanged. Correcting both restores a healthy Gateway, `GET /api/models`
  answers `acme-lightning-1`, `acme-anvil-9`, and the generated file contains
  no credential value.
- Re-checked live afterwards: both installed client families build from the
  effective config (`ChatOpenAI acme/lightning-1-2099
  https://api.acme.invalid/openai max_tokens=4096`, `ChatAnthropic
  acme-anvil-9-20991231 ... {'type': 'enabled', 'budget_tokens': 2048}`),
  neither client's dump carries `pricing` or `context_window`, the console's
  map reads `USD 1.25 5.0`, and the client's key is the value the Gateway
  expanded from the environment -- the reference contract working end to end
  while the literal never touches disk.
- Not covered here: the same real-provider gaps as above. This follow-up needed
  no provider call, and makes no new claim about one.

Diagnostics, part two (2026-09-10, P-y second follow-up). The consuming
deployment reran the three suites in one process -- 117 passed -- and found two
refusal branches still repeating what the operator typed: the duplicate-name
message printed the shared `name`, and the client-class message printed the
resolver's exception, which quotes the supplied `use`. Reproduced with the same
`FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY` string, offline, no provider call.

- Both branches, both CLI modes, over a rendered baseline: two entries sharing
  a sentinel `name` now answer `rendered models must carry distinct names, and
  these entries share one: operator model file … models[0], models[1]`, and
  `use: langchain_openai:FAKE-...` answers `models[0] field `use` names a
  module that defines no such attribute`. Neither repeats the sentinel; both
  exit 1 from `--check` and from `--output`; the baseline bytes are unchanged
  and `home/` holds only `config.yaml`. A corrected file then renders.
- Every other way to get `use` wrong routes around the resolver's message too:
  a value with no colon and one that is only a colon answer `must name a class
  as `module.path:ClassName``; `langchain_FAKE-...:ChatOpenAI` answers `names a
  module this release does not install`; a real class that is not a chat model
  answers `does not name a chat model client`. None quotes the value.
- A third branch found in the same review: unknown top-level keys were listed
  by name, so a paste at the top level was echoed. They are counted now.
  Credential-form refusals count offending fields per entry (`models[0]: 1`)
  instead of naming the field, for the same reason.
- The contract is now a test rather than a claim: seventeen placements of the
  sentinel -- `name`, a duplicate `name`, `use`, `model`, `base_url`, a literal
  and a referenced `api_key`, a nested `Authorization`, a credential-shaped and
  a plain unknown field, an unknown top-level key, a capability flag, a context
  window, a pricing field, malformed YAML, a non-mapping document, an entry
  with no name, and a non-list `models:` -- are each run through both CLI
  modes. Thirteen must refuse and none of those refusals contains the sentinel;
  the four that are legitimate content (an identity, a provider id, an
  endpoint, a note) render, which is where that content belongs.
- Evidence here is CLI-only. This was a message-only repair with no change to
  what is accepted, so no Gateway drill was repeated; the live evidence in the
  block above still stands for the paths it covers.

Diagnostics, part three (2026-09-10, P-y third follow-up). The suites passed
again -- 142 in one process -- and the review found the remaining hole: a
*location* could still repeat an operator-typed mapping key. Two ways in. The
credential path was assembled as text and then sliced at its last `]`, so a
nested list under an invented key kept that key
(`models[0].FAKE-..._options[0]`), and a `]` typed into a key was
indistinguishable from a generated index (`models[0].FAKE-...]`). And a
pydantic `loc` was treated as trusted text, although an `invalid_key` error
carries the rejected key itself -- reachable with a `!!binary` key, which
printed `b'FAKE-...': invalid_key`.

- Locations are now built from path components, not from text: a top-level key
  of the template, then indices this renderer generated, stopping at the first
  operator-typed key. An index below such a key goes with it, because it means
  nothing without the key above it. Schema locations print only field names
  `ModelConfig` declares; anything else, including an `invalid_key`'s rejected
  key, prints as `(key)`.
- All three counterexamples, both CLI modes, over a rendered baseline: each
  exits 1 from `--check` and `--output`, the baseline keeps its checksum
  (`934aa4...`), `home/` holds only `config.yaml`, and no stdout or stderr
  contains the sentinel. Cases 1 and 2 answer `Offending fields by entry:
  models[0]: 1`; case 3 answers `models[0] is not a model the Gateway will
  load: (key): invalid_key (Keys should be strings)`. Restoring the baseline
  renders and returns the file to `934aa4...`.
- Nothing about what is accepted changed, which the tests pin: an entry
  carrying `vendor.options[0]`, a Unicode key and a nested
  `extra_body.routing` list still renders with all of it intact. Rejecting odd
  keys or nested lists to make a message easy would have been an interface
  change, not a diagnostic repair.
- Coverage for the shapes: a list under an operator key, a bracket inside a
  key, a mapping inside a list inside a mapping, a credential-shaped key, a key
  full of dots that tries to forge a path, and a non-string key -- each through
  both CLI modes.
- CLI-only again, for the same reason: no change to acceptance, so the Gateway
  drill was not repeated.

Readiness budget (2026-09-12 and 2026-09-13, P-z). The repair above (a
configured budget, ownership before the wait, a deadline that is a deadline)
was proved on this development host, which is **not** a tenant VM: a Proxmox
host on an Intel Xeon Gold 6138 at 2.0 GHz with 8 vCPUs and 24 GiB visible to
the daemon, kernel 7.0.12-1-pve, Docker Engine 28.4.0, `runsc`
release-20260817.0 (systrap). The estate's observation was a four-vCPU KVM
guest on `runsc` release-20260831.0 and Docker 29.8.0, so the figures below are
one Intel data point on a faster and differently virtualised machine, not the
fleet-wide bound the acceptance gate asks for; no AMD host and no VM-class host
was available here.

- The offline suites: 455 in the provider, reconciliation, readiness, budget and
  compose files (including 30 new ones), 231 in the neighbouring sandbox
  suites, 107 in the blocking-I/O gate; ruff clean; agent guidance 0 errors.
- The live regression (`pytest -m live tests/test_restricted_runsc_readiness_live.py`,
  one sample): 5 passed in 8 m 45 s. Every sandbox inspected as `Runtime=runsc`,
  `NanoCpus=1e9`, `Memory=MemorySwap=1 GiB`, `PidsLimit=384`, `User=1000:1000`,
  `CapDrop=[ALL]`, no `CapAdd`, `no-new-privileges` and `seccomp=builtin`, no
  published port, on its internal network only; the sidecar on the internal
  and egress networks, published on `127.0.0.1` only, on the daemon's default
  runtime. A missing relay token answered 401 or 403, a wrong one likewise,
  the right one 200. Sync path: `create` 6.4 s, ready after 57.4 s (53
  probes), margin 62.6 s. Async path: 5.6 s, 49.6 s (46 probes), margin
  70.4 s. Two concurrent starts: 23.0 s and 25.4 s. Never-ready control
  (service port 1), sync and async: the acquisition raised `failed to become
  ready within 120s` after 144.0 s and 142.9 s in total (`create` 4.2 s and
  4.3 s, the 120 s budget, cleanup 19.9 s and 18.6 s against the 60 s
  allowance); the diagnostics taken while it ran carried the inner listener
  table and the python-server and nginx logs and not the relay token; the
  sandbox, sidecar and both networks were gone afterwards, the fences never
  refused, no lease or mark remained.
- Repeated cold starts (`HARTMESH_READINESS_SAMPLES=5`, 3 passed in 19 m 40 s),
  readiness measured after `create` returned, each at one CPU. Serial, sync:
  45.3 / 48.0 / 48.3 / 50.2 / 53.4 s (40 to 49 probes, `create` 4.3 to 4.8 s),
  worst margin 66.6 s. Serial, async: 49.0 / 51.5 / 51.8 / 53.5 / 53.7 s (40
  to 48 probes), worst margin 66.3 s. Concurrent pairs: 35.3 + 38.3, 29.9 +
  29.1, 30.7 + 29.6, 32.7 + 30.4, 36.3 + 33.8 s, worst margin 81.7 s. On this
  host the image was in the page cache from the first sample on; a rebooted
  guest is colder. The concurrent starts being faster than the serial ones was
  observed, not explained, and is the opposite of what a shared four-vCPU
  guest should be expected to show.
- Not proved here, and the estate's gate: readiness on the tenant VM classes
  (Intel and AMD, four vCPUs, nested `runsc`) serially and in concurrent
  pairs, with the margin against 120 s recorded per start; a cold chat and a
  tool execution through the Gateway; post-reboot acceptance on the drill
  tenant. The two 80 to 91 s observations that motivated this change give
  120 s roughly a third of margin on that class; whether that holds across
  the fleet is what those runs decide.

Slim services profile and four slots (2026-09-15). Same development host as
the readiness-budget entry above (`runsc` systrap, eight vCPUs), so a
development-host stand-in, not the tenant-class gate.

- The offline suites: `pytest tests/test_compose_profile.py` 85 passed (six
  tests new or rewritten: the six switches as exact strings, slot count,
  512 MiB and 256 pids, the 5120 equality and the open-mode bound, the render
  carrying the switches into a valid `AppConfig`, the measurement script's
  flags against a stub `docker`); with `test_aio_sandbox_local_backend.py`
  and `test_sandbox_image_contract.py` 213 passed (three live tests
  deselected); ruff clean.
- Boot and idle with `scripts/measure-sandbox-boot.sh`: the tables under
  "Slim services profile". Four slim sandboxes at 512 MiB each building a
  5,000-row report and rendering PDF, Word and Excel at once, two runs:
  `memory.peak` 334 to 355 MiB, `pids.peak` 58 to 62, no OOM kill, renders
  12.2 to 13.4 s each; the fan-out, bind-mount and root-filesystem probes
  there, which moved the pid limit from a 128 candidate to 256.
- The live regression against the new profile (`pytest -m live
  tests/test_restricted_runsc_readiness_live.py`, 5 passed in 8 m 16 s), the
  template's switches applied and asserted in each container's environment,
  limits asserted from `compose.yaml`: sync path `create` 6.1 s, ready after
  22.9 s (23 probes), margin 97.1 s; async path 3.6 s, 10.7 s (11 probes),
  margin 109.3 s; four concurrent starts, one CPU each: ready after 10.8,
  11.6, 11.7 and 11.9 s (`create` 5.6 to 6.4 s), worst margin 108.1 s.
  Never-ready control, sync and async: `failed to become ready within 120s`
  after 146.4 s and 147.0 s in total (`create` 3.5 and 3.9 s, cleanup 22.9
  and 23.1 s against the 60 s allowance); sandbox, sidecar and both networks
  gone afterwards. The full-profile figures on the same host were 57.4 s
  sync, 49.6 s async and 23.0 to 38.3 s in concurrent pairs. Re-run after
  the pid limit moved from the 128 candidate to 256 and the switches were
  routed through the provider's resolver (5 passed in 8 m 03 s): sync
  10.9 s, async 10.8 s, four concurrent 11.8 to 12.8 s (worst margin
  107.2 s), never-ready controls 146.7 and 146.4 s.
- Not proved here, and the estate's gate: the slim boot on the tenant VM
  class (serial and four at once, ten runs, both CPU quotas) and the
  four-way render there with the skill at its data-disk path; both
  invocations are under "Slim services profile". Also open: package
  installation through the proxy at 512 MiB (the load that decided the full
  profile's figure), the Gateway's peak at four concurrent turns (its
  1088 MiB is set against a two-turn peak), and the shipped pinned image
  under the render load (the render here used the image built from the
  current tree). The readiness budget stays at 120 until the first of the
  tenant-class runs has run.

Public skill library (2026-09-15). Same development host, the Gateway image
built from this tree (`docker build -f backend/Dockerfile --build-arg
UV_EXTRAS=postgres .`), so a stand-in for the image the next cut pins.

- The Docker context: a scratch Dockerfile that only copies `skills/public`
  under the repository's `.dockerignore` produced exactly the tracked tree
  (30 `SKILL.md` files, no `__pycache__` or `.ruff_cache`), and a probe
  context with `.env`, `.env.local`, `node_modules/` and `.venv/` planted
  under `skills/public` kept all four out while keeping `SKILL.md`; the
  built image holds the 24 packages at `/app/skills/public`, root-owned,
  and `seed_skills.sh` run inside it as uid 1000 seeded the profile's set,
  files `0644`, directories `0755`, in under a second.
- A gateway-only stack (postgres, redis, gateway) from a scratch copy of this
  directory with `skills.path` pointed at the scratch data disk of the P-s
  entry (its `home/skills/` still empty from that run), the Gateway image
  replaced by the tree build, `HARTMESH_SANDBOX_RESOLV_CONF=/etc/resolv.conf`
  because this host runs no systemd-resolved. First start, healthy after
  26 s: the seed line `13 public skills seeded into .../home/skills/public
  from /app/skills/public (excluded: chart-visualization claude-to-deerflow
  find-skills github-deep-research image-generation music-generation
  podcast-generation skill-creator vercel-deploy-claimable video-generation
  web-design-guidelines)`, then `Ensured the public skill projection`, the
  persistence bootstrap to head `0037`, `Application startup complete`;
  `GET /health/ready` 200 `{"status":"ready",...}`. On the data disk
  `home/skills/public` and `home/skills_view/public` list the same 13
  packages, owned `1000:1000`, directories `0755`, files `0644`;
  `business-report/SKILL.md` is byte-identical in the tree, on the disk and
  in the projection (SHA-256 `4e0a1185…`); the excluded names are absent;
  `custom/` was not created (the storage creates it on the first install).
  `docker compose restart gateway`: healthy after 19 s, the seed line and the
  projection line a second time, 13 and 13 again, no `public.seed` or
  `public.old` left.
- The older-image shape, with the tree image's `/app/skills` shadowed by an
  empty read-only mount so `/app/skills/public` is absent (what the
  previous release's pinned Gateway image looks like to `run.sh`): healthy
  after 25 s, the log line `/app/skills/public is absent; this Gateway image
  predates the public skill library. Leaving .../home/skills/public as it
  is.`, the 13 packages and the projection untouched.
- The offline suite: `pytest tests/test_compose_public_skills.py` 23 passed
  (the image layer and context rules, the seed's contract on a synthetic
  tree and byte for byte on the real one, the older-image degrade, the
  empty, linked, relative and non-name refusals, the exclusion list and that
  every review exclusion is still refused while no policy exclusion is, the
  README counts, the projection to `/mnt/skills/public`, and the governed
  tool plane's capture, validation, promotion, `unmanaged` before adoption,
  restart without drift, upgrade to `governed` with `drift: true` and its
  repair, all under the template's own `tool_plane` values); with
  `test_compose_profile.py` 109 passed; ruff and shellcheck clean.
- Not proved here: a sandbox opened through a chat listing
  `/mnt/skills/public/business-report` (the mapping is pinned offline; the
  live stack ran without the frontend and with no signed-in user), the
  adoption through the HTTP endpoints (the service path is what the offline
  test drives), and the pinned Gateway image itself, which the next cut
  builds from this tree.

Normal chat with the seeded library (2026-09-15). The tenant-class
qualification of v2.1.0+hartmesh.13 found the first browser turn of a fresh
user failing in under three seconds with `AcceptedSkillSandboxBindingError`
before any sandbox existed, on a healthy stack that had seeded the 13
packages. Reproduced and repaired on the same development host and
gateway-only stack as the entry above, this time with a model: the operator
model file (`HARTMESH_MODELS_FILE`) selecting the repository's scripted
probe model (`backend/tests/_turn_phase_probe_model.py`, mounted into the
Gateway; it streams a fixed answer and calls no tool), the Gateway image the
previous entry built, and turns driven over the released run-stream route
by a registered user.

- Cause: every Gateway run is an accepted invocation; with a nonempty
  effective-skill snapshot the worker must materialize it before the run
  starts; since 2026-09-03 the worker refused a provider without a qualified
  durable materializer whenever a run record existed, which is always, and
  the local container backend never offers one. No earlier tenant release
  carried a skill, so the snapshot was empty and the guard never fired. The
  worker now decides by the deployment profile: `local_development` runs the
  accepted-skills projection, the durable profiles refuse as before
  (`backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`, "Which population a
  deployment profile runs").
- Before the repair, the image as it is: `Run created`, `Using local
  container sandbox backend` and `Run failed ...
  error_class=AcceptedSkillSandboxBindingError` in the same second; the
  stream `metadata`, `error` (`Runtime operation failed (reference: ...)`),
  `end` after 3.8 s; no sandbox container.
- After, with the repaired worker mounted over the image's file: the first
  turn on a new chat streamed `metadata`, six `messages`, `end`. Its
  container `deer-flow-sandbox-<id>-accepted` was created before the model
  was called (first text 30.3 s after the request on this host under
  runsc: the cold start now precedes the model), mounting
  `/mnt/user-data/{workspace,uploads,outputs}` read-write and
  `/mnt/skills/.accepted` read-only, no `/mnt/skills/public` (this settles
  the previous entry's open item the other way: a chat sandbox does not list
  `/mnt/skills/public/business-report`; that mount belongs to sandboxes of
  runs with no accepted material, which no chat turn is); then
  `Released sandbox ... to warm pool (container still running)`. The second
  turn on the same chat: `Reclaimed warm-pool sandbox <same id>` in the same
  second, first text after 9.8 s, the same container. A third turn on a
  second chat created a second container; during it,
  `/mnt/skills/.accepted/<snapshot digest>/public/` listed the 13 seeded
  packages with `business-report/SKILL.md` intact, while the idle chat's
  container showed an empty `.accepted` (cleared at release, re-projected at
  the next bind; superseded 2026-09-17 — a parked chat now keeps its verified
  view, so that observation no longer reproduces, and the rest of the entry
  stands).
- Offline: `test_worker_materialization_follows_the_deployment_profile`
  (three profiles) and `test_seeded_skill_gateway_stream_e2e.py` (the real
  route, admission and worker with one seeded public skill); the accepted
  material, AIO provider, turn-phase and warm-reuse suites, 274 passed.
- Observed, not this repair's: the Gateway logs `Refused to recreate missing
  authoritative lifecycle row ... during completion persistence` at ERROR
  after every turn, failed or successful; and a reclaimed sandbox still
  costs about ten seconds before first text on this host, the warm-acquire
  latency the warm-reuse entry left unmeasured.
- Not proved here: the tenant class itself (the tenant-class qualification
  is to be rerun on a release that carries the repair), and a turn that runs
  a tool in the projected skill through a real model.

A turn's log after the seeded library (2026-09-15). The tenant-class rerun of
v2.1.0+hartmesh.14 passed the normal chat and the report workflow, and left two
observations the log itself owed: the `turn phase timings` line carried no
timings, and the lifecycle ERROR of the previous entry still followed every
turn, this time after successes. Both reproduced on the same development host,
now on the released profile shape (this directory's `compose.yaml` and
`config.yaml`, PostgreSQL and Redis, `run_ownership.heartbeat_enabled` at its
default `false`, the probe model as above, `SANDBOX_RUNTIME=runsc`), and
repaired. As in the entry above, the Gateway ran the previous entry's image
with four working-tree files (`turn_phases.py`, `runs/manager.py`,
`logging_config.py`, `runs/worker.py`) and the probe model mounted over it, so
the figures below are this host's, not a pinned image's.

- The timing line was emitted only into the log record's `extra`, which the
  default text format drops and the JSON formatter rebuilt without; a turn
  printed the bare words `turn phase timings`. It now renders the reading into
  the message ("Reading a turn's timing" above) and the JSON formatter carries
  the structured field. Measured here: a cold chat `sandbox_create@417ms+4777ms
  sandbox_readiness@5201ms+10654ms model_request@16176ms
  first_stream_text@16387ms terminal@16624ms`; the next turn on that chat
  `acquisition=accepted_warm_reclaim sandbox_acquire@336ms+159ms
  first_stream_text@715ms total=967ms` — the reclaim-to-first-text figure the
  qualification could not observe is 220 ms, readable off one line (the reclaim
  finished at `336 + 159 = 495ms` and first text left at `715ms`; a phase with
  a `+` duration ends at `@ + duration`). About 300 ms of that warm turn is the
  probe's own scripted delay (`HARTMESH_PROBE_FIRST_TEXT_DELAY_S=0.2`,
  `HARTMESH_PROBE_TAIL_DELAY_S=0.1`), visible as the 201 ms between
  `model_request` and `first_provider_text`: the line's shape is what this
  entry proves, not a model-latency figure.
- `Refused to recreate missing authoritative lifecycle row ... during
  completion persistence` was neither a missing row nor only noise. A durable
  store stamps the terminal projection whether or not lease heartbeats run and
  refuses a completion write that does not name it; the manager supplied that
  authority only with heartbeats on, so on this profile **every** turn's token
  counts, message count and message previews were dropped, and the refusal was
  then misreported as a missing row. Before: `total_tokens 0, message_count 0,
  last_ai_message NULL` on both runs of a two-turn chat, with the ERROR after
  each. After: `message_count 2`, the answer preview present, no ERROR. (The
  probe model reports no token usage, so `total_tokens` stays 0 here; a real
  provider's usage rides the same write.)
- Offline: the run-manager, turn-phase, logging and Gateway stream-e2e suites,
  including a new e2e assertion that the line a deployment prints carries
  `first_stream_text@`, and a single-worker completion regression.
- Not proved here: the tenant class itself; the pinned release image (the
  Gateway ran the previous entry's image with four working-tree files mounted
  over it); token counters against a real provider (the probe reports no usage,
  so only `message_count` and the previews were observed repaired); the
  heartbeat-enabled path this change restructures, exercised offline only; and
  a four-turn or concurrent measurement of what the added line costs (it is one
  formatted string per turn, built from the journal already taken).

Where a warm turn's seconds go, and what acquired its sandbox (2026-09-16). The
tenant-class rerun of v2.1.0+hartmesh.15 passed its report workflow and its
package-installation tests, and returned two things the line still got wrong.
Every one of its seven turns read `acquisition=accepted_active` — including the
cold turn that also carried `creates=1` and a measured `sandbox_create` — and
between the sandbox lookup ending and the binding starting each turn spent 2.6
to 3.4 s that no phase accounted for. Both reproduced on the same development
host — the unaccounted gap measuring about 6.0 s here rather than the tenant's
2.6 to 3.4 s — on the released profile shape (this directory's `compose.yaml` and
`config.yaml`, PostgreSQL, Redis, `SANDBOX_RUNTIME=runsc`, the 13-package
seeded library, the probe model, the previous entry's image with the
working-tree `turn_phases.py` and `runs/worker.py` mounted over it), and
repaired.

- A turn acquires in stages: the worker projects the accepted skills before
  the graph runs, and the sandbox middleware binds later against what is by
  then active. The journal took the last word, so the middleware's "already
  active" observation overwrote the create or the reclaim. `acquisition=` is
  now the turn's origin and the later observation rides beside it as
  `reused=`. Measured here: a cold chat `acquisition=created
  reused=accepted_active ... creates=1 sandbox_create@119ms+4302ms`, and the
  next turn on it `acquisition=accepted_warm_reclaim reused=accepted_active`.
  Before the repair both lines said `acquisition=accepted_active`.
- The unaccounted window is now four phases, and the answer is not the
  container. That warm turn: `sandbox_lookup@22ms+143ms` inside
  `skill_materialization@21ms+5825ms`, then `agent_build@5857ms+232ms`,
  `checkpoint_preflight@6109ms`, `graph_start@6115ms`,
  `sandbox_binding@6131ms+2677ms`, `model_request@8821ms`. Finding the warm
  container cost 143 ms; projecting the accepted snapshot into it and binding
  it cost 8.5 s of a 9.3 s turn. The cold turn's projection encloses its create
  and readiness: `skill_materialization@39ms+20969ms` around
  `sandbox_create@119ms+4302ms sandbox_readiness@4423ms+10722ms`.
- Offline: the turn-phase, warm-reuse, rediscovery-provenance, cleanup-outcome
  and Gateway stream-e2e suites, including a new e2e assertion that admission,
  assembly, `agent_build`, `checkpoint_preflight`, `graph_start` and
  `model_request` are present and in order on a real streamed turn.
- Not proved here: the tenant class itself, whose next Part A reads these
  fields; the pinned release image; and *why* the projection costs what it
  does — this entry measures the phase, it does not reduce it. The figures are
  this host's, with a 13-package library and ~200 ms of the probe's scripted
  first-text delay between `model_request` and `first_provider_text` (201 ms
  here), with ~100 ms more in its tail.

What the projection costs, and what removes it (2026-09-17). The previous
entry measured the phase without explaining it, and the tenant-class turn
lines of `.19` showed the same shape: the accepted material was staged twice
per turn — once at launch, once at the bind — with a `fsync` per file, and
both copies were deleted when the run ended. Measured on this development
host, on the released profile's `config.yaml` with the 13-package seeded
library the image carries (43 files, 415,749 bytes; `HARTMESH_MODELS_FILE`
unset, a probe key in the environment), driving the same functions a turn
drives:

- Staging the snapshot at launch: 2,048 ms with the digest pass alone and
  2,396 ms with the `fsync` counter installed, 45 `fsync`s. Verifying the
  retained tree instead: 23 ms, no `fsync`, with `resolve_agent_revision`
  end to end at 34 to 39 ms.
- Staging the view at the bind: 3,211 ms, 43 `fsync`s. Verifying the
  retained view instead: 12 to 13 ms, no `fsync` — including the first bind
  of the *next* run, which is the bind that used to re-stage.
- The material is retained and re-verified rather than trusted: a tree whose
  bytes no longer match its digest is replaced (and logged), one that drifts
  under a live lease is still `skill_snapshot_drift`, and startup removes
  every tree no live lease holds.
- Offline: the accepted-snapshot, projection, provider, middleware and
  lifecycle suites, including new cases for the release path a retained view
  must not wedge, a drifted tree found with and without a live lease, a
  symlinked snapshot root, a warm-pool teardown clearing the thread's view,
  and startup reclaiming every retained digest.
- Not proved here: the tenant class, where neither the cost nor the repair
  has been measured — the `.19` figures quoted in the acknowledgement work
  come from that class's own turn lines, and the next Part A is what reads
  these fields after the repair; the pinned release image; and the disk
  behaviour over a long-lived process with many users, which the bound in
  § "Public skills" states rather than measures.

Keyless web fetch that answers, and a refusal that stops (2026-09-18). The
profile advertised keyless `web_fetch` through a hosted reader that answers a
tenant's server address with HTTP 401 for every page, so a research turn made
three futile calls and a report turn thirteen, each to a different address, and
both answered from search snippets alone. § "Web fetch" records what replaced
it. Proved here:

- Probed from this host on 2026-09-17, the seventeen exact addresses those two
  turns asked for: fourteen answer a plain `GET` with `200 text/html`, two
  refuse with 403 (a reference site and a blog platform that gate automated
  readers, which this profile does not solve, evade or shop around), one timed
  out; the hosted reader answered none of them without a key.
- `backend/tests/test_direct_fetch.py`, `test_provider_refusal_middleware.py`
  and `test_web_fetch_default_gateway_stream_e2e.py`: the address checks,
  per-hop redirect checks, size and content-type caps and the typed
  origin/provider split as unit tests; the Gateway-stream cases run the real
  route, admission, worker, receipt middleware and tool dispatch with the wire
  under the fetch client scripted. Mutation-proved: connecting to the name
  instead of the pinned address, following redirects with the client, taking
  provider as the default scope, dropping the caps, or keeping the tool bound
  after a provider refusal each fail them.
- `scripts/measure-searxng.sh` against the pinned image with the profile's
  limits, mounts, read-only root, `pids_limit` and CPU count, replaying the
  two turns' own 22 queries at the Gateway's concurrency of four: 90 queries
  across three shapes, `memory.peak` 144 to 149 MiB, anonymous working set
  near 110 MiB, every query answered, `memory.events max` 0.
- Not proved here: the tenant's own 170 ceiling events, which this host does
  not reproduce; the fetch from a tenant's address, where the two 403s and any
  address-level gating may differ; and the real-model composed turns, which
  are the tenant class's to run.

Keyless web search that answers (2026-09-17). The DuckDuckGo HTML endpoint
behind the profile's keyless `web_search` answers a server address with an
anomaly challenge on every query, so a tenant whose model reached for search
got an error where an answer should have been. § "Web search" records the
engines measured and the SearXNG service that replaced it. Proved on this
host, on the profile's own `compose.yaml`, `config.yaml`, `images.txt` and
`searxng/` bundle, with the Gateway's harness and app source mounted over the
image and the turn-phase probe as the model:

- `up -d --wait searxng gateway`: both healthy; `docker inspect` of searxng
  `Memory=MemorySwap=192 MiB`, `PidsLimit=128`, `ReadonlyRootfs=true`,
  `User=1000:1000`, no port bindings; of the Gateway `1152 MiB`. The
  instance's `/config` lists exactly the engines `settings.yml` names and
  `safe_search` 1; `/etc/searxng/settings.yml` is byte-identical to the
  bundle's; loading that file without the environment yields the template's
  placeholder secret, so no secret is on disk; 91.95 MiB resident after a
  turn at 8 processes, with the fallback pair alone answering 12/12 at
  10/12 authoritative and 0.73 s while Google was gated. On a container created fresh the declared volume path is the
  profile's tmpfs; Compose carries an existing container's anonymous volume
  across a recreate, so a service that ever ran without the mount needs
  `--renew-anon-volumes` once.
- A `probe:search` turn through the real route: the tool result in the thread
  history is the SearXNG JSON, Wikipedia's Paris article first; the run's
  events carry `tool_receipt.started.v1` and `tool_receipt.outcome.v1` and no
  `retrieval.observation.v1`; the stream ends with `end` and no `error`.
- The same turn with `docker compose stop searxng`: the tool result is the
  one unavailable sentence, the model answered after it, the stream ended
  with `end`, and the Gateway logged `web_search (SearXNG) failed:
  ConnectError` with no query text. `up -d --wait searxng` returned it to
  healthy.
- Offline: the Gateway stream suite's new SearXNG cases (results reach the
  conversation, a down service ends the turn in words, no retrieval evidence
  claim), the profile suite (six services on the 2880 MiB line, the search
  service private and read-only, the settings naming exactly the three
  engines, the render selecting SearXNG for a keyless tenant), and the
  SearXNG tool suite.
- Offline, after the review: the question travels in the POST body and not
  the URL, the query stays out of the log on the status-error path as well
  as the connection one, a malformed tool configuration raises instead of
  claiming the service did not answer, only web addresses with bounded
  fields reach the model, and four searches per process run at once.
- Live, after the review: the same two turns on the reviewed bundle. With
  the service up the tool returned Wikipedia's Paris article first and the
  Gateway logged only `POST http://searxng:8080/search`, no question; with
  it stopped, the unavailable sentence and a `ConnectError` line naming no
  query.
- Live, `image_search`: the profile's own tool against a live instance of
  the shipped bundle returned five usable image addresses for a product
  query, from Unsplash, Openverse and Wikimedia Commons. A general query in
  the same instance reached only Bing and Yahoo, so the two engine groups do
  not mix.
- Not proved here: the pinned Gateway image (the source was mounted over
  it), and gating thresholds on any address but this host's.

# HartMesh tenant VM compose profile

The `frontend` image is built from `frontend-hm/Dockerfile`, with the app still
installed at `/app/frontend`. Service names and cache mounts remain stable.
The sibling `frontend/` source is an upstream reference. Source changes take
effect on tenants only after the normal candidate publish and digest pinning;
the checked-in image pins continue to identify their existing release.

One KVM guest per customer. Inside it the whole stack runs under Docker
Compose: gateway, frontend, nginx, PostgreSQL 16, Redis 7, with sandboxes
created by the Gateway's local Docker backend as containers under gVisor
(`runsc`). This directory is the profile that guest boots from, and it is
the deployment path this distribution releases. Nothing of Kubernetes is
present in the guest.

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

Once a tenant uses provider keys set in the product, this command and every
other `up` in this README also need `HARTMESH_PROVIDER_KEYS_SECRET` in the
command's own environment, for example
`HARTMESH_PROVIDER_KEYS_SECRET="$(cat /path/off/the/data/disk)" docker compose ... up -d`;
an `up` without it leaves those providers with no key (§ "Provider keys in
the product").

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
| `HARTMESH_PUBLIC_HOST` | nginx `server_name`, and in sign-on-only mode the callback and the frontend address `gateway/render_config.py` derives from it (§ "Sign-in"). Nothing else in the application consumes it: the frontend derives its API origin from the page and its `NEXT_PUBLIC_*` values are baked at build time. |
| `HARTMESH_TRUSTED_PROXIES` | nginx `set_real_ip_from`, one per comma-separated address or CIDR. The front-door proxies whose `X-Forwarded-For` is trusted as the client address. |
| `HARTMESH_LISTEN` | nginx's published port, `<bind address>:<port>`; the only published port in the stack. |
| `HARTMESH_DATA_DIR` | Every bind mount, `DEER_FLOW_HOME`, `DEER_FLOW_HOST_BASE_DIR`, the rendered `config.yaml`, and the service-level `env_file` (`${HARTMESH_DATA_DIR}/.env`). Fixed at `/srv/hartmesh` by `config.yaml`, see below. |
| `SANDBOX_RUNTIME` | `DEER_FLOW_SANDBOX_RUNTIME`: the OCI runtime name each sandbox container runs under (`runsc` on the VM). |
| `SANDBOX_EGRESS` | `allowlist` (also when absent) or `open`; selects the `sandbox.network` block of the rendered `config.yaml`. Any other value refuses to render and the Gateway does not start. |
| `POSTGRES_PASSWORD` | `initdb` on the first start and `DATABASE_URL` on every start. Stable for the tenant's lifetime. |
| `REDIS_PASSWORD` | `redis-server --requirepass` and `DEER_FLOW_STREAM_BRIDGE_REDIS_URL`. Stable for the tenant's lifetime. |
| `AUTH_JWT_SECRET` | The Gateway's session-signing secret, so sessions survive a `home/` restored from backup. |
| `HARTMESH_SIGN_ON_ISSUER`, `HARTMESH_SIGN_ON_CLIENT_ID`, `HARTMESH_SIGN_ON_CLIENT_SECRET` | The sign-in mode, one side or the other: all three present is sign-on only, the identity provider as the one way in (§ "Sign-in"). Read by `gateway/render_config.py`; the secret reaches the Gateway as the reference `$HARTMESH_SIGN_ON_CLIENT_SECRET` and is never written to disk. |
| `HARTMESH_LOCAL_PASSWORDS` | The other side: exactly `allowed`, and none of the three above, is local passwords. Neither side, both, one or two of the three, or local passwords without `HARTMESH_LOCAL_REGISTRATION` refuses to render and the Gateway does not start. |
| `HARTMESH_LOCAL_REGISTRATION` | Local-password mode only, and required there: exactly `open` (visitors may create their own account) or `closed` (an administrator adds each person). Absent, empty or any other value refuses to render; set beside the sign-on keys it refuses too (§ "Local passwords"). |

Provider keys follow verbatim, any subset of the `*_API_KEY` names
`config.example.yaml` references; nothing guarantees any particular one is
present. Both datastore passwords are embedded in DSNs as-is, so they must be
URL-safe (the operator generates them that way).

Values pass through Compose's dotenv parser twice (the `--env-file` and the
Gateway's `env_file` are the same file): a bare `$NAME` or `${NAME}` inside a
value is interpolated, ` #` after whitespace starts a comment, and surrounding
quotes are stripped. A secret carrying a `$` would therefore be silently
rewritten before the Gateway saw it. The operator single-quotes every value it
does not fix itself (the bao secrets, every provider key, and the sign-on
client secret), which disables interpolation entirely, and refuses a value
containing a single quote or a newline, which the format cannot carry. The
fixed keys are quote-free by construction; an issuer URL carrying `#` or `$`
must be single-quoted too.

Only the Gateway receives the whole `.env` (`env_file`). The frontend and nginx
get explicit `environment:` entries and never see a provider key.

### Optional keys

| Key | Consumed by |
| --- | --- |
| `HARTMESH_USERS` | User slot count, 1–100; absent 10. (§ "Storage and agents"). |
| `HARTMESH_USER_STORAGE_GIB` | User capacity, 1–1024 GiB; absent 1. (§ "Storage and agents"). |
| `HARTMESH_AI_ASSISTANTS_PER_USER` | AI assistant slots per user, 0–10; absent 1. (§ "Storage and agents"). |
| `HARTMESH_AI_ASSISTANT_STORAGE_GIB` | Assistant capacity, 1–1024 GiB; absent 1. (§ "Storage and agents"). |
| `HARTMESH_AI_EMPLOYEES` | AI employee slots, 0–100; absent 10. (§ "Storage and agents"). |
| `HARTMESH_AI_EMPLOYEE_STORAGE_GIB` | AI employee capacity, 1–1024 GiB; absent 1. (§ "Storage and agents"). |
| `HARTMESH_COMPANY_STORAGE_GIB` | Company capacity, 1–1024 GiB; absent 1. (§ "Storage and agents"). |
| `HARTMESH_TELEGRAM` | Reserved: `on` or `off`; absent means off until the unfinished feature is offered. (§ "Telegram bots"). |
| `HARTMESH_TELEGRAM_VOICE` | Reserved voice provider: `auto`, `openai`, `gemini` or `off`; absent `auto`. (§ "Telegram bots"). |
| `HARTMESH_TELEGRAM_MAX_BOTS` | Reserved deployment bot-link limit, 1–500; absent 100. (§ "Telegram bots"). |
| `HARTMESH_TELEGRAM_MAX_BOTS_PER_PERSON` | Reserved per-person bot-link limit, 1–40; absent 10. (§ "Telegram bots"). |
| `HARTMESH_APP_SUBNET` | The `app` bridge's IPAM subnet **and** the Gateway's `AUTH_TRUSTED_PROXIES`, which are the same reference. Absent -- which is what every existing tenant `.env` is -- both take the shipped default, `10.201.26.0/24`. Set it only when that range collides with something the guest must still reach (§ "Network model"). |
| `HARTMESH_PRODUCT_NAME` | What the product is called: the heading on the sign-in and setup pages, the browser tab, the workspace when the tenant bundle names no company, the assistant's own name, and DingTalk's message title. Rendered into `ui.product_name` (the template must not carry it); one line of at most 40 characters, surrounding spaces trimmed. Absent -- which is what every existing tenant `.env` is -- `HartMesh`. A name the Gateway would refuse (a control or reordering character, over 40 characters), or one beginning with `$` (the Gateway would read it as an environment variable), refuses to render. It is read at Gateway start: a new name takes a restart. Quote a name containing `#` (`HARTMESH_PRODUCT_NAME="Acme #1"`), or `.env` reads the rest as a comment. A company name from the tenant bundle still wins inside the workspace after sign-in (§ "Tenant bundle"). |
| `HARTMESH_MODELS_FILE` | The path of the operator's own model file, read by `gateway/render_config.py` at every Gateway start. Absent -- which is what every existing tenant `.env` is -- the rendered `models:` section comes from the bundled provider catalog exactly as before. Set, that one file is the whole model list (§ "Operator-managed models"). |
| `HARTMESH_PROVIDER_KEYS_SECRET` | The wrapping key for provider keys an administrator sets in the product, at least 32 characters (`openssl rand -base64 32`). Absent -- which is what every existing tenant `.env` is -- nothing changes and the product refuses to store a key, saying why. Best supplied from the environment of the `docker compose` command rather than this file, which shares a disk with the database it protects (§ "Provider keys in the product"). |
| `HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS` | Only while rotating that key: the old value, so keys wrapped under it are read and rewrapped under the new one at the next start (§ "Provider keys in the product"). |
| `SANDBOX_READY_TIMEOUT` | The cold-start readiness budget, `sandbox.ready_timeout` in the rendered `config.yaml`: whole seconds from 60 to 600. Absent -- which is what every existing tenant `.env` is -- the template's 120 applies. Anything else (zero, a negative or fractional number, text, a value outside the range) refuses to render and the Gateway does not start, so no value can turn the deadline off (§ "Sandbox readiness budget"). |
| `SANDBOX_CAPACITY_WAIT_TIMEOUT` | How long an acquisition waits for one of the two sandbox slots when both are in active use and nothing is parked to evict, `sandbox.capacity_wait_timeout` in the rendered `config.yaml`: whole seconds from 0 to 60, where 0 refuses at once. Absent, the template's 5 applies. Anything else refuses to render, so no value makes the wait unbounded (§ "Memory budget"). |
| `HARTMESH_SIGN_ON_ADMINS` | Sign-on only. Comma-separated email addresses that become the provider's `admin_emails`: an address on it is created as `admin` at its first sign-in, every other address as `user`. Absent, the deployment has **no administrator** (§ "Sign-in"). |
| `HARTMESH_SIGN_ON_SCOPES` | Sign-on only. Extra scopes to request, separated by spaces or commas, appended to the default `openid email profile`; some providers emit a claim only when its scope is asked for. Absent, the default three. |
| `HARTMESH_SIGN_ON_CLIENT_AUTH` | Sign-on only. How the Gateway authenticates at the token endpoint: `client_secret_post` (absent) or `client_secret_basic`. The deployer registers the client to match. |
| `HARTMESH_SIGN_ON_NAME` | Sign-on only. The label on the sign-in button ("Continue with …"), at most 64 printable characters. Absent, `Single sign-on`. |
| `HARTMESH_SIGN_ON_ACCESS_CLAIM` | Sign-on only, with `_ACCESS_VALUES`. The literal name of one claim the token must carry for a sign-in to be admitted (§ "Membership follows the claim"). Absent, admission is as before. |
| `HARTMESH_SIGN_ON_ACCESS_VALUES` | Sign-on only, with `_ACCESS_CLAIM`. Comma-separated values of that claim that admit (commas only: a value may contain a space, and is trimmed). One without the other refuses to render. |
| `HARTMESH_SIGN_ON_ROLES` | Sign-on only, with the two above. `<value>=admin,<value>=user`, comma-separated, one entry per admitting value: the role then follows the claim at every sign-in, and `HARTMESH_SIGN_ON_ADMINS` must be absent. |
| `HARTMESH_SIGN_ON_CLOCK_SKEW` | Sign-on only. How far this VM's clock may disagree with the provider's on an ID token's timestamps, whole seconds 0 to 300. Absent, 60. A guest between NTP polls is routinely a second or two out, and with no tolerance that refuses every sign-in by everyone until it catches up; outside the tolerance the refusal logs the measured skew, so a clock problem reads differently from a bad token. |
| `AUTH_TOKEN_EXPIRY_DAYS` | Both modes. The session lifetime in whole days, 1 to 30 (the product's bound). Absent -- which is what every existing tenant `.env` is -- 7, exactly as before. Anything else refuses to render and the Gateway does not start. |
| `HARTMESH_SANDBOX_RESOLV_CONF` | The Docker host's upstream DNS file, default `/run/systemd/resolve/resolv.conf` on the Debian tenant VM. The Gateway receives a read-only view; open-mode runsc sandboxes bind the validated file at `/etc/resolv.conf`. On hosts without systemd-resolved, select an existing resolver file containing reachable upstream IP addresses. A loopback stub file is refused (§ "DNS under gVisor"). |

These are absent from `.env.example`: the fixed keys are what onboarding
writes for every tenant. Optional keys adapt the profile when its defaults
do not suit the host or tenant. The resolver source must exist even when
allowlist mode is selected; Compose refuses a missing file instead of
creating an empty directory in its place.

The seven storage keys reach `gateway/render_config.py` through the Gateway's
`env_file`. Compose also interpolates each once, with an empty default, into
`storage-init`, the only other service that reads these tenant keys.

They reach the stack by different routes, on purpose. Both `HARTMESH_APP_SUBNET`
uses are the same `${HARTMESH_APP_SUBNET:-...}` reference, so an override
cannot move the network without moving the Gateway's trust with it.
`HARTMESH_MODELS_FILE`, `HARTMESH_PRODUCT_NAME`, `SANDBOX_READY_TIMEOUT`,
`SANDBOX_CAPACITY_WAIT_TIMEOUT`, the sign-in keys and the
`HARTMESH_SIGN_ON_*` options are not interpolated by `compose.yaml` at all:
they reach the Gateway through `env_file` and are read inside the container by
`gateway/render_config.py`, so leaving one unset is simply an unset variable
rather than a hole in the rendered Compose document.
The resolver source and its Gateway environment value share the same
interpolation, so validation reads the file Docker will bind into the sandbox.
`HARTMESH_PROVIDER_KEYS_SECRET` and its `_PREVIOUS` are interpolated by
`compose.yaml` with an empty default and reach only the Gateway, so they may
come from the environment of the `docker compose` command itself: the shell's
value wins over the `--env-file` one, and absent from both they are simply
unset.

## Sign-in

Who may enter a tenant is the `.env`'s to say, and it says it one of two ways.
Nothing is assumed: a `.env` that says neither, says both, sets one or two
of the three sign-on keys, or selects local passwords without saying whether
visitors may sign up refuses to render, `gateway/run.sh` exits, and the
Gateway never becomes ready, with the refusal naming every key involved in
its journal. This deliberately breaks the convention of the optional keys
above, where an absent key means "exactly as before". The reason is the
direction of failure on a tenant published on the internet: a forgotten key
that leaves local registration open is noticed by nobody, while a forgotten
key that leaves a tenant nobody can enter is noticed at once.

### Sign-on only (the three sign-on keys)

```
HARTMESH_SIGN_ON_ISSUER=https://login.example.com/realms/tenant
HARTMESH_SIGN_ON_CLIENT_ID=hartmesh
HARTMESH_SIGN_ON_CLIENT_SECRET='…'
```

All three present, the rendered `config.yaml` carries one OIDC provider,
named `sso`, enabled, with local passwords switched off
(`auth.local.enabled: false`, `allow_registration: false`). The identity
provider is the one way in:

- **The callback** the deployer registers at the provider is
  `https://<HARTMESH_PUBLIC_HOST>/api/v1/auth/callback/sso`, computable from
  the public host alone before the VM exists; the frontend address is
  `https://<HARTMESH_PUBLIC_HOST>`. The host is a bare hostname (a value with
  a port or a scheme refuses to render), and the issuer must be an
  `https://` URL: the ID token's `iss` is checked against it exactly.
- **Client authentication** at the token endpoint is `client_secret_post`
  unless `HARTMESH_SIGN_ON_CLIENT_AUTH=client_secret_basic`; register the
  client to match. The authorization-code flow uses PKCE (S256) and a nonce,
  and the ID token is checked for signature, issuer, audience and expiry.
- **Scopes** requested are `openid email profile`, plus whatever
  `HARTMESH_SIGN_ON_SCOPES` adds.
- **Accounts** are created at a person's first sign-in and a verified email
  is required (the product's defaults). An address on
  `HARTMESH_SIGN_ON_ADMINS` is created as `admin`, any other as `user`.
  First-admin initialization is closed in this mode, so that list is the only
  way a sign-on-only deployment gets an administrator: **a deployment without
  it has no administrator.** What each role may do is unchanged: an administrator holds
  the developer screens and the administrative routes (lockouts, skill
  install, MCP and integration configuration, sharing removal); a `user`
  runs ordinary chats with every tool and the sandbox.
- **The session** lives `AUTH_TOKEN_EXPIRY_DAYS` days (7 unless set).
- **The button** reads "Continue with `HARTMESH_SIGN_ON_NAME`" (default
  `Single sign-on`), and it is the only thing the login page offers: no
  local form, no create-admin page, no register link.

What this mode closes, whatever the admin count and on whatever database the
Gateway starts on -- an empty one, one with accounts but no admin, one that
already holds local accounts, which is what a restore produces:

- `POST /api/v1/auth/initialize`, `/login/local`, `/register` and
  `/change-password` answer `403` (`change-password` `401`) with the code
  `sign_on_required` and create nothing;
- an account without a provider identity is **inert**: no session and no
  personal access token of such an account is honoured, including ones
  minted before the switch (`401`);
- `GET /api/v1/auth/setup-status` is the constant
  `{"needs_setup": false, "registration_enabled": false, "sign_on_only": true}`;
- the `reset_admin` command refuses and says why, before it opens the
  database;
- `DEER_FLOW_AUTH_DISABLED=1` refuses the start, whatever `DEER_FLOW_ENV`
  says;
- the Gateway's internal-caller headers (`X-DeerFlow-Internal-Token`,
  `X-DeerFlow-Owner-User-Id`) are blanked by nginx in every location it
  proxies, in both modes: the published port is nginx, so whatever a client
  sends in them dies there, whether or not `DEER_FLOW_INTERNAL_AUTH_TOKEN`
  is set. Internal services keep working; their one sender is the channel
  manager inside the Gateway process, which never crosses nginx.

**The readiness signal.** `GET /health` on the Gateway carries `"auth_mode":
"sign_on_only"` or `"local"`, and `"registration": "open"` or `"closed"`
(always `closed` in this mode), readable without credentials from inside the
deployment (`gateway:8001` on the `app` network) and also through nginx,
which proxies `/health`; what they name is what the login page shows
anyone anyway. The Gateway's journal has one line at start, `auth mode:
sign_on_only (local passwords off; provider sso (…); registration closed)`,
or in local mode `auth mode: local (local passwords on; registration open)`
or `… registration closed)`. An apply asserts either before it publishes the
tenant.

**Local accounts from before the switch.** A database that already holds
local-password accounts keeps them, inert. Their owners see the provider's
sign-in and nothing else; a session they still hold answers `401
sign_on_required` and the page sends them to sign in. Their address is
still held by the inert row, so signing in through the provider with that
address is refused with `sso_account_exists` until the row is gone -- an
identity-provider account is never linked onto a local one. Clearing them is
the deployer's job, with the Gateway stopped:
`DELETE FROM users WHERE password_hash IS NOT NULL AND oauth_provider IS NULL;`
(their conversations stay in the database under the old account id and are
not reachable by the new one). A tenant deployed in this mode starts with no
local accounts and needs none.

**An account never crosses issuers.** Each provider-created account records
the issuer that created it (`users.oauth_issuer`, migration
`0027_account_access`), and a sign-in whose subject matches but whose
issuer does not is refused with `sso_not_allowed` -- so pointing
`HARTMESH_SIGN_ON_ISSUER` at another provider cannot hand an account to
whoever holds the same subject there. Accounts created before this column
existed carry no issuer; the first start on this release pins every such
row under `sso` to the issuer configured at that moment (the last moment it
is known for certain), and one a bare deployment misses adopts the
configured issuer at its next sign-in.
When the issuer's address legitimately changes (the same provider at a new
URL, the same subjects), re-point the recorded issuer with the Gateway
stopped, then change the key and start:
`UPDATE users SET oauth_issuer = '<new issuer>' WHERE oauth_provider = 'sso' AND oauth_issuer = '<old issuer>';`

**The address follows the sign-in.** A person's subject at the provider is
who they are; their address is something the provider says about them. At
every sign-in of an account that already exists, the account takes the
address the token carries, so a company that corrects someone's address at
the provider does not have to correct it here too. Five things leave the
stored address alone, and each of them still signs the person in: a token
carrying no address; an unverified one where the deployment requires
verification; one from a domain `allowed_email_domains` does not allow (the
same rule, and the same list, that governs account creation -- an address a
new person may not have is not one an existing person may acquire); one no
account record can hold (the paragraph below); and one another account
already holds.

That last is the deployer's to resolve, and what to do depends on the
holder, which the journal names -- subject, provider and issuer, or "a local
password account". `release-email` applies only to a **turned-off provider
account**: it refuses a local-password account, and it refuses one that is
still on, because while a person can sign in their address is theirs. A
leftover local-password account is cleared the way this guide's sign-on-only
section already describes; an address held by someone who still works there
is a duplicate to fix at the provider, not here.

Where no role mapping is configured, the administrators' list is read from
the address the sign-in settles on, so a changed address can promote or
demote in the same sign-in. That is why the domain rule above is not
optional: without it, acquiring an address on the administrators' list would
be a promotion the domain list exists to prevent.

**An address another account holds.** A sign-in whose subject has
no account yet, carrying an address that one does, is refused -- an identity
is never given an existing account. What the person is told depends on which
account holds it. A local-password account of their own is theirs to sign in
with: `sso_account_exists`, as before. A **provider account of another
subject** is not, and telling them to use a password would be false, so that
answers `sso_email_taken` with "That email address belongs to another
person's account here. Ask your administrator to release it." The two are
deliberately unlike each other read aloud, because their remedies are
opposite: one is a stale account to clear, the other is a live account of
someone else's, and clearing that would delete a person. The journal names
the holder's subject, provider and issuer -- the provider too, since two
configured providers may point at one issuer. The deployer's remedy is
`release-email` on the account holding it, once it is turned off.

**An address no account can hold.** The address the provider asserts becomes
the account's address, and an account record holds only an address that
parses as one: a special-use domain (`.invalid`, `.test`, `.local`,
`.localhost`, `.arpa`) is refused, so is a domain with no dot at all --
`pat@companyad`, which an on-prem directory readily emits -- and so is any
malformed address. No mail is sent and no name is looked up, so an address
at a plausible domain is held whether or not anyone reads it. Such a sign-in
is refused with `sso_email_unusable` -- its own code, because nothing exists
for it to collide with: no account is created, and the person sees "Your
organization's sign-in did not provide a usable email address. Ask your
administrator to correct it." The journal carries the issuer and the
subject, and the line beside it names the address, which is what you
correct: it is a provider-side fix (correct the address on the identity, or
have the provider assert a real one), never a row to clear from the
database.

**A sign-in mode must be chosen.** A tenant whose `.env` carries neither
side **stops at start**: put `HARTMESH_LOCAL_PASSWORDS=allowed` (with
`HARTMESH_LOCAL_REGISTRATION`; § "Local passwords") or the three sign-on
keys into `.env` first. To check the render before a restart, run the
bundle's renderer against the edited keys passed explicitly -- `docker compose exec` sees the running container's
environment from its creation, not the edited file, so without `-e` it
reports exactly the refusal the check is meant to rule out (the same caveat
as § "Operator-managed models"):

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env \
  exec --user 1000 -e HARTMESH_LOCAL_PASSWORDS=allowed -e HARTMESH_LOCAL_REGISTRATION=closed gateway \
  sh -c 'cd /app/backend && PYTHONPATH=. uv run --no-sync python /opt/hartmesh/gateway/render_config.py --template /opt/hartmesh/config.yaml --catalog /opt/hartmesh/providers --check'
```

(or `-e HARTMESH_SIGN_ON_ISSUER=… -e HARTMESH_SIGN_ON_CLIENT_ID=… -e
HARTMESH_SIGN_ON_CLIENT_SECRET=…` for sign-on) reports
`sign-in=sign_on_only (provider sso, callback …)` or `sign-in=local
(registration closed)`, or the refusal. A tenant started without the key shows the refusal in the
Gateway's journal (`render_config: refusing to render: no sign-in mode is
selected …`), the container exits and restarts until the key is added and
`up -d` is run again. A sign-on option (`HARTMESH_SIGN_ON_ADMINS`, `_SCOPES`,
`_CLIENT_AUTH`, `_NAME`, `_ACCESS_CLAIM`, `_ACCESS_VALUES`, `_ROLES`) set
beside `HARTMESH_LOCAL_PASSWORDS=allowed` also refuses rather than being
ignored. `.env.example` shows sign-on-only mode.

### Membership follows the claim (optional)

A consumer that shares one identity provider across many tenants keeps the
membership there: for each deployment the provider knows who belongs and
whether each person administers it, and says so as one claim at sign-in.
Three optional keys make the tenant read that claim; without them
sign-on-only mode behaves exactly as above, which is right for a consumer
with one company per issuer.

```
HARTMESH_SIGN_ON_ACCESS_CLAIM=urn:zitadel:iam:org:project:roles
HARTMESH_SIGN_ON_ACCESS_VALUES=admin,member
HARTMESH_SIGN_ON_ROLES=admin=admin,member=user
```

- **Admission.** With the claim and its values set, a sign-in is admitted
  only when the token carries one of the values under that claim -- checked
  at **every** sign-in, before an account is created and before an existing
  one is returned. The name is taken literally (colons and dots are
  characters of the name, not a path). The claim is read from the ID token,
  and from userinfo only when the ID token does not carry the name at all.
  It may be a list of strings, a single string, or an object whose keys are
  the values (`{"admin": {…}, "member": {…}}`). A claim that is missing,
  empty or of another type refuses; nothing falls back to an email-domain
  filter. The person sees "You have no access to this workspace. Ask your
  administrator." (`sso_no_access`), which does not name the claim; the
  Gateway's journal carries the issuer and subject, never a token.
- **Role.** With `HARTMESH_SIGN_ON_ROLES` set, each admitting value carries
  `admin` or `user`, and the role is re-read and written to the account at
  every sign-in, in both directions; `admin` wins when a token carries
  several. Every request reads the account's role afresh, so once the
  demotion is written no open session of that account is an administrator
  any more, in any browser. The mapping covers every admitting value exactly
  (a value without a role, or a role for a value that does not admit,
  refuses to render), it needs the admission claim, and it replaces
  `HARTMESH_SIGN_ON_ADMINS`: setting both refuses to render. Without the
  mapping, roles come from the administrators' list at account creation, as
  before.
- **A half-set pair** -- the claim without values, values without the claim,
  or the mapping without the claim -- refuses to render and names the keys.
  Values and role entries are separated by commas only (a value may contain
  a space); positions in a refusal count from 0.
- **Readiness.** The renderer's summary line carries `admission by claim` and
  `roles from claim` when the keys are set (`sign-in=sign_on_only (provider
  sso, callback …, admission by claim, roles from claim)`).

**The deployer can turn one account off, end its sessions, limit its role,
or release its address.** An operator
command, run inside the deployment like `reset_admin` and never a network
route: nothing reachable over HTTP turns an account off or on or ends
another account's sessions, with any credential. Accounts are addressed by
the provider's **issuer and subject**, which exist before an account does;
`--email` is a convenience that must resolve to exactly one provider
account. Every form prints one JSON document on stdout and exits non-zero
on failure (the document then carries `error`), and every form is
idempotent.

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env \
  exec --user 1000 gateway \
  sh -c 'cd /app/backend && PYTHONPATH=. uv run --no-sync python -m app.gateway.auth.accounts list'
# … disable       --issuer https://login.example.com --subject 3141592
# … enable        --issuer https://login.example.com --subject 3141592
# … end-sessions  --issuer https://login.example.com --subject 3141592 [--end-running-work]
# … release-email --issuer https://login.example.com --subject 3141592
# … limit-role    --issuer https://login.example.com --subject 3141592 [--end-running-work]
# … lift-role-limit --issuer https://login.example.com --subject 3141592
# … disable       --email pat@example.com
# … disable       --issuer https://login.example.com --subjects 3141592 2718281 1414213
```

- `disable` records the refusal (`disabled_identities`, keyed by issuer and
  subject, migration `0027_account_access`), and ends the sessions and
  revokes the personal access tokens of **every account the identity
  covers**, so `enable` revives none of them. From that row every path that acts for
  the account derives its refusal at the next request: the session cookie
  and personal access tokens (`401`), the browser WebSocket, the LangGraph
  auth hook, an internal caller acting for that owner (an IM channel bound
  to it), and every process-internal launch for the owner -- a due
  scheduled task, a channel message, an MCP task notification -- which the
  scheduler records as a failed occurrence naming the refusal, so no run
  starts. A sign-in is refused even when the claim admits, with "Your
  access to this workspace has been turned off. Ask your administrator."
  (`sso_access_off`) and a journal line naming issuer and subject. **A
  connection that was already open is not closed by this command**: a run's
  stream ends because the run is cancelled (next), a download already in
  progress completes, and a browser WebSocket stays open until its client
  closes it or the Gateway restarts. It also
  **ends the running work of every account the refusal covers** (a subject
  with an account under each configured provider is refused on both, and both
  have their runs ended): each run is cancelled the way the person's own
  cancel would be, the Gateway that owns it applies that, and the command
  waits for each run to reach a terminal status before it answers. What is
  *guaranteed* is that the run is cancelled, its stream ends, and no new run
  starts. Cancelling a run also **attempts** to kill the sandbox command it
  has in flight, along with the children and detached processes that command
  started; that reach depends on the sandbox provider, is best-effort on the
  remote one, and a provider that cannot reach its commands leaves them to
  their own timeout. After the wait the command looks once more for a run
  admitted meanwhile. The output says what was done (`sessions_ended`,
  `tokens_revoked` -- across every covered account -- `schedules_held` --
  the addressed account's active schedules when the command ran, which it
  now holds (below), so 0 on a re-run while `held.schedules` still names
  them; `surfaces.internal_launches.count` counts those of every covered
  account -- plus
  `runs_found`, `runs_cancelled`, `runs_finished_first` and
  `runs_unconfirmed`), and when each surface stopped (below).
- Exit statuses: **0** done; **1** the command refused and changed nothing
  (with `--subjects`, some identity was refused or failed: see *Many people
  in one call*; the document carries `error`; an `error` from an unexpected failure, such
  as a database fault, may leave the command part-done, and re-running it is
  safe); **2** it did what was asked but a run named
  in `runs_unconfirmed` had not reached a terminal status when the wait ran
  out, or a surface named in `surfaces_unconfirmed` was not confirmed
  (something it tried to hold or end is still there, counted under
  `not_ended`). For `enable --restore-held`, 2 means something it held
  could not be turned back on (named under `stayed_off` as
  `restore_failed`, its surface under `surfaces_unconfirmed`). **2 is not
  "nothing happened"** -- the refusal is recorded, the
  sessions are ended and the tokens are revoked either way, and nothing new
  starts. A run can be unconfirmed because it is still unwinding or because
  the Gateway is not answering, and the command cannot tell those apart from
  the database, so it does not guess: re-run it to see whether the run has
  since stopped, and only investigate the Gateway if it stays unconfirmed.
  `--wait-seconds` moves the bound (default 120). Ids under
  `runs_finished_first` are runs that completed on their own before the
  cancellation reached them -- they stopped, but their results were
  delivered.
- `disable` for a subject that **has no account yet** records the refusal
  anyway and says so (`"account": null`); a person removed before their
  first sign-in cannot create an account later. The row survives a restart
  and comes back with a restored database.
- `enable` withdraws the refusal. Sessions ended and tokens revoked by
  `disable` stay ended and revoked; the person signs in again.
- **Rejoining revives nothing.** `disable` **holds** every covered account's
  active schedules (paused) and connected channel bindings (a held binding
  starts nothing: while the person is off, a message through it gets the
  same "access turned off" answer, and the web app shows it as not
  connected), and names them under `held` (`schedules`, `channel_bindings`:
  ids). It forgets the connect codes the person had not used yet
  (`channel_bindings.connect_codes_ended`), so none can bind a chat account
  or turn a held binding back on. It records what it held for the identity
  -- migration `0027_account_access` -- before it acts on it, so an
  interrupted command leaves a record a re-run completes; `list` shows every
  record still held, under `holds`.
  It also **ends the work waiting to run** for the person, so none of it
  runs once they are back: every queued scheduled occurrence, manual
  triggers included. A launch that reads the refusal ends the same way instead of
  being retried, and one that got in before it is a run `disable` cancels;
  either way a schedule's pause survives the launch, the cancellation of its
  run and the scheduler's own bookkeeping. After a plain `enable` the held schedules and
  bindings **stay off until their owner turns them on** -- resuming the
  schedule, or connecting the channel again -- and the record is discarded
  (the document lists it under `held`), so no later restore can revive it.
  **`enable --restore-held`** is for lifting a suspension of everyone: it
  turns back on exactly what the matching `disable` held and nothing the
  person had paused themselves, each schedule at its next occurrence from
  now (never the ones it missed), and reports `restored` and, under
  `stayed_off`, what it left: a `once` schedule whose time passed during
  the hold stays paused (`time_passed`), one the person changed or removed
  since is left as it is (`changed_since`, `gone`), and one that could not
  be turned back on (`restore_failed`) stays in the record, its surface
  named under `surfaces_unconfirmed` with exit **2**, for a re-run to try
  again. A restore that finds no record says so in its `note`: the
  `disable` held nothing, or a plain `enable` discarded it. Sessions and tokens never come back,
  with or without the flag. A plain `enable` followed by another `disable`
  and a restore turns on only what that second `disable` held.
- `end-sessions` signs one account out everywhere without turning it off and
  without touching its tokens: every open session is refused at its next
  request and the next sign-in re-reads the claim. A run already executing
  keeps going, because demoting someone is not removing them and their work
  is still theirs; `--end-running-work` cancels it too, reporting the same
  run counts `disable` does. Until the person signs in again, their stored
  role is still the one the last sign-in read; `limit-role` is the form that
  changes it at once and keeps it changed.
- `limit-role` holds a person below administrator (`--role user`, the
  default and today the only limit) until the deployer lifts it, **whatever
  the provider's claim says** -- including a provider restored from a backup
  taken before the demotion, whose claim says `admin` again. The limit is
  one row in `role_limits` (migration `0027_account_access`), keyed by issuer
  and subject, so it holds for a subject with no account yet and for every
  account the identity has here. Every read of an account takes its role
  from it, so at once: the stored role reads `user`; a session's next
  request, and what it may see of other people's runs, is a `user`'s; a run
  a personal access token starts carries `user`; a scheduled or channel
  launch runs as `user`; and a sign-in whose claim says `admin` stores
  `user`, applied in the same write that stores the role. The limit, its
  lift and every sign-in of that identity take turns (on PostgreSQL, one
  transaction lock per identity), so a sign-in that read `admin` just before
  the limit cannot store it after. It ends the sessions of every covered
  account the way `end-sessions` does, in the transaction that records the
  limit, so a command interrupted after that commit leaves none open, and
  leaves tokens working at the limited role. A run already executing keeps
  the role it started with, and is left alone unless `--end-running-work` is
  given, which cancels only the runs admitted with a role above the limit (or
  with none recorded): work the person starts afterwards runs as `user` and
  is theirs to finish. It looks once more when the wait is over, for a run
  that a request authenticated just before the limit committed inserted
  meanwhile; a later pass finds anything slower. That
  run's role matters only where something reads it -- `authorization.enabled`,
  or `guardrails` with a provider -- and this profile sets neither, so here
  it can do nothing a `user` could not; the document's `running_work` entry
  names what reads it (`role_read_by`, empty here). **Re-applying it on every
  pass is safe, with or without `--end-running-work`**: with nothing to lower
  it changes nothing (`"changed": false`) and signs nobody out, and the flag
  finds nothing above the limit to cancel. `lift-role-limit` withdraws the
  limit and changes nothing else: the stored role stays where the limit held
  it until a sign-in reads the role again -- the next sign-in where roles
  follow the claim, and where they come from `admin_emails` instead, only a
  sign-in that changes the account's address. Personal access tokens never
  reach an administrator route, whether or not a limit holds. With local
  passwords on it refuses to limit the **last administrator**: a deployment
  with none offers first-boot setup to whoever reaches it first. Restoring a
  database backup taken before a limit was set drops that limit with it;
  re-apply your record afterwards, as after any restore.
- The `disable` and role-limit documents say **when each surface
  stopped**: `started_at` (UTC), `elapsed_ms` for the whole command, and
  under `surfaces` one entry per surface with its `action`, its `count`,
  `stopped_after_ms` on a monotonic clock from the command's start and
  `stopped_at`, that offset added to `started_at`; the role-limit documents
  add `changed` (whether this run changed anything). For `disable` the
  surfaces are `sign_in` and `internal_launches` (`refused_at_next_use`:
  covered accounts, active schedules), `sessions` (`ended`: covered
  accounts), `personal_access_tokens` (`revoked`), `running_work` (`ended`;
  `confirmed_by: run_status`, reported when the run rows went terminal),
  what it holds and the work it ends that was waiting to run (the bullet
  above): `schedules` and `channel_bindings` (`held`, counting the
  identity's whole hold record) and `scheduled_occurrences` (`ended`,
  counting what this command ended), each reported when the command's writes
  were done and looked at again once the runs are over, and
  `channel_ingress` (the person's live channel bindings when the refusal
  committed; `refused_at_next_use`: a channel message from them is answered
  with the words the web app gives the same refusal, before any thread is
  created). Each held or ended surface carries `not_ended`: for a held
  surface, what could not be held; for the waiting work, what a Gateway
  still had in hand at the last look (a launch in flight), which its own
  path ends once it reads the refusal. Any `not_ended`, or a queue that
  could not be read, leaves the surface unconfirmed, and a re-run looks
  again.
  **What the document does not name was not examined.** This command does
  not look at what a Gateway process keeps for a person between requests (a
  parked sandbox, a pooled MCP server session, a headless browser, a queued
  memory update, an account export being prepared), at a background MCP
  task, at a subagent batch, or at a channel message waiting to be
  processed, and none of them appears under `surfaces`. A parked sandbox is
  stopped by its idle timeout (`sandbox.idle_timeout`); a background MCP
  task the person started keeps running at its remote server until it
  finishes or is cancelled there.
  For an identity with no account every surface is named with a count of 0. A surface refused or limited at its next use, and
  a role limit's `sessions`, which end in the limit's own transaction,
  report the commit. A surface the
  command could not confirm (`running_work`, when a cancelled run did not
  stop within `--wait-seconds`, or a held or ended surface as above) is named
  under `surfaces_unconfirmed` and the exit status is **2**. For `limit-role` the surfaces and their counts
  are: `stored_role` (stored roles lowered; 0 when every covered account
  already sat at or below the limit), `sign_in` (covered accounts),
  `sessions` (covered accounts whose sessions were ended, or would have
  been), `personal_access_tokens` (live tokens), `internal_launches`
  (active schedules) and `running_work` (runs admitted above the limit,
  `role_above` naming the limit; `confirmed_by: run_status` when ended, since
  the stop time is read from the run rows reaching a terminal status). The
  actions so far are `lowered`, `ended`, `limited_at_next_use`,
  `already_limited`, `left_alone`, `refused_at_next_use`, `revoked`,
  `held`, `not_reached` and, for
  `lift-role-limit`'s one surface `role_limit`, `lifted`; a later form may add surfaces and actions, so a
  script should treat an unknown one as information, not failure.
- **Many people in one call.** `disable`, `enable`, `limit-role` and
  `lift-role-limit` take `--subjects` in place of `--subject`: several
  subjects at the one `--issuer`, each its own word (`--subjects A B C`, or
  `--subjects=A --subjects=B` -- the `=` form passes a subject that begins
  with a dash). Every other flag applies to all of them. Each step is taken
  for every identity before the next: every refusal or limit commits, and
  every run is asked to stop, before the command waits, and then it waits
  **once** for them all, so twenty people cost one wait, not twenty. The
  document carries `identities`, one entry per identity in the order given
  (a subject named twice is one), each the document the one-subject form
  prints. An identity the command refused or could not finish has `verdict`
  `refused` or `failed`, an `error` and `returncode` 1, and **does not stop
  the others**. `refused` -- a subject that names two accounts, the last
  administrator in local mode -- comes back the same on every call until
  that changes (address the account by `--email`, or fix it at the
  provider). `failed` -- a database fault -- may be part-done; re-run it. A
  fault in a step every identity shares, such as the wait, fails each
  identity it left unfinished, every one still with its entry. Those
  entries carry no `surfaces` or run counts, so read `verdict` first.
  `totals` counts the `identities` and their `verdicts`, names the
  `subjects_refused`, `subjects_failed` and `subjects_unconfirmed`, and sums
  the runs (`runs_found`, `runs_cancelled`, and the ids under
  `runs_finished_first` and `runs_unconfirmed`), with every surface some
  identity left unconfirmed (`surfaces_unconfirmed`) or not reached
  (`surfaces_not_reached`). One person's run that does not stop leaves that
  person unconfirmed, not the others. The exit status is the worst: **1** if
  some identity was refused or failed -- which here means *some*, not that
  nothing changed; the others were done -- else **2** if any was
  unconfirmed, else **0**. A re-run is safe, as always, and re-checks
  everyone. A document without `identities` means nothing was done: the
  call was refused as a whole (more than 100 subjects, a malformed command
  line, a lookup that failed before anything changed), or the release has no
  `--subjects` (see the upgrade note). **Limits:** at most 100 subjects a
  call (more is refused before anything changes; split them). Each subject
  is its own argv word, so the command line is the subjects' own length plus
  one byte each -- a hundred 255-byte subjects are 26 KB, inside Linux's
  128 KB per word and 2 MB in all; a runner's own limit may be lower. The
  `sh -c '…'` form above splices the words into a shell script, where a
  subject with a quote, a space or a `$` breaks or is interpreted; to keep
  each one a word, pass them after the script:
  `sh -c 'cd /app/backend && PYTHONPATH=. exec uv run --no-sync python -m app.gateway.auth.accounts "$@"' accounts disable --issuer … --subjects …`.
  The document is one line of about 5 KB an identity for `disable` and 1-2
  KB for the others (a hundred people's `disable` printed 500 KB), so size
  a batch to the runner's output limit too. The waits end `--wait-seconds`
  after the command starts, and the work done for each identity in turn
  comes on top: on SQLite on a development machine, under 100 ms an identity
  for `disable` and under 60 ms for the other three (a hundred people's
  `disable`, each with a token, a schedule and a chat binding, took 5 to 10
  s plus the wait); PostgreSQL was not measured. Size the runner's timeout
  at `--wait-seconds`, plus 0.25 s an identity, plus what a one-subject call
  with `--wait-seconds 0` takes through the runner; and give a large batch a
  longer wait, since the work before the wait comes out of it.
- A malformed command line -- an unknown flag, a missing value -- is refused
  like any other refusal: one document with `error`, exit **1**.
- **What `disable` reaches, and what it does not.** It reaches what the
  document names under `surfaces`, and nothing else: sign-in, sessions,
  personal access tokens, launches the process makes for the person, their
  running runs, their schedules and channel bindings, and their queued
  scheduled occurrences. It does not close a connection that is already
  open, and it does not look at a parked sandbox, a pooled MCP session, a
  headless browser, a queued memory update, a background MCP task, a
  subagent batch or a channel message waiting to be processed. A surface the
  document does not name is not covered, not confirmed.
- `release-email` gives up the address of an account that is **turned off**,
  so a person may hold it again. `users.email` is unique, so one address
  belongs to one account for good -- right while the account is someone's,
  wrong once it is nobody's. Two ordinary acts at the provider send a *new*
  subject carrying an address an old account still holds, and until this
  existed every sign-in of theirs was refused with no deployer command that
  could change it: **deleting a person and inviting them again** (the provider's own
  remedy for an invitation that reached the wrong person), and **giving a
  departed person's address to someone new**. The command refuses an account
  that is still on -- while a person can sign in, their address is theirs --
  so `disable` first. The account keeps its subject, its issuer, its role,
  its turned-off state and everything it holds; only the address changes, to
  one at `released.example`, a domain reserved by RFC 2606 that can never be
  registered and to which nothing can ever be delivered. What it held is
  recorded (`users.email_released_from`, migration
  `0027_account_access`), so `list` still says which address it was.
  **The returning person gets a new, empty account.** They sign in under a
  new subject at the provider, so the product gives them a new account;
  every thread, file and share stays on the old one, which is still there
  and still turned off. This frees an address, it does not hand work over.
  **There is no undo**: once a sign-in takes the address, it is that
  account's. An account that later holds a real address again -- turned back
  on, signing in with an address nobody holds -- is no longer released, and
  can be released again if the deployer turns it off again.
- Every form is addressed by issuer and subject, or by `--email`. One
  subject at one issuer is normally one account; the uniqueness the schema
  enforces is (provider, subject), so a deployment with **two providers
  configured at the same issuer** can have two accounts for one subject.
  The role-limit verbs act on both, since a limit is the person's. Every
  other form then refuses the pair, names both accounts and their provider,
  and asks for `--email` -- rather than acting on whichever it found first,
  which would release or sign out the wrong person. In that deployment the
  refusal `disable` records is still keyed by issuer and subject, because it
  is the *person at the provider* who is turned off: it covers both of their
  accounts. Ending sessions, revoking tokens and releasing an address act on
  the one account addressed, and the `disable` document lists the others the
  refusal reached under `identity_also_covers`.
- `list` shows every account -- issuer, subject, email, role, whether it is
  off and since when, whether its address was released and which one it held,
  and its last sign-in (`users.last_sign_in_at`, stamped
  at every provider sign-in), and its `role_limit` -- plus every identity
  turned off before it had an account (`disabled_without_account`) and
  every role limit held for one (`role_limits_without_account`), for the
  deployer to compare with the provider's list.
- The account's content stays where it is, owned by the account; nothing is
  exported, reassigned or deleted.

Sample output of `disable` on an account with two tokens and one schedule,
with no run in flight:

```json
{"account": {"disabled": true, "disabled_at": "2026-10-01T23:58:06.920848+00:00", "email": "pat@example.com", "id": "…", "issuer": "https://login.example.com/realms/tenant", "last_sign_in_at": "2026-10-01T23:58:06.848332+00:00", "provider": "sso", "released": false, "released_from": null, "role": "user", "role_limit": null, "subject": "sub-pat"}, "command": "disable", "elapsed_ms": 74, "held": {"channel_bindings": [], "schedules": ["…"]}, "identity": {"issuer": "https://login.example.com/realms/tenant", "subject": "sub-pat"}, "note": "sessions are refused at their next request; every run this identity had executing was cancelled and its stream ended with it; no new run starts", "returncode": 0, "runs_cancelled": 0, "runs_finished_first": [], "runs_found": 0, "runs_unconfirmed": [], "schedules_held": 1, "sessions_ended": true, "started_at": "2026-10-01T23:58:06.913819+00:00", "surfaces": {"channel_bindings": {"action": "held", "connect_codes_ended": 0, "count": 0, "not_ended": 0, "stopped_after_ms": 59, "stopped_at": "2026-10-01T23:58:06.972819+00:00"}, "channel_ingress": {"action": "refused_at_next_use", "count": 0, "stopped_after_ms": 8, "stopped_at": "2026-10-01T23:58:06.921819+00:00"}, "internal_launches": {"action": "refused_at_next_use", "count": 1, "stopped_after_ms": 8, "stopped_at": "2026-10-01T23:58:06.921819+00:00"}, "personal_access_tokens": {"action": "revoked", "count": 2, "stopped_after_ms": 8, "stopped_at": "2026-10-01T23:58:06.921819+00:00"}, "running_work": {"action": "ended", "confirmed_by": "run_status", "count": 0, "stopped_after_ms": 63, "stopped_at": "2026-10-01T23:58:06.976819+00:00"}, "scheduled_occurrences": {"action": "ended", "count": 0, "not_ended": 0, "stopped_after_ms": 59, "stopped_at": "2026-10-01T23:58:06.972819+00:00"}, "schedules": {"action": "held", "count": 1, "not_ended": 0, "stopped_after_ms": 59, "stopped_at": "2026-10-01T23:58:06.972819+00:00"}, "sessions": {"action": "ended", "count": 1, "stopped_after_ms": 8, "stopped_at": "2026-10-01T23:58:06.921819+00:00"}, "sign_in": {"action": "refused_at_next_use", "count": 1, "stopped_after_ms": 8, "stopped_at": "2026-10-01T23:58:06.921819+00:00"}}, "surfaces_not_reached": [], "surfaces_unconfirmed": [], "tokens_revoked": 2, "verdict": "disabled"}
```

and of `release-email` on that account, then of running it a second time:

```json
{"account": {"disabled": true, "disabled_at": "2026-09-21T10:00:00+00:00", "email": "released-0b5f…@released.example", "id": "0b5f…", "issuer": "https://login.example.com", "last_sign_in_at": "2026-09-21T09:12:00+00:00", "provider": "sso", "released": true, "released_from": "pat@example.com", "role": "user", "role_limit": null, "subject": "3141592"}, "command": "release-email", "identity": {"issuer": "https://login.example.com", "subject": "3141592"}, "note": "…", "released": "pat@example.com", "verdict": "released"}
{"account": {"…": "…"}, "command": "release-email", "identity": {"issuer": "https://login.example.com", "subject": "3141592"}, "note": "…", "released": "pat@example.com", "verdict": "already_released"}
```

and of `limit-role` on an administrator with a token, a recurring schedule
and a run in flight:

```json
{"accounts": [{"disabled": false, "disabled_at": null, "email": "pat@example.com", "id": "…", "issuer": "https://login.example.com", "last_sign_in_at": "2026-09-26T09:12:00+00:00", "provider": "sso", "released": false, "released_from": null, "role": "user", "role_limit": "user", "subject": "3141592"}], "changed": true, "command": "limit-role", "elapsed_ms": 64, "identity": {"issuer": "https://login.example.com", "subject": "3141592"}, "note": "…", "returncode": 0, "role": "user", "runs_cancelled": 0, "runs_finished_first": [], "runs_found": 0, "runs_unconfirmed": [], "started_at": "2026-09-26T10:00:00.004210+00:00", "surfaces": {"internal_launches": {"action": "limited_at_next_use", "count": 1, "stopped_after_ms": 21, "stopped_at": "2026-09-26T10:00:00.025210+00:00"}, "personal_access_tokens": {"action": "limited_at_next_use", "count": 1, "stopped_after_ms": 21, "stopped_at": "2026-09-26T10:00:00.025210+00:00"}, "running_work": {"action": "left_alone", "count": 1, "role_above": "user", "role_read_by": [], "stopped_after_ms": null, "stopped_at": null}, "sessions": {"action": "ended", "count": 1, "stopped_after_ms": 21, "stopped_at": "2026-09-26T10:00:00.025210+00:00"}, "sign_in": {"action": "limited_at_next_use", "count": 1, "stopped_after_ms": 21, "stopped_at": "2026-09-26T10:00:00.025210+00:00"}, "stored_role": {"action": "lowered", "count": 1, "stopped_after_ms": 21, "stopped_at": "2026-09-26T10:00:00.025210+00:00"}}, "surfaces_unconfirmed": [], "verdict": "limited"}
```

and of `enable --restore-held` on an account a suspension of everyone had
turned off, with a recurring schedule and a Slack binding held, once the
suspension is lifted:

```json
{"account": {"disabled": false, "…": "…"}, "command": "enable", "held": {"channel_bindings": ["…"], "schedules": ["…"]}, "identity": {"issuer": "https://login.example.com", "subject": "3141592"}, "note": "…", "restore_held": true, "restored": {"channel_bindings": ["…"], "schedules": ["…"]}, "returncode": 0, "stayed_off": [], "verdict": "enabled"}
```

and of `disable --subjects` on three people -- one done, one whose run had
not stopped when the wait ran out, one whose subject names two accounts --
each entry being the document above, shortened here:

```json
{"command": "disable", "elapsed_ms": 116412, "identities": [{"command": "disable", "identity": {"issuer": "https://login.example.com", "subject": "3141592"}, "returncode": 0, "verdict": "disabled", "…": "…"}, {"command": "disable", "identity": {"issuer": "https://login.example.com", "subject": "2718281"}, "returncode": 2, "runs_unconfirmed": ["…"], "surfaces_unconfirmed": ["running_work"], "verdict": "disabled", "…": "…"}, {"command": "disable", "error": "2 accounts have subject '1414213' at this issuer, one per configured provider: sso (sam@example.com), sso-basic (sam.lee@example.com); address one of them by --email", "identity": {"issuer": "https://login.example.com", "subject": "1414213"}, "returncode": 1, "verdict": "refused"}], "issuer": "https://login.example.com", "note": "…", "returncode": 1, "started_at": "2026-09-27T10:00:00+00:00", "totals": {"identities": 3, "runs_cancelled": 1, "runs_finished_first": [], "runs_found": 2, "runs_unconfirmed": ["…"], "subjects_failed": [], "subjects_refused": ["1414213"], "subjects_unconfirmed": ["2718281"], "surfaces_not_reached": [], "surfaces_unconfirmed": ["running_work"], "verdicts": {"disabled": 2, "refused": 1}}}
```

An account that is still on, and a subject with no account, each answer with
an `error` and a non-zero exit instead:

```json
{"command": "release-email", "error": "the account of subject '3141592' at issuer 'https://login.example.com' is not turned off; releasing an address is for an account nobody can use any more, so turn it off first with `disable`"}
{"command": "release-email", "error": "no account exists for subject 'nobody' at issuer 'https://login.example.com'; there is no address to release"}
```

The account tables this section names (`disabled_identities`,
`role_limits`, `identity_holds`, and the `users` columns) are created by
migration `0027_account_access`, which runs at the first start.

### Local passwords (`HARTMESH_LOCAL_PASSWORDS=allowed`)

```
HARTMESH_LOCAL_PASSWORDS=allowed
HARTMESH_LOCAL_REGISTRATION=closed
```

People sign in with an email and a password this deployment keeps. The
rendered `auth` block is the template's lockout policy plus
`auth.local.enabled: true` and `auth.local.allow_registration` from the
second key, which is **required** in this mode and takes exactly one of two
values:

- `HARTMESH_LOCAL_REGISTRATION=closed`: nobody signs themselves up.
  `POST /api/v1/auth/register` answers `403` `registration_disabled`, the
  login page offers no create-account form (`setup-status` carries
  `"registration_enabled": false`), and an administrator adds each person
  (below). The value for a tenant published on the internet.
- `HARTMESH_LOCAL_REGISTRATION=open`: anyone who can reach the login page
  may create an ordinary (`user`) account, as every local-password
  deployment of this profile did before the key existed.

Absent, empty or any other value refuses to start and names the key --
nothing is assumed, for the reason this section opens with. Set beside the
three sign-on keys it refuses as a conflicting key, because in sign-on-only
mode nobody creates a local account. The template names neither
`auth.local.enabled` nor `auth.local.allow_registration`, in either mode,
and the renderer refuses one that does: the renderer owns both, so one
unmodified bundle serves a sign-on-only `.env` and a local-password one.

The first administrator is created on the first-admin page (or
`/api/v1/auth/initialize`) while there is none, whichever the key says --
anyone who reaches the tenant first can take that page, so create the first
administrator before the tenant is published. `reset_admin` works as before,
and its reset account is confined like an added one (below) until its person
completes setup. `/health` names the state (`"auth_mode":
"local"`, `"registration": "open"` or `"closed"`), and so does the start line
(`auth mode: local (local passwords on; registration closed)`).

#### Adding a person

With registration closed, an administrator adds each person; with it open
they can too. Two surfaces, one operation: the new account has role `user`
(an administrator is not created this way) and a **one-time password**.

- **In the product**: Settings → Account → _Add a person_, shown to an
  administrator in local-password mode. Behind it is
  `POST /api/v1/auth/users` with `{"email": "…"}`, which takes an
  administrator's interactive session only -- the authority of the lockout
  routes; a personal access token is refused -- and answers `201` with
  `{"id", "email", "system_role": "user", "needs_setup": true,
  "one_time_password"}` and `Cache-Control: no-store`. It sets no cookie: the
  administrator stays signed in as themselves.
- **In the deployment**, for a deployer with no browser session:

  ```bash
  docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env \
    exec --user 1000 gateway \
    sh -c 'cd /app/backend && PYTHONPATH=. uv run --no-sync python -m app.gateway.auth.add_user --email pat@example.com'
  ```

  prints one JSON document on stdout: on success (exit `0`)
  `{"command": "add-user", "id", "email", "system_role": "user",
  "needs_setup": true, "one_time_password"}`; on a refusal (exit `1`,
  nothing added) `{"command": "add-user", "error", "message"}`, where
  `error` is `email_already_exists`, `sign_on_required`, `email_invalid`,
  `usage` or `failed`. The document is the answer: a remote runner's own exit
  status may not carry the command's.

Both refuse an address any account already holds (`email_already_exists`,
what `/register` answers), and both refuse in sign-on-only mode
(`sign_on_required`), where the identity provider decides who has an
account.

**The one-time password** is shown once, in that one response, and never
again: only its hash is kept, it is never logged, and nothing can show it
later. Hand it to the person yourself. They sign in with it and land on the
setup page, where they enter it once more and choose their own password.
Until they have, the account can do nothing else: its sessions are refused
everywhere but "who am I" and completing setup (`403` `setup_required`), and
it holds no token, channel or schedule and cannot create one. The moment they
choose their own, the one-time password opens nothing (`401
invalid_credentials`). It keeps opening the setup page until then -- a person
who signs in and closes the tab can come back to it -- because nothing in the
product issues a second one: an administrator cannot reset another person's
password. If it may have reached anyone else, the person should sign in and
choose their own at once. The deployer can still start over: `reset_admin
--email <address>` gives any local account a new one-time password (written
to `.deer-flow/admin_initial_credentials.txt`, mode `0600`), ends its
sessions, and returns it to setup.

The same confinement applies to an account `reset_admin` resets, and to
sessions only: the reset account's personal access tokens, channels and
scheduled tasks keep working, since the reset exposed none of them.

**The key is required.** A
local-password tenant whose `.env` lacks `HARTMESH_LOCAL_REGISTRATION` **stops at start**, with
`render_config: refusing to render: HARTMESH_LOCAL_PASSWORDS=allowed selects
local passwords, and HARTMESH_LOCAL_REGISTRATION must then say …` in the
Gateway's journal. Add `HARTMESH_LOCAL_REGISTRATION=open` to keep the
sign-up form open, or `=closed` to close it; check the render with the
bundle's renderer as in § "Sign-in" above. A bundle whose
template was edited to name `allow_registration` must drop that edit: the
renderer refuses it in both modes. Sign-on-only tenants
change nothing.

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
  it, and it outlives the conversations it came from) and the company's
  Shared area under `home/shared/` (mounted read-only into every sandbox at
  `/mnt/user-data/shared`; written only by the Gateway's publish route, with
  the publication records in the database), so the pre-created
  `uploads/` and `artifacts/` directories are unused by this profile and stay
  empty.
- `/srv/hartmesh/operator`, mounted **read-only** into the Gateway at the same
  path: operator-owned deployment material that is not release content: the
  optional model file `HARTMESH_MODELS_FILE` names (§ "Operator-managed
  models") and the tenant bundle at `operator/tenant/` (§ "Tenant bundle").
  Its `agents-hold` file stops AI assistant and AI employee execution (§ "Storage and agents").
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

`/srv/hartmesh/spaces`, created by `storage-init`, holds the inventory, images
and mounts. The Gateway binds it at the same host path with slave propagation;
`storage-init` binds only this directory (§ "Storage and agents").

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

The profile also has a privileged one-shot, `storage-init`, which runs as root
with no network and exits before the Gateway starts (§ "Storage and agents").

### Tenant bundle

`/srv/hartmesh/operator/tenant/` is where the operator writes what the
workspace says about the company it serves. Two readers, one directory, no
copy: every sandbox mounts it read-only at `/mnt/tenant` (a `sandbox.mounts`
entry in `config.yaml`), which is where the `business-report` skill picks up
the company name, logo and colours by itself, and the Gateway reads the same
path (`tenant_bundle.path`) for the workspace header, the About page and
Home's starter grid, agent roles and provider support text. A person sees the company after signing in; the login
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
├── provider.json       {"display_name": "Example Hosting",
│                        "support_url": "https://support.example.com/help"}
├── starters.json       [{"id": "business-review", "title": "...", "prompt": "..."}]
├── roles.json          optional role briefs (§ "Roles and the provider’s name")
│                       -- the same shape and rules as `ui.starters`: at most
│                       six, distinct ids, plain text; present, it *is* the grid
└── report-profiles/    <name>.json, loaded by the skill by name ahead of its
                        own `profiles/` (a `services-generic.json` here replaces
                        the built-in one)
```

`provider.json` configures the workspace menu's **Contact support** action.
`display_name` is an optional plain line of at most 80 characters; it names the
service provider without changing the customer brand or product name.
`support_url` must be an absolute HTTPS URL of at most 2048 characters, without
credentials, whitespace or backslashes. Missing or invalid URLs hide the action;
invalid names leave a generic **Contact support** label. Validation problems
name only the field/rule in the Gateway log. The destination appears through
`/api/features` after sign-in. The app adds no conversation, account or credential data to the link and
suppresses the referrer. It does not submit a
support request automatically. Keep credentials out of the configured URL.

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
name and a logo are set, and how many starters there are -- and still renders.
Report profiles remain owned by the skill, which loads and validates them itself;
generic Gateway diagnostics do not enumerate that directory.

The profile runs the business profile (`ui.profile: business` in
`config.yaml`) and explicitly enables the installed `hartmesh-legacy-report`
compatibility plugin through `plugins:`. Historical cards, projection and filing
use that package; disabling its entry and restarting leaves ordinary file access.
The provider template owns this selection.
It also explicitly selects the existing monthly review, document summary and
spreadsheet starters in `ui.starters`. Generic business defaults contain only the
two document/spreadsheet tasks. Tenant `starters.json` takes precedence, including
an intentionally empty list.

With the business profile, the screens for building the deployment -- skills, tools,
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
until that start an edited public skill is live, so the seed is a restore,
not a tamper guard. A skill the operator adds goes in `home/skills/custom/`,
which the seed never touches. A chat's sandbox mounts the library read-only
at `/mnt/skills/public`. The Gateway's own projection, `home/skills_view/`,
is rebuilt from `public/` at startup.
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
  `skill-creator` uses `subprocess`; and `vercel-deploy` declares a
  sensitive capability. The skill review refuses each, so the profile does
  not ship them. `backend/tests/test_compose_public_skills.py` pins that each
  is still refused and that the policy exclusions are not review refusals; an
  exclusion the review no longer requires fails the suite, and the skill goes
  to tenants at the next release.

The list can only subtract: it names skills the image carries, and there is
no entry that adds one. It lives in `run.sh`, so changing it is a profile
change, not a tenant setting. A skill the operator wants goes in
`home/skills/custom/`. 13 skills are seeded at this release; the seed's line
in the Gateway log says how many and which names were excluded.

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

An AI agent workspace and its relay publish no host port: the Gateway reaches
them through `docker exec`. Each ordinary chat sandbox does publish one host port by design: its proxy's under
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

An AI agent workspace uses the same restricted network pieces, keyed by its
attachment ID. Its relay runs under the daemon default runtime; the configured
sandbox runtime applies to the workspace itself.

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
fresh container). A run on the tenant VM class (release `2.1.0+hartmesh.18`) measured the same report on the
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
nothing: it never evicts a parked sandbox some thread will reclaim, and one no
sandbox-backed tool call claims within about 300 s is stopped rather than
holding the slot for the 1800 s idle timeout. Only a sandbox-backed
tool call claims it, so a chat that only talks for five minutes loses its
prewarm and pays the cold start at its first tool call. So with one thread active and one chat freshly
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
turn gets an error, never a hang. Every acquisition path, synchronous or
asynchronous, enforces the same value. The harness default is 60 seconds and, until 2026-09-12, it was a
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
A sandbox is acquired by a turn's first
sandbox-backed tool call. A turn that calls none answers without one; a new
chat's first tool call waits out the whole cold start (80 to 91 s measured on
one CPU, 9.0 to 11.7 s on the slim profile), unless the thread's prewarm has
already started it, and a chat whose sandbox was evicted pays it again; a
reused sandbox does not.

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
send and read timeouts; the adoption probe `discover()` runs on a
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
`storage-init`'s 128 MiB is outside that sum: the one-shot exits before the
Gateway starts, and its startup `mount-all` helper uses the remaining VM headroom.
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

Memory writer idleness alone does not quiesce an agent’s filesystem or a Work
attempt. Use `ai list`/`ai status` to identify current work before the maintenance
window. For a quiesced snapshot, follow § "Backup and restore": stop the stack,
confirm/remove actual container holders and run `si quiesce` before snapshotting.

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
docker ps --filter name=hartmesh-space- --filter name=deer-flow-sandbox- --filter name=deer-flow-netproxy-
docker rm -f $(docker ps -q --filter name=hartmesh-space- --filter name=deer-flow-sandbox- --filter name=deer-flow-netproxy-)
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

`replicas` is a **hard budget** with **LRU eviction of warm sandboxes** in
front of it: a third acquisition first evicts the least-recently-used sandbox
that no thread is using, which is what keeps the count at two while at least
one slot is idle and is what every saturation event in estate qualification
has taken so far. With both slots in active use there is nothing to evict,
and the acquisition then waits up to `SANDBOX_CAPACITY_WAIT_TIMEOUT` seconds
(default 5, the overlap at the edges of two turns) and is otherwise refused
with a retryable capacity outcome. The slot is reserved before any container
work and released only once the container, its sidecar and its networks are
confirmed absent, so concurrent acquisitions cannot each take the last slot.

A running AI agent workspace, with its relay, takes one of these same two slots.
Work attempts are globally limited to one while replies may share a workspace.
Scheduled tasks share these two slots and add no container and no limit to the
budget: `scheduler.max_concurrent_runs` is 1, so unattended work holds at most
one of them (§ "Scheduled tasks" says what that does and does not leave a
person). A scheduled run is one more run in the Gateway process, at most one
at a time; the Gateway's own limit was measured under interactive load and has
not been measured separately under scheduled work.

That is a change of kind, and the reason for it is the arithmetic above: a
third container (1024 + 96 MiB of limit) already exceeds the 1024 MiB the line
leaves unallocated, so three concurrently active people would put 6240 MiB of
limits against 6144 MiB of RAM (the four-slot profile reached that point at
five to six). Nothing then bounds them but the guest's own memory, and the
victim of an OOM kill is whatever the kernel scores highest rather than the
sandbox that overshot. Until 2026-09-22 `replicas` was a soft maximum: the
provider logged `All 2 replica slots are in active use; creating sandbox ...
beyond the soft limit` and created the third anyway, and nothing refused a
fourth.

A refusal is customer-visible and deliberately so. On this profile a turn
asks for its sandbox at its first sandbox-backed tool call, so the refusal is
raised from inside that call and reaches the model, which is told the
workspace is running as much sandboxed work as it has room for and to finish
with what it has rather than call the tool again. The run records
`sandbox_capacity_exceeded` as its stop reason when the lead agent's tool call
was refused (a refusal inside a delegated task is reported in that task's
result; § "Reading a turn's timing"), and a scheduled task's occurrence with
that stop reason is recorded as failed, since nobody reads an unattended
answer. A turn that calls no sandbox-backed tool (a plain chat, or one that
only asks a clarifying question) never acquires, waits for or evicts a
sandbox, and answers while both slots are busy. Stop during the wait ends it
at once and the turn is recorded as cancelled, holding no slot. Stop while a
container the turn started is still booting ends the turn within seconds too:
it is torn down at its next readiness probe instead of after `ready_timeout`
(the `docker run` and the teardown still take their few seconds), and its slot
is free when the turn ends. A turn waiting on the chat's prewarm build is the
exception: Stop waits for that build, which the chat keeps. The control UI
is unaffected; nothing queues behind the budget. Eviction is
customer-visible too: a thread whose sandbox was evicted
gets a fresh one at its next sandbox-backed tool call (its files persist under
`home/`, and that call waits a cold start; a plain reply does not).
`idle_timeout: 1800` keeps an idle sandbox warm for thirty minutes (only a
sandbox-backed tool call refreshes that clock, so a long stretch of plain chat
can let it lapse): the budget
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



## Download all my data

Any signed-in person can download all of their own work from the web app's
account menu, _Settings and more_ → _Download all my data_
(`POST /api/account/export`). The dialog follows it while it is prepared and
links each part. The download covers every conversation as the
page shows it, every file in its uploads, outputs and workspace, their own
files and skills, their memory, their schedules and their custom agents.
Nobody can export someone else's: the routes act only for the session that
calls them, so neither an administrator nor a personal access token can reach
another person's.

It is prepared on the data disk, under the Gateway's `exports/` directory,
before it is downloaded. The profile runs the defaults below, and no `.env`
key changes them:
- **Free space.** It is refused, with nothing written, when it would leave the
  disk less than 1 GiB (`account_export.min_free_bytes`) after what other
  exports still have to write. If the disk fills while it runs, it stops and
  is removed.
- **Parts.** Past 2 GiB (`account_export.part_bytes`), the download comes as
  numbered parts. A single file larger than that is a part of its own.
- **Deletion.** The export is deleted ten minutes after every part has been
  downloaded (a part can be downloaded again until then), or once an hour
  passes with no part downloading (`account_export.expires_after_seconds`).
  Anything left there is deleted when the Gateway stops or next starts, and a
  person turned off loses theirs.
- **Load.** One export per person at a time, and at most two
  (`account_export.max_concurrent`) being prepared at once.

An export needs as much free space as the person's data until it is
downloaded or expires, so an account larger than the free space minus 1 GiB
cannot be exported until the data disk grows. The logs say when one starts and
finishes, with its counts and size, and never a title or a file name.

## Telegram bots

Telegram bots are not offered in this release. The key `HARTMESH_TELEGRAM` is
reserved for them. While it is absent, the renderer writes no `telegram_bots`,
`channel_connections` or `channels` section and removes `show_web_page` from
the tool list. `on` is for qualification of the unfinished feature and is not
supported in this release. No managed bot supervisor runs yet.

Configuration version 55 reserves all managed-bot settings. Explicit `on` or
`off` renders the section with its corresponding `enabled` value, the public
URL from `HARTMESH_PUBLIC_HOST` when it passes the renderer's hostname check,
and the voice provider and bot limits above. Empty environment values use the
absent defaults. Invalid values refuse rendering with the key's name. The
template must omit `telegram_bots.enabled`, `telegram_bots.public_base_url`,
`channel_connections` and `channels.telegram`; the renderer owns them.
The rendered bound-identity configuration disables deployment-wide Telegram
setup through both existing token routes. It does not migrate an existing bot.

The render/check summary follows the storage/agents fragment with `telegram=off`
when absent or explicitly off. For explicit `on`, its first applicable message is:

| Fragment | Meaning |
| --- | --- |
| `telegram=on, bots cannot be added (HARTMESH_PROVIDER_KEYS_SECRET not set)` | No wrapping secret was supplied. |
| `telegram=on, bots cannot be read (HARTMESH_PROVIDER_KEYS_SECRET is shorter than 32 characters)` | The supplied wrapping secret is too short. |
| `telegram=on, no link buttons (HARTMESH_PUBLIC_HOST is not a hostname)` | No valid public hostname; the rendered public URL is null. |
| `telegram=on` | These prerequisites are present. This does not enable the unfinished connection flow. |

Neither a missing secret nor a non-hostname public host refuses a render.
The secret value is never written into the rendered section or printed in the
summary. Revision `0042_telegram_bots` adds the schema foundation, and
`0043_telegram_inbox_chat_scope` scopes retained inbox message identities to
their chat. Startup upgrades through the current head on SQLite or PostgreSQL.
The latter preserves prior records; downgrade refuses if different chats have
retained the same link/message pair, because the older key cannot represent both.

## Scheduled tasks

The profile ships with the scheduler on. It was off only because the template
declared no `scheduler:` block, so `scheduler.enabled` took its `false`
default. The Gateway re-renders the tenant's own `config.yaml` from the bundle
at every start, so the bundle is the only place the setting is turned on or off:
a change in the tenant's copy has no effect, and there is no `.env` key for it.
The scheduled-tasks page shows a notice when no scheduler is running (turned
off in configuration, turned on but stopped, or none at all) and tells the
person that saved times will not arrive on their own; while one is running it
shows nothing (`GET /api/scheduler` names the state). The levers a person or
the deployer has beside the bundle are pausing or deleting a schedule and
`accounts disable`, which holds a person's schedules.

Nobody watches a scheduled run, and the two 1 GiB sandbox slots are the
tenant's whole hard budget, so each value is a choice:

| Setting | Value | Why |
| --- | --- | --- |
| `scheduler.max_concurrent_runs` | 1 | `sandbox.replicas` minus one: the scheduler never holds both slots. |
| `scheduler.max_run_seconds` | 900 | Longer than a cold sandbox (80 to 91 s measured) and a report build, which has not been measured under a scheduled run; short enough that a run that never finishes frees the slot. |
| `scheduler.queue_timeout_seconds` | 7200 | Eight full-length runs: how long an occurrence may wait behind the run ahead of it, counted from when it was admitted. |
| `scheduler.recursion_limit` | 1000 (unchanged) | A scheduled run does the work a person would ask for. The lead-agent graph spends about 11 steps a model turn (counted with a scripted model), so a run ends near 90 turns, before the 500-turn execution budget. |

`backend/tests/test_compose_profile.py` pins these, and derives the first from
the replicas. `recursion_limit` reaches the run through the launch intent; a run
launched without one gets the Gateway default of 100 steps, about nine turns.

**Two occurrences due together.** Each 5 s poll admits at most one due task
(oldest first), and only one scheduled run executes at a time. A scheduled run
takes a sandbox slot at its first sandbox call and keeps it until the run ends,
so the second occurrence waits, durably, as `queued`, and launches on a later
poll once the first has ended. If it is still waiting when `queue_timeout_seconds`
have passed since it was admitted, it ends `failed` with
`scheduled task queue wait timeout exceeded`.

**A person's turn while a scheduled run holds a slot.** The scheduler never holds
both slots, so a person's turn finds one it can take. That is all the profile
promises. Two slots are two slots: while a scheduled run and one person's turn
hold both, a second person's sandbox turn waits `SANDBOX_CAPACITY_WAIT_TIMEOUT`
and is refused, as it is between two people, and it is refused for as long as
the scheduled run lasts (up to `max_run_seconds`). A scheduled run's new sandbox
can also evict a person's idle warm sandbox, so that person's next turn pays a
cold start (80 to 91 s measured). Scheduled work at round times (a Monday 09:00
review) overlaps with people opening the app. With room for two scheduled runs
at once (`max_concurrent_runs` 2), two runs hold both slots and a person's turn
is refused after the wait; `backend/tests/test_scheduled_capacity_admission.py`
shows both cases.

**A scheduled occurrence that cannot get a slot.** With both slots held by
people, its first sandbox call waits and is refused. The occurrence ends `failed`
and says `no sandbox was free when the task ran, so its tools did not run`, on
the task's *Last error* and on the run's row. It starts no third container, and
it is not retried: the next occurrence is the next attempt.

**What bounds a scheduled run**, and what its owner reads when it ends (in the
task's *Last error* and on the occurrence's row of the run history). Nothing
notifies a person: they read the page.

| Bound | Ends as | The page says |
| --- | --- | --- |
| Wall time, `max_run_seconds` | occurrence `failed`; the run is asked to stop and ends `interrupted` when the Stop lands | `the task did not finish within 15 minutes, so it was stopped` |
| Step limit, `recursion_limit` | occurrence `failed`; run `error`, `stop_reason: recursion_limit_reached` | `the task used up the number of steps it is allowed, so it stopped before it finished` |
| A guard or execution budget (`loop_capped`, `turn_budget_exhausted`, and the like) | occurrence `failed`; run `success` with that `stop_reason` | words about the limit, never the code |
| No sandbox free | occurrence `failed`; run `success`, `stop_reason: sandbox_capacity_exceeded` | `no sandbox was free when the task ran, so its tools did not run` |
| Waiting behind the run ahead, `queue_timeout_seconds` | occurrence `failed`, no run | `scheduled task queue wait timeout exceeded` |

The wall-time bound ends the occurrence first and then asks the run to stop,
once; the occurrence is `failed` whether or not the run stops. The bound is
measured from when the run started, so a poll can end it up to 5 s late. After a
Stop the scheduler launches nothing until the stopped run's worker has finished,
for up to two minutes, because the run keeps its sandbox until then. A person's
own Stop still ends the occurrence `interrupted`.

**When the scheduler starts.** There is no misfire grace. On the first poll after
a start, every enabled schedule whose next time has already passed runs once,
oldest due first, one at a time: a recurring schedule runs once however many
times it missed, and a one-time schedule whose time has passed runs then, late.
Schedules saved while scheduling was off, and any that came due while the
Gateway was restarting, are in that set. An occurrence still waiting after
`queue_timeout_seconds` fails as above, and a one-time task then ends `failed`.
A paused schedule does not run. Each run uses the task's current prompt, as the
person who saved it, and spends the tenant's model budget; a schedule a person
does not want to run is paused or deleted before the start.

**Disable, enable and role limits with the scheduler running.** They are
exercised against the running scheduler
(`backend/tests/test_scheduler_on_disable_and_role_limit.py`):

- `disable` holds the person's active schedules; a held schedule is not
  claimed at its next due time, and it stays held after `enable`. `enable
  --restore-held` turns back on exactly what the last `disable` held, and it
  runs at its next due time.
- An occurrence the scheduler claimed just before the disable is refused at
  launch: it ends `failed`, creates no run (no model call, no sandbox) and is
  not retried. One waiting in the queue behind a running occurrence is ended
  `interrupted` by the disable and never launches.
- A demoted administrator's scheduled task runs as `user`: `limit-role` holds
  the identity at `user` and the scheduled launch resolves the owner's role at
  the launch.

**What is controlled, and what still depends on the model or provider.**
Controlled: how many scheduled runs execute at once, that they never hold both
slots, the wall-clock and step bounds, and how each ends. Not controlled:
whether a run finishes its task inside those bounds, how many model turns a real
task takes, the answer, or a person's turn being refused while a scheduled run
and another person hold the slots. The single slot is workspace-wide (no
per-person cap or minimum interval), and *Trigger now* is subject to the same
one-at-a-time budget and shows `queued` while it waits. Not measured: an
unattended run against the tenant's own model route on a leased guest, and the
Gateway's memory under scheduled work.

An assignment is separate from the scheduled queue. A scheduled run and a Work
attempt can occupy both sandbox slots; scheduler concurrency does not count Work.

## Storage and agents

An absent key means its default. With all seven keys absent, Compose prepares
31 slots of 1 GiB: ten user slots, ten AI assistant slots and ten AI employee
slots, plus one company slot. This reserves 31 GiB plus the disk headroom below.
Each person may add one AI assistant; administrators may add up to ten company
AI employees. Preparing slots does not create those agents. There is no key that turns storage off.
To offer no agents, set `HARTMESH_AI_ASSISTANTS_PER_USER=0` and
`HARTMESH_AI_EMPLOYEES=0`; personal and company files remain enabled.
Complete the shared-mount and disk preflight before the first start or upgrade.

A **Space** is a fixed-capacity ext4 filesystem inside a fully allocated image
on the data disk. A **slot** is a prepared filesystem, assigned or free, with
kind `user`, `assistant`, `employee` or `company`. The **pool** holds the slots
at `/srv/hartmesh/spaces`. An AI assistant is a person's own agent; an AI
employee is the company's agent. Counts and capacities below plan their storage;
the counts permit creation and qualified execution at the defaults or the
operator's configured limits. The product screens distinguish both kinds
and support Files, Work, Memory, Access and Settings.

**Qualification and operator hold.** With managed storage enabled, the Gateway
keeps execution closed until it has reconciled durable run ownership, retained
workspace attachments and the complete Docker resource census. Unknown
containers or paired networks keep it closed. Recovery of original resident
resources uses retained immutable identities and preserves live foreign leases;
unknown resources and unconfirmed cleanup remain charged.
The existing maintenance task retries qualification every 60 seconds.
Creating `/srv/hartmesh/operator/agents-hold` refuses new agent execution and
causes maintenance to contain locally owned workspaces. A foreign Gateway's
live lease remains protected. Remove the file to lift the hold; qualification
must still succeed before execution resumes. The directory can be overridden
with `HARTMESH_OPERATOR_DIR` in deployments that configure another operator
path.

Examples use these shell functions, defined in the operator's shell:

```bash
hm() { docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env "$@"; }
si() { hm run --rm -T storage-init "$@"; }
ai() { hm exec -T gateway sh -c 'cd /app/backend && exec setpriv --reuid=1000 --regid=1000 --groups="$(stat -c %g /var/run/docker.sock)" --inh-caps=-all --no-new-privs env HOME=/tmp/gateway-home DOCKER_CONFIG=/tmp/gateway-home/.docker PYTHONPATH=. uv run --no-sync python -m app.gateway.agent_instances "$@"' agent_instances "$@"; }
accounts() { hm exec -T --user 1000:1000 --workdir /app/backend -e PYTHONPATH=. gateway /app/backend/.venv/bin/python -m app.gateway.auth.accounts "$@"; }
```

`ai` enters through the container’s root only to perform the entrypoint’s
privilege drop: uid 1000, gid 1000, the Docker socket’s actual group, no inherited
capabilities and `no_new_privs`. The account helper does not need Docker access;
its explicit uid/gid and image environment remain as shown. `-T` keeps JSON output
usable through pipes. `uv run --no-sync` uses the image environment without an
installation.

Supply `HARTMESH_PROVIDER_KEYS_SECRET` in the command environment for `hm up`
when provider keys have been stored through the product (§ "Provider keys in
the product").

### What the profile prepares

The data disk must be a shared mount before starting this profile, including
when the storage keys are absent. Both the one-shot and Gateway use an `rslave`
bind so mounts made in the host namespace become visible inside the containers.
Check before `up`:

```bash
findmnt -no TARGET,PROPAGATION --target /srv/hartmesh
findmnt -no PROPAGATION --target /srv/hartmesh
```

The propagation must be `shared`. If it is not, an administrator runs
`mount --make-shared /srv/hartmesh` on the VM and adds `shared` to the data disk's
mount options in `/etc/fstab` so the setting survives a reboot. If `findmnt`
names a different mount target, use that target for the remedy. An unshared
mount causes Docker to refuse the bind before the Gateway starts; skipping the
one-shot does not bypass the Gateway's identical requirement.

Storage preparation also needs these host checks:

```bash
findmnt -no FSTYPE --target /srv/hartmesh  # ext4
ls -l /dev/loop-control                  # character device
uname -r                                # Linux 6.0 or later
command -v mkfs.ext4 e2fsck resize2fs losetup mount umount findmnt fallocate
```

The final line must find eight tools from `util-linux` and `e2fsprogs`.
The slot verifier requires filesystem identity support from Linux 6.0 or later.
Missing storage prerequisites leave storage unavailable; they do not replace
the configured backing with an ordinary directory.

The one-shot checks the complete missing plan, prepares slots
one at a time in company/user/assistant/employee order, mounts existing slots,
and updates its fstab block. The Gateway verifies the inventory and backing,
then probes whether Docker sees the same bytes. It creates the company Space
and assigns one User Space per eligible account. Startup and each 60-second
maintenance pass attempt released-account reclamation, missing assignments and
company grants, then interrupted-operation settlement. A failed or uncertain
operation remains pending; the interval is not a completion guarantee.

My Files and Projects are `data/files` and `data/projects` of that person's
Space. Every chat uses `data/conversations/<thread>` for its working directory,
uploads, outputs and ACP workspace. Shared is `data/shared` of the company
Space; its `data/projects` is reserved and empty. Each member has READ/EXPORT
on Shared, while the Gateway writes publications and removals in that member's
name. Company custody itself grants no access.

An ordinary chat binds its own workspace/uploads/outputs and My Files read-write,
the company Shared folder read-only, and its ACP workspace read-only. The Gateway
verifies the backing and current roles before creation and reuse; labels
`hartmesh.storage.space.<slot>` record each mounted Space. These chat binds have
no storage attachment row. An unavailable User Space permits mountless text chat
with no personal or Shared bind and no legacy-folder fallback. An unavailable
company Space omits Shared while personal files remain usable. First-party file
routes return a structured refusal when their own role is unavailable.

**When an agent cannot reach a site.** A workspace reaches the sites of the standing list, `sandbox.network.allow_domains` of this profile's `config.yaml`, and no other (§ "`SANDBOX_EGRESS=allowlist` (the default)"). That list is the provider's, and nobody at the customer changes it. A person is asked about a blocked site in one case only: in a conversation of that person's own with the agent, when that person can manage it. The card "A site is blocked" then offers "Allow for 5 minutes" and "Keep blocked", and an allowance holds for every conversation and assignment of that agent for those minutes. Nobody is asked while an agent works on an assignment, nor in a conversation of a person who cannot manage it: the connection stays blocked, the work goes on, and the conversation shows one line that names the site. A reader who can manage the agent reads "{name} tried to reach {host}, which is not on the list of sites allowed here. Allow {host} for {minutes} minutes for all of {name}'s conversations and assignments, or keep {host} blocked. The permanent list is set by {provider}." Every other reader reads a line that says "which is not on the list of sites allowed here" and whom to ask. A request to add a site therefore reaches the provider, and widening the standing list remains the operator's decision, as for every sandbox. Web search and web fetch are not governed by the list: they run from the Gateway, as they do for an ordinary chat.

### Fixed-image trim policy

Each Space image must stay fully allocated on the data disk. Trimming the ext4
filesystem inside a loop image punches holes in that image, releasing its reserved
capacity. The verifier then refuses the Space, even if its files remain readable.

Storage preparation mounts images with `nodiscard` and writes `X-fstrim.notrim`
in their managed `/etc/fstab` entries. The standard util-linux scheduled service
uses `fstrim --listed-in /etc/fstab:/proc/self/mountinfo` and skips these entries.
It can still trim the underlying data disk and other eligible filesystems.

For an existing pool, update only the managed entries with `si fstab-sync` or run
normal preparation using the updated image. This preserves the inventory and files.
`mount-all` does not rewrite fstab or remount an already mounted filesystem; new
mount options take effect at the next normal unmount/mount maintenance window.

Custom host trim automation must also exclude these loop filesystems. `fstrim -a`
and direct `fstrim <Space mountpoint>` bypass the fstab exclusion. `nodiscard`
prevents online discard when blocks are freed; it does not block an explicit
FITRIM request. Neither this update nor `fstab-sync` repairs an already sparse
image, and qualification never silently refills one. Inspect unavailable backings
through the existing storage recovery procedure before resuming work.

### The keys

All seven keys are validated, including when both agent counts are zero. Values are decimal whole
numbers with surrounding blanks trimmed; empty means absent. A change takes
`hm up -d --wait`. The template contains neither `storage_spaces` nor
`agents_api`; the renderer writes both sections.

| Key | Range | When absent | What it plans |
| --- | --- | --- | --- |
| `HARTMESH_USERS` | 1–100 | 10 | User slots; choose at least the number of accounts, including disabled accounts. |
| `HARTMESH_USER_STORAGE_GIB` | 1–1024 | 1 | Whole GiB per user slot. |
| `HARTMESH_AI_ASSISTANTS_PER_USER` | 0–10 | 1 | AI assistant slots per planned user. |
| `HARTMESH_AI_ASSISTANT_STORAGE_GIB` | 1–1024 | 1 | Whole GiB per assistant slot. |
| `HARTMESH_AI_EMPLOYEES` | 0–100 | 10 | Company AI employee slots; `on` and `off` are invalid. |
| `HARTMESH_AI_EMPLOYEE_STORAGE_GIB` | 1–1024 | 1 | Whole GiB per AI employee slot. |
| `HARTMESH_COMPANY_STORAGE_GIB` | 1–1024 | 1 | Whole GiB for the company slot. |

The required allocation and fit checks are:

```text
required_bytes = users × U + users × assistants_per_user × A + employees × E + C
spaces         = users × (1 + assistants_per_user) + employees + 1  (at most 256)
fits           = free_bytes − missing_bytes ≥ max(4 GiB, 0.10 × disk_bytes)
                 and free_inodes − 4 × missing_count ≥ 1,024
max_inodes     = max_bytes // 8192  per Space
```

`U`, `A`, `E` and `C` are the capacities converted from GiB to bytes.
Missing bytes/counts include only slots still to prepare. The defaults below
reserve 31 GiB; writing them explicitly has the same effect as leaving them absent:

```dotenv
HARTMESH_USERS=10
HARTMESH_USER_STORAGE_GIB=1
HARTMESH_AI_ASSISTANTS_PER_USER=1
HARTMESH_AI_ASSISTANT_STORAGE_GIB=1
HARTMESH_AI_EMPLOYEES=10
HARTMESH_AI_EMPLOYEE_STORAGE_GIB=1
HARTMESH_COMPANY_STORAGE_GIB=1
```

This prepares
ten user, ten assistant, ten employee and one company slot. On a 50 GiB data
disk it also requires 5 GiB left free. About 48 GiB initially free would leave
about 12 GiB beyond that reserve. This is arithmetic, not a measured workload.
Setting only `HARTMESH_USERS=10` has the same 31-slot plan. Both agent counts
set to zero plan eleven 1 GiB slots, with personal and company files still enabled.

People's files, Shared and Projects live in Spaces and ordinary chats mount
them. Missing or unverified assignments remain visible without preventing text
chat. Creation respects each kind's allowance and each person's assistant limit,
including administrators. Unfinished, paused and archived agents count; exact
creation retries preserve their identity and files. AI assistant and AI employee
files use slots of their matching kind. Files and conversation uploads write
directly while the workspace runs or is closed, deletion reclaims the matching
kind's slot after verified cleanup, and separate lists and creation flows
distinguish the two kinds. Qualified nonzero counts permit execution; holds and
unavailable storage refuse new replies, activations and lifecycle changes.
Work projections report readiness, containment and capacity, with details limited by Inspect.
A successful qualification writes `storage: ready (…)` and `agents: ready (…)`
when a count is nonzero, or `agents: none (…)` when both are zero.
The bytes and inodes per conversation are unmeasured;
the allocation example is not a workload-sizing recommendation.

Each refusal follows `render_config: refusing to render: `:

| Refusal text |
| --- |
| `HARTMESH_USERS must be a whole number from 1 to 100 (or absent)` |
| `HARTMESH_USER_STORAGE_GIB must be a whole number of GiB from 1 to 1024 (or absent)` |
| `HARTMESH_AI_ASSISTANTS_PER_USER must be a whole number from 0 to 10 (or absent)` |
| `HARTMESH_AI_ASSISTANT_STORAGE_GIB must be a whole number of GiB from 1 to 1024 (or absent)` |
| `HARTMESH_AI_EMPLOYEES must be a whole number from 0 to 100 (or absent); on and off are not values of this key` |
| `HARTMESH_AI_EMPLOYEE_STORAGE_GIB must be a whole number of GiB from 1 to 1024 (or absent)` |
| `HARTMESH_COMPANY_STORAGE_GIB must be a whole number of GiB from 1 to 1024 (or absent)` |
| `the keys call for <n> Spaces (<u> users, <u> x <a> AI assistants, <e> AI employees, the company); at most 256 can be prepared on one VM` |
| ``template `agents_api` must be absent; HARTMESH_AI_ASSISTANTS_PER_USER and HARTMESH_AI_EMPLOYEES select it`` |
| ``template `storage_spaces` must be absent; HARTMESH_USERS and the capacity keys select it`` |

### The pool

`storage-init` owns this layout; the Gateway runs as uid/gid 1000:

| Path under `/srv/hartmesh/spaces` | Owner | Mode | Purpose |
| --- | --- | --- | --- |
| Pool directory | root:1000 | 0750 | Pool boundary. |
| `inventory.v2.json` | 1000:1000 | 0440 | Slot identities, kinds and bounds. |
| `.storage-init.lock` | root:root | 0600 | Exclusive operator lock. |
| `.gateway-owner.lock` | 1000:1000 | 0600 | Stable empty runtime ownership file. |
| `.probe/` | 1000:1000 | 0700 | Temporary Docker nonce probes. |
| `<slot>.img` | root:1000 | 0640 | Fully allocated ext4 image. |
| `<slot>.volume` mount point | root:1000 | 0750 | Mount point beneath the mounted root. |
| `<slot>.volume/data/` | 1000:1000 | 0770 | Visible files. |
| `<slot>.volume/control/` | 1000:1000 | 0700 | Private staging, receipts and restore points. |

Slot IDs have 32 hexadecimal characters. The mounted filesystem parent is
root:1000 0770; its unmounted mount point stays root:1000 0750. Visible and private bytes consume the same fixed capacity;
an empty 1 GiB slot already occupies 1 GiB of the data disk. Inventory schema 2
adds a kind to every slot; the standalone helper still writes schema 1.
Never edit the inventory by hand.

One participating native Gateway runtime may own a managed pool. `prepare` and
`mount-all` create or qualify the stable `.gateway-owner.lock`; a valid file held
by a live Gateway is untouched. Do not delete or replace this file. The Gateway
acquires it before native provider construction and holds it through sandbox,
storage and database shutdown. Docker commands inherit ownership, so an in-flight
child can retain it after its launcher exits. A missing, busy or unverifiable
file leaves native storage unavailable; the Gateway keeps history and mountless
text chats available and retries qualification every minute. Stop the existing
Gateway before starting a replacement against the same pool. This is runtime
exclusion, not evidence that every storage container has been removed.

Assignment creates kind-specific folders at 0700: user `files/`, `projects/`,
`conversations/`; company `shared/`, `projects/`; agent `uploads/`, `outputs/`,
`.agent-skill-packages/`. Feature plugins only look up assigned roles and folders.
The Gateway assigns the company and eligible accounts, retaining failed assignments
for maintenance repair.

The fstab block, derived only from inventory, lies between
`# hartmesh-storage begin` and `# hartmesh-storage end`. Entries use
`loop,nodev,nosuid,nofail,x-systemd.requires-mounts-for=/srv/hartmesh,x-systemd.before=docker.service`.
`nofail` permits boot with a damaged slot; it does not prove that slot usable.
If fstab cannot be written, the result records a problem and Gateway `mount-all`
can still mount existing slots at start.

### storage-init

This is the profile's only privileged service: the same backend image digest
as the Gateway, `pid: host`, no network, 128 MiB, 64 processes, and no restart.
It binds only the pool and enters the host mount namespace with `nsenter -m -t 1`.
It reads the data-directory and planning environment, never app configuration
or the database. It knows slots and kinds, not accounts or agents.

At every `up`, `prepare --at-start` has a 600-second limit and exits zero so a
storage failure does not prevent Gateway startup. Its JSON `returncode`,
`verdict` and `problems` retain the actual outcome. An interrupted plan continues
at the next `up` or `si prepare`. Docker restart alone does not rerun the one-shot;
the fstab block and Gateway helper mount existing slots instead.

Gateway startup may run only `mount-all`, through its pinned
`HARTMESH_STORAGE_INIT_IMAGE`, with a 60-second deadline. Unconfirmed helper
creation/removal blocks another call until its exact owned helper is resolved;
an empty container listing alone is not proof that creation finished.

Each operator verb holds the pool lock, waiting at most 30 seconds. The lock
remains held by inherited host-tool descriptors through offline repair/growth.
Unlisted images are retained and reported; only explicit `forget` removes them.
When Docker cannot start the one-shot itself, `hm up -d --wait --scale storage-init=0`
can start the rest, provided the shared-mount preflight passed. It prepares no
missing slots and does not cure a Gateway helper failure.

### Reading the start-up lines

| Output | Meaning |
| --- | --- |
| `storage: storage-init mount-all exit …` | Helper outcome for existing mounts. |
| `storage: ready (…)` | Pool admitted, with prepared/verified counts and bounds. |
| `storage: ready, … spaces short (…)` | Incomplete plan; inspect whether the missing allocation fits. |
| `storage: unavailable (…)` | Inventory, data-disk, mount helper or Docker admission failed. |
| `storage: space … unverified (…)` | One slot could not be verified; its existing identity is retained. |
| `storage: company space created (<slot>)` | The exact company role was committed. |
| `storage: user space of <account id> assigned (<slot>)` | That account's User role was committed. |
| `storage: company grant written for <account id>` | That member's READ/EXPORT grant was committed. |
| `storage: file operations settled <n>` | Confirmed interrupted intents settled; unknown outcomes remain pending. |
| `storage: user space reclaimed (<slot>)` | Verified cleanup freed a released account's binding. |
| `agents: none (…)` | Both rendered agent counts are zero. |
| `agents: ready (<n> attachments fenced at start; <n> work attempts settled)` | Resident restoration and containment completed. |
| `agents: unavailable (…)` | Qualification remains closed; maintenance retries. |
| `agents: attachment <id> of <id> contained (<reason>)` | Exact workspace containment confirmed. |
| `agents: work attempt <id> of <id> settled failed (…)` | Interrupted attempt failed without promoting its candidate. |
| `agents: held (…)` / `agents: hold lifted` | Operator hold found / removed. |

The renderer reports the planned counts/capacities, including defaults for absent
keys, followed by `agents=on` when either agent count is nonzero,
or `agents=none` otherwise. It reads no disk. `/health` reports
state words only; `/api/features` adds authenticated readiness and counts,
without paths or byte capacities. See [Storage Spaces](../../docs/STORAGE_SPACES.md).

### Fresh install

Complete the shared-mount, Linux/tool and disk preflight. With all keys absent,
`si status` computes the 31 GiB plan and required headroom before preparation.
Start with `hm up -d --wait` and read
`hm logs --no-log-prefix storage-init gateway`. Expect 31 prepared/mounted slots,
31 fstab entries, `storage: ready (…)` and `agents: ready (…)`, with
`"storage": "ready"` and `"agents": "ready"` in `/health` after qualification.
The renderer reports ten users, one AI assistant per user and ten AI employees.
The first administrator takes the first account seat and assigned User Space.
The first allocation can take minutes; read the actual preparation outcome and
readiness. Insufficient disk, missing prerequisites or incomplete preparation
remain visible and do not fall back to the earlier directories.

### Upgrading from release 45

Before replacing a bundle:

```bash
findmnt -no TARGET,PROPAGATION --target /srv/hartmesh
findmnt -no FSTYPE --target /srv/hartmesh
uname -r
df -B1 --output=size,avail /srv/hartmesh | tail -n 1
du -sh /srv/hartmesh/home/users /srv/hartmesh/home/shared /srv/hartmesh/postgres
accounts list
```

Read these while the old Gateway still runs. Count accounts, including disabled
ones, before choosing `HARTMESH_USERS`; disabling an account keeps its seat.
Free bytes less the missing allocation must leave the larger
of 4 GiB and 10% of the disk size. The explicit 31 GiB example needs at least
36 GiB free on a 50 GiB disk. If existing accounts exceed the configured seats,
the Gateway still starts; excess accounts work without personal files until the
key is raised and assignment succeeds.

1. Agree the maintenance window and snapshot the data disk at the platform.
   Keep the snapshot: it is the supported way back to release 45.
2. Stop the stack with `hm down`. Inspect and remove the old Gateway's exact
   orphaned sandbox/relay/bridge containers using the existing upgrade procedure
   (§ "Upgrading a guest that already runs sandboxes").
3. Replace `/opt/hartmesh` with the new pinned bundle. Complete its shared-mount,
   Linux/tool and disk preflight; set `HARTMESH_USERS` at or above the account
   count. With the stack down, `si status` must report that the plan fits.
4. Run `hm up -d --wait`, supplying the provider-key wrapping secret if needed.
   Absent keys now prepare the default 31 GiB plan, plus required headroom.
5. Inspect storage-init/Gateway logs and `/health`; expect prepared/verified
   storage and qualified agent readiness as on a fresh installation. Normal
   bootstrap applies revision `0043_telegram_inbox_chat_scope` on SQLite or PostgreSQL.

The snapshot is taken before the database upgrade. No user-file transfer is
performed by this upgrade. For the additive table/columns and guarded
technical downgrade, see the [schema notes](../../CHANGELOG.md#schema-changes).

The new storage layout applies to every upgrade, including when all seven keys
are absent. Give the administrator the [member upgrade notes](../../CHANGELOG.md#member-upgrade-notes)
before the maintenance window. The following directories are not migrated into Spaces.

**The directories of release 45 stay on the disk, and nothing reads them.** After this upgrade My Files, Shared and Projects are folders inside each person's Space and the company's, and they start empty. What people kept before the upgrade is still on the data disk, untouched, at `/srv/hartmesh/home/users/<id>/files/` (each person's earlier My Files), `/srv/hartmesh/home/shared/` (the earlier Shared) and `/srv/hartmesh/home/users/<id>/projects/` (each person's earlier project documents). The product does not read them and does not copy them: a person who had files in My Files opens an empty My Files. Tell the customer's administrator before the upgrade. Once the snapshot taken before the upgrade is kept, you may remove the three directories; `du -sh` on each says what returns to the data disk. The conversations' own directories of release 45, `/srv/hartmesh/home/users/<id>/threads/<thread>/` with everything under them, are likewise left unread: a conversation that existed before the upgrade opens with an empty working directory, and its earlier uploads, produced files, skills projection and ACP workspace stay on the data disk until you remove `threads/` as a whole. Removing the directories is the only action this release offers for them; there is no copy into a Space.

First-party file and chat paths resolve through verified roles rather than those
legacy directories. Account, conversation and schedule
metadata remain. Snapshot restore is the way back.

**How long it takes.** The first start allocates, formats and mounts the plan one
slot at a time. The 31-slot example's 94 seconds is illustrative, not a measurement
on the supported tenant VM class. A later start mounts existing images.
Preparation has a 600-second limit; a partial plan continues at the next `up` or
`si prepare`, with missing allocation reported. Inspect actual `seconds` and
Gateway readiness instead of promising a fixed outage.

### Rolling back

Returning to release 45 requires restoring the data-disk snapshot taken before
the upgrade. Replacing only the bundle cannot roll back revision 0041.
Stop the stack and any containers holding pool filesystems, use `si quiesce`,
and confirm it reports no holders or busy loop devices. The data disk must be
unmounted while restored; use a stopped guest if required by the platform.
Restore the disk, restore the release-45 bundle at `/opt/hartmesh`, then start
that bundle with its original environment and read readiness/logs.

Agents can be stopped or their workspace containers removed without changing
storage. Files remain in their Space. Before a bundle swap, inspect and remove
owned holders with the upgrade commands, including `hartmesh-space-` workspaces.

### After a reboot or a Gateway restart

After a VM reboot, fstab and the Gateway's `mount-all` restore the pool mounts.
Recorded missing workspaces are fenced; interrupted Work attempts settle failed
only after original-container absence and no run in flight are confirmed. The
Gateway records attachment and attempt IDs with their reasons, then reports
`agents: ready (…)`. Incomplete recovery stays unavailable and is retried every
60 seconds; the health endpoint still exposes the actual state.

A Gateway-only restart preserves a running idle workspace and its files. It
stops work in flight during graceful shutdown; a forced kill grants no drainage.
On startup the host contains workspaces associated with orphaned runs and settles
their attempts as failed. Work can then be explicitly resumed. Nothing restarts
an assignment automatically. An idle workspace expires after 1800 seconds in
this profile. Restart when people and AI agents are not working.

### Commands

Every command resolves absent keys to their defaults. Keep any tenant-specific
counts and capacities in `.env` during verification, growth and removal;
removing an override restores its default and does not remove existing slots.
`status` checks host prerequisites and validates inventory before reporting the plan.

Each verb emits one sorted JSON document. Exit 0 means success/nothing to do,
1 refusal, and 2 partial or uncertain completion. `prepare --at-start` always
exits zero while retaining the actual return code in its document.

| Command | Effect and limits |
| --- | --- |
| `si prepare` | Check the whole missing plan, allocate/mount missing slots, publish inventory after each, synchronize fstab and verify. Resume an interrupted plan. |
| `si mount-all` | Mount existing listed slots only; report `mounted`, `already`, `failed`. Partial mount failures exit 2. |
| `si fstab-sync` | Rewrite only the marked inventory-derived fstab block; malformed/multiple blocks are refused. |
| `si verify [<slot> ...] [--repair]` | Verify all or selected slots. Without repair, report only; repair restores supported ownership/mount/allocation facts, not lost bytes. |
| `si resize <slot> <GiB>` or `si resize --kind <kind> <GiB>` | Offline growth only. Check complete added allocation and all holders before growth; preserve identity and files, update inventory, remount. Never shrink. |
| `si forget <slot> [--discard]` | Permanently remove a listed slot or unlisted image. Always refuse holders; nonempty slots require `--discard`. No knowledge of SQL bindings. |
| `si quiesce` | Unmount listed/unlisted pool images only when no container holds them; confirm loop devices detached. Do not restore while any holder/busy entry remains. |
| `si status` | Read pool/plan/mount/fstab/disk facts, including absent inventory, without changing them. |

Common refusals include invalid keys, missing host tools/namespace permission,
an inventory outside the pool, and a held lock. `resize`, `forget` and `quiesce`
inspect container processes' mount tables before unmounting: a successful host
unmount alone cannot prove that a container released its own mount.
Stop the Gateway and every actual holder before those offline operations.
`forget --discard` destroys files; it cannot establish that an image is unused
by an application binding. Inspect durable bindings before removing a slot.

The account helper uses the image's Python environment, uid/gid 1000 and its
backend working directory; it does not synchronize or install packages.

| Account command | Effect |
| --- | --- |
| `accounts list` | Account identities, disablement and release facts. |
| `accounts disable --email ADDRESS` | Turn off access while retaining the seat and files. |
| `accounts release-seat --email ADDRESS` | Release an existing disabled account; preserve account/history rows and mark its User Space deleted. |
| `accounts release-seat --id ACCOUNT_UUID` | Retry unfinished cleanup after email release using the stable ID. |

Each invocation emits the account tool's own JSON document. `release-seat`
refuses an enabled account with `error: not_disabled` and exit 1. Exit 2 with
`cleanup_pending: true` means release committed while cleanup is unconfirmed;
retry the same account ID. Its `assistants` counts the personal AI assistants
matched by this release operation, including interrupted creations.

| Agent command | Effect |
| --- | --- |
| `ai list [--all]` | Current identities, custody, files, workspaces and unresolved work; `--all` includes deleted/released agents. |
| `ai status` | Pool plan, execution, hold and actual Gateway-lifetime counts. |
| `ai close-workspace <id> [--end-running-work] [--wait-seconds 120]` | Close exact workspaces and wait for captured run/attempt outcomes. |
| `ai verify` | Verify catalogue slots and report their kind, use and unclaimed/surplus state. |
| `ai export <id> [--end-running-work] [--wait-seconds 120]` | Quiesced tar export under `home/agent-exports`; operator retains/deletes it. |
| `ai supervise <id> --email ADDRESS` | Recover supervision of a company AI employee with no eligible human manager. |
| `ai delete <id> [--end-running-work] [--wait-seconds 120]` | Tombstone an eligible orphan; the running Gateway reclaims its storage. |
| `ai adopt-host --from OLD_ID --acknowledge-old-host-gone` | Acknowledge replacement only after the old host is gone. |
| `ai settle --space ID --operation ID [--discard]` | Inspect/reconcile one interrupted file operation or deliberately record failure. |

#### Output and exit codes

The three tools print exactly one JSON document on stdout for every invocation, on one line, produced by `json.dumps(document, sort_keys=True)`, which is what the account commands do. There is no `--json` flag and no other output format. Anything a tool writes besides the document goes to stderr; `storage-init`'s progress lines are there.

Every document has these fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `command` | string or null | The verb. Null only in the refusal of a command line that named no verb. |
| `returncode` | integer | 0, 1 or 2: what the verb returned. It equals the process's exit status, except for `prepare --at-start`, whose exit status is always 0. |
| `note` | string | One sentence that says what happened, for a person. |
| `error` | string | Present only when `returncode` is 1: the refusal, a refusal or operational failure reported by the verb. |

A document whose `returncode` is 0 or 2 carries every field of its verb's table; a field that does not apply is null, 0 or empty, as its type allows. An exit-1 document carries `command`, `returncode` 1, `error` and `note`, preserving applicable reason/diagnostic fields. A pre-effect refusal uses `refused; nothing was changed`. Known late refusals (`CommandFailed`) and unexpected failures use `failed; run the verb again`: effects may already be committed or unconfirmed. Inspect current state and retained evidence before retrying. For example, this pre-effect refusal changed nothing:

```json
{"command": "close-workspace", "error": "no AI assistant or AI employee has id 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f", "note": "refused; nothing was changed", "returncode": 1}
```

Exit status 0 means the command completed or was already done. Exit status 1
means refusal or operational failure; it does not by itself establish that
nothing changed. Exit status 2 means incomplete or unconfirmed work, with the
remaining facts named in the document. Parser failures (unknown verb, missing or
invalid arguments) return one refusal JSON document, including when no command
could be identified. Known late refusals preserve their error and diagnostic
fields with `failed; run the verb again`; unexpected exceptions additionally
write their exception type/text to stderr. Neither path erases retained evidence.
For example, export may have closed a workspace before a later archive failure,
and deletion may be committed before a subsequent observation fails. Retry the
same intent and re-read its recorded/current state rather than assuming rollback.
A remote runner’s exit code may differ from the command’s; inspect `returncode`.


Ids of agents, Spaces, slots, attachments, assignments, attempts and file operations are 32 hexadecimal characters. Ids of people and of runs are UUIDs. A Docker host is named by the engine's ID, the value `docker info --format '{{.ID}}'` prints. A kind is one of `user`, `assistant`, `employee`, `company`.

`close-workspace`, `export`, `supervise` and `delete` act on a workspace that may be in use. Each refuses while a reply or an assignment of that agent is in flight. A run counts as in flight while the run store holds it unfinished under a lease that has not expired. `--end-running-work` lifts the refusal, and it cancels nothing. The verb closes the workspace first, with the reason `operator`, and the Gateway ends each run of that workspace at the run's next model call: a reply ends with the error `ResidentInterrupted` and the reason `operator`, and an assignment's attempt is settled as failed with the same reason. `--wait-seconds <n>` bounds how long the verb waits for those runs to end and for those attempts to be settled; the default is 120. A run that is waiting for a long answer of the model, or for a tool that does not use the workspace, ends only after that, and is listed as unconfirmed until then. A run whose model was already writing its last answer makes no further model call: it ends with that answer, and its conversation shows no notice. The two flags have the names the account commands use (§ "Sign-in"); only the bound on the wait means the same.

What people read after `--end-running-work`: in a conversation whose reply was cut, "{name}'s workspace was restarted, which stopped this reply. Send your message again."; on an assignment that was running, "Interrupted", "{name}'s workspace was closed for maintenance by {provider} while {name} was working on this.", "What {name} had done is kept in {name}'s files; nothing was submitted for review." and the button Resume. Without the flag nothing is in flight and nobody notices: the next message starts a workspace.

#### `python -m app.gateway.agent_instances`

Run through `ai` (§ "Storage and agents"). The tool runs in the Gateway container as uid 1000, gid 1000, with the Docker socket's group and no capability. It loads `home/config.yaml`, reaches the database, the pool and the Docker daemon, and observes the running Gateway’s process-checked private lifetime receipt for `status`. It does not start or take over a Gateway runtime. The Gateway reads committed changes on subsequent use and maintenance passes; an interval is not a completion guarantee.

Every verb can return these refusals, exit 1:

- `the configuration /srv/hartmesh/home/config.yaml cannot be loaded: <text>`
- `the database cannot be reached: <text>`
- `Storage Spaces are off in the rendered configuration (HARTMESH_USERS absent); there is nothing to act on`, if a configuration outside this Compose profile explicitly disables Storage Spaces; absent keys in this profile use defaults

##### `list`

```bash
ai list
ai list --all
```

It reads every AI assistant and AI employee from the database, each one's Space, the record of its workspace, the state of that container at the Docker daemon, and its runs in flight. It changes nothing. Without `--all` it leaves out the deleted agents whose Space holds no slot any more, and counts them in `deleted_released`.

| Field | Type | Meaning |
| --- | --- | --- |
| `count` | integer | Number of entries in `instances`. |
| `deleted_released` | integer | Deleted agents whose Space holds no slot any more. They are listed only with `--all`. |
| `instances` | array of objects | One per agent, in the order of `created_at`: every one that is not deleted, every deleted one whose Space still holds a slot and, with `--all`, every other deleted one. |
| `instances[].id` | string | The agent's id. |
| `instances[].name` | string | Its name. |
| `instances[].custody` | string | `personal` for an AI assistant, `company` for an AI employee. |
| `instances[].kind` | string | `assistant` or `employee`: the kind of its Space, which follows `custody`. |
| `instances[].status` | string | `provisioning` (its setting up did not finish; its creator sees it under "Not finished"), `active`, `suspended` (shown to people as Paused), `archived` (Retired) or `deleted`. |
| `instances[].created_at` | string | ISO 8601. The Gateway removes a `provisioning` row 24 hours after this time. |
| `instances[].owner_id` | string or null | The owner's account id for an AI assistant; null for an AI employee. |
| `instances[].owner_live` | boolean or null | For an AI assistant, whether its owner's account can sign in: false when that account is turned off, released or gone, which is when the AI assistant no longer works and `delete` is admitted. Null for an AI employee. |
| `instances[].supervisor` | object | `id` (string), `email` (string or null), `live` (boolean: false when that account is turned off, released or gone). |
| `instances[].space` | object | The nine fields that follow. Every agent has a Space from its creation; a `provisioning` row whose creation stopped before the assignment has `space` null. |
| `instances[].space.space_id` | string | The agent's Space. |
| `instances[].space.space_status` | string | `active`, `archived` or `deleted`. The Space of a deleted agent is `deleted` too. |
| `instances[].space.space_generation` | integer | The Space's generation. |
| `instances[].space.slot_id` | string or null | The slot bound to the Space; null once the slot has been freed. |
| `instances[].space.max_bytes` | integer or null | The capacity the Space is bound to; null without a slot. |
| `instances[].space.used_bytes` | integer or null | Bytes in use; null without a slot or when the Space does not verify. |
| `instances[].space.verified` | boolean or null | Whether the Space verifies; null without a slot. |
| `instances[].space.reason` | string or null | The verification text when `verified` is false. |
| `instances[].space.release` | string or null | Null for an agent that is not deleted. For a deleted one: `pending`, the Space is deleted and still holds its slot, and the Gateway frees it by itself; `done`, the Space holds no slot. |
| `instances[].workspace` | object or null | Null when no workspace record is open. Otherwise `attachment_id` (string), `phase` (`pending`, `active` or `fence_pending`), `container_id` (string or null), `container` (`running`, `exited`, `absent` or `unknown`), `host_id` (string: the Docker host that started it). `unknown` preserves a foreign host's unobservable container state or a preparation that returned no container ID; neither proves absence. Such records remain visible. |
| `instances[].execution` | string or null | `ready` or `unavailable`, for this agent; null for a `provisioning` or `deleted` one. |
| `instances[].execution_reason` | string or null | `host-replaced`, `space-unverified`, `operator-hold` or `storage-unavailable` when `execution` is `unavailable`. |
| `instances[].runs_in_flight` | integer | Replies and assignments in flight. |
| `instances[].attempts_unresolved` | integer | Assignment attempts that are neither finished nor settled. |
| `instances[].last_run_at` | string or null | When its last run started, ISO 8601. |

Example:

```json
{"command": "list", "count": 2, "deleted_released": 0, "instances": [{"attempts_unresolved": 0, "created_at": "2026-10-20T08:31:07+00:00", "custody": "company", "execution": "ready", "execution_reason": null, "id": "4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f", "kind": "employee", "last_run_at": "2026-11-03T09:14:02+00:00", "name": "Finance analyst", "owner_id": null, "owner_live": null, "runs_in_flight": 1, "space": {"max_bytes": 1073741824, "reason": null, "release": null, "slot_id": "5b6c7d8e9f0a4b1c8d2e3f4a5b6c7d8e", "space_generation": 4, "space_id": "8e21f4a6c0d94b7e8a5f3c1d2e6b9a70", "space_status": "active", "used_bytes": 429496729, "verified": true}, "status": "active", "supervisor": {"email": "pat@example.com", "id": "1f9b6a2c-4d3e-4f5a-8b7c-9d0e1f2a3b4c", "live": true}, "workspace": {"attachment_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "container": "running", "container_id": "9b0e6c1d2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c", "host_id": "7b9c1e6f-4d2a-4c5e-9f0a-1b2c3d4e5f60", "phase": "active"}}, {"attempts_unresolved": 0, "created_at": "2026-10-22T10:05:44+00:00", "custody": "personal", "execution": null, "execution_reason": null, "id": "9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d", "kind": "assistant", "last_run_at": "2026-11-01T15:40:12+00:00", "name": "Researcher", "owner_id": "6a7c8d9e-0f1a-4b2c-8d3e-4f5a6b7c8d9e", "owner_live": true, "runs_in_flight": 0, "space": {"max_bytes": 1073741824, "reason": null, "release": "pending", "slot_id": "7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a", "space_generation": 3, "space_id": "2c9d4e5f6a7b4c8d9e0f1a2b3c4d5e6f", "space_status": "deleted", "used_bytes": 1048576, "verified": true}, "status": "deleted", "supervisor": {"email": "lee@example.com", "id": "6a7c8d9e-0f1a-4b2c-8d3e-4f5a6b7c8d9e", "live": true}, "workspace": null}], "note": "2 agents listed: 1 AI employee active, 1 AI assistant deleted whose slot is being freed; 1 workspace running; 1 run in flight; 0 deleted left out", "returncode": 0}
```

Refusal, exit 1, beside the common ones: `the Docker daemon did not answer: <text>`.

##### `status`

```bash
ai status
```

It independently observes the rendered plan, inventory and verified slots,
the Docker daemon, pool byte-coherence probe and hold file. Its process-checked
private Gateway receipt supplies the actual running lifetime used for counters;
missing or stale lifetime evidence refuses the command. It does not initialize
or inspect the live resident registry’s complete admission/accounting gate. Its
`execution` is derived from storage, configured counts and hold, and therefore
cannot certify that a new run will be admitted or reproduce live `/health` or
`/api/features` readiness. A run request still undergoes current admission.
Status exits 0 when it can provide this snapshot, including unavailable storage;
the document reports the state.

For each kind it reports the one allowance the Gateway, `/api/features` and the creation refusals all read: `prepared` is the number of slots of the kind in the inventory; `allowed_key` is what the keys call for, `HARTMESH_USERS` for `user`, `HARTMESH_USERS × HARTMESH_AI_ASSISTANTS_PER_USER` for `assistant`, `HARTMESH_AI_EMPLOYEES` for `employee`, 1 for `company`; `in_use` is, for `user`, the seated accounts, and for an agent kind the agents of that kind that are not deleted; `allowed` is the smaller of `prepared` and the larger of `allowed_key` and `in_use`; `free` is `allowed` less `in_use`. So a key lowered below use leaves `allowed` at `in_use`, a key lowered above use leaves surplus slots prepared, and a plan not finished leaves `allowed` at `prepared`. Screens show `allowed`; nothing shows `prepared`.

A Docker daemon that does not answer from inside the container is not a refusal of this verb: it is the state `unavailable` with the code `admission-failed`, and the fields that need the daemon are null.

| Field | Type | Meaning |
| --- | --- | --- |
| `storage` | string | `ready`, `incomplete` or `unavailable`, independently derived from the current storage snapshot. |
| `storage_reason` | string or null | With `unavailable`: `disk-too-small`, `storage-pool-missing`, `inventory-unreadable`, `host-unqualified` or `admission-failed`. |
| `execution` | string | `ready`, `none`, `held` or `unavailable`, derived from storage/plan/hold; not proof of the live Gateway admission gate. |
| `reason` | string or null | With `unavailable`: `storage-unavailable`, and the storage reason is in `storage_reason`; with `none`: `agents-none`; with `held`: `operator-hold`. |
| `reason_text` | string or null | The text that follows the code in the start-up line. |
| `spaces` | object | One object per kind, `user`, `assistant`, `employee`, `company`, each with the seven fields that follow. |
| `spaces.<kind>.allowed` | integer | The allowance of the kind, as above. For `assistant` it is the deployment's total; the per-person count is `per_user`. |
| `spaces.<kind>.allowed_key` | integer | What the keys call for. |
| `spaces.<kind>.prepared` | integer | Slots of the kind in the inventory. |
| `spaces.<kind>.in_use` | integer | Seated accounts, or agents of the kind that are not deleted, or 1 for the company once its Space exists. |
| `spaces.<kind>.free` | integer | `allowed` less `in_use`. |
| `spaces.<kind>.unverified` | integer | Slots of the kind that fail verification. |
| `spaces.<kind>.capacity_bytes` | integer | The capacity the key sets for new Spaces of the kind. |
| `per_user` | integer | `HARTMESH_AI_ASSISTANTS_PER_USER`. |
| `unclaimed` | object | Kind to count: slots bound to no Space that hold files or fail verification. Each is a slot to repair or to reclaim (§ "Full Spaces, the data disk, and unverified Spaces"). None of them is counted in `free`. |
| `surplus` | object | Kind to count: slots beyond `allowed_key` that are bound to nothing. |
| `reserved_bytes` | integer | The sum of the sizes of every slot in the inventory. |
| `required_bytes` | integer | What the keys call for, at their capacities. |
| `headroom_bytes` | integer | The larger of 4 GiB and 10 % of the data disk. |
| `data_disk` | object | `size_bytes`, `free_bytes` and `free_inodes` (integers). |
| `shortfall` | object or null | When the plan is not complete: `missing` (kind to count), `missing_bytes`, `fits` (boolean). Null when every Space the keys call for exists. |
| `hold` | object or null | When the hold file exists: `since` (string, ISO 8601, the file's modification time) and `reason` (string or null, its first line). Null otherwise. |
| `instances` | object | The number of agents in each status, by kind: `assistant` and `employee`, each with `provisioning`, `active`, `suspended`, `archived`, `deleted` (integers). |
| `releases` | object | `pending` (integer: deleted Spaces of every kind whose slot the Gateway is still to free). |
| `sandbox_slots` | object | `limit` (integer: `sandbox.replicas`), `workspaces_running` and `chat_sandboxes_running` (integers, or null when the daemon did not answer: running containers named `hartmesh-space-…` and `deer-flow-sandbox-…`). |
| `runs_in_flight` | integer | Replies and assignments of agents in flight. |
| `attachments_active` | integer | Workspace records that are not closed. |
| `attempts_unresolved` | integer | Assignment attempts neither finished nor settled. |
| `file_operations_pending` | integer | File operations and publications on Spaces of every kind with no recorded outcome. |
| `since_gateway_start` | object | `at` (string, ISO 8601: when the running Gateway started), `attachments_fenced` (object: fence reason to count, for records that the Gateway or an operator command closed since then), `work_attempts_settled` (integer), `file_operations_settled` (integer). |
| `host_id` | string or null | This VM's Docker engine ID; null when the daemon did not answer. |
| `foreign_hosts` | array of objects | Other Docker hosts named by workspace records that are not closed: `host_id` (string), `attachments` (integer), `adopted` (boolean: whether `adopt-host` has recorded that host as replaced by this one). Empty when `host_id` is null. |
| `runtime` | string | The sandbox runtime, from `SANDBOX_RUNTIME`, or `default` when the key is unset. |

Example:

```json
{"attachments_active": 1, "attempts_unresolved": 0, "command": "status", "data_disk": {"free_bytes": 13314398617, "free_inodes": 3041203, "size_bytes": 52721533747}, "execution": "ready", "file_operations_pending": 0, "foreign_hosts": [], "headroom_bytes": 5272153374, "hold": null, "host_id": "7b9c1e6f-4d2a-4c5e-9f0a-1b2c3d4e5f60", "instances": {"assistant": {"active": 4, "archived": 0, "deleted": 1, "provisioning": 0, "suspended": 0}, "employee": {"active": 3, "archived": 0, "deleted": 2, "provisioning": 0, "suspended": 0}}, "note": "storage ready: users 7 of 10 seats, AI assistants 4 of 10, AI employees 3 of 10, company 1 of 1; 31 spaces prepared, 31 verified; agents ready; 1 workspace running of 2 sandbox slots; 1 run in flight", "per_user": 1, "reason": null, "reason_text": null, "releases": {"pending": 1}, "required_bytes": 33285996544, "reserved_bytes": 33285996544, "returncode": 0, "runs_in_flight": 1, "runtime": "runsc", "sandbox_slots": {"chat_sandboxes_running": 0, "limit": 2, "workspaces_running": 1}, "shortfall": null, "since_gateway_start": {"at": "2026-11-03T08:00:11+00:00", "attachments_fenced": {"container-absent": 1, "idle": 1, "run-orphaned": 1}, "file_operations_settled": 0, "work_attempts_settled": 1}, "spaces": {"assistant": {"allowed": 10, "allowed_key": 10, "capacity_bytes": 1073741824, "free": 6, "in_use": 4, "prepared": 10, "unverified": 0}, "company": {"allowed": 1, "allowed_key": 1, "capacity_bytes": 1073741824, "free": 0, "in_use": 1, "prepared": 1, "unverified": 0}, "employee": {"allowed": 10, "allowed_key": 10, "capacity_bytes": 1073741824, "free": 7, "in_use": 3, "prepared": 10, "unverified": 0}, "user": {"allowed": 10, "allowed_key": 10, "capacity_bytes": 1073741824, "free": 3, "in_use": 7, "prepared": 10, "unverified": 0}}, "storage": "ready", "storage_reason": null, "surplus": {"assistant": 0, "company": 0, "employee": 0, "user": 0}, "unclaimed": {"assistant": 0, "company": 0, "employee": 0, "user": 0}}
```

Refusals, exit 1: the common ones only.

##### `close-workspace`

```bash
ai close-workspace 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f
ai close-workspace 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f --end-running-work --wait-seconds 60
```

It removes one agent's workspace container. It starts nothing: the next message or assignment starts a new workspace, in about half a minute. Use it before `resize` of that agent's slot, when a workspace misbehaves, or when the agent's Space must be still. In order:

1. It finds the agent, whatever its status.
2. It reads the workspace records of its Space that are not closed. With none, it stops: verdict `already_closed`, exit 0.
3. It reads the runs of that agent that are in flight. With one or more and no `--end-running-work`, it refuses.
4. For each record, it removes the workspace container, its relay and its two networks, requires the Docker daemon to confirm that the container is gone, and marks the record closed with the reason `operator`, this Docker host as the one that closed it, and the time. It cancels no run and starts no container.
5. It waits, for at most `--wait-seconds`, until every run found in step 3 has ended and every assignment attempt of that agent that was unfinished is settled. It settles nothing itself. The Gateway does both: it ends each run of the closed workspace at the run's next model call and then settles its attempt as failed with the reason `operator`; an unfinished attempt that has no run in flight is settled at the Gateway's next pass, within 60 seconds. A run that is waiting for a long answer of the model, or for a tool that does not use the workspace, ends only after that; when the wait runs out first, it is listed in `runs_unconfirmed`.

| Field | Type | Meaning |
| --- | --- | --- |
| `instance_id` | string | The agent. |
| `verdict` | string | `closed`; `already_closed`; or `unconfirmed`, when the daemon did not confirm a removal. |
| `attachments` | array of objects | Each record this command closed: `attachment_id` (string), `container_id` (string or null), `fence_reason` (string: `operator`, or `host-replaced` for a record of a Docker host that `adopt-host` has recorded as replaced), `fenced_at` (string, ISO 8601). |
| `attachments_unconfirmed` | array of strings | Ids of the records whose container the daemon did not confirm gone. They stay open. |
| `runs_found` | integer | Runs in flight when the command started. |
| `runs_ended` | integer | Of those, the runs that had ended when the command returned. |
| `runs_unconfirmed` | array of strings | Ids of the runs that had not ended when the wait ran out. |
| `attempts_settled` | array of objects | The assignment attempts the Gateway settled during the wait: `attempt_id`, `work_id` (strings), `run_id` (string or null), `reason` (string or null: the stored containment reason; null when the attempt completed normally during the wait). |
| `attempts_unconfirmed` | array of strings | Ids of the attempts still unfinished when the wait ran out. |
| `waited_seconds` | number | How long step 5 waited. |

Example, with `--end-running-work` while an assignment was running:

```json
{"attachments": [{"attachment_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "container_id": "9b0e6c1d2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c", "fence_reason": "operator", "fenced_at": "2026-11-03T09:20:41+00:00"}], "attachments_unconfirmed": [], "attempts_settled": [{"attempt_id": "77aa0b1c2d3e4f5a8b6c7d8e9f0a1b2c", "reason": "operator", "run_id": "0c3e9a1b-2d4f-4a6c-8e0b-1d3f5a7c9e2b", "work_id": "5d10a2b3c4d54e6f8a7b9c0d1e2f3a4b"}], "attempts_unconfirmed": [], "command": "close-workspace", "instance_id": "4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f", "note": "workspace of 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f closed: 1 container removed; 1 run ended; 1 attempt settled as failed (operator); the next message starts a new workspace", "returncode": 0, "runs_ended": 1, "runs_found": 1, "runs_unconfirmed": [], "verdict": "closed", "waited_seconds": 3.4}
```

Refusals, exit 1, beside the common ones:

- `no AI assistant or AI employee has id 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f has 1 run in flight; wait for it to end or pass --end-running-work`
- `attachment a1b2c3d4e5f60718293a4b5c6d7e8f90 was started by Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f, not this one; see adopt-host`
- `the Docker daemon did not answer: <text>`

Exit 2, with what was done in the document:

- a run in `runs_unconfirmed` or an attempt in `attempts_unconfirmed`: the workspace is closed, and the note ends `1 run had not ended after 120 s; the Gateway settles its attempt when it ends; run the command again to see`. Such a run is waiting for the model or for a tool that does not use the workspace, and the Gateway ends it at its next model call.
- verdict `unconfirmed`, with the note `the Docker daemon did not confirm that container 9b0e6c1d2f3a is gone; its record is not marked closed; run the command again`: the removal was asked for and its outcome is unknown.

##### `verify`

```bash
ai verify
```

It does what the Gateway does at its start, in a process of its own, as the Gateway's user: it loads the inventory, verifies every slot, and runs the proof that the Docker daemon reads the pool, which starts and removes one short-lived container named `hartmesh-probe-<nonce>`. It also says what each slot is used for, from the database, and compares the inventory with the plan of the keys. It changes nothing else, and it never runs `storage-init`. Use it after a repair, before restarting the Gateway, to find a surplus or unclaimed slot, and to read a Space's `used_bytes`.

| Field | Type | Meaning |
| --- | --- | --- |
| `pool` | object | `path` (string), `probe` (`ok` or `failed`), `probe_error` (string or null). |
| `plan` | object | Kind to `{allowed_key, prepared, in_use}`, as `status` computes them. |
| `spaces` | array of objects | One per slot of the inventory, and one per slot that a Space is bound to and the inventory no longer lists. The twelve fields that follow. |
| `spaces[].slot_id` | string | The slot. |
| `spaces[].kind` | string | Its kind from the inventory; null for a slot the inventory no longer lists. |
| `spaces[].listed` | boolean | Whether the inventory lists it. |
| `spaces[].state` | string | `verified` or `unverified`. |
| `spaces[].reason` | string or null | The verification text when `state` is `unverified` (§ "Full Spaces, the data disk, and unverified Spaces"). |
| `spaces[].use` | string | `user`, `assistant`, `employee` or `company`: bound to a Space of that kind that is not deleted; `releasing`: bound to a deleted Space, which the Gateway frees by itself; `surplus`: bound to nothing, verified and empty, beyond what the key of its kind calls for; `unclaimed`: bound to nothing and holding files or unverified; a free slot within the key is `free`. |
| `spaces[].holder_id` | string or null | The account id, the agent's id or `company`, for a bound slot. |
| `spaces[].space_id` | string or null | The Space the slot is bound to. |
| `spaces[].space_status` | string or null | That Space's status: `active`, `archived` or `deleted`. |
| `spaces[].used_bytes` | integer or null | Bytes in use; null when the slot does not verify. |
| `spaces[].max_bytes` | integer | The slot's capacity. |
| `spaces[].empty` | boolean or null | Whether `data/` and `control/` are both empty; null when the slot does not verify. |
| `verified` | integer | Listed slots that verified. |
| `total` | integer | Slots in the inventory. |
| `unclaimed`, `surplus` | integers | Slots whose `use` is `unclaimed`, and `surplus`. |

Example:

```json
{"command": "verify", "plan": {"assistant": {"allowed_key": 10, "in_use": 4, "prepared": 10}, "company": {"allowed_key": 1, "in_use": 1, "prepared": 1}, "employee": {"allowed_key": 10, "in_use": 3, "prepared": 10}, "user": {"allowed_key": 10, "in_use": 7, "prepared": 10}}, "note": "30 of 31 spaces verified; 1 unclaimed; 0 surplus; the Docker daemon reads the pool", "pool": {"path": "/srv/hartmesh/spaces", "probe": "ok", "probe_error": null}, "returncode": 2, "spaces": [{"empty": false, "holder_id": "1f9b6a2c-4d3e-4f5a-8b7c-9d0e1f2a3b4c", "kind": "user", "listed": true, "max_bytes": 1073741824, "reason": null, "slot_id": "3f2a9c0d5b7e4f1a8c6d2e0b9a7f5c31", "space_id": "0b1c2d3e4f5a4b6c8d7e9f0a1b2c3d4e", "space_status": "active", "state": "verified", "use": "user", "used_bytes": 214748364}, {"empty": null, "holder_id": null, "kind": "employee", "listed": true, "max_bytes": 1073741824, "reason": "Backing must be an independent provider-mounted ext4 filesystem with nodev/nosuid", "slot_id": "6e7f8a9b0c1d4e2f8a3b4c5d6e7f8a9b", "space_id": null, "space_status": null, "state": "unverified", "use": "unclaimed", "used_bytes": null}], "surplus": 0, "total": 31, "unclaimed": 1, "verified": 30}
```

The example lists two of the 31 entries. Exit 0 when every slot verified and the proof passed. Exit 2 when a slot is `unverified` or `probe` is `failed`.

Refusals, exit 1, beside the common ones:

- `no inventory at /srv/hartmesh/spaces/inventory.v2.json; run: storage-init prepare`
- `the inventory cannot be loaded: <the Gateway's text>`

##### `export`

```bash
ai export 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f
ai export 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f --end-running-work
```

It writes one agent's Space to a tar file on the data disk. It acts on agents alone; a person's files are the person's "Download all my data", and Shared is in the snapshot. In order:

1. It finds the agent and its Space, requires a status other than `deleted`, and verifies the Space.
2. It refuses unless the data disk keeps 1 GiB free after an archive as large as the Space's used bytes, which is the rule of the account export (§ "Download all my data").
3. It closes the workspace exactly as `close-workspace` does in its steps 2 to 5, with the same refusal while a run is in flight and the same two flags.
4. Holding the Space's row lock, so that no workspace starts meanwhile, it archives everything under `data/` into `/srv/hartmesh/home/agent-exports/<id>-<UTC time>.tar`, mode `0600`, owner 1000, without following a link out of the Space, and computes the SHA-256. New workspace admission and Gateway file publication serialize behind this Space lock. An upload may be received/spooled meanwhile, but its publication waits until the export’s lock is released; it is not promised inclusion in that archive.

The directory is deliberately not `home/exports/`, which the Gateway empties at every start. Nothing deletes an export; remove it once it is copied.

| Field | Type | Meaning |
| --- | --- | --- |
| `instance_id` | string | The agent. |
| `verdict` | string | `exported`. |
| `space` | object | `space_id`, `slot_id` and `kind` (strings). |
| `path` | string | The archive. |
| `bytes` | integer | Its size. |
| `files` | integer | Entries archived. |
| `sha256` | string | Its SHA-256, 64 hexadecimal characters. |
| `workspace_closed` | boolean | Whether a workspace record was open and this command closed it. |
| `attachments` | array of objects | The records this command closed, as for `close-workspace`. |
| `attachments_unconfirmed` | array of strings | Always empty in a document of this verb: a removal the daemon did not confirm is a refusal here. |
| `runs_found` | integer | Runs in flight when the command started. |
| `runs_ended` | integer | Of those, the runs that had ended when the command returned. |
| `runs_unconfirmed` | array of strings | Ids of the runs that had not ended when the wait ran out. |
| `attempts_settled` | array of objects | As for `close-workspace`. |
| `attempts_unconfirmed` | array of strings | As for `close-workspace`. |
| `waited_seconds` | number | How long the wait of step 3 lasted. |

Example:

```json
{"attachments": [{"attachment_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "container_id": "9b0e6c1d2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c", "fence_reason": "operator", "fenced_at": "2026-11-03T09:25:01+00:00"}], "attachments_unconfirmed": [], "attempts_settled": [], "attempts_unconfirmed": [], "bytes": 432643891, "command": "export", "files": 1820, "instance_id": "4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f", "note": "files of 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f exported: 1820 entries, 412.6 MiB; nothing deletes this file", "path": "/srv/hartmesh/home/agent-exports/4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f-20261103T092502Z.tar", "returncode": 0, "runs_ended": 0, "runs_found": 0, "runs_unconfirmed": [], "sha256": "9d1f0a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7", "space": {"kind": "employee", "slot_id": "5b6c7d8e9f0a4b1c8d2e3f4a5b6c7d8e", "space_id": "8e21f4a6c0d94b7e8a5f3c1d2e6b9a70"}, "verdict": "exported", "waited_seconds": 0.0, "workspace_closed": true}
```

Refusals, exit 1, beside the common ones; a refusal after step 3 leaves the workspace closed and no file:

- `no AI assistant or AI employee has id 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f has no files yet (status provisioning)`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f is deleted; its files were deleted with it`
- `space 8e21f4a6c0d94b7e8a5f3c1d2e6b9a70 (slot 5b6c7d8e9f0a4b1c8d2e3f4a5b6c7d8e) is unverified: <text>; see storage-init verify`
- `the archive needs 0.4 GiB and would leave the data disk under 1 GiB free`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f has 1 run in flight; wait for it to end or pass --end-running-work`
- `attachment a1b2c3d4e5f60718293a4b5c6d7e8f90 was started by Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f, not this one; see adopt-host`
- `the Docker daemon did not confirm that container 9b0e6c1d2f3a is gone; nothing was exported; run the command again`
- `the Docker daemon did not answer: <text>`
- `Backup does not support special filesystem nodes`: the Space holds a device file or a named pipe.
- `Backup exceeds the supported entry count`: the Space holds more than 100,000 entries.

Exit 2: a run in `runs_unconfirmed` or an attempt in `attempts_unconfirmed`. The archive is written and complete, because the workspace was closed before it was made; only the Gateway's settlement had not finished when the wait ran out.

An exit-1 failure after workspace closure does not reopen it or roll back earlier
effects. The operational-failure note preserves the original error and fields.
A successfully published archive is retained even if later Gateway settlement
remains unconfirmed; use its recorded path/hash and current command state.

##### `supervise`

```bash
ai supervise 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f --issuer https://login.example.com --subject sub-lee
ai supervise 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f --email lee@example.com
```

It is for one situation: an AI employee that nobody at the customer can manage any more, because its supervisor and every other manager have left and their accounts are turned off or released. While one manager can still sign in, the supervisor is changed in the product, not here. An AI assistant has no supervisor to replace: it stops with its owner's account, and `release-seat` or `delete` removes it. The person is named as the account commands name one: by the identity provider's issuer and subject, or by `--email`, which must resolve to exactly one account. It also takes `--end-running-work` and `--wait-seconds`. In order:

1. It finds the agent and requires company custody and the status `active`, `suspended` or `archived`.
2. It resolves the person and requires an account that can sign in.
3. If that person is already the supervisor and holds Manage, it stops: verdict `unchanged`, exit 0.
4. If a workspace record of the Space is open, it closes the workspace exactly as `close-workspace` does in its steps 3 to 5, with the reason `operator`, the same refusal while a run is in flight and the same two flags. The grant of the next step can be written only while no workspace is open.
5. By the host's authority it gives that person what the supervisor named at creation receives, in two writes that a repeat of the command completes if it is interrupted between them. First it gives Use, Inspect and Manage on the AI employee. Then, under the Space's row lock, it gives read, write, export and administer on the Space: the Space's generation advances by one, and its event row is a `grant` whose actor is that person and whose details carry `operator_host_id`, this VM's Docker engine ID. The order is fixed: a person may read an agent's files only with "Can see all work" on that agent, and the grant on the Space is refused without it. This is the only authority an operator command creates, and the only grant on any Space that is not written by the Gateway itself.
6. As that person, it performs the product's own change of supervisor, the lifecycle action `supervise`, which records the change with that person as its actor and leaves the AI employee in the status it had. A workspace that someone started between steps 4 and 6 is closed by that action, with the reason `lifecycle`.

If step 6 cannot complete because an earlier lifecycle operation of that AI employee is still pending, the command stops with verdict `manager_added` and exit 2: the person now holds Manage, signs in, resolves the pending operation on the AI employee's Settings tab and changes the supervisor there.

Implementation clarification: recovery records an immutable Space event binding the exact operator host, target, original instance/Space/backing/grants and selected lifecycle operation. It reserves generation room before granting authority, including a possible terminal handoff event. Every phase repeats current account eligibility under the owning transaction. An exact completed authority pair can permit a later independent recovery only after full current orphan eligibility, with no exemption for the earlier target; existing pending lifecycle operations remain intact. A terminal handoff event supports retries of `manager_added` without granting authority again. A known refusal after committed closure or grants uses the operational-failure envelope (`failed; run the verb again`), preserving evidence and avoiding a claim that nothing changed.

| Field | Type | Meaning |
| --- | --- | --- |
| `instance_id` | string | The AI employee. |
| `verdict` | string | `changed`, `unchanged` or `manager_added`. |
| `previous` | object | The supervisor before: `id` (string), `email` (string or null), `live` (boolean). |
| `supervisor` | object | The person named: `id`, `email` (strings), `issuer` and `subject` (strings or null for a local account). |
| `grants` | object | `instance` (integer: normally 7, Use 1, Inspect 2, Manage 4) and `space` (integer: normally 27, read 1, write 2, administer 8, export 16). With `unchanged`, report the actual current bits without adding permissions. |
| `generation` | integer | The AI employee's generation afterwards. |
| `lifecycle_operation_id` | string or null | The id of the recorded change; null with `unchanged` and `manager_added`. |
| `workspace_closed` | boolean | Whether a workspace record was open and this command closed it. |
| `attachments` | array of objects | The records this command closed, as for `close-workspace`. |
| `attachments_unconfirmed` | array of strings | Always empty in a document of this verb: a removal the daemon did not confirm is a refusal here. |
| `runs_found` | integer | Runs in flight when the command started. |
| `runs_ended` | integer | Of those, the runs that had ended when the command returned. |
| `runs_unconfirmed` | array of strings | Ids of the runs that had not ended when the wait ran out. |
| `attempts_settled` | array of objects | As for `close-workspace`. |
| `attempts_unconfirmed` | array of strings | As for `close-workspace`. |
| `waited_seconds` | number | How long the wait of step 4 lasted. |

Example:

```json
{"attachments": [], "attachments_unconfirmed": [], "attempts_settled": [], "attempts_unconfirmed": [], "command": "supervise", "generation": 6, "grants": {"instance": 7, "space": 27}, "instance_id": "4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f", "lifecycle_operation_id": "c7d8e9f0a1b24c3d8e5f6a7b8c9d0e1f", "note": "supervisor of 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f changed; the new supervisor holds Use, Inspect and Manage and can administer the files", "previous": {"email": "pat@example.com", "id": "1f9b6a2c-4d3e-4f5a-8b7c-9d0e1f2a3b4c", "live": false}, "returncode": 0, "runs_ended": 0, "runs_found": 0, "runs_unconfirmed": [], "supervisor": {"email": "lee@example.com", "id": "6a7c8d9e-0f1a-4b2c-8d3e-4f5a6b7c8d9e", "issuer": "https://login.example.com", "subject": "sub-lee"}, "verdict": "changed", "waited_seconds": 0.0, "workspace_closed": false}
```

Refusals, exit 1, beside the common ones:

- `no AI assistant or AI employee has id 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f is an AI assistant: it stops with its owner's account and cannot be handed to another person; its owner can make an AI employee with the same brief, and release-seat or delete removes it once the owner's account is turned off`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f is deleted; supervise acts on an AI employee that is active, paused or retired`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f is not set up yet (status provisioning)`
- `no account exists for subject 'sub-lee' at issuer 'https://login.example.com'`
- `2 accounts have subject 'sub-lee' at this issuer; address one of them by --email`
- `no account, or more than one, holds the address 'lee@example.com'`
- `the account of subject 'sub-lee' is turned off; a supervisor must be able to sign in`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f has 1 run in flight; wait for it to end or pass --end-running-work`
- `attachment a1b2c3d4e5f60718293a4b5c6d7e8f90 was started by Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f, not this one; see adopt-host`
- `the Docker daemon did not confirm that container 9b0e6c1d2f3a is gone; nothing was granted; run the command again`
- `the Docker daemon did not answer: <text>`
- `give --issuer and --subject, or --email`

Exit 2: verdict `manager_added`, with the note `the person holds Manage on 4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f; a lifecycle operation is pending, so the supervisor is unchanged; the person resolves it in the product`; or a run in `runs_unconfirmed` or an attempt in `attempts_unconfirmed`, with the supervisor changed.

##### `delete`

```bash
ai delete 9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d
ai delete 9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d --end-running-work --wait-seconds 60
```

It is for an agent that nobody at the customer can delete. For an AI assistant that is one whose owner's account is turned off or released: the AI assistant stops working with the account, cannot be handed to another person, and holds its slot until it is deleted; `release-seat` deletes it with the seat, and this verb deletes it alone when the seat is to be kept. For an AI employee it is one whose supervisor and every manager have left; `supervise` is the usual answer, which gives it a manager who decides, and `delete` is for an AI employee the customer's administrator asks to have removed outright while nobody can sign in to do it. The verb does what Delete does in the product, by the host's authority, and for no agent that someone at the customer can still delete. Nothing brings a deleted agent back, so run `export` first when the customer wants the files. It also takes `--end-running-work` and `--wait-seconds`. In order:

1. It finds the agent.
2. For an AI assistant it requires that the owner's account cannot sign in, because it is turned off, released or gone; otherwise it refuses with the reason `owner-active`. For an AI employee it requires that the supervisor and every holder of Manage cannot sign in; otherwise it refuses with the reason `managers-active`.
3. It refuses an agent whose setting up did not finish. For one that is already deleted it goes on with step 6.
4. It closes the workspace exactly as `close-workspace` does in its steps 2 to 5, with the reason `operator`, the same refusal while a run is in flight and the same two flags. When the wait runs out while a run has not ended or an attempt is not settled, it stops there: verdict `workspace_closed`, exit 2, and nothing is deleted. Running it again continues from there.
5. By the host's authority, in one transaction, it marks the agent and its Space deleted. The deletion is recorded as an act of the host on the owner's or the supervisor's behalf, not as an act of a person. From this moment the agent is in nobody's list and counts toward no number.
6. It does not free the slot itself. The running Gateway does, at the next pass of its 60-second task, exactly as it completes a deletion that a person started: it empties the slot and removes its tie to the Space, and the slot is then free for the next agent of its kind. The verb waits for that, for at most `--wait-seconds` again, and reports what it finds. When the Gateway has freed the slot it writes `agents: assistant 9a8b… deleted by the operator; space 7d8e… released`, or `employee` for one.

The Gateway frees the slot, and not the verb, because the Gateway keeps its own list of the slots in use: a slot freed behind it would stay taken until its next restart. For the same reason the release waits while storage is `unavailable` for the deployment (§ "Reading the start-up lines").

A run in flight is treated as by every verb that closes a workspace. Without `--end-running-work` the verb refuses while a reply or an assignment of that agent is in flight, and nothing is changed. With it, the verb closes the workspace with the reason `operator`, the Gateway ends each run at the run's next model call and settles its attempt as failed with that reason, and the deletion goes ahead only once every such run has ended and every such attempt is settled. An AI assistant starts no run while its owner's account is turned off, so only work that was in flight at that moment is concerned.

Afterwards `list` shows the agent only with `--all`. While its slot is not free yet, `list` still shows it without the flag, with `"release": "pending"`.

| Field | Type | Meaning |
| --- | --- | --- |
| `instance_id` | string | The agent. |
| `kind` | string | `assistant` or `employee`. |
| `verdict` | string | `deleted`; `already_deleted`, when it was deleted before this run; or `workspace_closed`, when the work had not ended within the wait and nothing was deleted. |
| `owner` | object or null | For an AI assistant, the owner, whose account cannot sign in: `id` (string), `email` (string or null). Null for an AI employee. |
| `space` | object | The four fields that follow. |
| `space.space_id` | string | The agent's Space. |
| `space.slot_id` | string or null | The slot the Space held when the command started; null when it held none any more. |
| `space.release` | string or null | As in `list`: `done`, the Space holds no slot any more; `pending`, the Gateway had not freed the slot when the wait ran out. Null with `workspace_closed`. |
| `space.use` | string or null | What `verify` says of that slot when the command returns: `free`, emptied and free for its kind; `assistant` or `employee`, nothing was deleted, or another agent has taken the slot already; `releasing`, not freed yet; `unclaimed`, released without being emptied. Null without a slot. |
| `workspace_closed` | boolean | Whether a workspace record was open and this command closed it. |
| `attachments` | array of objects | The records this command closed, as for `close-workspace`. |
| `attachments_unconfirmed` | array of strings | Always empty in a document of this verb: a removal the daemon did not confirm is a refusal here. |
| `runs_found` | integer | Runs in flight when the command started. |
| `runs_ended` | integer | Of those, the runs that had ended when the command returned. |
| `runs_unconfirmed` | array of strings | Ids of the runs that had not ended when the wait ran out. |
| `attempts_settled` | array of objects | As for `close-workspace`. |
| `attempts_unconfirmed` | array of strings | As for `close-workspace`. |
| `waited_seconds` | number | How long the waits of steps 4 and 6 lasted together. |

Example:

```json
{"attachments": [], "attachments_unconfirmed": [], "attempts_settled": [], "attempts_unconfirmed": [], "command": "delete", "instance_id": "9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d", "kind": "assistant", "note": "AI assistant 9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d deleted; slot 7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a emptied and free for the next AI assistant", "owner": {"email": "sam@example.com", "id": "3d4e5f6a-7b8c-4d9e-8f0a-1b2c3d4e5f6a"}, "returncode": 0, "runs_ended": 0, "runs_found": 0, "runs_unconfirmed": [], "space": {"release": "done", "slot_id": "7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a", "space_id": "2c9d4e5f6a7b4c8d9e0f1a2b3c4d5e6f", "use": "free"}, "verdict": "deleted", "waited_seconds": 41.0, "workspace_closed": false}
```

Exit 0: verdict `deleted` or `already_deleted`, with `space.release` `done` and a `space.use` other than `unclaimed`.

Refusals, exit 1, beside the common ones. A late failure can leave a workspace closed or deletion committed; `failed; run the verb again` preserves that uncertainty. Inspect the current agent and release state before retrying:

- `no AI assistant or AI employee has id 9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d`
- `9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d is an AI assistant and its owner's account is active: the owner deletes it in the product`; the document also carries `reason` with the value `owner-active`
- `4f1c2d7e9a0b4c6d8e1f3a5b7c9d0e2f is an AI employee with a manager who can sign in: a manager deletes it in the product`; the document also carries `reason` with the value `managers-active`
- `9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d is not set up yet (status provisioning); the Gateway removes it 24 hours after it was created`
- `9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d has 1 run in flight; wait for it to end or pass --end-running-work`
- `attachment a1b2c3d4e5f60718293a4b5c6d7e8f90 was started by Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f, not this one; see adopt-host`
- `the Docker daemon did not confirm that container 9b0e6c1d2f3a is gone; nothing was deleted; run the command again`
- `the Docker daemon did not answer: <text>`
- `9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d was not deleted: <text>`: the deletion itself was refused, and `<text>` says why, for example an attachment that is not fenced, or a generation that is exhausted.

A refusal of the second or the third kind carries `reason` beside the four fields of every refusal:

```json
{"command": "delete", "error": "9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d is an AI assistant and its owner's account is active: the owner deletes it in the product", "note": "refused; nothing was changed", "reason": "owner-active", "returncode": 1}
```

Exit 2, with what was done in the document:

- verdict `workspace_closed`, with the note `workspace of 9a8b7c6d5e4f4a3b8c2d1e0f9a8b7c6d closed; 1 run had not ended after 120 s; nothing was deleted; run the command again`.
- `space.release` `pending`, with a note that ends `the Gateway had not freed slot 7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a after 120 s; agent_instances list shows when it has`. The agent is deleted. The Gateway frees the slot at a later pass or at its next start; while it cannot, its journal says why (§ "Full Spaces, the data disk, and unverified Spaces", "Deleted agents and their slots").
- `space.use` `unclaimed`, with a note that ends `slot 7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a could not be verified and was released without being emptied; reclaim it with: storage-init forget 7d8e9f0a1b2c4d3e8f4a5b6c7d8e9f0a --discard`. The agent is deleted, and the Gateway has written its line for such a slot. Reclaim the slot as § "Full Spaces, the data disk, and unverified Spaces" says. A repeat of the command finds no slot tied to the Space and answers `already_deleted` with exit 0; `ai verify` goes on showing the slot until it is reclaimed.

##### `adopt-host`

```bash
ai adopt-host --from 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f --acknowledge-old-host-gone
```

It records the operator's statement that the Docker host named by `--from` no longer exists and that this VM has taken its place. It is used once after a restore onto a VM whose Docker engine has another identity, when the Gateway's line asks for it (§ "Backup and restore"). The value of `--from` is the id that line prints, which `ai status` repeats under `foreign_hosts`. It concerns agents alone: a chat sandbox is recorded by no attachment. In order:

1. It reads this VM's Docker engine ID.
2. It counts the workspace records that are not closed and name the old host.
3. Without `--acknowledge-old-host-gone` it refuses and reports the count.
4. With the flag it writes one row to `storage_host_replacements`: the old id, this id, the count and the time. A row that already exists gives verdict `already_adopted`, exit 0.

It closes nothing itself. At its next start the Gateway closes each of those records, with the reason `host-replaced`, once its own daemon confirms that no such container exists here, and settles the assignment attempts that were running in them. Run `hm restart gateway` after it.

| Field | Type | Meaning |
| --- | --- | --- |
| `from` | string | The old Docker host. |
| `host_id` | string | This VM's Docker engine ID. |
| `acknowledged` | boolean | Whether the flag was given. |
| `attachments_named` | integer | Workspace records, not closed, that name the old host. |
| `verdict` | string | `adopted` or `already_adopted`. |

Example:

```json
{"acknowledged": true, "attachments_named": 3, "command": "adopt-host", "from": "0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f", "host_id": "7b9c1e6f-4d2a-4c5e-9f0a-1b2c3d4e5f60", "note": "Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f recorded as replaced by 7b9c1e6f-4d2a-4c5e-9f0a-1b2c3d4e5f60; 3 attachments are closed at the next Gateway start", "returncode": 0, "verdict": "adopted"}
```

Refusals, exit 1, beside the common ones:

- `3 attachments are held by Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f; confirm that VM is powered off or destroyed, then pass --acknowledge-old-host-gone`; the document also carries `attachments_named`
- `0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f is this Docker host; there is nothing to adopt`
- `no attachment names Docker host 0d6c2b1e-8f3a-4c7d-9e5b-2a1f0c3d4e6f; hosts on record: 7b9c1e6f-4d2a-4c5e-9f0a-1b2c3d4e5f60`
- `give --from <Docker engine ID>`
- `the Docker daemon did not answer: <text>`

##### `settle`

```bash
ai settle --space 0b1c2d3e4f5a4b6c8d7e9f0a1b2c3d4e --operation 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80
ai settle --space 9c4d1e6f8a0b4c2d9e7f5a3b1c8d6e40 --operation 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80 --discard
```

Every write the Gateway makes to a Space, a file given to an agent, a Keep into My Files, a project document, a publication to Shared or its removal, leaves a `pending` row in `storage_space_file_operations` until its outcome is recorded. The Gateway settles a row the restart or the crash left `pending` by itself, at its start and in every pass of its 60-second task: a file write by replaying its record against the files, a Keep or a project document as complete because each publishes by a hard link under a unique name and records nothing the filesystem does not show, a publication to Shared by reading its receipt `control/host-workflow-<operation id>.json` and reconciling the record with the files. A row that it cannot settle, because the receipt is missing or the reconciliation raises, is left `pending`, and the journal says so: `storage: publication 6e2d… not settled: <text>`. While such a row stands, a request that includes a write on that Space is refused as `storage-busy`; the person reads "Shared is being changed right now. Try again in a moment." or "Your files are being changed right now. Try again in a moment.", and reads it again at every try. This verb is the operator's way to end that. In order:

1. It finds the row by Space and operation id and requires the phase `pending`.
2. It prints the row's facts under `request`, the receipt under `receipt` when one exists, and what the folder holds under `files`, so that the operator can read them before acting; with neither flag it stops here when the settlement would raise, verdict `unsettled`, exit 2.
3. Without `--discard` it runs the Gateway's own settlement for the row's kind, by host authority under the host workflow lock, and records the outcome: `complete` with the resolution `host-settled` or `host-reconciled`, or `failed` when the replay says the write never landed.
4. With `--discard`, after the operator has inspected the receipt and the files, it marks the row `failed` with the resolution `host-discarded` and changes no file. What the operation had already written stays where it is; a publication that half-landed is then a file in `shared/` with no record, which the Shared tab lists as a file nobody published and an administrator can remove.

| Field | Type | Meaning |
| --- | --- | --- |
| `space_id`, `operation_id` | strings | The row. |
| `kind` | string | The Space's kind. |
| `namespace` | string or null | `hm.shared`, `hm.my-files`, `hm.projects` for a feature workflow; null for a file write. |
| `action` | string | The row's `request.action`: `write`, `put`, `mkdir`, `rename`, `remove` or `feature-workflow`. |
| `on_behalf_of` | string or null | The person on whose behalf the host wrote, for a host-written row. |
| `request` | object | The row's request as stored. |
| `receipt` | object or null | The receipt's content, or null when none exists. |
| `files` | array of strings | The names under the folder the operation targets that the receipt or the request names. |
| `verdict` | string | `settled-complete`, `settled-failed`, `discarded` or `unsettled`. |
| `resolution` | string or null | `host-settled`, `host-reconciled`, `host-discarded`, or null with `unsettled`. |

Example:

```json
{"action": "feature-workflow", "command": "settle", "files": ["q3-review.pdf"], "kind": "company", "namespace": "hm.shared", "note": "publication 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80 on the company space settled complete (host-reconciled): the file is in shared/ and its record now names it", "on_behalf_of": "1f9b6a2c-4d3e-4f5a-8b7c-9d0e1f2a3b4c", "operation_id": "6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80", "receipt": {"digest": "sha256:4a5b…", "name": "q3-review.pdf", "operation_id": "6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80", "source_space_id": "0b1c2d3e4f5a4b6c8d7e9f0a1b2c3d4e"}, "request": {"action": "feature-workflow", "by": "host", "intent": "publish", "namespace": "hm.shared"}, "resolution": "host-reconciled", "returncode": 0, "space_id": "9c4d1e6f8a0b4c2d9e7f5a3b1c8d6e40", "verdict": "settled-complete"}
```

Refusals, exit 1, beside the common ones:

- `no file operation 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80 on space 9c4d1e6f8a0b4c2d9e7f5a3b1c8d6e40`
- `file operation 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80 is already complete`, or `already failed`
- `space 9c4d1e6f8a0b4c2d9e7f5a3b1c8d6e40 is unverified: <text>; see storage-init verify`
- `another workflow holds the lock of space 9c4d1e6f8a0b4c2d9e7f5a3b1c8d6e40; run the command again in a moment`

Exit 2: verdict `unsettled`, with the note `publication 6e2d7f8a9b0c4d1e8f2a3b4c5d6e7f80 cannot be settled: <text>; inspect receipt and files, then run with --discard to record it as failed`. Nothing was changed.

### Changing the counts and capacities

Raising a count prepares missing slots on the next `up` if the complete addition
fits. Lowering a count removes nothing. A larger capacity applies to new slots;
existing slots require offline `resize` and a Gateway restart. A smaller capacity
affects future preparation only. An interrupted growth is continued with the
same command; a smaller target cannot truncate an already extended image.

Deleting an AI assistant or AI employee marks its Space deleted with it, and the
Gateway removes files and backups before returning the slot to the same kind.
A deleted agent cannot be restored. If `space_released` is false, cleanup remains
unconfirmed; the product checks the release for up to three minutes without
claiming that elapsed time proves completion. Lowering an agent count removes
no existing agent. An unverified slot remains unavailable until operator repair.

### Seats, and the account of a person who has left

Every new account takes one seat, including setup-pending accounts, whether
created in Settings, registration, sign-on or the `add_user` tool. Creation past
the allowance returns `no_seats_left`. Settings explains the count and provider
support; existing accounts are not removed when the configured count is lower.
Each eligible account receives its User Space and company READ/EXPORT grant
after account commit; assignment failure preserves the account and is retried
by maintenance.

Turning an account off keeps its seat and Space. Releasing it is permanent:
confirm the administrator's intent and retain needed files or a platform snapshot
first. Capture the stable ID from `accounts list`, then run:

```bash
accounts disable --id ACCOUNT_UUID
accounts release-seat --id ACCOUNT_UUID
# Retry unfinished cleanup by the same ID, never by the released email:
accounts release-seat --id ACCOUNT_UUID
```

The release transaction records `released_at`, releases the address and marks
the exact User Space and all personal AI assistants with their bound Spaces
deleted. An unfinished AI assistant without a Space is removed with its grants;
the receipt counts currently matched assistants and a retry excludes that removed
intent. Company AI employees stay intact. Sessions and running-work cleanup follow commit;
a released account cannot be enabled again. Its account/history rows and Shared
publications remain. Maintenance attempts to remove positively owned chat
sandboxes before emptying the Space and freeing its binding for another account.
Unverified or uncontained storage stays reserved. The bundle gate proves reclamation
after two allowlist chats, rechecks both complete resource sets before disablement
and verifies every original container/network ID and name is absent before reuse.
Partial paired cleanup survives restart through the private journal. A free seat does not guarantee an
immediately reusable User slot.

An AI assistant whose owner cannot sign in keeps its slot until the seat is
released or `ai delete <id>` removes that assistant alone. Export its files with
`ai export` before permanent removal when they are needed. AI assistants cannot
be handed to a new owner. An AI employee with no live supervisor or manager can
be recovered with `ai supervise`, or deliberately removed with `ai delete`.
Both commands recheck current human eligibility and require explicit authorization
to close running work.

### Stopping agents in an emergency

Place the hold file to refuse new AI assistant and AI employee execution without
restarting the stack. Maintenance closes locally owned workspaces at its next
60-second pass. Ordinary chats and people's files remain available.

```bash
printf 'incident 4711: runaway assignment\n' > /srv/hartmesh/operator/agents-hold
chown 1000:1000 /srv/hartmesh/operator/agents-hold; chmod 0640 /srv/hartmesh/operator/agents-hold
hm logs --no-log-prefix gateway | grep 'agents: held'
# later
rm /srv/hartmesh/operator/agents-hold
hm logs --no-log-prefix gateway | grep 'agents: hold lifted'
```

The Gateway reads this operator-owned file through its read-only mount. Its owner
is `1000:1000`, mode `0640`; the optional first line appears in the journal only.
`/health` reports agents `held`. Removing it lifts the hold at admission, but
qualification must still succeed. The next message or explicit activation starts
work; removing the file starts no assignment.

### Full Spaces, the data disk, and unverified Spaces

A person's My Files, Projects and conversation files share one capacity.
Uploads, Save and Share preserve existing successful files and explain a refusal
with the current support contact. A new conversation requires at least 16 MiB
and 256 free inodes; a refused first message keeps its draft. Its first run returns
HTTP 507 before admission or streaming. Recorded retries still recover their run.
Old conversations and history remain readable. Commands in a running chat can
receive `No space left on device`; the model explains how to free room.
An unavailable User Space instead permits text chat without files. Company
capacity/refusal affects Shared without closing personal file access.


An agent whose Space is full refuses a new reply or assignment with `files-full`;
commands already running can receive `No space left on device`. Its files and
history remain. Free files through authorized Space controls while its workspace
is contained, or grow its identified slot after removing holders. An unverified
Space retains its identity and refuses new execution until verification or
qualified recovery succeeds. No folder is substituted for it.

Each filesystem enforces bytes and inodes. `si status` separates prepared,
missing and reserved capacity; `si verify` identifies unverified slots. Freeing
files does not shrink an allocated image. Pool admission also preserves its
configured free-data-disk floor (1 GiB and 1,024 inodes); below it storage is
unavailable until capacity is restored.

Preparation/growth require the larger 4 GiB/10% headroom. If the missing plan
does not fit, no new slot is prepared. Grow the data disk or reduce the planned
addition, then repeat preparation. A slot with damaged/missing identity stays
unverified; ordinary directories and matching path strings are no substitute.
Use repair only for the reported repairable fact, or restore the platform
snapshot for lost data. Unassigned slots containing files are never silently
claimed by another resource.

### Backup and restore

The platform snapshot of the complete data disk is the backup of record: it keeps
the database, pool image identities, people’s files and Shared at one moment.
A running snapshot is crash-consistent, not proof that a file or database writer
was idle. For a clean unmounted snapshot, stop the stack and inspect/remove the
old Gateway's exact sandbox/relay/bridge holders using the upgrade procedure,
then run `si quiesce`. Its confirmed no-holder/no-busy-loop result precedes the
snapshot. Start again with `hm up -d --wait`.

Restore the whole data disk from one snapshot with the stack stopped, every
holder removed, Spaces quiesced and the data-disk filesystem unmounted. Remove
only inspected tenant-owned orphan probes and sandbox networks when restoring;
never prune the daemon. A busy unmount means containment is incomplete. Restore
the matching pinned bundle, mount the disk with shared propagation, complete
preflight and read storage-init/Gateway readiness after start.

A tar of `data/`, a sparse image copy, or images from one moment beside a database
from another cannot restore the binding identities. Do not copy directories over
a live disk. Generic per-Space lifecycle/recovery refuses managed User and company
roles with `mounts-unrecorded`; chat binds have no attachment records to establish
quiescence. Restore their data disk instead.

One agent's Space also supports qualified per-Space backup, restore and recovery
through the authenticated Spaces API. Current Space ADMIN and generation are
required; those operations contain recorded attachments with `storage-recovery`
before physical effects. They preserve existing files when containment is uncertain.
`ai export <id>` produces a quiesced external archive; product Files controls
support direct transfers. Export verifies the Space and disk headroom before
closing its workspace. `--end-running-work` explicitly permits interruption.
User and company roles use the whole data-disk snapshot instead.

The Gateway attempts interrupted ordinary file and feature-workflow settlement
at startup and in minute-based maintenance. It confirms private receipts and
current binding identities; it never replays an unknown write or claims success
from elapsed time. Uncertain copy/lifecycle/controller outcomes remain pending
for inspection. `ai settle --space <id> --operation <id>` inspects and reconciles
one exact row. Use `--discard` only after inspecting the request, receipts and
files: it records host/operator failure and replays no write. A missing receipt
or uncertain publication can remain unsettled with exit 2.

### Roles and the provider's name

Two things in the tenant bundle (§ "Tenant bundle") shape what a person sees when adding an AI assistant or an AI employee. Both are optional, and both are read live: an edit reaches the next page load, with no restart.

**Roles.** When a person adds an AI assistant or an administrator adds an AI employee, the product offers a short list of roles and, always, a blank brief. A role is a starting point: a name, a description, the standing instructions the agent begins with, the skills it may use and whether it accepts assignments. The release ships three roles that name no industry, in `ui.roles` of the template: a reporting analyst, a researcher and a newsletter writer. The operator replaces the list for one tenant with `/srv/hartmesh/operator/tenant/roles.json`:

```json
[
  {
    "id": "reporting-analyst",
    "name": "Reporting analyst",
    "description": "Turns exports into periodic business reviews.",
    "soul": "You are a reporting analyst at this company: you turn the exports people give you into business reviews with every number checked, and you say plainly when data is missing or does not add up. You keep each period's report and source files among your own files for comparison with the next.",
    "skills": ["business-report", "data-analysis"],
    "work_policy": {
      "enabled": true,
      "default_priority": "normal",
      "review_required": true,
      "allow_derived": false,
      "max_derived_per_activation": 3,
      "responsibilities": [
        {"key": "periodic-review", "label": "Business reviews"},
        {"key": "data-questions", "label": "Questions about uploaded data"}
      ]
    }
  }
]
```

The example is the first of the release's three roles, written as this file: installed as it stands, it offers that one role and the blank brief. Start from it and change the texts.

The file is a JSON list of at most 8 roles. `id` is 1 to 40 characters of `a` to `z`, `0` to `9` and `-`, and distinct. `name` is one plain line of at most 60 characters. `description` is one plain line of at most 200. `soul` is plain text of at most 16,000 characters. `skills` is optional: at most 16 names from the public skill library (§ "Public skills"); a name the library does not carry is dropped and named in the journal. `work_policy` is optional and has the fields shown; `default_priority` is `low`, `normal`, `high` or `urgent`, and `max_derived_per_activation` is 1 to 20. An unknown key refuses the file.

The file follows the rule of `starters.json`. A missing file leaves the release's three roles. A malformed one leaves them too and is named in the journal as `tenant bundle: roles.json: <rule>`, never with the value. `[]` is a deliberate choice: only the blank brief is offered. Install it as the other bundle files are installed, readable by uid 1000:

```bash
install -o 1000 -g 1000 -m 0640 roles.json /srv/hartmesh/operator/tenant/roles.json
```

A role is used once, when the agent is added: the instructions become that agent's own and are edited in its Settings afterwards. Changing or removing a role changes no existing agent.

**The provider's name.** Several sentences tell a person to ask the provider: when storage is unavailable, when files are full, when no seat is free, when the number of AI employees allowed is reached, when a workspace does not start. They take the name and the link from `provider.json` in the tenant bundle, the same file that already names the provider in the workspace menu (§ "Tenant bundle"):

```json
{"display_name": "Example Hosting", "support_url": "https://support.example.com/help"}
```

With a name, the sentences read "ask Example Hosting"; without one, "ask your service provider". With a link, the sentence is followed by the menu's own "Contact Example Hosting support"; without one, the sentence stands alone. There is no `.env` key for either.

### What has been verified

The evidence tiers establish different facts. Each is recorded separately:

- `managed-storage` runs all 26 required native cases on real loop-mounted ext4
  filesystems and actual containers, using the resident builder and byte-coherence
  probes. It rejects every skip; directories and engine doubles cannot replace it.
- `bundle-instances` runs the shipped profile through nginx with all seven keys,
  a scripted model and 27 named acceptance cases. It checks accounts/seats,
  ordinary chats, files/publication, full-storage refusals, resident persistence,
  Work/restart containment, live/closed file transfers and 40 MiB integrity,
  Stop, count refusals, same-kind reuse, denied egress, supervisor questions and
  operator/killed-workspace settlement. Its `hartmesh-ci` runtime is a runc alias:
  this proves runtime selection, not gVisor behavior or model judgment.
- A per-release `tenant_class` receipt is a person’s qualification on the actual
  VM class under `runsc`, covering default preparation/timing, real transfers,
  cold start, full storage, egress, two conversations, hold, reboot and snapshot
  restore. This is separate infra/consumer work, never performed by the release
  session and not a release-cut gate. GitHub CI cannot supply it; this runbook
  does not claim it is complete. A supplied receipt is strictly validated.

Offline SQL/contract tests, strict blocking-I/O checks and browser suites qualify
additional functional boundaries. More than one Gateway worker and data disks
outside the supported ext4 host model are not qualified by these results.

## Durability

Deliberately relaxed, because the guest's disks sit on a replicated tier that
acknowledges every write it has journaled and the recovery point the product
quotes is one hour: PostgreSQL runs `synchronous_commit=off` with
`wal_writer_delay=200ms` (it still fsyncs on its own cadence; a guest crash
loses at most the last 200 ms of commits) and Redis uses `appendfsync
everysec`. This keeps the tenant's fsync rate near one per second, and the
storage tier's write budget is the density ceiling, so do not "fix" it upward.

## Deployment profile

The Gateway migrates the database itself at start; no migration job is
needed. Every store is PostgreSQL, and run events are kept in the database
(`run_events.backend: db`) so they survive a Gateway restart.

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
serves a frontend that reports no model configured (unless an administrator
has since set one in the product: that line is the `.env` view, § "Provider
keys in the product"). That is the correct
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

`image_search` returns only results carrying a direct image
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

### Provider keys in the product

The key in `.env` is where a tenant starts. An administrator can add,
replace or remove the key of any provider in the catalog above -- the eleven
model providers under `providers/models/` and the nine search and fetch
providers under `providers/tools/` -- from **Settings → Account → Provider keys**, so
the company rotates its own key or changes provider without anyone editing
`.env`. **With no key set in the product, nothing changes**: the Gateway
serves the `config.yaml` `gateway/run.sh` rendered, writes no second file,
and `.env` is the only source of keys, exactly as before.

**Precedence.** A key set in the product outranks the `.env` key for its
provider, and the Gateway applies it at every start, before anything is
built from the configuration. So it keeps winning:

| After | The provider uses |
| --- | --- |
| a Gateway restart | the product's key |
| a `.env` re-render and `up -d` with the original key still in it | the product's key |
| a database restore taken after the key was set | the product's key |
| a database restore taken *before* the key was set | the `.env` key again, if `.env` carries one: the restored database holds no product key |
| a restore under a different `HARTMESH_PROVIDER_KEYS_SECRET` | **no key**: the stored one cannot be read, and the `.env` key is never used in its place |
| an administrator removing the key | the `.env` key if there is one, otherwise none |

**How a change takes effect, without a restart.** The config loader and the
search tools read keys from the Gateway's process environment
(`api_key: $OPENAI_API_KEY`). On every change, and at start, the Gateway
puts the product's keys into that environment, over the `.env` values, and
renders the configuration again with this profile's own
`gateway/render_config.py` -- so a provider that had no key gains its
models and one whose key is gone loses them, by the same rules as `.env`.
The result goes to `home/config.effective.yaml` beside `home/config.yaml`,
and the Gateway points its own `DEER_FLOW_CONFIG_PATH` at it and reloads.
`home/config.yaml` stays what `.env` alone renders, so every command run
with `docker compose exec` still loads it; neither file holds a key, only
`$NAME` references. A run already going keeps the configuration it started
with -- its models and their keys -- to its end; the next run uses the new
key, and so does a search tool's next call, since the search tools read their
variable when they are called.

**Nothing reads a key back.** The key is write-only: the page, every route
(`GET /api/provider-keys` shows each provider's source, whether a key is
set and when and by whom it last changed), every error and every log line
leave it out. A write is refused unless it comes from an administrator's
interactive session; a personal access token is refused even for reading.

**Where each key comes from, for the deployer.** Inside the deployment:

```bash
docker compose --project-directory /opt/hartmesh --env-file /srv/hartmesh/.env \
  exec --user 1000 gateway \
  sh -c 'cd /app/backend && PYTHONPATH=. uv run --no-sync python -m app.gateway.provider_keys.status'
```

prints one JSON document: every catalog provider with `source` --
`product`, `environment` or `none` -- plus `product_key` (`absent`, `set`,
or `unreadable`), `wrapped_with` (`current` or `previous`), `changed_at`
and `changed_by`, and `refusal`, the reason an administrator cannot set
keys now (null when they can), and `wrapping_key`: `set`, `absent` (the
Gateway started with no `HARTMESH_PROVIDER_KEYS_SECRET`: an `up` that did
not carry it) or `invalid` (one shorter than 32 characters). `absent` or
`invalid` beside an `unreadable` key means the key is intact and the secret
is what is missing; `set` beside one means the secret is not the one it was
stored under. It never prints a key. It exits `1` with
`error` and `message` when it cannot answer; the document is the answer
either way. `environment` means the `.env` key is in use; once a provider
says `product`, the `.env` copy of its key is no longer used and can be
deleted. The `render_config: wrote ... provider keys found: ...` start line
is still the `.env` view alone; the line after it, `provider keys: set in
the product for ...`, and this command give what the Gateway actually uses.

**At rest.** A key set in the product is stored in PostgreSQL
(`provider_keys`), Fernet-encrypted (AES-128-CBC with HMAC-SHA256) under a
key derived (HKDF-SHA256) from `HARTMESH_PROVIDER_KEYS_SECRET`, together with its
variable name, so a row copied onto another provider does not become its
key. A database backup therefore holds only ciphertext. Without the secret
the product refuses to store a key and says why; with one shorter than 32
characters it refuses too. The secret must not live on the disk the
database does: `/srv/hartmesh/.env` shares the data disk with
`/srv/hartmesh/postgres`, so a snapshot of that disk would carry both the
ciphertext and the key that opens it. Supply it from the environment of the
`docker compose` command instead (`compose.yaml` interpolates it, and the
shell's value wins); Docker keeps a container's environment with the
container, so a restart keeps it, but **every** later `up` must carry it
too, or the Gateway starts with no secret and the providers with stored
keys have no key (the start log names them, and the command says
`unreadable` with `wrapping_key: "absent"`). An administrator's page says
the saved key is intact and asks for the host to restore the setting;
**Remove** there discards the key, so do not use it to recover from a
missing secret. Keep the secret durably somewhere other than the data disk:
losing it is the "different `HARTMESH_PROVIDER_KEYS_SECRET`" row above for
every stored key, and the only remedy is setting each key again.

**Rotating the secret.** Start the stack with the new value in
`HARTMESH_PROVIDER_KEYS_SECRET` and the old one in
`HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS`. The Gateway reads under either and
rewraps every stored key under the new one at start; when the command shows
`wrapped_with: current` for every stored key, drop the previous value. A
start with the new value and without the previous one leaves every stored
key `unreadable` with `wrapping_key: "set"`; start again with both.

**The record.** Every add, replace and remove is kept in
`provider_key_events` -- which provider, which action, the administrator's
id and address, when -- and shown under the list (`GET
/api/provider-keys/events`). The value is never part of it.

**What is refused.** A provider outside the release's catalog (`404
unknown_provider`); a body that is anything but `{"key": "..."}`, so there
is no way to supply a base URL or a model entry here (those belong in
`HARTMESH_MODELS_FILE`); a key that is not one printable token. With
`HARTMESH_MODELS_FILE` set the deployer curates the models, so every write
is refused with a message saying so and nothing stored is applied: the
operator file's `$NAME` references resolve against `.env` alone, as they
always did.

**Rolling back, and a stored key that stops rendering.** A release earlier
than the one that added this has no record of its database table, so its
Gateway does not start on a database this release migrated; roll back by
restoring a backup taken before the upgrade, which is the "restore taken
*before*" row above. A key is rendered before it is stored, so a stored key
that no longer renders at start takes a release that changed the catalog;
the Gateway then logs the refusal and does not start. Start it once without
`HARTMESH_PROVIDER_KEYS_SECRET` -- the stored keys are then unreadable and
not rendered -- remove the key in the product, and start it again with the
secret.

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

Rotating a provider key is a re-render of `.env` followed by the same `up -d`
-- unless an administrator has set that provider's key in the product, which
outranks the `.env` one until they remove it (§ "Provider keys in the
product"; `python -m app.gateway.provider_keys.status` says which applies).
Where any key is stored in the product, that `up -d` carries
`HARTMESH_PROVIDER_KEYS_SECRET` in its environment like every other `up`;
without it Compose sees the value change to empty, recreates the Gateway,
and the stored keys are not applied.
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
wrapped here only to fit the page. This one is from the repository's own
end-to-end test, with a model stub and no sandbox, not from a tenant VM:

```text
turn phase timings run=<run id> correlation=<id> total=3959ms outcome=success model=1/936ms busy=936ms \
phases=admission@0ms assembly@13ms agent_build@13ms+35ms checkpoint_preflight@48ms graph_start@49ms \
model_request@71ms first_provider_text@672ms first_stream_text@673ms model_completion@1006ms terminal@3959ms \
unobservable=browser_first_text(requires_a_browser_measurement_through_public_ingress)
```

`@` is an offset from the turn's start and `+` the phase's own measured
duration, both in milliseconds, so a phase carrying both **ends** at
`@ + duration` and the questions are arithmetic on one line. A span is printed
when it *ends*, so an enclosing span appears after the spans it contains.

- `total` and `outcome` are the turn's wall time and how it ended.
- `model=<calls>/<time>` and `tools=<calls>/<time>` say how much of the turn
  was the provider and how much was tools; `busy` is their sum, and the rest
  of `total` is the turn's own bookkeeping and persistence.
- `queue` appears when the run waited to start.
- `acquisition` appears when the turn took a sandbox, and says where it came
  from: `in_process` (the thread already held it), `warm_reclaim` (a parked
  container, which is also what a prewarmed first turn reads), `discovered`
  or `rediscovered` (a container another process or an earlier Gateway
  started), `created` (a cold start). A cold start carries the phases
  `sandbox_create` and `sandbox_readiness`; their durations are the wait
  § "Sandbox readiness budget" is about.
- `first_stream_text` is when the first text left the Gateway. What the
  person saw is later by the path through nginx and the browser, which this
  line cannot observe and says so (`unobservable=browser_first_text`).

A first turn whose sandbox was built when the chat opened logs `Sandbox <id>
was built <n>s ahead of this turn and reclaimed warm` beside its timing line.
A turn that is slow before `model_request` with `acquisition=created` paid a
cold start; one that is slow between `model_request` and
`first_provider_text` waited for the provider.

## Release pinning

At release the `image:` references in `compose.yaml`, `sandbox.image` and
`network.proxy_image` in `config.yaml`, and the lines of `images.txt` are the
same digest-pinned strings, written by `scripts/pin_compose_images.py` before
the tag (see `RELEASING.md`, "HartMesh distribution releases"). Between cuts the tree
carries the **previous release's digest pins**: the pin commit is the last
thing a release changes and nothing restores placeholders, so a bundle built
from `main` is grammatical and boots the previous release's images. A cut
re-points this repository's four image lines with `--release`; a third-party image is bumped by
putting its new tag form in place of the old digest string in all three files
and running the pin script. Eight images are pinned: gateway, frontend,
sandbox, the network proxy (built under this repository's own name,
`<repo>-sandbox-network-proxy`), `postgres`, `redis`, `nginx` and
`searxng/searxng`.

The backend digest appears in three Compose values: the Gateway image, the
`storage-init` image and `HARTMESH_STORAGE_INIT_IMAGE`. The pin script rewrites
all three together; `images.txt` still has eight images.

## Not here

TLS, the front door (Traefik on the platform cluster, forwarding plain HTTP
with `X-Forwarded-For` / `X-Forwarded-Proto`), backups, and the host firewall
are all outside the VM and outside this profile.

## What has been verified

This profile's files are checked offline by the repository's test suite
(`backend/tests/test_compose_*.py`, `test_sandbox_image_contract.py`): the
render, the pins, the limits, the sign-in modes, the seed and the nginx
configuration. The sandbox image is built and exercised under the restricted
runtime and these limits by the sandbox smoke workflow.

The measurements quoted in this document (sandbox memory and start times, the
memory budget, search behaviour) were taken on the earlier HartMesh release
line, `2.1.0+hartmesh.N`, and are named by release where the release matters.
The sandbox image, the limits and the profile's services are the same here;
the Gateway is a different build. Figures for what passes through the Gateway
-- a turn's timing above all -- are to be measured again on a tenant VM
before they are relied on, and this build has not yet been run on one.

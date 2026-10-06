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
├── provider.json       {"display_name": "Example Hosting",
│                        "support_url": "https://support.example.com/help"}
├── starters.json       [{"id": "business-review", "title": "...", "prompt": "..."}]
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
name and a logo are set, how many starters and report profiles there are --
and still renders. Report profiles are listed, not validated: the skill
validates a profile when it loads one and says what is missing.

The profile runs the business profile (`ui.profile: business` in
`config.yaml`) and explicitly enables the installed `hartmesh-legacy-report`
compatibility plugin through `plugins:`. Historical cards, projection and filing
use that package; disabling its entry and restarting leaves ordinary file access.
The provider template owns this selection.

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

# Hartmesh frontend ownership

Hartmesh builds and deploys `frontend-hm/`. The sibling `frontend/` is an exact
snapshot of upstream DeerFlow at the commit recorded in
`.github/upstream-frontend.json`. This separation lets upstream UI changes
merge into their original paths while Hartmesh develops its own interface.

The initial copy preserved the entire frontend at Hartmesh commit
`f916f235beaeaf0f97fd1faf72e89b06643d04d8`, including governance, evidence,
authentication, streaming/reconnection, translations, assets, and tests.
Its pure-copy commit is `367f321b`. The upstream reference starts at
`0f7d8709d3bbf0be26460b6277fbad9329302243`.

## Development

Use root `make` commands for the full Hartmesh stack and `frontend-hm/` for
UI work:

```bash
make frontend-config
make install
make dev
# Or run only frontend commands:
cd frontend-hm
pnpm check
pnpm test
pnpm exec playwright test
pnpm exec playwright test -c playwright.auth.config.ts
pnpm exec playwright test -c playwright.real-backend.config.ts
pnpm exec playwright test -c playwright.gateway-auth.config.ts
```

`make frontend-config` initializes the ignored `frontend-hm/.env`. It keeps
an existing destination; otherwise it copies `frontend/.env` when available,
or the new app's `.env.example`. It never overwrites the old file or prints
settings. `make setup`, `make config` on a fresh installation, and Docker
development setup also initialize the new app's settings. No environment
files or build output belong in the committed copy.

The host runner's Hartmesh form is
`python3 scripts/pnpm.py --project frontend-hm -- <pnpm arguments>`. It selects
that project's working directory and pinned package manager; omitting the
selector retains the legacy upstream Makefile behavior. There is no fallback
from a missing Hartmesh app to the reference app.

The product app owns its dependencies, lockfile, API clients, components, and
assets. Do not import or link from `frontend/`. Port useful upstream changes
explicitly and record their origin. Existing shared fixtures in `contracts/`
remain valid dependencies. Backend API, SSE, and WebSocket compatibility must
be tested even when Git merges cleanly.

## Builds and releases

`frontend-hm/Dockerfile` copies the new source into `/app/frontend`. Runtime
service names, ports, nginx routing, Helm cache mounts, the release component
`frontend`, and `ghcr.io/altakleos/hartmesh-frontend` remain unchanged. The
container path is not a dependency on the repository's `frontend/` directory.

Version tooling coordinates `frontend-hm/package.json` with the backend and
chart. The upstream package keeps its upstream version. Only the normal
candidate-build, digest-pin, and tag procedure updates released images;
existing tenant image pins still identify the earlier release until then.
See [release instructions](../RELEASING.md).

## Upstream syncs

1. Select and merge upstream through the existing workflow. Keep both source
   directories; do not rename or delete the reference directory.
2. Review shared tooling/backend conflicts. The resulting `frontend/` must
   equal the upstream commit actually merged, including tracked additions,
   deletions, and file modes. Investigate unexpected conflicts rather than
   masking them with a blanket merge preference.
3. Update `upstream_commit` and `frontend_tree` in the marker. Keep the
   original seed provenance. Use `git rev-parse <merged-commit>:frontend` for
   the tree ID. Do not advance the marker to an unmerged remote tip.
4. Stage the sync and run `make check-frontend-isolation`. Review the diff
   of `frontend-hm/` separately: ordinary upstream UI changes should not touch
   it. Port desired fixes explicitly, then run the Hartmesh compatibility
   suites. Review new build/workflow paths to ensure they target the product.

The default isolation check examines both staged and working material using
a temporary Git index; it does not alter the real index or files. Ignored
local settings and outputs are excluded. For committed checks use:

```bash
python3 scripts/verify_frontend_isolation.py --revision HEAD
```

CI runs this in the existing frontend lint/build job with full history, then
moves the reference source out of the checkout before installing/building the
Hartmesh app. A missing ancestor, wrong tree, local change to the upstream
snapshot, or direct
cross-app source input fails the check. The source scan catches literal paths,
package/config references, Docker COPY inputs, and escaping symlinks; it is
not a JavaScript interpreter. Validation also builds/tests the Hartmesh app
in a disposable checkout without the reference source to exercise indirect
dependencies.

The marker is reviewed sync metadata. Tree equality and ancestry do not prove
the origin of an arbitrarily substituted marker; maintainers must verify the
upstream revision during sync review. Never edit upstream files for Hartmesh
versioning, formatting, tests, or guidance. If an upstream convention differs,
adapt external tooling policy instead.

Hartmesh does not maintain the reference app as a second supported runtime.
Its dependency/security fixes need deliberate consideration for the product
copy. For rollback, use the previous complete Hartmesh change/release and
image set; deploying the reference app would omit Hartmesh behavior.

# Hartmesh frontend isolation

Status: design and implementation plan; application changes have not started.
Branch: `plan/frontend-hm-isolation`.
Date: 2026-09-14.

## Decision and scope

Create `frontend-hm/` in this repository, seeded from Hartmesh's current
frontend. Make it the application built, tested, and deployed by Hartmesh.
Restore `frontend/` to the exact tracked tree of the last integrated upstream
commit, and keep that equality after every subsequent upstream sync.

The first migration preserves the current interface and behavior. Visual
redesign, framework changes, dependency upgrades, a separate repository, and
new backend APIs are later work. The existing Gateway remains the application
boundary. Keeping both applications in this repository allows a backend/API
change and its Hartmesh UI adaptation to land together.

This plan authorizes no release or deployment. Implementation should produce
one migration PR with reviewable commits and a coherent final tree.

## Verified starting point

These values were read from the local repository; no upstream fetch or merge
was performed during planning.

| Item                                    | Value                                        |
| --------------------------------------- | -------------------------------------------- |
| Hartmesh seed commit                    | `f916f235beaeaf0f97fd1faf72e89b06643d04d8`   |
| Seed `frontend` tree                    | `4b3ee6ca3b806b80026d42dc2f573ea619c7621e`   |
| Last integrated upstream commit         | `0f7d8709d3bbf0be26460b6277fbad9329302243`   |
| Upstream `frontend` tree at that commit | `038e616e0dfb40d31b2d80d2f3ec1eef0c7832d4`   |
| Upstream repository                     | `https://github.com/bytedance/deer-flow.git` |
| Local upstream remote / branch          | `deerflow/main`                              |
| Current application version             | `2.1.0+hartmesh.12`                          |

The merge base with the local `deerflow/main` ref is `0f7d8709`, also present
as the upstream parent of merge `f7703656`, integrated through Hartmesh PR #42.
The local remote-tracking tip is newer (`98ae762d`); it is not the restoration
target. Recheck the seed and baseline if development or an upstream sync lands
before implementation.

The seed contains 813 tracked frontend files, about 19.8 MB of uncompressed
content. Relative to the integrated upstream baseline, 32 files differ:
2,588 added lines and 32 removed lines. Preserve the whole seed first,
including its tests, generated UI primitives, assets, and attribution.
Notable fork differences include:

- Execution evidence: `src/core/evidence/`, `thread-evidence.tsx`, both chat
  entry points, translations, and unit/E2E coverage.
- Governed tool-plane settings: `src/core/tool-plane/`, the governance notice,
  skills/tools/integration controls, translations, and unit/E2E coverage.
- Production image startup: the shared, readable Corepack cache and pinned
  pnpm toolchain, usable by the non-root runtime without a download.
- Package version, pnpm build-dependency policy, and frontend guidance.

The existing API/stream client also contains authentication, replay-gap,
terminal-run, and history-ordering behavior. Copying all current source avoids
losing these contracts even when they are not part of the 32-file delta.

## Ownership and boundaries

| Surface                             | Ownership after migration                                                |
| ----------------------------------- | ------------------------------------------------------------------------ |
| `frontend/**`                       | Exact upstream snapshot; updated only through upstream syncs             |
| `frontend-hm/**`                    | Hartmesh source, assets, dependencies, tests, build config, and guidance |
| Gateway HTTP/SSE/WebSocket behavior | Backend contract consumed by `frontend-hm/`                              |
| `contracts/**`                      | Shared protocol fixtures and schemas, where already appropriate          |
| Root scripts, CI, Compose, Helm     | Integration wiring; small explicit Hartmesh changes                      |

`frontend-hm/` must have its own package manifest, lockfile, pnpm policy,
TypeScript/Next.js configuration, test configuration, and Dockerfile. Keep
the current package-manager and dependency versions during the split. Rename
its private package to `hartmesh-frontend` without changing runtime behavior.

No imports, symlinks, package links, TypeScript aliases, asset references, or
build steps may source application code from `frontend/`. Keep the existing
API clients and hooks inside `frontend-hm/src/core/`; a published SDK or shared
component package is not required for this migration. Existing shared
contract fixtures can remain under `contracts/`.

Hartmesh does not promise that the retained upstream application works with
its modified backend. Required product checks target `frontend-hm/`.
Reference-only upstream checks can be run explicitly without creating an
obligation to patch `frontend/`.

### Source names and runtime names

Keep these established deployment identifiers:

| Identifier                                 | Decision                                          |
| ------------------------------------------ | ------------------------------------------------- |
| Source directory                           | Change the product source to `frontend-hm/`       |
| Compose/Helm service and release component | Keep `frontend`                                   |
| Fork image repository                      | Keep `ghcr.io/altakleos/hartmesh-frontend`        |
| Container application path                 | Keep `/app/frontend`                              |
| Container/local Next.js port               | Keep `3000`                                       |
| Public entry                               | Keep nginx on the existing configured ingress     |
| Browser API paths                          | Keep `/api/langgraph/*` and other `/api/*` routes |

The new Dockerfile can use `COPY frontend-hm ./frontend` from the repository
build context. `/app/frontend` is then the Hartmesh app inside the image; it
does not refer to the checkout's upstream directory. This retains existing
Helm working directories, cache mounts, nginx upstream names, image-manifest
component keys, and topology field names without a naming migration. New
image digests still change the corresponding release/topology identity.

### Versioning

Move the frontend member of the coordinated version contract to
`frontend-hm/package.json`. It must match `backend/pyproject.toml`, the root
entry in `backend/uv.lock`, and Chart `version` / `appVersion`.
`frontend/package.json` retains the upstream version, even when it differs.
The new application's About-page fallback and `APP_VERSION` build override
continue to use its own package metadata. Splitting directories alone does
not require a release bump.

### Local configuration and tools

All root Hartmesh commands target `frontend-hm/`, including install, setup,
doctor, support-bundle, development, production, and skipped-build checks.

Extend `scripts/pnpm.py` with an explicit selector, for example
`--project frontend-hm -- <pnpm arguments>`. Keep the existing no-selector
behavior targeting `frontend/` for the unchanged upstream Makefile; Hartmesh
callers always select `frontend-hm` explicitly. Resolve executable paths
before changing directory, retain direct pnpm/Corepack fallback behavior,
and run in the selected project's directory. Reject a missing or invalid
selected project rather than falling back to upstream.

The Hartmesh environment file becomes `frontend-hm/.env`. New setup commands
create it from `frontend-hm/.env.example`. For existing installations, provide
a one-time local migration that copies an existing `frontend/.env` only when
the destination is absent, without printing values or overwriting either
file. Never include local environment files, dependencies, build output,
caches, reports, or runtime data in the committed source copy.

## Implementation sequence

### 1. Capture the seed and baseline

- Confirm a clean worktree, inspect the current integrated upstream history,
  and update the recorded values above if needed.
- Run the current frontend checks and relevant backend tooling suites to
  record the pre-migration result; retain any baseline failures explicitly.
- Add `frontend-hm/` as a copy of tracked files from the seed commit. Keep
  `frontend/` in place. Preserve file modes and upstream license notices.
- Record seed provenance. Verify that the pure-copy commit's subtree equals
  the seed frontend tree before making Hartmesh path/configuration edits.

Deliverable: an independently reviewable copy commit, followed by small
integration changes. Do not simultaneously redesign screens.

### 2. Route development and setup to the Hartmesh app

Write failing regression tests for project selection, local config migration,
and startup paths before updating the scripts.

| Files                                                                  | Planned work                                                                                                                                   |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/pnpm.py`, new `frontend-hm/Makefile`, root `Makefile`         | Explicit Hartmesh project selection; preserve pinned pnpm behavior and shell invocation conventions                                            |
| `scripts/serve.sh`                                                     | Install/launch from `frontend-hm`; inspect its `.next/BUILD_ID` for `SKIP_FRONTEND_BUILD`; retain local `PORT=3000`, daemon behavior, and logs |
| `scripts/configure.py`, `scripts/setup_wizard.py`, `scripts/docker.sh` | Generate/migrate the Hartmesh env file and use it in Docker setup                                                                              |
| `scripts/check.py`, `scripts/doctor.py`, `scripts/support_bundle.py`   | Inspect the Hartmesh project's toolchain and env-file presence; preserve redaction                                                             |
| `.pre-commit-config.yaml`                                              | Run product eslint/prettier only in `frontend-hm`; upstream syncs must not reformat the reference tree                                         |
| `.gitignore`, copied ignore files                                      | Cover Hartmesh reports/caches and preserve ignored local configuration                                                                         |

Verify the copied app's sibling paths, including backend replay/recording
scripts and shared contract fixtures. Audit Next.js workspace inference and
TypeScript resolution with both project lockfiles present.

### 3. Route images and releases to the Hartmesh app

| Files                                                                                            | Planned work                                                                                                                                            |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `frontend-hm/Dockerfile`                                                                         | Build only Hartmesh source into `/app/frontend`; retain dev/prod targets, frozen-lockfile installs, version stamping, and offline non-root pnpm startup |
| `docker/docker-compose-dev.yaml`                                                                 | Select the new Dockerfile; map `frontend-hm/src`, `public`, config, and env file to existing container paths                                            |
| `docker/docker-compose.yaml`                                                                     | Select the new Dockerfile and env file; retain service/network/readiness contracts                                                                      |
| `.dockerignore`, Dockerfile-specific ignore files                                                | Apply corresponding exclusions to the new source path; ensure no local secrets or build/runtime output enters images                                    |
| `.github/workflows/container.yaml`                                                               | Point the frontend build matrix at `frontend-hm/Dockerfile`; preserve component/image names and candidate-build versus pinned-digest-adoption behavior  |
| `scripts/bump_version.sh`, `scripts/verify_versions.sh`, `.github/workflows/verify-versions.yml` | Change the coordinated frontend version source and diagnostic paths                                                                                     |
| `deploy/compose/`, `deploy/helm/deer-flow/`                                                      | Verify the preserved image/runtime contract and update build instructions; no service or cache-path rename needed                                       |

Keep release-manifest parsing, image pinning, and qualification image fields
named `frontend`. Existing published digest pins still identify the old
release; do not invent new pins or claim the tenant profile serves the new
build until the normal candidate-build/pin/release process is completed.
Record that release handoff separately from implementation verification.

`nightly.yaml` currently gates its publisher on `bytedance/deer-flow`.
Retain that upstream-only workflow's scope and frontend path; do not enable
a fork nightly publisher as a side effect. Any future Hartmesh nightly must
explicitly select `frontend-hm/`.

### 4. Restore upstream and enforce the boundary

After the seed is preserved and product consumers have moved, restore the
tracked `frontend/` tree from the integrated upstream commit. Remove
Hartmesh-only tracked files from that tree as part of the restoration.
Preserve ignored local files. Verify exact subtree equality, including file
modes and additions/deletions; a textual comparison of common files is
insufficient.

Add a small `.github/upstream-frontend.json` marker outside `frontend/` with
a schema version, upstream repository, integrated upstream commit, its
frontend tree ID, and the original Hartmesh seed commit. Add
`scripts/verify_frontend_isolation.py` with a root
`make check-frontend-isolation` entry point and a step in the existing
frontend lint/build job.

The verifier must:

- Check that the pinned commit and subtree exist and the commit is an
  ancestor of the checked Hartmesh revision.
- Check that the declared upstream tree matches that commit's `frontend`
  tree and that the checked revision's `frontend` subtree is identical.
- Report changed paths without rewriting files. Support an explicit revision
  for CI and a worktree/index check for local use.
- Fail with an actionable message when history is unavailable. CI should
  fetch sufficient history, such as `fetch-depth: 0`; missing objects are
  not a passing or skipped check.

The marker changes only in an upstream-sync change, after reviewing the
upstream commit actually merged. Tree equality and ancestry detect drift;
they are not independent proof that an arbitrarily edited marker names an
authentic upstream revision. Review its provenance during syncs.

Enforce the application's source boundary through import/dependency/config
checks. In a disposable validation checkout, build and test `frontend-hm/`
with the reference directory unavailable. That catches indirect asset and
build dependencies as well as explicit imports. The drift verifier itself
still needs `frontend/` and runs in the normal checkout.

### 5. Move CI, repository assertions, and documentation

Repoint product jobs in `lint-check.yml`, `frontend-unit-tests.yml`,
`e2e-tests.yml`, and `replay-e2e.yml` to `frontend-hm/`. Update working
directories, path filters, toolchain/cache inputs where present, and report
artifact paths. Keep existing required job/status identifiers where possible.
Run the upstream-tree verifier on every relevant PR, including changes to
`frontend/` or the marker; it must run before any product-only path gating.

Replay checks must still run for backend changes, shared contract changes,
and changes to their own setup/workflow. Run the copied real-backend suite
against the actual replay Gateway; mocked browser tests alone cannot detect
an API mismatch. Update `triage.yml` path classification for the new app.

Retarget backend tests that currently read product files in `frontend/`.
In particular, image/runtime and pnpm assertions must inspect `frontend-hm`.
Update guidance-shape assertions for the two new Hartmesh guides and preserve
guidance budgets. Upstream guide content is owned by the snapshot: if future
upstream guidance violates a Hartmesh-only convention, handle that in the
external checker policy rather than editing the reference tree.

Update the root `README.md`, `AGENTS.md`, `Install.md`, `CONTRIBUTING.md`,
`RELEASING.md`, `docs/ARCHITECTURE.md`, `scripts/AGENTS.md`, and relevant
Compose/Helm docs. Adapt `frontend-hm/README.md`, `AGENTS.md`, and
`src/AGENTS.md` for Hartmesh ownership and commands. Preserve the copied
`CLAUDE.md` import shim unchanged. Update active backend references and
`contracts/slash_skill_contract.json`'s descriptive frontend path. Leave
historical plans/reports describing past changes intact; review translated
entry points so current guidance does not send Hartmesh developers to the
upstream app.

## Validation and acceptance

Use existing suites before adding coverage. Add tests for new selection,
migration, and isolation behavior, rather than duplicating every existing UI
test. Follow backend TDD and its required offline/blocking-I/O checks for
implementation changes there.

| Concern             | Required evidence                                                                                                                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Source preservation | Pure-copy subtree equals seed; evidence/governance components, locales, and tests survive                                                                                                               |
| Upstream equality   | Verifier passes; synthetic Git tests reject changed/added/deleted files, mode changes, wrong trees, non-ancestor pins, and missing history; an intentional upstream sync advances the marker and passes |
| Independence        | No cross-app imports/package links/assets; Hartmesh installs, builds, and tests without upstream source available                                                                                       |
| Tooling/config      | Tests cover both pnpm selections, default compatibility, fallback/error propagation, absent project, env migration without overwrite/disclosure, diagnostics, and skipped-build lookup                  |
| Product quality     | `pnpm install --frozen-lockfile`, `pnpm format`, `pnpm check`, `pnpm test`, and production build in `frontend-hm/`                                                                                      |
| Browser behavior    | Mocked E2E, auth E2E, and `playwright.real-backend.config.ts` against the replay Gateway; retain route performance budgets via `pnpm perf:check`                                                        |
| API behavior        | Login/session/CSRF, normal and custom-agent chat, streaming/reconnect/gaps, stop and reload/history, uploads/artifacts, human input, governance failure states, and evidence display/export             |
| Packaging           | Dev/prod image builds use the new source; non-root production startup works with package-manager network access disabled; local/dev/prod Compose render checks and Helm render/runtime-path tests pass  |
| Releases            | Version tests reject Hartmesh version drift while allowing the reference app's upstream version; build workflow targets the new Dockerfile and retains release provenance rules                         |
| Merge behavior      | In a temporary Git fixture or disposable worktree, upstream edits/additions/deletions under `frontend/` merge without changing the Hartmesh subtree; advancing the marker restores the equality gate    |

Relevant existing backend regression anchors include:

- `test_pnpm_script.py`, `test_check_script.py`, `test_doctor.py`,
  `test_setup_wizard.py`, `test_support_bundle.py`, and
  `test_serve_frontend_skip_build.py`.
- `test_frontend_image_runtime.py`,
  `test_dockerignore_excludes_runtime_data.py`,
  `test_docker_sandbox_mode_detection.py`, `test_compose_profile.py`,
  `test_compose_default_bind_host.py`, `test_helm_fs_group_change_policy.py`,
  and applicable Helm deployment contract tests.
- `test_release_version_tooling.py`, `test_release_workflows.py`,
  `test_release_manifest_verifier.py`, `test_agent_guidance_check.py`,
  `test_slash_skill_contract.py`, and `test_replay_golden.py`.

Inspect which API scenarios the retained suites actually exercise and add
focused replay/integration cases for material gaps. Preserve real-backend
auth coverage as well as the separate auth-page tests, which mock responses.
No live provider call is required for the standard replay gate. Missing
Docker, browser, or other required infrastructure is an unpassed check to
report, not evidence of completion. This migration provides no new
`durable_two_gateway_v1` qualification.

Completion means all supported Hartmesh entry points use `frontend-hm/`,
the reference tree matches its pin, required checks pass, and the existing
user-visible behavior is preserved. Image publishing and tenant rollout
follow the normal release procedure afterward.

## Subsequent upstream syncs

1. Fetch and select the upstream commit to integrate through the normal
   merge workflow. Do not restore the reference tree to an unmerged tip.
2. Merge upstream. Frontend edits normally land in `frontend/`; inspect any
   shared tooling/backend conflicts and resolve them under this ownership
   policy. Do not automatically overwrite the whole reference tree to conceal
   unexplained conflicts or an unexpected upstream directory reorganization.
3. Update the marker to the merged upstream commit and tree; verify exact
   equality and confirm the upstream merge did not change `frontend-hm/`.
4. Review upstream API/protocol and dependency/security changes. Deliberately
   port relevant fixes into `frontend-hm/`, recording their origin, then run
   Hartmesh compatibility tests. Its dependencies now need their own upkeep.
5. Review newly introduced root/CI/deployment references to `frontend/` so
   upstream tooling cannot silently become the Hartmesh product build again.

Keep both source paths. Git's default merge strategy detects renames, so a
directory move alone is not isolation. `-X ours` also accepts nonconflicting
upstream changes; no blanket merge preference is part of this design.
See the [Git merge reference](https://git-scm.com/docs/git-merge#_merge_strategies).

## Rollback and later work

Before release, revert the migration as a coordinated change if necessary;
keep source routing, version tooling, hooks, checks, and the upstream marker
consistent. In deployment, use the existing previous release/image rollback
procedure. Do not point the Hartmesh deployment at the retained upstream
application as a fallback: it lacks the preserved fork behavior.

After the boundary lands, redesign routes/components inside `frontend-hm/`
incrementally. A separate repository or extracted API package becomes a
separate decision if team ownership or release cadence later requires it.

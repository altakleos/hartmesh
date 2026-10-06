# HartMesh frontend ownership and scope

`frontend-hm/` is the product application. It owns its source, dependencies,
assets, environment example, Dockerfile, tests and release version. Root
orchestration and release builds select it explicitly.

`frontend/` is a byte-for-byte upstream reference. Its tree and ancestor commit
are recorded in [the snapshot marker](../.github/upstream-frontend.json). Do not
edit, format, version-bump or add guidance there for HartMesh. Product fixes
belong in `frontend-hm/`, including fixes first discovered in upstream code.

## Product scope

HartMesh retains its authenticated workspace and selects upstream UI features
deliberately. Backend availability does not imply that the product has a page
for that feature.

| Surface | HartMesh behavior |
| --- | --- |
| `/`, `/workspace` | Redirect to the workspace's new-chat route; signed-out users enter sign-in or first-run setup. |
| `/login`, `/setup`, `/auth/callback` | Account sign-in, initial setup and configured SSO callback. |
| `/workspace/chats`, `/workspace/chats/[thread_id]` | Conversation list, new chat (`new`), streaming, history, artifacts, reports and transcript export. |
| `/workspace/files` | Personal files and the Shared tab. |
| `/workspace/extensions/[namespace]/[surface_id]` | Pages declared by installed, enabled trusted plugins. Navigation and conversation actions use the same authenticated plugin snapshot; no browser installer is included. |
| `/workspace/agents`, `/workspace/agents/new`, `/workspace/agents/[agent_name]/chats/[thread_id]` | Custom-agent selection, creation and conversations, subject to configuration and permissions. |
| `/workspace/scheduled-tasks` | Scheduled-task management. |
| `/artifacts/view` | Standalone artifact rendering. |
| Settings dialog | Account/provider keys, appearance, notifications, memory, channels and developer settings. The deployment profile and role control visibility; the Gateway enforces authorization. |
| Projects, document shelves, project trash and server-side preferences | Gateway APIs and persistence are present. Project/trash pages and the upstream preference editor are outside the current product UI. Account export includes owned active/archived project definitions and active shelf documents created through those APIs; trash is excluded. |
| Upstream landing pages, documentation site and public showcase navigation | Kept in the upstream reference. HartMesh opens its workspace; bundled static-demo fixtures serve development and tests. |

The scope above describes the shipped routes, not a promise to port every
upstream component. A new product surface needs its own UI, API compatibility,
authorization and regression review. Do not make omitted pages appear by
importing their upstream source at build time.

## Development and verification

From the repository root:

```bash
make frontend-config
python3 scripts/pnpm.py --project frontend-hm -- check
python3 scripts/pnpm.py --project frontend-hm -- test
make check-frontend-isolation
```

The pnpm runner resolves executables before changing into the selected app.
Without `--project frontend-hm`, it retains the upstream `frontend/` default
for compatibility. `make frontend-config` preserves an existing product `.env`
or migrates the old app's settings; real environment files stay untracked.

The isolation check compares both staged and working-tree material with the
pinned upstream tree. It also rejects product symlinks escaping the app and
literal source/build references to `frontend/`. This is a static guard, not a
JavaScript interpreter. CI additionally removes the upstream directory before
building the product, so computed build dependencies must work independently.
For a committed revision use:

```bash
python3 scripts/verify_frontend_isolation.py --revision HEAD
```

The production [Docker acceptance journey](../RELEASING.md#docker-acceptance)
exercises real application services with synthetic inference. The
[frontend guide](../frontend-hm/AGENTS.md) describes the unit, mocked-browser
and real-Gateway test suites.

## Taking an upstream update

1. Review and merge the chosen upstream commit according to the repository's
   normal branch and PR process. Preserve `frontend/` as that exact upstream
   tree; resolve HartMesh changes in `frontend-hm/` separately.
2. Port selected fixes into the product with focused tests. Review backend
   protocol changes against the product's clients, even when no upstream UI
   feature is selected.
3. After the upstream commit is an ancestor and the reference tree matches it,
   update the marker with its exact reviewed commit:
   ```bash
   python3 scripts/verify_frontend_isolation.py --pin <upstream-commit>
   ```
   This records provenance and verifies ancestry/tree equality. It does not
   authorize arbitrary reference edits or replace review of the upstream merge.
4. Stage the marker with the merge, run isolation and product checks, and keep
   HartMesh versions in the four sources listed in [RELEASING.md](../RELEASING.md#version).

Do not move the marker merely to silence an isolation failure. Investigate the
reference diff and restore the exact reviewed upstream tree first.

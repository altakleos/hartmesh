# Repository guidance details

Detail moved out of the root `AGENTS.md` so every directory-scoped guidance chain stays within the budgets in `scripts/check_agent_guidance.py`. These rules still apply; the root guide links each section.

## Third-party extensions

Third-party extensions come from the operator-controlled `plugins:` list in
`config.yaml`, never API-writable `extensions_config.json`: they import code
with Gateway privileges and must be trusted. They contribute middleware,
lifecycle observers, Gateway services, routers and experimental full-stack
plugins. Manage them with `deerflow extensions` or `make extension-*`; restart
required. See [the extensions guide](../backend/packages/harness/deerflow/extensions/AGENTS.md).
The upstream reference manual is under `frontend/src/content/{en,zh}/harness/extensions/`.

## Skill review and CI waivers

Skill review: `skills/public/skill-reviewer/` is the read-only reviewer using
`review_skill_package` and `contracts/skill_review/`. Model-visible data is
compact and tag-neutralized; raw payloads stay in artifacts. See the backend
guide for activation and skill-creator boundaries.

CI waivers (`.github/skill-review-waivers.v1.json`, enforced by
`scripts/review_changed_public_skills.py`) come only from the trusted base
manifest, bind one error to its file SHA-256 and expiry, stay visible, and never
waive blockers. An entry may preapprove future full-file hashes, effective once
that manifest lands in the trusted base. Merge the waiver before changing the
skill, then promote the consumed hash to `file_sha256`.

## Extension make targets

```bash
make extension-install SOURCE=...  # Install and enable a trusted Python extension
make extension-upgrade SOURCE=...  # Replace an installed extension and keep its config
make extension-list                # List configured Python extensions
make extension-enable NAME=...     # Enable an installed extension (restart required)
make extension-disable NAME=...    # Disable without uninstalling (restart required)
make extension-remove NAME=...     # Remove package and config entry (restart required)
```

## HartMesh pnpm wrapper

HartMesh host calls use `scripts/pnpm.py --project frontend-hm --`: it prefers
direct pnpm, falls back to Corepack, and resolves executable paths before
changing to that project. The no-selector form retains `frontend/` for upstream
compatibility. `make frontend-config` initializes the product's ignored `.env`,
preserving an existing destination or migrating the old app's settings.

## Scheduled-task notes

- `scheduler.tool_enabled` (default off) offers owner-bound schedule tools only
  through Gateway capability admission. Interactive turns create/manage their
  schedules; scheduled turns can request stopping only their own schedule.
  Per-occurrence goal success does not end a recurring schedule.
- The scheduled-task MVP adds a workspace page at `/workspace/scheduled-tasks` plus a
  background scheduler gated by `config.yaml -> scheduler.enabled`.
- Scheduled runs are non-interactive: `ask_clarification` is excluded. `non_interactive`,
  `disable_clarification`, and `github_token` are honored only for internally-authenticated
  callers; client-supplied copies are dropped.
- Busy occurrences persist as `queued`; `scheduler.queue_timeout_seconds` bounds the wait.
  Do not reintroduce skip-on-overlap.

## Skill script tests

Skill-creator tests must preserve YAML values/body, restore bytecode settings,
and check invalid metadata through offline CLI subprocesses.
Public skill script regressions live in `tests/skills/` and run in Skill Review CI
independently of package review. Exercise dependency fallbacks with the optional
import unavailable and mock HTTP transport to keep tests offline.
Video, podcast, and chart-visualization CLI failures exit non-zero;
diagnostics on stderr, success URLs on stdout; test offline.

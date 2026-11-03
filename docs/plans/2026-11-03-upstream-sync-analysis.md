# DeerFlow → HartMesh: upstream changes since the last sync

*Analysis date: 2026-11-03. HartMesh `main` @ `59a3500` (v2.2.0+hartmesh.50). Upstream `bytedance/deer-flow` `main` @ `14abd9a` (2026-11-02).*

---

## 1. Executive summary

**Where things stand**

- HartMesh last took upstream at **`67db3d8`** (upstream #6020, 2026-10-01). That port was HartMesh PR #177.
- Since then, upstream `main` has gained **950 squash-merged commits** from 205 authors: about 390k inserted lines across 2,453 files.
- In the same month HartMesh made 363 commits of its own (releases 38 → 50).
- **None of upstream's commits since the merge base have been cherry-picked.** (`git log --cherry-mark` finds zero patch-equivalent commits.)

**HartMesh's version label is misleading.** It calls itself `2.2.0+hartmesh.N`, but it contains upstream only up to `2.2.0-dev` of Oct 1.

- Upstream cut `v2.2.0-rc0` on Oct 29 from `main@b537d9c`. That point holds 877 of the 950 commits.
- Upstream `main` is now `2.3.0-dev`.

**Headline findings**

1. **Ten security or data-loss problems HartMesh ships today, each fixed upstream.** Each is small and can be cherry-picked or hand-ported now, without the merge (§4.1). The worst:
   - a rollback cancel that can wipe a conversation's entire checkpoint history;
   - an SSRF hole that lets any signed-in user point a personal MCP server at cloud metadata (100.64.0.0/10, including Alibaba's `100.100.100.200`);
   - a backend lockfile with **93 known advisories** (Pillow 41, PyJWT 15, certifi critical, mcp `$ref` fetches), against 2 in upstream's lock;
   - `frontend-hm` on **Next.js 16.2.11**, with 5 critical advisories, including the unauthenticated Image Optimizer RCE;
   - an open redirect on sign-in;
   - `review_skill_package` reading other users' skills.
2. **The full merge is large, and several of its breaks would raise no conflict.**
   - The trial merge has **178 conflicted files and 576 conflict hunks**. Hot spots: `aio_sandbox_provider.py` (39), `runs/manager.py` (32), `runs/worker.py` (27), `gateway/services.py` (20).
   - The bigger risk is a dozen *semantic* breaks with no conflict markers (§5.1). Examples: a cleanly deleted module that HartMesh still imports, a startup `ImportError` in `deps.py`, an every-bind outage in `sandbox/lease.py`, mixed wall/monotonic clocks in the warm pool, and an output-evidence allowlist that would reject every Telegram bot turn.
3. **Upstream built its own versions of several things HartMesh already has** (§5.2). The two versions usually differ in design: login throttling, account disable, sandbox prewarm, run-cancel process cleanup, terminal-commit authority, inbound IM files, and human-input routing. The merge has to pick one design per area rather than union the code. HartMesh's version is usually stronger or more product-specific.
4. **Several upstream features are safe only if they stay off in HartMesh** (§6): Queue/Steer on autonomous Work runs, speculative sandbox prewarm under the 2-slot budget, the admin users HTTP API, shared blob storage, run-event producer fencing in strict mode, LLM shelf summaries, and managed models.
5. **Conflict count grows by about 3.5 files per day of upstream activity**:

   | Merging up to | Conflicted files |
   |---|---|
   | Upstream commit #100 (5 days after the merge base) | 52 |
   | #300 | 103 |
   | 2.2.0 point | 171 |
   | Upstream head | 178 |

   Staging the merge doesn't reduce the total, but syncing more often after this catch-up would.

**Recommended path** (§7):

1. Take the "now" fixes this week as small PRs.
2. Land the preparation PRs: AGENTS.md trimming, the skill-review waiver, a migration plan, and the version-number policy.
3. Merge upstream **to `b537d9c` (upstream 2.2.0)** first, so HartMesh's version label becomes true.
4. Take the remaining 73 post-2.2.0 commits in a second, smaller merge. That slice holds most of the hardest design collisions: Queue/Steer, run-level command cancellation, the event-producer fencing trigger with migrations 0049–0051, and the admission lock split.

---

## 2. Scope and method

- Unshallowed the clone and added `upstream` = `bytedance/deer-flow`.
- Computed the merge base and ran a trial `git merge-tree` of HEAD against upstream/main.
- Split the 950 commits into nine themes by the files they touch. Each theme was analysed against HartMesh's code: sandbox, runtime/persistence, gateway/auth, agent core, skills/MCP, IM channels, extensions/community/projects, frontend, and infra/deps.
- Spot-checks:
  - per-commit `git merge-tree --merge-base=<c>^` to see whether each candidate cherry-pick applies;
  - `ruff` on take-ours and take-theirs variants;
  - an OSV advisory scan of the three lockfiles (merge base, HartMesh, upstream);
  - `pnpm audit` on `frontend-hm`;
  - a trial `uv lock` regeneration;
  - a simulation of HartMesh's Skill Review CI over upstream's skill changes.
- I wrote a static import checker and ran it over the merged tree, plus "conflicts → ours" and "conflicts → theirs" variants. It found every cross-module name that one side needs from the other's version of a conflicted file (see Appendix B to rerun it).
- I re-verified every CRITICAL claim below directly in HartMesh code.
- **Limits:**
  - Nothing was executed against a running stack.
  - Line numbers are HartMesh HEAD unless they say "merged".
  - The analysis made no repository changes beyond this document.

---

## 3. What upstream did this month (by area)

| Area | Commits | Main arcs | HartMesh relevance |
|---|---|---|---|
| Runtime and persistence | 94 | Owner-confirmed terminal commits (#6263→#7664). `run.terminal` evidence. Producer fencing trigger. Queue/Steer. Conversation schedules plus IM notification outbox. NaN-safe serialisation. 26 migrations (0027–0051). | **High.** HartMesh built a parallel lifecycle in `runtime/runs/*`. The two need reconciling. |
| Sandbox | 79 | AIO lifecycle hardening (quarantine, retryable teardown, shutdown fence). Speculative AIO prewarm. Run-scoped command cancellation. Audit and file-tool fixes. About 30 LocalSandbox fixes. | **High.** HartMesh's session provider and Spaces containment overlap heavily. |
| Gateway, auth, authz | 103 | Account disable and admin API. DB-backed login lockout. Credentials cipher. Middleware-declared tool authz. Upload companion records. Many 500→4xx fixes. OIDC fixes. | **High.** HartMesh's identity and seat model is a superset. Some upstream parts merge silently. |
| Agent core | 179 | Skill tool policy deleted. New always-on middlewares. Subagent lifecycle and durable batch leases. DeerMem fairness and compaction fixes. Managed models. Config validation. | **High.** The lead-assembly and output-inventory allowlists break. |
| Skills, SkillScan, MCP | 94 | `allowed-tools` demoted to metadata. ~20 scanner fixes. Review-tool confinement. Guarded skill evolution. MCP reconciliation, sessions keyed by ownership domain, ResourceLink handling, OAuth token persistence. | **Medium–High.** One CRITICAL (#6580), one CI merge blocker. |
| IM channels | 82 | Multi-instance channels (leader lease, DB bindings, encrypted shared config). Exactly-once IM runs. Notification outbox delivery. QQ, Feishu cards, `/model`. | **Low for product, high for merge.** HartMesh's managed Telegram rides the channel manager. |
| Extensions, community, projects, knowledge | 114 | extension-api 0.2.4→0.2.9 (agent runs, `PluginToolResult`). Workflow and agent-team example plugins. Knowledge phases 1–3. SSRF hardening. Provider robustness. | **Medium.** One CRITICAL (#6202). Extension-API version collision. Always-on assembly evidence. |
| Frontend (`frontend/`) | 151 touching | ~60 robustness fixes. Conversation schedules UI. Goal outcomes. Queue/Steer UI. Token dashboard. Admin users. Next 16.4.1, zod 4. | **High via `frontend-hm`.** Nothing merges automatically. Ports are deliberate; two CRITICAL. |
| Infra, deps, CI | 142 | ~20 Dependabot security bumps. Version gate (`uv lock --check`, harness lockstep). nginx pin. Loopback binding. CI sharding. AGENTS.md size budgets. TUI and Windows fixes. | **High.** Advisories, size budgets, version policy. |

---

## 4. Take now: fixes HartMesh needs before (and independent of) the merge

### 4.1 Security and data-loss (CRITICAL)

| # | Upstream | Problem in HartMesh HEAD | How to take it |
|---|---|---|---|
| 1 | `bec63a6` (#6513) | **Checkpoint wipe.** A rollback cancel that lands before `_capture_rollback_point()` finishes leaves `snapshot_capture_failed=False` (`runtime/runs/worker.py:1187`). The finalizer then calls `adelete_thread` (`:3208`) and deletes every checkpoint in the conversation. Reachable through the public cancel `action=rollback` and `multitask_strategy="rollback"`. *(verified)* | Cherry-pick (clean). |
| 2 | `7f7668d` (#6202) | **SSRF to cloud metadata through personal MCP.** `url_safety.is_blocked_address` (`community/url_safety.py:56`) allows `100.100.100.200`, `100.64.0.0/10` and `::ffff:100.x`. Any signed-in user can add an HTTP/SSE personal MCP server; `validate_public_http_url` is the only screen (`routers/personal_mcp.py:48`, `mcp/personal_network.py`). *(verified by running the predicate)* HartMesh's `direct_fetch` is already safe. | Hand-port: add `not address.is_global`, plus upstream's tests. |
| 3 | ~20 Dependabot bumps | **Backend lock: 93 distinct advisories** (Pillow 12.3 ×41, PyJWT 2.13 ×15, aiohttp ×8, mcp 1.28.1 ×4, certifi emSign CRITICAL, checkpoint-postgres namespace crossing, langchain-anthropic, urllib3…). Upstream's lock has 2. Direct exploit paths into HartMesh: *(advisory counts verified from the scan data)*<br>• **mcp GHSA-rwrf**: the client fetches server-chosen `$ref` URLs (http/file) on the event loop with no timeout. That gives blind internal fetches, local file reads, or a frozen Gateway, from any user's personal MCP server. It bypasses `_PersonalTransport`.<br>• **pyjwt GHSA-8wjv**: nested headers raise `RecursionError`, so `decode_token` returns 500 without authentication. | `uv lock --upgrade-package` on the listed packages now. A trial regeneration of upstream's lock with HartMesh's pyproject deltas resolved cleanly with uv 0.11.1, and `uv lock --check` passed. |
| 4 | #6402/#6523/#7384 (frontend) | **`frontend-hm` is on `next@16.2.11`**, older than the merge base. `pnpm audit`: 5 critical / 13 high, including the Image Optimization RCEs (GHSA-2xp9, GHSA-2464), `use cache`, and an image-optimizer SSRF. `/_next/image` is live: `images.unoptimized` is unset and nginx `location /` proxies it without auth. | Bump to Next ≥16.4.1, and set `images: { unoptimized: true }` as defence in depth (frontend-hm doesn't use `next/image`). Also bump zod to 4.6.7 (memory-exhaustion DoS; one `z.record` edit in `core/extensions/api.ts:41`) and source-map-js. |
| 5 | `9c00fd8` (#7467) | **Open redirect after sign-in.** `frontend-hm/src/core/auth/next-path.ts` lets `next=/%0A//evil.example` through, and `login/page.tsx:210` `router.push`es it. The backend's `validate_next_param` (`routers/auth.py:1284`) has the same gap. | Mechanical port to both files. |
| 6 | `16877b9` (#6580) | **Cross-user skill read.** `review_skill_package` accepts any local path under the Gateway's working directory or `/tmp` (`tools/builtins/review_skill_package_tool.py:120`). In `make up`/`make dev`, `DEER_FLOW_HOME` sits under the working directory, so user A can review user B's custom skills. The Compose profile still admits `/tmp`. *(verified)* | Cherry-pick (code clean, docs conflict). |
| 7 | `9d05099` (#7374) | **Deliverable overwrite.** `write_file` accepts the tool-output elision placeholder. HartMesh still emits that placeholder (`tool_output_budget_middleware.py:717`), so a model that echoes it can replace a finished file with 194 bytes of placeholder. Upstream issue #7319 reports exactly that. | Cherry-pick (import conflict in `sandbox/tools.py`). |
| 8 | `b4b2a68` (#7665) | `/api/memory/config` and `/status` return the *resolved* memory `backend_config`, including API keys, to anyone with `memory:read` (`routers/memory.py:592`). Exploitable only when memory credentials are configured. | Cherry-pick, keeping the `secret_masking` imports. |
| 9 | `c0ba948` (#7266), `544b8b2` (#7217) | The OIDC state cookie presented as a session token gives an unauthenticated 500 or a crashed WebSocket. Upload/delete/attach 500s leak host paths (`routers/uploads.py:455,529`, `project_documents.py:423,430`). | Both cherry-pick cleanly. Add #7255, #7275 and #7155, which fix the same path-leak problem elsewhere. |
| 10 | `62a5b6c` (#6831) | **SSO breaks with IdPs whose authorize URL already has a query** (for example Azure AD B2C `?p=`): `oidc.py:212` builds `f"{endpoint}?{urlencode(...)}"`. | About a 5-line hand-port. |

### 4.2 High-value correctness fixes (clean or near-clean)

- **Postgres JSON NaN.**
  - `9e6ce2f` (#7670): NaN/inf breaks the SSE stream and REST reads.
  - `3027ab6` (#7733): Postgres jsonb rejects NaN, so the whole run write fails with `DataError`. HartMesh runs Postgres and ships pandas skills. Drop the `topology.py` hunk.
- **Audit middleware.** `7d50f3f` (#6307): the sandbox audit warning is appended *after* `Exit Code: N`, so HartMesh's subagent executor reads a failed command as success. The rebuilt message also drops `deerflow_tool_meta`. Clean.
  - Also take `48c884a` (#7290) and `8f78afe` (#7247), which block audit bypasses such as `bash <(curl …)`.
- **DeerMem.**
  - `bdc2d12` (#6672): a single process-wide 30-second debounce timer, re-armed on every enqueue, starves extraction for everyone under steady traffic.
  - `ee02e51` (#7152): a second pre-compaction snapshot overwrites the first, so that partition is never extracted.
  - `30b3a01` (#7530): fixes a HartMesh bug where async compaction runs the memory flush hook inline, `AgentExecution.validate_sync()` refuses on the owner loop (`agent_instances/conversations.py:77`), and the error is swallowed. **AI employees silently lose memory from before compaction.** I traced this call path but didn't execute it. HartMesh's test hides it by using `to_thread`.
- **Checkpoint scans.**
  - `c6ad5ed` (#5988): on Postgres, duration-only checkpoint leaves are mistaken for conversation state, which affects branch, regenerate and history.
  - `3fd03ec` (#6929): manual compaction drops a pending tool call.
- **Redis stream bridge (HartMesh's production bridge).** `b731d97` (#6690) and `38c55c6` (#6605): a malformed or future `Last-Event-ID` makes Redis error or wait forever. Also `2adc667` (#6951).
- **Persistence bootstrap.** These hide real migration errors behind a spurious `CREATE DATABASE`, which matters before the big migration merge:
  - `e05ca0c` (#6349) → `6dc28f5` (#7410) → `9a92d2e` (#7489);
  - `7f2c0fb` (#6306).
- **Middleware robustness.**
  - `9a49fb2` (#7762): a provider that reuses a call ID makes a clarification replace the previous question.
  - `7c52d5a` (#6150): a shared `.tmp` file during tool-output externalisation.
  - Hand-port #6107's hashed filename: a reused call ID overwrites another output.
  - `fb28305` (#6511): file-gate waiters exhaust the executor threads.
  - `b0a545e` (#6946) and `51d59a1` (#6965): Responses API length-cap and content-filter handling. HartMesh ships `gpt-5-responses`.
- **Skills and MCP.**
  - `16b4fcd` (#6506): slash activation loses `SKILL.md` on an LLM retry.
  - `3725ba1` (#6359) and `24c9881` (#7081): SkillScan OOM, and padded ELF/PE binaries passing as text.
  - Hand-port #6794: MCP discovery errors can log credentials resolved from `$VAR` (`mcp/tools.py:970,1055`).
  - `3f8f91d` (#6133): MCP sessions isolated per ownership domain.
  - `ee8e676` (#7400): saving bundled personal connections returns 500.
- **Projects trash.** `eb5d1e1` (#7732): a symlinked `.deer-flow`, `users/` or `projects/` tree makes the trash sweep delete live originals. Also `bee9702` (#7683) and `0181546` (#7499).
- **Scheduler.** `75de356` (#6975) and `fafe904` (#6185). Hand-port `619132d` (#7196): a permanent 409 is retried for the full 7,200 s.
- **Goals.** Hand-port `059758f` (#7167): stand down on `loop_capped`.
- **`frontend-hm` ports.** All are bugs that exist in frontend-hm today:
  - #7449: `z.string().email()` rejects EmailStr-valid addresses, which locks those accounts out of every server-side render.
  - #7122 + #7342: rejected uploads are sent silently.
  - #6701: relative assets in HTML artifact previews return 401. Decide on a CSP when porting.
  - #6827: Markdown report images.
  - #7650: save racing a stale read gives a false 412.
  - #6702: duplicate thread-list rows.
  - #7465: no "tool failed" marker.
  - #6407: side-chat draft lost after a failed send.
  - #7589: a malformed CSRF cookie breaks every call.
  - About 20 mechanical one-liners (list in §8.8).

### 4.3 HartMesh-owned gaps surfaced by upstream fixes

These are HartMesh's own code, with the same class of bug upstream just fixed.

- **SearXNG result URLs are unscreened.**
  - SearXNG is the Compose default. `normalize_results` and `normalize_image_results` check only an http(s) prefix.
  - frontend-hm renders `<a href>` and `<img src>` from them (`messages/message-group.tsx:738,785`).
  - Fix: port `safe_public_result_url` (#6740/#6819/#7037).
- **Blocking config reads on the event loop.**
  - `get_app_config()` stats and SHA-256-hashes `config.yaml` on every call.
  - It runs on the event loop in SearXNG and `direct_fetch` tools, in `sign_on_only()`/`_local_registration_enabled()` inside `require_live_account` (every authenticated request, `deps.py:1094`), and in register, setup-status and initialize.
  - Fix: follow upstream's #7557, #7576 and #7580 pattern.
- **The run journal duplicates events on a lost acknowledgement** (`journal.py:1142-1150`): ordinary runs retry a failed `put_batch`, which duplicates events on Postgres when the commit succeeded but the ack was lost. Mirror #6846: retry only when the store proves nothing was committed.
- **Admission lock blocks heartbeat renewal.** `RunManager.create_or_reject` holds `_lock` across Store I/O. With a 30-second lease, a 10-second heartbeat and `command_timeout: 30`, a slow admission can delay renewal, so ownership is fenced on healthy runs (the hazard upstream #7664 removes).
- **The shipped Anthropic model is retired.**
  - `deploy/compose/providers/models/20-anthropic.yaml` lists `claude-sonnet-4-20250514` as its only model *(verified)*. That model was retired 2026-06-15.
  - Its `budget_tokens` and `thinking: disabled` templates are rejected by current models.
  - The merge only fixes the wizard and `config.example.yaml`; the Compose catalog is HartMesh-owned.
- **Managed Telegram turns get a recursion limit of 100.** Web runs get 1000 (`frontend-hm/src/core/threads/hooks.ts:2764`). Upstream #7521 removes the manager's default, so the managed path would fall back to the Gateway default, which is also 100 in Compose.
- **Compose Gateway process handling.**
  - No `init: true`: uv as PID 1 doesn't reap zombie npx/uvx/docker children, which count against `pids_limit: 2048`.
  - No `--timeout-graceful-shutdown`: SSE connections can outlast `stop_grace_period: 60`.
- **The acceptance stack tests a different nginx than production.** `docker/acceptance` uses floating `nginx:alpine`. The pinned production digest is `1.31.5-alpine3.24` (musl 1.2.6), which hits the `pwritev2` restart loop (#7643) on older Docker daemons or kernels.
- **Local dev binds to all interfaces.** `scripts/serve.sh` and `backend/Makefile` bind the Gateway to `0.0.0.0`, and Next.js listens on all interfaces, so `/setup` is exposed on the LAN before the first admin exists. The merge brings #6587 and #6635.
- **The disabled-account error code isn't recognised.** `frontend-hm` `AUTH_ERROR_CODES` lacks `account_disabled`, so disabled users see "Authentication failed".
- **No Dependabot or audit gate** for either `backend/` or `frontend-hm/`. This is how both lockfiles fell behind.

---

## 5. The full merge: hazards and decisions

### 5.1 Breaks that would merge without (or despite) conflict markers

| Where | What happens | Fix |
|---|---|---|
| `skills/tool_policy.py` | Upstream deleted it. HartMesh never edited it, so the deletion merges silently. HartMesh's kept `skill_tool_policy_middleware.py` imports it, so the merged tree raises `ImportError` when the lead agent builds its middleware chain. *(found by the import checker)* | See §5.2(a). |
| `lead_agent/agent.py` | **The modify/delete resolution misses two other paths.** Upstream removes the middleware from the lead chain in a *clean* hunk, so keeping HartMesh's file alone silently loses hold-back and first-command for the lead agent. The subagent hunk resolved as "ours" gives a `NameError`, because the import was removed cleanly. | Same. |
| `_qualify_inventory_profile` (`lead_agent/agent.py` ~L918) | Exact allowlists of middleware types and state keys. Upstream adds `ModelContentCompatibilityMiddleware` (always on), `CompletedSubagentUsageMiddleware`, `RunInput*Middleware`, and the state keys `goal_outcome` and `applied_run_inputs`. The profile becomes `None`, so `worker.py:1773` raises `OutputEvidenceError` for **every inventory-v2 run, which includes every Telegram bot turn**. | Extend the allowlists deliberately. `test_output_inventory_profile.py` guards this. |
| `gateway/deps.py` (merged ~L999) | Upstream's install block imports `install_login_throttle_store` / `reset_login_throttle_store` from `auth/login_throttle.py`, but HartMesh's add/add module doesn't define them. Result: an `ImportError` at every startup with a database. | Keep HartMesh's throttle; delete upstream's install block, `_validate_login_throttle_storage` and `throttle_storage`. |
| `sandbox/lease.py` | Upstream's new `_bind_live_client` calls `_bind_locked` without HartMesh's `storage_candidate`. With Storage Spaces on, `storage_candidate_is_current(None)` is False, so **every bind raises `BackingUnavailable`: a total outage**. | Thread the token through, with an async variant. |
| `sandbox/sandbox_provider.py` | The auto-merged body assigns `_default_sandbox_provider` without the `global` HartMesh dropped. Ruff reports F823; a failed shutdown raises `UnboundLocalError`. | Re-add `global`. |
| `aio_sandbox_provider.py` | **Clocks.** Upstream's `_reap_expired_warm` uses `time.monotonic()`, while HartMesh's warm-pool and prewarm writers use `time.time()`. Warm sandboxes would never be reaped, unclaimed prewarms would be reaped instantly, and LRU eviction would pick wrong entries. **Shutdown.** #6231's `_stop_idle_checker` raises on a slow join; HartMesh never resets `_shutdown_called`, so teardown aborts for good. **Compilation.** Neither take-ours nor take-theirs compiles (F821s). | Make every warm-pool, last-activity and prewarm timestamp monotonic. Adopt upstream's retryable shutdown. Resolve by hand. |
| AIO/local sandboxes (#7772) | Upstream's run-scoped command cancellation turns on through auto-merged lines (`supports_command_cancellation=True`, `command_owner`, `command_lock`) even if `tools.py` is resolved to HartMesh's per-call abort. | Choose one mechanism (§5.2). |
| Channels `reply_locale.py` (#7591) | Connect wording moves to templates that say "connected to DeerFlow". HartMesh's adapter de-branding is lost silently, and its tests then fail. New DeerFlow strings also appear: `ALREADY_ADMITTED_MESSAGE` and attachment-failure texts. | Rebrand the templates. |
| Channel idempotency (#7104) | Internal callers' run keys change from `http-run:sha256(owner,thread,key)` to `internal-run:sha256(owner,key)`. Managed Telegram is internal (`services.py:1799`), so publications created before the upgrade can't be found by `lookup_run`, and their recovery stalls. They don't run twice. | Keep `http-run:` for `channel_execution`, or look up the legacy digest. Pass deterministic human-message IDs (#6401). |
| `extensions/ordering.py` | A clean merge drops the `DeferredToolPromotionAudit → SkillToolPolicy` ordering rule. | Re-add the rule for whatever replaces the middleware. |
| Upload companions (#6101 + 6) | `list_uploaded_files` will hide only converted Markdown that has a companion record. HartMesh's Storage Spaces ingestion (`chat_upload_ingestion.py:89-131`) never registers companions, so converted `.md` files appear as uploads and every PDF/DOCX loses its outline. | Call `register_companion` in `ChatUploadSession.finalize`. |
| Admin users (#6566/#6641) | The `admin_users` router is mounted cleanly, `set_disabled`/`update_system_role` land in `sqlite.py` (with a duplicate `list_users`, F811), and extra disabled checks land in `auth_middleware` and in `local_provider` *before* the password check. | Don't mount it. Implement `validate_resolved_session_user` via `account_refusal`. Remove the extra checks. |
| `contracts/*.json` | `slash_skill_contract.json` gains `model`; `subagent_status_contract.json` gains `model_length_capped`. frontend-hm contract tests fail (`core/skills/slash.ts:12`, `core/tasks/subtask-result.ts:53`). | Port both in the merge PR. |
| `config.example.yaml` `config_version` | HartMesh went 50→55, upstream 50→64; the numbers in between mean different schemas on each side. | Set ≥65 and update Compose config and `test_config_version_is_55`. |
| Credentials key (#6611) | Because Compose sets `channel_connections.enabled: true`, the Gateway would auto-create `{base_dir}/.credentials_key` on the data disk. That contradicts HartMesh's environment-only key policy (`HARTMESH_PROVIDER_KEYS_SECRET`). | Supply `DEER_FLOW_CREDENTIALS_KEY` from the environment (or derive it with HKDF), and keep it out of backups. |
| `deerflow_extension_api` | HartMesh used 0.2.5–0.2.7 for artifact presentations, storage/actors and human input; upstream used 0.2.5–0.2.9 for agent runs, `PluginToolResult` and knowledge. The loader accepts any ≤ host version, so a merged host misdescribes one side. | Take the union of both contract shapes and pick a new `API_VERSION` (≥0.2.10). Stop spending shared patch numbers on HartMesh-only contracts. |

**Cross-module names.** Beyond these, 174 (importer, name) pairs in HartMesh code depend on HartMesh-only names inside conflicted modules, and 114 pairs in upstream code depend on upstream-only names. Examples: `paths_scope` and `USER_FILES_VIRTUAL_PREFIX` in `config/paths.py`; the storage contracts in `deerflow_extension_api`; `_enforce_credentials_key` and the run admission/completion hooks in `deps.py`. Rerun the check from Appendix B against the resolved tree; a resolution that drops any of them fails at import time.

### 5.2 Design collisions: decide before resolving

**(a) Skill tool policy (#6935).** Upstream makes `allowed-tools` metadata only and deletes the middleware. HartMesh's 265 added lines in that middleware (bf00a4ac5) are not about `allowed-tools`. They are *same-message hold-back* (a tool call chosen in the same message as a skill's first `SKILL.md` read is refused as `not_run`) and *`first-command` ordering* (used by `business-report`).

HartMesh does not rely on `allowed-tools` for security: AI-employee authority comes from runtime scope, Work denials and the authorization provider. The only loss is that `skill-reviewer` is no longer mechanically read-only.

- **Recommendation:** accept the deletion. Move the hold-back and first-command hooks into a HartMesh-owned `SkillLoadOrderMiddleware` at the old slot in both chains (`first_command_order=False` for subagents).
- Update `_qualify_inventory_profile` and the 9 affected tests.
- This still passes upstream's `test_skill_tool_authority.py`.

**(b) Login throttling (add/add `auth/login_throttle.py`).**

- Upstream (#6501): a per-IP lockout store in the database (migration 0035).
- HartMesh: 734 lines covering a per-account lock plus a per-source spray guard, Redis-backed, fail-closed with 503, with admin list/clear.
- **Keep HartMesh's.** Take from upstream: trusted proxies given by hostname (#6519) and the off-loop config reads (#7576). Keep migrations 0035 and 0036 so the chain stays intact.
- Fix HartMesh's `docker/` compose and `make dev`: there, `AUTH_TRUSTED_PROXIES` is unset, so every login appears to come from nginx and the spray guard can 429 everyone.

**(c) Account disable and admin API.**

- Upstream: a `users.disabled` column plus HTTP PATCH.
- HartMesh: identity-keyed disable that works before an account exists; it bumps `token_version`, revokes PATs, cancels runs, holds schedules and keeps the seat. It is deliberately not reachable over HTTP.
- Problems if both coexist:
  - two sources of truth;
  - re-enabling through PATCH revives old sessions;
  - releasing the seat then refuses;
  - the last-admin guard ignores HartMesh's state;
  - custom roles break frontend-hm's `z.enum(["admin","user"])`.
- **Keep HartMesh's model; don't mount `admin_users`.**

**(d) Sandbox prewarm.**

- Upstream #6998: speculative, triggered by `abefore_model`, an ordinary acquire.
- HartMesh: provider-level prewarm when a conversation opens; never evicts, try-lock only, parked in the warm pool, with a claim timeout.
- They're complementary, but upstream's can evict another user's sandbox, wait out `capacity_wait_timeout`, or hold a slot under the 2-slot Compose budget.
- **Keep `sandbox.prewarm: false`.** If the worker/executor hunks are taken, take `register_sandbox_prewarm_cleanup` together with unconditional release. Otherwise HartMesh's executor (`subagents/executor.py:1975`) leaks unclaimed prewarms.
- Rename HartMesh's `test_sandbox_prewarm.py` (add/add) to `test_aio_open_prewarm.py`.

**(e) Run-cancel process cleanup.**

- Upstream #7772: run-scoped; kills everything the run's owner started, including earlier background jobs, and fires on run error.
- HartMesh (b4d5252df, bbdf46008, 7e1e1e859): per tool call; qualified with real-bash tests.
- **Keep HartMesh's and leave `supports_command_cancellation` False.** Upstream's worker calls then become no-ops.
- Separately decide whether "stop current work" should also kill jobs backgrounded earlier.

**(f) Run lifecycle and terminal authority.** Both sides built owner/lease/status compare-and-set terminal commits with staged-terminal renewal:

- upstream: #6263, #6620, #6846, #6914, #6919, #7134, #7500, #7619, #7664;
- HartMesh: `terminal.py`, `finalize_owned`, af680a14e, `GATEWAY_STOP_REASON`.

`manager.py` (35 conflict regions) and `worker.py` (31) are a redesign, not a textual merge.

- **Recommendation:** take upstream's structure as the base: split admission locks, journal write ownership, `run.terminal`, `_drain_run_finalization`. Re-layer HartMesh's output capture, AgentExecution environment, storage provider, customer-administration policy and `GATEWAY_STOP_REASON` on top.
- Upstream deleted `_ensure_delivery_receipt`, which HartMesh's orphan reconciler still calls (`manager.py:2961`).
- Re-apply HartMesh's `_check_open()`, `check_health()` (used by `redis_health.py`) and drained `close()` onto upstream's `stream_bridge/redis.py`.

**(g) Queue and Steer (#7729, post-2.2.0).** It turns on automatically whenever a run has an agent-runs host, the store supports inputs, and `non_interactive` is unset. HartMesh's Work attempts set `interaction_mode="autonomous"` but not `non_interactive`, so a run owner could **steer an autonomous Work attempt, an AI-employee run or an output-captured run mid-flight**. That breaks single-input output evidence and the `report_work`/Attention model.

- Gate it on `allows_clarification`, `agent_execution is None` and capture off, with tests, before enabling.

**(h) Scheduler capacity.**

- **Retirement-pending runs stop counting.** Upstream's fair queue counts executing rows by `("launching","running")`, which ignores HartMesh's `__execution_retirement_pending__` charge (`scheduled_task_runs/retirement.py`). A timed-out, still-running scheduled run stops counting, and a second one starts despite `max_concurrent_runs: 1`.
- **Capacity is freed too early.** #7269's `reconcile_terminal_runs` can free capacity between HartMesh's terminal-status commit and its lease release.
- **Fix:** use `holds_execution`/`blocks_thread` in the claim queries, skip live `task_run_ids()` in reconciliation, and add a capacity test.
- Add HartMesh's `reject_legacy_binding` guard to the new agent scheduling tool path (`scheduled_task_access.py`), which bypasses the router.

**(i) Inbound IM files (add/add `channels/inbound_files.py`).** Same filename, unrelated modules: upstream's filename/size helpers, HartMesh's `InboundFileLease` and context.

- Keep upstream's file and move HartMesh's code to its own module (e.g. `inbound_leases.py`).
- `saved_file_key` must skip `INBOUND_FILE_CLEANUP_KEY`, whose value is a callable.
- Re-apply #6476's exclusive write into HartMesh's rewritten `_ingest_inbound_files`.

**(j) Human-input naming.** Upstream added channel human-input (Feishu clarification cards; `HUMAN_INPUT_*_METADATA_KEY`) independently of HartMesh's Attention requests. Its card answers set `hide_from_ui=True`, which HartMesh's preflight refuses ("one visible human message", `services.py:1816`). Keep the managed path's own builder.

**(k) Projects on Storage Spaces.**

- **`documents.py` (13 hunks).** Upstream's staging hardcodes `project_documents_dir/.staging`. Take its two-phase convert/publish, but route paths through HartMesh's `FeaturePaths` and keep `mark_effect()`, placements and SHA checks.
- **LLM shelf summaries (#6581).**
  - The generator reads legacy paths outside `@storage_feature("hm.projects")`.
  - Its write lacks `mark_effect()`.
  - Compose ships `models: []`.
- Keep summaries disabled, and add a startup refusal when Storage Spaces is on.

**(l) Durable subagent batches.**

- HartMesh's per-batch owner re-checks (`_owner_allowed`, `_cancel_for_owner`) lived inside the renewal loop that upstream replaced with `BatchItemLease`.
- Re-weave them: before assembly, after executor build, periodically, and in acceptance renewal.
- Keep `subagent_batches` off until then.

### 5.3 Database migrations

- **The chains fork.** Both chains start at `0026_mcp_task_lease_tokens`:
  - upstream adds 26 files (0027–0051, with an internal 0049/0049→0050→0051 diamond);
  - HartMesh adds 18 (`0027_account_access` … `0044_telegram_assistant_ids`).
- **No collisions.** Revision IDs and tables don't collide (number prefixes repeat, names differ, all IDs ≤32 characters).
- **Startup fails until the heads are joined.** `bootstrap._get_head_revision()` raises on multiple heads.
- **Recommended: append-import**, which HartMesh's `test_migration_chain_head.py` already models. Set upstream `0027_notification_deliveries.down_revision = "0044_telegram_assistant_ids"` and keep upstream's IDs and diamond. The alternative is a two-parent merge revision.
- **The upgrade is one-way.** The previous release refuses an unknown revision. Document restore-from-backup as the only rollback in RELEASE_SOP.
- **Notable upstream migrations:**
  - 0049 installs a Postgres trigger (`run_event_producer_guard`) on every `run_events` insert. Keep `producer_fencing: off`: HartMesh's own event writers carry no authority (`worker.py:1547`, `output_inventory_capture.py:657`, `routers/threads.py:1290`, `services.py:1609`), and strict mode can't be downgraded.
  - 0032 builds an index on `runs` without `CONCURRENTLY`, which blocks writes during the upgrade.
  - 0037, 0045/0046 and 0048 always run even though their features are off.
- **In a two-step merge**, step 1 ends at upstream head 0048. The 0049–0051 trigger, run inputs and history-seed authority migrations arrive in step 2.

### 5.4 Process blockers on the merge PR

- **AGENTS.md size budgets** (`scripts/check_agent_guidance.py`, identical on both sides). HartMesh passes today with almost no headroom: `gateway/AGENTS.md` is 49,135 of 49,152 bytes. Union merges overflow:

  | File or chain | Estimated merged size | Hard limit |
  |---|---|---|
  | `scripts/AGENTS.md` | ≈40.4 KB | 32 KiB |
  | `backend/tests/AGENTS.md` | ≈52.6 KB | 48 KiB |
  | `channels/AGENTS.md` | ≈53.3 KB | 48 KiB |
  | `skills/AGENTS.md` | ≈59.8 KB | 48 KiB |
  | `gateway/AGENTS.md` | ≈62 KB | 48 KiB |
  | routers chain | ≈100.1 KB | 96 KiB |
  | middlewares chain | ≈98.3 KB | 96 KiB |
  | migrations chain | ≈101.8 KB | 96 KiB |
  | sandbox chain | over | 96 KiB |

  Trim *before* the merge, using upstream's own pattern (#7522): move detail to `backend/docs/*-details.md`.
- **Skill Review CI waiver.** Upstream changed `skill-creator/scripts/improve_description.py` and `run_eval.py`. HartMesh's trusted-base manifest only preapproves the merge-base hashes, so the merge PR fails two `python-subprocess` errors (verified by simulation). Land a waiver-only PR first that preapproves:
  - `sha256:83e82d6049147a1d8b28a5155b35eed270e9053e9bde8f99893b597a2e7c3974`
  - `sha256:9de295d782b3d8e88b03c2489ea2bac64c894bab1f88302ce68b8fc61f72c535`
- **`data-analysis` skill.** HartMesh's no-network pandas loader conflicts with upstream's `INSTALL spatial`/`st_read`. Upstream's `tests/skills/test_data_analysis_*` now run in Skill Review CI (#7350), and one expects a "pip install" message that contradicts HartMesh's.
- **Lockfile.** Don't hand-merge `backend/uv.lock`. Take upstream's, resolve the pyproject files, then run `uv lock` and `uv lock --check` with uv 0.11.1.
- **Frontend isolation pin.** After the merge, run `scripts/verify_frontend_isolation.py --pin <merged upstream sha>` and stage `.github/upstream-frontend.json`. The `frontend/` tree itself merges with zero conflicts.
- **Version policy.**
  - Stay on `2.2.0+hartmesh.51+`. The release tooling requires `X.Y.Z+hartmesh.N`, and after step 1 the base really is 2.2.0.
  - Adopt upstream's five-source lockstep (#7696): the harness pyproject has said `2.1.0` all along and leaks into assembly descriptors. Also adopt `uv lock --check` (#6588).
  - Reword RELEASING to say "X.Y.Z = latest upstream release the build contains".

---

## 6. Keep off, skip or decide deliberately

**Keep disabled in Compose**

| Setting | Reason |
|---|---|
| `sandbox.prewarm` | 2-slot budget; see §5.2(d). |
| `run_events.producer_fencing` (stay `off`; never activate strict) | HartMesh's event writers carry no authority. |
| `blob_storage.enabled` | Bytes would live outside Storage Space custody, backup and deletion. Never take #6329 without #7604. |
| `scheduler.tool_enabled` | Agent self-scheduling is off by default. |
| `projects.summaries_enabled` | See §5.2(k). |
| `subagent_batches` | Until the owner re-check is re-woven (§5.2(l)). |
| `channels.leader_lease` | Leave `auto` with one worker; it doesn't cover managed bots. |

**Skip**

- the `admin_users` router and upstream's `users.disabled` writes;
- upstream's login-throttle resolver;
- the persistent sandbox quarantine store (it duplicates `_cleanup_pending`);
- run-scoped command cancellation;
- the Lark broker and provisioner attestation;
- the Feishu, WeChat, QQ, DingTalk and Lark features;
- the TUI, Helm and Windows-only fixes (all LOW).

**Hide or refuse**

- `/api/managed-models`. It is a second credential store merged into every `get_app_config()`, bypassing `HARTMESH_MODELS_FILE` curation and per-account provider keys.
- `task` `model_name` with authorization off: it lets the model choose any configured model for a child task.

**Measure**

- #6920's always-on assembly evidence: an extra run-store write on every run, and a new "Unable to persist assembly evidence" failure.
- #5645's per-read enrollment probe: a Postgres connection on every user-scoped skill read even with no enrolled owners.
- `quickjs` (#6858) entering the production image through the `dev` dependency group.

**Behaviour change**

- #7455 flips `elide_superseded_writes` from true to false. Compose relies on the default, so decide explicitly.

**Product decisions for frontend-hm**

- Queue/Steer UI.
- Conversation schedules and run history.
- Goal-outcome UI (mostly mechanical: the files are byte-identical).
- Token-usage dashboard (the backend `/api/console/*` already exists).
- Scoped model controls.
- `context.locale` replies (#7294).
- Active-run rejoin (#5536 plus fixes). Without it, chats opened mid-run (scheduled, IM, another tab) don't stream live.

---

## 7. Recommended plan

**Phase 0: this week (small PRs, no merge needed)**

1. **Lockfiles.**
   - Backend: `uv lock --upgrade-package` pyjwt mcp certifi pillow aiohttp urllib3 langgraph langgraph-sdk langgraph-checkpoint-postgres langgraph-checkpoint-sqlite langchain-anthropic pyasn1 dnspython opentelemetry-api oauthlib soupsieve multidict mako. Run the full suite, especially OIDC and the Telegram image tests.
   - frontend-hm: Next ≥16.4.1, `images.unoptimized: true`, zod 4.6.7, source-map-js, happy-dom 21.
2. **Security cherry-picks and ports from §4.1**, in order:
   1. #6513
   2. #6202 (hand-port)
   3. #6580
   4. #7374
   5. #7665
   6. #7266
   7. #7217 (+#7255, #7275, #7155)
   8. #7467 (frontend and backend)
   9. #6831
   10. #7449 (frontend)
   11. #6794 (hand-port)
3. **Correctness fixes from §4.2.** NaN first, then the audit middleware, DeerMem, checkpoint scans, Redis bridge, bootstrap, middleware, skills/MCP, trash, scheduler and goals.
4. **HartMesh-owned gaps from §4.3**:
   - SearXNG URL screening;
   - off-loop config reads;
   - the run-journal duplicate fix;
   - the admission-lock fix;
   - the Anthropic model catalog;
   - the Telegram recursion limit;
   - Compose `init: true` and graceful-shutdown timeout;
   - pinning the acceptance nginx;
   - loopback dev binding;
   - `account_disabled`;
   - enabling Dependabot and adding a `pnpm audit` gate.

**Phase 1: prepare the merge (separate PRs)**

5. Trim the AGENTS.md files and chains below budget on HartMesh's side.
6. Land the skill-review waiver preapproval.
7. Write the migration plan (append-import), a one-way-upgrade release note, and RELEASE_SOP rollback guidance.
8. Decide §5.2(a)–(l) and record the decisions: one design per collision.
9. Decide the version policies: extension-api numbering, `config_version` ≥65, harness lockstep.
10. Retire the stale `origin/upstream-offer/*` branches. Upstream landed none of the four seams, PR #177 already deleted the old session provider, and three of the four no longer apply cleanly.

**Phase 2: merge upstream to `b537d9c` (upstream 2.2.0; 877 commits, 171 conflicted files)**

11. Resolve by area. Rerun the import check from Appendix B. Run ruff (F821/F823/F811) on every resolved Python file, and fix every §5.1 item.
12. Re-pin the frontend isolation marker, port the contract changes into frontend-hm, regenerate the lock, run the full backend, frontend and Docker acceptance suites, and release as `2.2.0+hartmesh.N`, now accurately labelled.

**Phase 3: merge `b537d9c..upstream/main` (73 commits)**

13. This slice holds:
    - Queue/Steer (#7729, #7790, #7753, #7755, #7776);
    - run-scoped command cancellation (#7772);
    - the event-producer fencing trigger and migrations 0049–0051 (#7619);
    - the admission lock split (#7664);
    - middleware `apply_to` scoping (#7661);
    - post-release fixes (several already taken in Phase 0).

    Apply the gating from §5.2(e) and (g).

**Ongoing**

14. Sync weekly or every two weeks. Conflicts accumulate at about 3.5 files per day of upstream activity, so the catch-up cost is mostly a function of the gap.

---

## 8. Area detail (condensed)

### 8.1 Sandbox (79 commits)

- **What HartMesh ships.** AIO provider with the local Docker backend under runsc, 2 slots, allowlist egress, Redis ownership, and open-time prewarm. LocalSandbox is the dev default with `allow_host_bash: false`. E2B, BoxLite, Tenki, OpenSandbox and the provisioner are unused and merge cleanly (LOW).
- **Also take** #6673 (AIO double-resolves `$` in env values, mangling secrets), #6712 (`view_image` pulls 100 MB into Gateway memory before rejecting), #6834 (`read_file` leaks pydantic error text), #7507 (sandbox ID derived on the event loop), #5667, #6343, #6782, #7393, all LocalSandbox fixes, the browser egress pinning (#6201, #6301; LOW while browser tools are off), and the benchmarks.
- **Take with the merge:** the lease-manager fixes from #6436 (a cancelled acquire marks its release pending; release keeps the holder until cleanup), `hold_sandbox_shutdown_ownership` and `_raise_if_shutting_down` (#7560).
- **Real gap:** HartMesh's uncertain-outcome flags live only in memory, so after a restart a container with an ambiguous shell outcome can be adopted. Upstream persists that state.
- **Upstream offers:** none of the four seams landed. Upstream moved toward more duck-typed flags rather than capability negotiation.

### 8.2 Runtime and persistence (94)

Covered in §4.2, §5.2(f)–(h) and §5.3. Additional notes:

- #6980 records the effective model per run (clean).
- #7153 records the partial reply after Stop; keep it out of captured-output evidence.
- #7588 adds a `recursion_capped` stop reason; HartMesh has no handling for it.
- #6051's `on_run_admitted` retention hook must run after HartMesh's durable admission and Work readiness.
- The checkpoint delta-cache fixes are dormant (HartMesh runs `checkpoint_channel_mode: full`).
- #7297's run-status tool is safe: it reads only the run's own state.

### 8.3 Gateway and auth (103)

Covered in §4.1, §5.1 and §5.2(b)–(c). Additional notes:

- **#6105 (Layer-1 tool authz bypass):** middleware-declared tools (extension tools via `IsolatedMiddleware`, `write_todos`) skipped the `tools` policy. It is latent while `authorization.enabled: false`, and it touches 24 files, so take it with the merge. Same for #6508 ActivationDecisions (fail-open under `fail_closed: false`).
- **Health:** `/health/ready` conflicts. Keep HartMesh's `redis` key and add the `provisioner` probe.
- **Range requests:** #6195 was fixed independently in HartMesh (`DescriptorFileResponse`). A leftover: `If-Range` still accepts a date for files over 2 MiB.
- **Uploads:** #7165 changes `success` to mean "≥1 stored", which needs the frontend-hm gate.
- **Smaller fixes:** #6250 bounds artifact-edit reads (legacy path). #7370 bounds the `event_types` SQL `IN`. #7258 handles IdP HTML error pages. #6319 and #6206 move blocking work off the loop.
- **History projection:** keep HartMesh's POST `/history` projection of artifacts, todos and goal (1f4d36f26) and add upstream's `goal_outcome`.

### 8.4 Agent core (179)

Covered in §4.2, §5.1 and §5.2(a). Additional notes:

- Subagent executor hardening (#7788, #7053, #7546, #7483, #7355, #7668) merges with constructor-kwarg conflicts only.
- `memory_get` (#6443) must be routed through HartMesh's `_manager(runtime)`.
- Implement the watermark methods (#7205) in `SqlMemoryStorage`.
- Provider fixes for DeepSeek, MiMo, StepFun and OpenAI (#6639, #6838, #7039, #6863, #7428) cherry-pick cleanly.
- The Claude, Codex, MindIE and MiniMax fixes are LOW: those providers aren't in Compose, which uses ChatAnthropic.
- `prompt.py`: put `public_skill_scope_enabled()` before upstream's managed-owner guard.

### 8.5 Skills and MCP (94)

Covered above. Additional notes:

- The SkillScan detector fixes (~15, MEDIUM) merge cleanly. Upstream's scanner run over HartMesh's 13 shipped public skills finds no blocker.
- The MCP reconciliation and discovery-reuse work (#6210, #6435, #6795, #6900…) stops admin MCP edits from resetting every server.
- OAuth refresh-token persistence (#7207) needs migration 0047, store wiring and the credentials-key decision.
- **Skills routers:** in `routers/skills.py`, keep HartMesh's owner/provider authorization and gate the new YAML-diagnostics endpoint (#6623) with `require_private_skill_owner`, not admin.
- **MCP routers:** `_run_drained_mcp_apply` takes positional arguments only, so bind `policy=` with `functools.partial`.
- **Follow-up:** add `first-command` to `skill-creator/scripts/quick_validate.py`.

### 8.6 IM channels (82)

Managed Telegram subclasses upstream's `TelegramChannel` for parsing only, and the pinned signatures still hold upstream. Covered in §5.1 and §5.2(i)–(j). Additional notes:

- Re-apply HartMesh's execution seams onto upstream's `manager.py`. They're additive.
- In `channel_connections/sql.py`, keep HartMesh's atomic `set_thread_id`. It issues a fresh row ID, which the Telegram link fence relies on; upstream's retry keeps the old ID and would weaken that fence. Adopt upstream's cipher and `claim_thread_id`.
- `ChannelStore` becomes a Protocol.
- Port the reserved `/model` slash name to `frontend-hm` `slash.ts`.
- Take now: #6502 (listener snapshot).
- The fail-closed allowlists (#6230, #6338, #6376, #6730, #6742) matter only for source deployments that enable upstream channels. At the merge base, an `allowed_users` list with no parseable ID allowed everyone.
- Notification-outbox hooks land in `gateway/app.py`, `scheduler/service.py` and `worker.py`, and stay dormant: managed Telegram has `proactive_notifications: False`.
- **Follow-ups:** per-bot polling arbitration if HartMesh ever runs more than one Gateway; Telegram delivery of scheduled-task results via the outbox.

### 8.7 Extensions, community, projects and knowledge (114)

Covered in §4.1, §4.3, §5.1 and §5.2(k). Additional notes:

- **`extension_agent_runs.py` (#6190)** is created unconditionally and calls HartMesh's customised route functions with a synthetic request, so no middleware runs.
  - Its handle recheck ignores HartMesh's `account_refusal`, so a disabled account could keep starting runs.
  - It reads artifacts from legacy outputs, not Storage Spaces.
  - It is dormant (no HartMesh plugin uses `agent_runs`), but fix both before one does.
- **Knowledge, workflows and agent teams** ship as example plugins only; keep them out of Compose. Their always-on host pieces are small: #7518 makes `/api/extensions/*` return 409 on a viewer mismatch, and frontend-hm already sends the header.
- The extension package CI check (#6755) doesn't cover `backend/extensions/sources/*`; extend it.
- The Compose fragment providers (Brave, Serper, Tavily, Serply, Tencent WSA, GroundRoute) get robustness fixes and merge cleanly.

### 8.8 Frontend (151 touching `frontend/`)

`frontend-hm` was seeded from HartMesh's older UI line, not from the merge base. Fixes to subsystems it lacks (active-run rejoin, @-mentions, table preview, capability center, knowledge scope) don't apply.

**Already fixed independently:**

| Upstream | HartMesh |
|---|---|
| #7146 | 7b13a54d5 |
| #6681 | 3d05f5dce |
| #6412 | — |
| #6530 | 9d2c576d7 |
| #7529 | own bounded retry |

**Ports by effort:**

- **Mechanical:** #7609, #7589, #7553, #6768, #6749, #6756, #7144, #7175, #7284, #6152, #6669, #6662, #7774, #5876, #7743, #6959, #6432, #7213, #7301, #6729, #7543 (drop `@xyflow/react` and `tokenlens`).
- **Mechanical with the merge:** goal outcomes (#6556, #7167, #7588), `errors.ts` for scheduled-task coded errors (#6378/#6393; otherwise a generic message is shown).
- **Adapt:** #6907 (agent settings clear reasoning defaults), #7672 (tool-call ID reuse; port together with the backend `message_identity.py` change).

**Out of scope:** knowledge UI, extension catalog, docs/MDX, Projects, managed models UI, IM notification UIs, admin Users page, mentions, and the slash-menu redesign.

### 8.9 Infra, deps and CI (142)

Covered in §4.1, §4.3 and §5.4. Additional notes:

- #6158 answers GET/HEAD on loopback-IP Hosts with a 301 to `localhost`. A test forces it into `deploy/compose/nginx/nginx.conf`, where it breaks the README's `curl http://127.0.0.1:2026/...` example and `scripts/docker_acceptance.py`: the cookie is set for 127.0.0.1, then the redirect goes to `localhost`. Decide how it should behave in the production profile and acceptance.
- **CI:** `make test` becomes four local shards (#6324). HartMesh's Docker-heavy suite hasn't been run that way. Keep HartMesh's `-v -o faulthandler_timeout=120`, and merge the two `.test_durations` maps.
- **Workflow conflicts:** in `e2e-tests.yml`, keep `working-directory: frontend-hm` and add the shards. `serve.sh` should combine both sides: `--project frontend-hm -- run dev --hostname 127.0.0.1`.

---

## Appendix A: Numbers

| Measure | Value |
|---|---|
| Upstream commits since the merge base | 950 (feat 98, fix 723, docs 37, chore 35, test 33, refactor 10, other 14) |
| Upstream authors | 205 (top: Hyeonsang Cho 91, NanPan 35, Totoro 33, Willem Jiang 30, dependabot 27) |
| Upstream commits per ISO week | W40 91, W41 264, W42 180, W43 269, W44 133, W45 13 |
| In upstream 2.2.0-rc0 / after it | 877 / 73 |
| HartMesh commits since the port | 363 (260 non-merge), plus 33 port commits |
| Files changed on both sides | 301 (178 conflicted, 123 clean merges that still need a semantic review) |
| Conflict types | 172 content, 4 add/add, 2 modify/delete |
| Conflict hunks | 576 |
| Backend advisories (distinct GHSA): merge base / HartMesh / upstream | 93 / 93 / 2 |
| frontend-hm `pnpm audit --prod` | 5 critical, 13 high (upstream `frontend/`: 0 critical) |

## Appendix B — Reproducing the working data

The trial merges, import checklist, advisory scans and per-area notes behind this analysis were produced in an ephemeral session and are not committed. To regenerate the core data:

```bash
git remote add upstream https://github.com/bytedance/deer-flow.git
git fetch upstream main
MB=$(git merge-base HEAD upstream/main)          # 67db3d883c38264e2a188d9aaad44f7a7b55015d at the time of writing
git log --no-merges --format='%h %ad %s' --date=short $MB..upstream/main   # the commit inventory
git merge-tree --write-tree --name-only HEAD upstream/main                  # conflicted files
git log --no-merges --cherry-mark --left-right HEAD...upstream/main | grep '^='   # already-equivalent patches
```

The "names each side needs" checklist (§5.1) came from a static AST import check over the merged tree, run twice: once with every conflicted file resolved to HartMesh's version and once to upstream's. Rerun that check against the resolved tree before opening the merge PR.

---

## Appendix C — Phase 0 status (2026-11-03)

Phase 0 landed on branch `claude/zen-ramanujan-vekdgx` as 84 commits after this
document. Upstream commits were cherry-picked with `-x` and their authors kept;
changelog hunks and the pinned `frontend/` copy were never taken; hand ports say
"ported from" and name what was left out. Every commit ran its own tests, and the
backend suite ran in full on the refreshed dependencies (34,181 passed; two failures
are environmental in the session container and fail identically on the base:
`/mnt/skills` exists, and `TRACEPARENT` is set). `frontend-hm`: `pnpm check`,
`pnpm test` (3,212 passed) and `pnpm build` pass.

**Security (§4.1), all taken:** #6513, #6202, #6580, #7374, #7665, #7266, #7217,
#7255, #7275, #7155, #7467 (control-character part, backend and frontend-hm),
#6831, #7449 (frontend-hm), #6794 (hand port), #7258.

**Dependencies:** backend lock refreshed to upstream's versions for the advisory set
(uv 0.11.1, `uv lock --check` clean); frontend-hm on next 16.4.1, zod 4.6.7,
happy-dom 21, plus in-range dompurify/lodash-es/source-map-js; the image optimizer
is off. `pnpm audit --prod`: 5 critical / 13 high → 0 critical / 3 high (transitive
pins with no compatible fix: chevrotain's lodash-es, @langchain/core's langsmith and
zod 3). The langgraph 1.2.14 bump needed upstream's #7703 test-double adaptation.

**Correctness (§4.2), taken:** NaN/inf (#7670, #7733); sandbox audit (#6307, #7247,
#7290); DeerMem (#6672, #7152, #7530 — also fixes the AI-employee pre-compaction
memory loss, with a HartMesh regression test); checkpoints (#5988, #6929); Redis
bridge (#6605, #6951, #6690); bootstrap (#6349, #7410, #7489, #6306); middleware
(#6150, #6363, #6511, #6803, #6946, #6965, #7762, #6107); skills/MCP (#6271, #6133,
#6342, #6321, #6359, #6473, #6481, #6506, #7081, #7400, #7677); projects trash
(#7732, #7683, #7499); scheduler (#6975, #6185, #7196); goal (#7167); gateway
(#7370, #7758, #7557, #6250, #7727); events (#7352); runtime (#6980); sandbox
(#6834, AIO part of #6673); wizard (#7585).

**HartMesh-owned (§4.3), done:** SearXNG result-URL screening; the retired
Anthropic model replaced in the Compose catalog; `account_disabled` recognised by
frontend-hm; the upload gate (#7122/#7342 adapted to HartMesh's `success` meaning);
CSRF and locale cookie hardening (#7589, #7553). Mechanical frontend-hm ports:
#7284, #5876, #7743, #6959, #7213, #7175.

**Merge preparation (§5.4):** the skill-review waiver preapproval for upstream's
skill-creator scripts is committed; it must reach `main` before the merge PR.

**Deferred, with reasons:**
- Need a Docker host for acceptance (none in the session): Compose `init: true` and
  `--timeout-graceful-shutdown`, pinning the acceptance nginx to the production digest.
- Belong to the run-lifecycle decision (§5.2 f): the journal lost-ack duplicate
  (#6846) and the admission lock split (#7664).
- Product decision: managed Telegram's recursion limit (100, versus 1000 for web).
- Need wider upstream context than a cherry-pick: #6712 (all providers' download
  signature), #7485 (needs `deerflow.utils.atomic_write`), #6701/#6827/#7650/#6702/
  #7465/#6407 (frontend-hm adaptations without an existing test harness to extend),
  the SSO landing part of #7467.
- Off-loop `sign_on_only()` and registration reads on every authenticated request.
- Dependabot: enable security updates in repository settings (upstream keeps no
  configuration file either).

## Appendix D — Merge decisions (2026-11-03)

Decided by the maintainer before the merge (references are to §5.2):

1. **Skill tool policy (a):** accept upstream's deletion of `SkillToolPolicyMiddleware`
   and `skills/tool_policy.py`; move same-message hold-back and `first-command`
   ordering into a HartMesh-owned `SkillLoadOrderMiddleware` at the old slot in both
   chains (`first_command_order=False` for subagents). `allowed-tools` becomes
   metadata, as upstream.
2. **Run lifecycle (f):** upstream's structure is the base (split admission locks,
   journal write ownership, `run.terminal`, finalization draining); HartMesh's output
   capture, AgentExecution environment, storage provider, customer-administration
   policy and `GATEWAY_STOP_REASON` are re-layered on top. The journal lost-ack
   duplicate (#6846) and the admission lock split (#7664) arrive with it.
3. **Staging:** two merges — first to `b537d9c` (upstream 2.2.0), released as
   `2.2.0+hartmesh.N`; then `b537d9c..upstream/main` (Queue/Steer, command scopes,
   producer fencing, admission-lock split).
4. **Managed Telegram step budget:** 1000, matching web chats and scheduled runs
   (done in Phase 0; a configured session value still overrides it).

The remaining §5.2 collisions follow the document's recommendations: keep HartMesh's
login throttle (b) and account-disable model, and do not mount `admin_users` (c);
keep `sandbox.prewarm: false` (d); keep HartMesh's per-call command abort with
`supports_command_cancellation` false (e); gate Queue/Steer on
`allows_clarification`, no `agent_execution` and capture off before enabling it (g);
count scheduler capacity with `holds_execution`/`blocks_thread` (h); move HartMesh's
inbound-file leases to their own module (i); keep the managed human-input builder
(j); adapt project staging to `FeaturePaths` and keep summaries off (k); re-weave the
batch owner checks before enabling batches (l).

**Order of work:** land Phase 0 (this branch) on `main` first — the skill-review
waiver must be in the trusted base before the merge PR — then trim the AGENTS.md
chains below budget (§5.4), then the step-1 merge on its own branch.

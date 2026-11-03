# Scripts guidance details

Detail moved out of `scripts/AGENTS.md` so it stays within the module budget in `scripts/check_agent_guidance.py`. These rules still apply; the scripts guide links each section.

## Blocking-IO alias tracing

That same-function alias tracing is deliberately narrower than the symbolic
names `dotted_name()` builds for blocking-call pattern matching elsewhere in
this module: receiver/alias extraction uses a restricted extractor that only
recognizes `Name`/`Attribute` chains, so a `Call` or `Subscript` result (e.g.
`factory().flush()`, or `client = factory(); client.flush()` /
`client = clients[0]; client.flush()`) is never treated as inheriting its
base's alias-worthiness — including when the unsupported node is buried
deeper in the chain (`factory().client.flush()`, `clients[0].client.flush()`):
an unrecognized shape anywhere in the chain makes the whole receiver
unresolved, it never falls back to just the chain's trailing attribute name,
or that name alone could still collide with an unrelated traced parameter or
local alias. Reassigning a traced name to a non-traceable value (anything
other than a `self.`/`cls.` attribute or an already-traced name) kills its
alias instead of leaving it traceable, so a stale alias from an earlier
assignment cannot keep exposing an unrelated same-named method after the
variable is reassigned to something else; the assignment's right-hand side is
always analyzed against the alias state as it stood *before* this kill-or-add
update, matching Python's own evaluate-then-bind order, so
`client = client.flush()` still resolves that call against `client`'s prior
(pre-reassignment) alias instead of the state after it's gone. `if`/`else`
branches get isolated alias state — an alias added in one branch cannot leak
into the other — and the state after the whole `if` is the union of what each
branch produced (a conservative may-alias join), so the result no longer
depends on which branch is textually `body` vs. `orelse`. This branch
isolation is deliberately scoped to `ast.If` only; `ast.Try`/`ast.Match` have
different, more complex control-flow semantics and keep the older unisolated
traversal. Finally, a function's decorators and parameter defaults are
analyzed in the *enclosing* scope rather than the new function's own, and
parameter/return annotations get the same enclosing-scope treatment unless
the module postpones annotation evaluation (`from __future__ import
annotations`), in which case they are skipped entirely, in either scope —
those expressions run at definition time, before the function has ever been
called (or, when postponed, never run at all), so a call there is never
attributed to the function being defined (it moves to whatever scope actually
contains the `def`, e.g. the enclosing function, or disappears if that scope
is module/class level and therefore never async-reachable). PEP 695
type-parameter bounds are not visited in either scope: CPython evaluates each
one lazily, in its own hidden function, only if something like `T.__bound__`
is actually accessed, never as part of running the `def` statement itself.
A `lambda`'s body and a bare generator expression's element/filters/later
`for` clauses are excluded from traversal ONLY while walking another
function's own definition-time expressions (decorators, parameter defaults/
annotations, return annotation): there, we know structurally that the
enclosing `def` statement is executing right now, and neither a lambda body
nor a generator's element runs just because the lambda/generator object is
created — only a lambda's own parameter defaults and a generator's
outermost iterable are genuinely eager at that moment. This exclusion is
absolute and has no exceptions: even a lambda that is immediately invoked at
its own definition site (`(lambda: ...)()`), or a generator passed directly
to an eager-consuming builtin, is still excluded when it appears inside
another function's decorator/default/annotation — a narrow, intentional
limitation given how rarely a definition-time expression contains an
executed call at all, preferred over special-casing specific shapes there.

Everywhere else — module level, class bodies, and ordinary function-body
statements — a lambda body or generator expression's element is scanned
unconditionally, the same conservative, over-report-rather-than-infer stance
this file already takes for reachability elsewhere (the `ast.If` may-alias
union, the bare-name call-graph resolution). This file does not attempt to
distinguish a lambda that is invoked immediately, invoked later through a
stored variable, passed as a callback, or never called at all, nor a
generator that is consumed by an eager builtin (`list`, `sum`, `any`, etc.),
wrapped in another lazy iterator (`map`, `filter`), or never consumed —
telling these apart in the general case would mean inferring evaluation
order and consumption across arbitrary code rather than reading a fixed,
structural fact, so none of them are special-cased; all are scanned the
same way. This is intentionally informational and is not run from CI in
this round.

## Blocking-IO runtime gate

Blocking-IO runtime gate (`tests/blocking_io/`):
- Wraps every item under `tests/blocking_io/` with a strict Blockbuster
  context scoped to `app.*` and `deerflow.*` (see
  `tests/support/detectors/blocking_io_runtime.py`). Any sync blocking IO
  call whose stack passes through DeerFlow business code while running on
  the asyncio event loop raises `BlockingError` and fails the test.
- Regression anchors live there: `test_skills_load.py` (locks the
  `asyncio.to_thread` offload around `LocalSkillStorage.load_skills`, fix
  for #1917); `test_sqlite_lifespan.py` (locks the offload around
  SQLite path resolution plus `ensure_sqlite_parent_dir`, fix for #1912);
  `test_jsonl_run_event_store.py` (locks `JsonlRunEventStore`'s async
  API — including idempotent singleton-event writes — offloading its file IO
  via `asyncio.to_thread`); `test_run_journal_callbacks.py` (locks
  `RunJournal.run_inline` tool callbacks to in-memory/event-loop-safe work);
  `test_integrations_router.py` (locks Lark integration install and auth
  completion route handlers offloading archive filesystem work and `lark-cli`
  subprocesses);
  `test_uploads_middleware.py` (locks `UploadsMiddleware.abefore_agent`
  offloading the uploads-directory scan off the event loop);
  `test_uploads_router.py` (locks Gateway upload/list/delete endpoints
  offloading upload directory creation, staged writes, chmod/cleanup,
  directory scans/deletes, and remote sandbox sync off the event loop);
  `test_feishu_receive_file.py` (locks Feishu attachment path preparation and
  persistence plus remote sandbox acquisition/sync off the event loop, and
  skips redundant sandbox sync when thread data is already mounted);
  `test_channel_outbound_files.py` (locks Feishu, Telegram, and WeCom outbound
  attachment open/read/hash work off the event loop);
  `test_openviking_memory_backend.py` (locks the official OpenViking backend's
  async add/context/search entrypoints offloading synchronous SDK and cursor
  filesystem IO); and
  `test_workspace_changes_recorder.py` (locks the offload around the snapshot
  text cache lifecycle — roots resolution, `mkdtemp`, and the `shutil.rmtree`
  on both the capture-failure branch and `record_workspace_changes`' `finally`).
- `test_gate_smoke.py` is a meta-test asserting the gate actually catches
  unoffloaded blocking IO and that the `@pytest.mark.allow_blocking_io`
  opt-out works.
- Coverage boundary: the gate only sees code that test execution actually
  touches. Static AST coverage is a separate concern (out of scope for
  this PR).
- CI: runs on every PR via `.github/workflows/backend-blocking-io-tests.yml`,
  hard-fail.

## Release artifact admission

`release_artifacts.py` admits only canonical `X.Y.Z+hartmesh.N` candidates;
remote Git tag and GitHub Release lookups must distinguish absence from errors.
Container and manifest workflows share the version lock. Tag publication first
verifies every immutable candidate's GitHub attestation, source and build-input
fingerprint, then passes those exact references to the matrix. Never rebuild on
a tag event, including the provisioner that the Compose profile does not pin.
Fingerprints conservatively include copied trees, Docker ignore controls and
release build configuration. Keep `BUILD_CONTEXTS` and `COMPONENT_INPUTS` aligned
with the matrix and Dockerfiles; new COPY/ADD sources outside that coverage fail.
Public skill Markdown is a build input. Root release notes, docs and Compose
pins may change after the candidate; a source change requires rebuilding it.
`release_acceptance.py` additionally requires one completed successful
same-repository image-mode workflow dispatch, both stores from one attempt,
exact candidate refs/running IDs, unchanged committed harness and authenticated
artifact SHA-256s. Read the fixed schema-1 record from the requested commit;
uncommitted/local success claims cannot qualify. Bound API/ZIP/JSON reads, never
extract archives or forward GitHub credentials to signed download URLs.
`record-acceptance` proves all candidates before atomically replacing the record;
there is no verification bypass. Both publication workflows need Actions read
permission and must run the shared gate before retagging images or publishing
a GitHub Release. Keep release
manifest schema 4 intact and attach the record as a separate asset. Before a
tag, rerun all acceptance jobs and refresh its record when needed; an incomplete
immutable tag with expired evidence needs a new version.

## Tenant qualification receipts

`tenant_class_receipt.py` adds an optional `tenant_class` entry to the existing
image-acceptance record. It admits bounded UTF-8 JSON/JUnit, exactly 27 named
bundle cases, successful actual case results and consistent declared totals.
Runtime and mount propagation are derived solely from the two required passed
cases. Every nested key/type, UTC timestamp, image digest and finite timing is
validated; failed qualification preserves existing record bytes. Commit the
record after a human-performed pinned-tenant run. CI runs all 27 bundle cases
with scripted inference and a runc alias; it cannot attest the tenant VM class
or gVisor. Tenant qualification belongs to infra/consumers and is never performed
in a release session. Its absence does not block release admission; verification
strictly validates any supplied receipt and compares
receipt backend/sandbox digests with pinned candidates and still authenticates
both image-acceptance stores. New image admission writes its five fields freshly
and removes an older tenant receipt; only a separate consumer qualification can
supply a replacement.

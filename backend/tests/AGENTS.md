# Backend Tests

`test_null_tool_result_pairing.py` pins null/empty/sentinel ID equivalence, provider-wire
pairing, and ambiguity rejection for incomplete parallel results.

`test_lark_broker_image.py` stages Python files from the broker Dockerfile's
final-stage COPY instructions. Run shim installation and the health endpoint
under `-I -S`, adding only the staged script directory to the import path;
repository paths or installed Harness packages would hide missing image files.

When adding a scoped `AGENTS.md`, update `EXPECTED_GUIDANCE_PATHS` in
`test_agent_guidance_check.py` in the same change. The explicit inventory pins
guidance ownership and extends hard-budget checks to each approved guide.

`test_image_result_url_safety.py` drives GroundRoute through `ainvoke` and an
offline HTTPX transport. Patch only the provider's `httpx` reference; preserve
the shared client and exercise real configuration resolution and URL screening.

`test_client_upload_snapshot.py` keeps upload publication and conversion on the
same private capture. Cover source edits/removal, both conversion branches,
same-file rejection, metadata and cleanup; retain sandbox symlink defenses.
Exercise both replacement and first-time publication; mutation hooks must actually
run on both paths. Force collision retries while editing/removing the source and
assert one source open, original metadata, preserved competing uploads, companion
ownership and capture cleanup. Retain the real XLSX conversion coverage.

Backend tests must preserve the runtime invariants they exercise without changing production execution topology.

Follow the [focused verification workflow](../../CONTRIBUTING.md#testing).
Subsystem coverage below applies to affected contracts, not every edit. Reuse
unchanged passing checks; preserve native/Compose restrictions and required CI
qualification.

CI shards print each running case and dump Python-thread stacks after 120 seconds
without ending the test. `gh run view --log` stops at the first line over 64 KiB, so give a
huge parametrized value an `id=`, and inspect raw run-log ZIP step files before assessing
missing results.

`conftest.py` freezes what collection built and runs full garbage-collection passes between
tests, never inside one. A pass over a shard's heap holds the GIL for about two seconds,
and a test with a wall-clock deadline reads that as a slow database. Do not widen a
deadline to get past a freeze; find what started it.

Factory default tests establish their own absent-config precondition and restore
ambient configuration afterward. AppConfig loading can set stream-bridge globals;
the no-config bridge test clears that global and its Redis environment fallback
with `monkeypatch`, preserving configured Redis tests and application defaults.

The real-Gateway probe fixture in `test_turn_phase_gateway_stream_e2e.py`
disables memory on the resolved app config so offline turns cannot enqueue
external inference. Its TCP relay bounds connection establishment only;
quiet SSE streams remain open until HTTP completion or explicit teardown.
Teardown closes connected sockets and drains relay workers.

`test_migration_chain_head.py` compares Alembic's fresh and previous-release
upgrade plans and pins published ancestry from release fixtures. Keep old
fixtures immutable; add a new one for a new published head. A single current
head alone cannot detect a skipped revision inserted behind an applied head.

## Telegram compatibility and persistence

Output-capture tests combine real offline LangGraph/model callbacks with both
SQL dialects. Assert the actual SDK-assigned human ID, canonical AI ID, committed
event sequence and exact text digest, then consume a worker seal through the
production route. Seeded projection rows alone do not qualify worker provenance.
Hold native writes, source reads, dirty progress and cancellation/rollback CAS
races; drain all tasks before teardown. Unknown event commits must never be
blindly appended again. Label API scaffolds separately from behavioral reds.
Prestart tests retain the actual start barrier and committed closure through
cancellation. Reclaim/replacement tests signal actual SQL dispatch before releasing
held clocks; inspect checkpoint reservations directly, since run lists filter them.
Keep startup failures distinct from cancellation through the real service/worker.

Telegram repository-test fixtures (`_telegram_repository_test_support.py`):
[details](../docs/tests-guidance-details.md#telegram-repository-tests).

Presentation tests use explicit route-table expectations, both locale catalogs
and UTF-16 boundaries. Provider tests use fake
clients or actual SDKs over socket-free mock transports, including timeout,
cancellation, endpoint/key capture and context-scoped payload-log suppression.
Keep recorded credentials synthetic and test artifacts on disk.

Ingress tests run the real Application and recorded SDK through the ordered gate.
Prove swallowed handler/SQL failures retain PollTask's suffix, while fixed replies
commit their route and delivery before acknowledgment. Token/stranger input must
reach no inbox, hash, bus or file consumer. Ledger churn, removal/re-add and
rotation retain bounded unexpired allowances.
Bind/unblock tests use fresh derived account access and exact captured SQL scope.
URL fallback tests retain part/source/claim identity, compose with plain fallback
and preserve unrelated 429/pacing floors through reconnect.
Exercise lifecycle notices through the actual supervisor, including concurrent
held-input/reconcile claims and a later genuine hold.

Conversation-preparation tests use both SQL dialects, the real public SDK and
socket-free HTTP/ASGI routes. Prove stored-owner projection instead of forged
metadata, strict binding-reader availability, separate GET after create/replay,
independent URL configuration and isolated owner/CSRF headers. Keep the saved UUID
and endpoint through lost responses and same-bot token changes; old attempts must
remain stale. Freeze the clock for mapping ABA tests and include generic and
fenced replacements in both directions. Reserved preparation cannot enter public
route/group basis. Cancellation barriers retain cold construction, SDK decode,
SQL, keyed-lock waiters and client cleanup; delayed HTTP cannot start a late writer.
Fixed status tests compare immutable origins/scope/topic/completion/source, strip
caller receipts, reject crossed source/page controls, and use the actual sender
and recorded Telegram SDK for the server label and URL/plain fallback orders.

Callback tests use that matrix for complete saved groups, historical aliases,
scope/adoption, pause policy and ten-minute prompts. Command integration runs the
actual pinned Telegram SDK and recorded Gateway HTTP, including confirm/keep,
locale Acts, explicit keyboard clearing and delivery-only repair. Hold the real
post-route/precheck boundaries to prove retirement cannot admit a later leaf.
Strict items cover the whole command/press lifetime and cold preference validation.
Default-supervisor tests omit injected repositories so wiring cannot be qualified
only by richer test fixtures. Do not add routine local full-suite runs here.

Human-input tests distinguish explicitly seeded request proofs from actual worker
provenance. The recorded-SDK composition runs a real request, saved choice/form
answer, second Gateway run and final output delivery. Network decision tests pass
the saved response through the existing middleware and a real disk policy DB;
the provider boundary is doubled and no host sandbox/network is opened. Hold
the final SDK limiter fence, preserve command-replay scope, and verify that UI
staging cannot consume a response control before actual publication confirmation.

Durable inbox/outbox tests use the same real SQL matrix. Exercise borrowed
rollback/autoflush, chat-scoped replay, publication/run fences, joined-input
completion and existing welcome plans. Worker barriers cover delayed same-process
completions, fresh claim IDs, exact-part acknowledgements, capacity release,
per-link pacing and repair. Advance injected clocks rather than real retry waits.
Inject receipt and subsequent release commit failures together; a healthy tick
must recover that finished claim without taking a live same-process claim.

Transport tests run the actual pinned SDK over both `RecordingRequest` clients.
Cover initialization failure cleanup, limiter overlap/cancellation, coalesced
401 checks, delayed responses after token replacement, and real sender-to-adapter
fallback/retry without recursive sends. Polling barriers own every suffix and
getUpdates operation; real SQL cases prove committed conflict fences and failed
writes. Product replay tests compare the exact origin set, including empty sets,
and preserve original topic identity through relocation. Logging tests restore
global filters/handlers and retain diagnostics while redacting credentials.
The managed transport strict test constructs SDK objects off-loop, then performs
all asynchronous lifecycle/send work on the Gateway loop. Clear the ZoneInfo
cache after seeding preferences so the quiet-hours case proves a cold timezone
lookup stays off-loop rather than benefiting from validation's warmed cache.

Supervisor tests use real SQL and actual recorded SDK transports for the health
barrier, ten-start limit, cancelled construction, same-token stale callbacks,
death budgets, missing-row reconciliation and shutdown. Retain every constructor,
task, barrier and client through teardown. Gateway lifetime tests cover later
startup failures, repeated cancellation and listener/client cleanup ordering.
Dispatch-authority barriers withdraw access or change bindings/protection during
rendering and between parts; a refreshed admission must persist protection before
the next raw call. Strict authority tests load an actual cold config and provider:
file reads/discovery run off-loop, construction/evaluation on the original loop.

CopyGuard tests use both SQL dialects, offline Docker-CLI stand-ins and controlled
startup poll records; no test probes the host daemon. CLI tests verify one JSON
document, safe projections, token classifications without rewrap, fresh-process
runtime-import blockers, refusal rollback and real snapshot consistency under an
independent writer. Existing-only engine tests prohibit bootstrap and preserve
live WAL data, foreign-key enforcement and configured PostgreSQL search_path.
The CLI/live-supervisor bridge removes real rows, reconciles twice and requires
client/poller retirement without chat/profile calls or row recreation. Expired
shutdown grace tests distinguish pending retry from an active SQL attempt, keep
that attempt owned, and forbid later retries before database teardown.

`test_agent_chat_tools.py` uses real ToolNode runtime injection and host-only
execution contexts for bounded page offers. Config tests use independent default
expectations; Compose tests load the real template, validate it through the same
strict model, and exercise legacy route refusals without a bot transport.

`test_telegram_compose_turn.py` sends real updates through the whole stack on the
rendered Compose template: its plugins, real provider SDK classes over a scripted
wire, and a person seated on a Space. Telegram could not answer anyone on Compose
while every test that chose its own configuration passed. When a turn needs a new
fact, prove it here instead of supplying it in a fixture.

`test_telegram_bot_migration.py` compares frozen migration DDL with cold ORM
creation on SQLite and isolated PostgreSQL schemas, including index names,
partial predicates, foreign-key deletion behavior and absent server defaults.
Numeric Telegram primary keys disable autoincrement so PostgreSQL adds no SERIAL
default. Rollback preflights all six history-bearing tables before any drop;
the partial-schema outbox case isolates its guard from the guarded link parent.

`test_telegram_library_pin.py` pins the Telegram library/API version, native
methods, error mapping, unknown-field passthrough and parent adapter contracts.
`_telegram_bot_test_support.py` supplies both socket-free bot transports with
explicit per-endpoint replies and event-controlled completion/cancellation.
Generate synthetic bot credentials at runtime; never record token-bearing URLs
or use real Telegram accounts in these tests.




## Project trash purge

Purge regressions use real original/companion files and the repository transaction.
Inject initial and persistent unlink failures to exercise Windows retry semantics
on every platform; assert rollback and a later successful purge. Plant real links
with `support.symlinks.symlink_or_skip`, preserve target bytes/modes, and cover
dangling links and removal before retry. Keep filesystem work off the event loop.
Todo production-chain regressions bind their explicit config through the runtime
AppConfig context, isolate sandbox/path singletons, and clean up the test provider.
Keep the missing ambient config path so a developer's `config.yaml` cannot mask
an invocation-time dependency; retain the real sandbox and middleware chain.

`test_threads_router.py::test_goal_mutations_reject_run_owned_by_another_worker`
feeds real PUT/DELETE 409 errors into the IM classifier. Keep this contract check
so Gateway wording changes cannot silently remove localized busy feedback.

Provider cleanup tests use concrete configuration models and event gates.
Surface owner-task failures while awaiting lifecycle events; join owners and
event waiters during teardown.

`test_bench_sandbox_prewarm.py` controls observation timestamps through the
benchmark module's clock binding. Keep asyncio's clock real, initialize
`Trace.started` from the controlled clock, and advance ticks at event gates.

`test_bench_opensandbox.py` owns provider-independent sandbox benchmark CLI
validation. Invalid timing values must be rejected through real `main()` before
fail-loud factories run; accepted controls must preserve factory inputs and JSONL
dimensions. Keep live providers stubbed and OpenSandbox's unsupported-option
checks distinct from shared timing validation.

`test_doctor_script.py` runs a copied doctor from a temporary checkout with an
empty-model config. Drop inherited `DEER_FLOW_*`, `PYTHONPATH` and `PYTHONHOME`
from child processes; cover help, invalid flags, normal reporting and an
unrelated parent config that stays untouched. Keep argument parsing at the CLI
entry point so in-process `doctor.main()` callers do not consume pytest's argv.

`test_run_terminal_protocol.py` exercises authoritative `run.terminal`
evidence through real Worker finalization and recovery takeover. Keep final
commit authority, producer drain, singleton confirmation, and Seal boundaries
observable. Recovery coverage must block after the committed claim, delete the
Run/events, then prove the lifecycle guard cannot recreate terminal history;
pin delivery-before-terminal order across all takeover entry points, and keep a
legacy `put_if_absent` signature on the owner path. `test_terminal_event_confirmation.py`
covers supported stores, non-replaceable one-shot recovery targets, and guarded
singleton rejection; use `TEST_POSTGRES_URI` for independent PostgreSQL pools
and real advisory contention. Release all storage/producer gates before bounded
cleanup, preserve healthy controls in refusal cases, and retain Journal/Seal
compatibility coverage. Protocol: [Run outcome](../docs/RUN_EVENT_STREAM.md#authoritative-run-outcome).
`test_goal_loop_stop_probe.py` runs the standalone Goal-loop reproduction through the native graph, loop middleware, worker and checkpoints. Only model responses are scripted; do not replace the Goal decision or simulate the stop-reason write.

`test_run_recursion_capped.py` drives a real graph through the worker until LangGraph raises `GraphRecursionError`. Only model responses are scripted; do not raise the error or write the stop reason by hand.

InfoQuest fetch log assertions install the real `UrlRedactionFilter` on the
capture handler with fixture-scoped cleanup. Expect scheme/host context with
redacted paths, so isolated runs and full-suite logging configuration agree;
keep payload omission and tool-result assertions intact.

Unbrowse final-URL regressions invoke the real fetch tool through mocked HTTP,
covering text/structured MCP payloads and warning/empty-page diagnostics. Strip
userinfo before the 500-character bound; keep ordinary URL spelling and `@` in
paths/queries. Invalid authorities must not echo raw credentials. Include long
userinfo and IPv6 cases in `test_unbrowse_target_status.py`.

`.github/workflows/backend-windows-live-policy.yml` runs the child-env contract and all
four live-test collection policy cases on Windows/Python 3.12, weekly, manually,
and for relevant PR changes. These children only collect live tests. Keep the
make-dependent checks in the default suite. The workflow-selection regression
in `test_client_live_policy.py` collects the configured pytest nodes and pins
all five policy checks, so a partial name filter cannot silently omit cases.

Runtime-callback cleanup tests keep DEBUG capture enabled and implement graph
checkpoint reads. Fixture-only exception tracebacks can retain the journal;
preserve the strict weak-reference assertion rather than clearing captured logs.

Config-upgrade checkout fixtures clear inherited `DEER_FLOW_CONFIG_PATH`,
`DEER_FLOW_PROJECT_ROOT` and `DEER_FLOW_ENV_FILE` before applying explicit test
overrides. Exercise selector leaks through the real script with temporary
configs; assert that the simulated developer config and its backup stay untouched.

Image-context budget regressions use real PNG files through `view_image`, the
state reducer, and both model-call wrappers. Check decoded payloads, cumulative
base64 size and skipped file reads; keep count and byte limits independent.
Re-view coverage must first observe omission, then restoration in model input.

Local nginx path fixtures convert MSYS output through the selected Git Bash's
PATH and decode both renderer and converter output as UTF-8. POSIX Git-layout
stubs exercise Windows-style locale decoding and missing/failed/empty converters;
native Windows keeps the real Git Bash renderer checks, including Unicode paths.

`test_skills_custom_multiprocess.py` starts independent spawn workers sharing a
temporary custom-skill root. Park edit/rollback before the write and before the
history append, observe the peer reaching the mutation lock, then check both
exclusion and the persisted predecessor chain. Always release and reap workers
on assertion failures; do not replace this with two threads sharing a mutex.

Search coercion ownership tests audit executable helper bodies/call sites and
inline integer assignments. Include Exa and SearXNG in the deferred census;
pin new-provider copies and docstring false positives with source fixtures.
This pattern gate does not establish arbitrary provider semantics or change
deferred providers' validation policy.
Include You.com in the shared-owner provider checks; it applies its 100-result
cap after shared normalization. Pin invalid-value defaults and positive count
bounds through the outbound request in `test_youcom_tools.py`.

Mixed-version resume admission in `test_thread_run_idempotency.py` uses a real
shared SQL repository and an old-column projection. The frozen pre-6499 helper
models only null-input resume retry matching from `02ce9ab2`; do not update it
to the new identity-aware policy or claim it runs a complete old Gateway.
Assert both writer formats: old workers reuse conflicting decisions, upgraded
workers reject conflicts, and identity-less legacy rows fail closed even for
identical retries. This pins why rollout requires routing to upgraded workers.

Run-store capability tests in that file check keyed input separately from resume
identity and assert that rejected admissions create no durable or local run.
An explicit `idempotency_key` parameter must keep supporting keyed input even
without resume support. For kwargs-only stores, pin the opt-in on the class
that defines `create_thread_operation_atomic`: replacing an opted-in parent's
method requires a fresh declaration; a flag-only child or mixin cannot opt in
another class's implementation. Also cover unchanged inheritance and durable
retry reuse, so the rejection tests cannot pass by rejecting every custom store.

Upload case-collision coverage uses separate HTTP requests and preserves both
reported payloads. Observe real filename-claim inputs to pin the disk seed;
case-insensitive hosts can otherwise mask a missing seed through link retries.

Same-name upload concurrency tests synchronize the first real claim: `os.open`
for direct writes and `os.link` for staged copies. Cover native/fallback options,
fresh/occupied copy names, every caller's exact bytes and no staging leftovers.
Drain workers and abort barriers on failure; do not rely on scheduling or sleeps.

Channel reload cancellation regressions must assert the worker returned the
stale snapshot before checking that newer runtime config survived. Completion
alone cannot prove the race: loader exceptions are caught and return `None`.

Browser egress session-close tests retain the real listener and its sockets,
verify a SOCKS handshake, then check listener shutdown, closed socket descriptors
and EOF on the established client. A new connection to the old port is not a
reliable ownership assertion on hosts with loopback forwarding or port reuse.
Keep ownership assertions inside the teardown-protected block; always drain
the client writer and close the saved proxy during teardown.

Read-before-write hash fixtures pin `newline=""` when writing LF/CRLF test
content, so native Windows cannot translate the bytes before the real read.

Browser-asset confinement tests use `support.symlinks.symlink_or_skip` for real
file and directory symlinks. Keep missing-file, duplicate-key, and size-limit
checks separate so they still run when the host cannot create symlinks.

Browserless reservation cleanup tests require a warning with the leftover path
and unlink error while preserving the original publication exception, including
interruption. Successful cleanup stays quiet.

Extension snapshot rollback and the managed plugins-block rewrite preserve the
original publish error when temporary cleanup or its warning log fails.
`tests/test_extension_atomic_write_cleanup.py` covers both writers, including
interruptions, an `fdopen` failure whose descriptor close also fails, a plugins
rewrite that fails while writing, a successful plugins rewrite's bytes and
mode, and a delete-branch restore whose unlink failure still lets a later
snapshot restore.
Skill archive upload copies do the same in
`tests/test_skill_archive_upload_cleanup.py`: a size-limit or read error stays
visible, a successful copy returns the original bytes, and the upload route's
post-install discard cannot replace a successful install or an install error.
A form-close failure cannot replace that outcome or skip the discard.
Custom-skill file writes and per-user skill-state writes do the same in
`tests/test_skill_storage_atomic_write_cleanup.py`, including
`KeyboardInterrupt` during a skill-state replace.

`blocking_io/test_web_tool_url_validation.py` resolves a synthetic `.invalid`
hostname to loopback by patching `_socket.getaddrinfo`, below the real
`socket.getaddrinfo` wrapper. Do not replace that wrapper or the production URL
guard: the strict gate must still reject on-loop resolution. Assert each tool
path reaches the native fixture so an unresolved-host rejection cannot mask it.
The IPv4 fixture accepts only `AF_UNSPEC` and `AF_INET`; unsupported families
must fail rather than receive a fabricated IPv4 answer.

Async community tools read their config off the loop, once per call.
`blocking_io/test_community_tool_config_reads.py` swaps each module's
`get_app_config` for a loader that records its thread before a real file read,
so a read the gate stops on the loop still counts; add new async tools there.
The gate only flags IO under `deerflow.*`/`app.*` frames, so its meta-check
calls a tool's sync config helper.

The local sandbox's UTF-8 subprocess guard inspects each text-mode call with
`ast`, checking both `encoding` and `errors`; module-wide literal counts can
hide unpinned calls behind unrelated settings.
Channel JSON state coverage likewise checks each writer and Discord reader call
within its owning class/method; unrelated encoding literals must not mask a regression.

## Project trash purge

Purge regressions use real original/companion files and the repository transaction.
Inject initial and persistent unlink failures to exercise Windows retry semantics
on every platform; assert rollback and a later successful purge. Plant real links
with `support.symlinks.symlink_or_skip`, preserve target bytes/modes, and cover
dangling links and removal before retry. Keep filesystem work off the event loop.

## PostgreSQL batch fixtures

Batch fixtures use `support.postgres.asyncpg_test_url`, delegating to production
`normalize_asyncpg_url` to map libpq `sslmode` to asyncpg `ssl`. Preserve TLS
modes, credentials and other query options; reject conflicting `ssl`/`sslmode`
and unsupported `channel_binding`. CI uses `?sslmode=disable`; validate
that URI shape against a real test database, not only a parameter-free local
URI. Reuse the normalized config for reopening and teardown, and drop only the
fixture's UUID schema. Keep the raw URI for production connection and CLI tests.

The 0033 batch-evidence migration fixture uses the same adapter. Its connection
contract probes execute the actual migration test setup through SQLAlchemy's
dialect argument conversion, stopping before database acquisition; the real
SQLite/PostgreSQL cases still exercise upgrade, downgrade and re-upgrade.

## Batch lease renewal races

`test_batch_lease_lifecycle.py` parks recovery after the per-item expiry SELECT,
identified by selected item columns; the parent query can contain the same expiry
predicate. Assert one rejected item UPDATE as well as the preserved row, so an
empty expiry scan cannot satisfy the concurrency guard.

## DingTalk stream tasks

DingTalk stream lifecycle tests drive `_serve_stream` with a fake websocket and
real handler tasks. Use events to pin pending-task retention, completion cleanup,
logged failures and quiet cancellation; drain handlers and keepalive tasks even
when an assertion fails.

## Real Compose tests

`support/compose.py` probes `docker compose version --short` and requires Compose
2.24+ for optional `env_file` syntax. Skip missing, old or unreadable clients with
an actionable reason; cover version detection offline in `test_support_compose.py`.
Real rendering and production entry-point tests use only read-only Compose calls.
Never start or stop a stack from these tests.

## Jina client options

Retry/response-budget compatibility tests pin client count and caller-supplied
`proxy`/`trust_env` settings. Additional provider-owned HTTPX options must not
break those assertions.

## Claude provider tests

`test_claude_provider_prompt_caching.py` exercises real Anthropic SDK serialization
through offline HTTP transports. Keep its directly imported `anthropic` SDK in
the backend `dev` dependency group rather than relying on `langchain-anthropic`
to supply it transitively.

## User repository ordering

`test_auth.py` pins `list_user_ids()` ordering with fixed UTC timestamps and UUIDs.
Cover both creation-time precedence and the lexical stored-ID tie-break for
equal timestamps through the real SQLite repository; do not assume wall-clock
calls are distinct or weaken the result to an unordered comparison.

## Router auth fixtures

For owner-scoped route assertions, pass a stable `user_factory` and
`bind_current_user=True` to `make_authed_test_app`. The default stub stamps
request state but leaves the ambient user context unchanged; the opt-in binding
uses the stub user during the request and restores the previous context after it.

## Lark skill-pack publication

`test_lark_skill_manifest_atomic.py` injects `os.replace` failures at the actual
guidance and manifest writes. Assert that existing file bytes survive and
temporary files disappear. Reinstallation tests use complete ZIP fixtures and
the real installer: the previous installed tree must stay byte-identical while
the write is still in staging and after it fails, with no staging or backup
tree left behind. Do not replace these behavioral checks with assertions about
source formatting or duplicate the runtime publication control in
`test_lark_cli_atomic_publish.py`. That runtime publisher must also keep the
original publish error when temporary cleanup or its warning log fails,
including interruptions. A successful publish still replaces the bytes, leaves
no temporary file, and keeps the temporary file's mode; callers chmod afterward.

## Channel staging cleanup

`test_channel_staging_cleanup.py` blocks the rename destination (a non-empty
directory, because an empty one would just swallow the staged file) and fails
`Path.unlink`, the descriptor `close()`, and the log handler in turn. Assert the
save error is what propagates, that the unlink is still attempted after a failed
close, and that a staged copy the cleanup could not remove is named in a warning.
Keep these as injected-failure behavior checks: an assertion that the handler
source merely contains `suppress` would pass while the close error still skipped
the unlink.

`test_channel_credential_staging.py` leaves copies through the real credential writers, then ages
one for restart cleanup. Preserve current credentials, recent peer staging, anonymous files,
symlinks, directories and nested attachments. Exercise SQL startup after the legacy file is gone,
plus failed unlink/listing and a real delayed FileHandler whose parent is missing. WeChat startup
cleanup must also pass the strict blocking-I/O gate. Buzz failure tests keep the original write
exception attributable through close/unlink/diagnostic errors, retain pending events for a later
flush, and propagate interruptions after cleanup.

## Lark CLI blocking-I/O fixtures

`blocking_io/test_integrations_router.py` uses a real local CLI stub: a `.cmd`
script on Windows and an executable shell script on POSIX. Resolve it through
the production PATH lookup, including a directory containing spaces. Seed fake
app credentials so auth completion reaches the CLI instead of returning early,
and keep fixture filesystem work behind `asyncio.to_thread`.

## Lark device-login single-flight

`test_lark_auth_singleflight.py` pins the `(user_id, generation)` single-flight
contract: request timeouts leave one shared CLI alive; supersession terminates
it, including its POSIX process group. Forked workers share a credential root;
every CLI gets the invocation-log environment so duplicate spawns are visible.
Publication grace starts at observed CLI exit, independent of login duration.
Cover publication between a foreign result read and its marker probe.
`test_lark_auth_lifecycle_failures.py` injects storage and startup failures:
terminal marker receipts prevent retrying uncertain single-use codes, cleanup
errors preserve published results, and failed starts reap children and wake
waiters. Treat unhandled worker-thread warnings as errors. Event-driven process
doubles must be released/killed and joined at teardown.
`test_lark_credential_tree_churn.py` removes real descendants at each walk step;
assert the injection fired and surviving secrets reached private modes. Missing
roots, permission errors and symlinks remain errors. Mutations must fail these
assertions; merely returning without an exception is insufficient.
Run mutations on a copied harness; never swap checkout sources during tests.

## Remote read fixtures

Remote ranged-read fixtures pass POSIX sandbox paths to adapters and map those
paths to native temporary files in fake transports. LocalSandbox keeps the native
host path; do not relax remote path validation to accommodate Windows fixtures.

## Shared sandbox search contracts

`test_sandbox_search_contract.py` runs shared `ls`/`glob`/`grep` scenarios through
Local, AIO, E2B, BoxLite, Tenki and OpenSandbox adapters. POSIX shell transports
execute production commands against temporary files; AIO file RPCs use an
independent filesystem-backed double. Do not substitute precomputed final search
results or import another test module's private fixtures. Provisioning, live SDK
compatibility, permissions enforced above Sandbox, and session lifecycle remain
in their existing suites. Windows skips this POSIX execution tier explicitly.

Compare result names/content, errors and completeness, allowing documented root
entries and directory suffixes to differ. First-stage cases use ordinary roots,
positive caps and a plain literal pattern: ignored-root fixes (#5667), E2B literal
metacharacters (#5627), and exactly-full local caps (#5491) have separate owners.
Add those shared scenarios after their fixes land; do not encode known bugs as
expected successful behavior or hide them with permanent xfails.

## External system-role admission

`poc_external_system_message_injection.py --help` is the opt-in live reproduction
and post-fix verifier; never run it automatically against an existing user's chat.
`test_poc_external_system_message_injection.py` tests that CLI offline.
`test_external_system_message_boundary.py` records model inputs with a fake model:
the same regression must fail on unfixed admission and pass after rejection,
without production-provider logging or interpreting model obedience as proof.

## Scope-isolation benchmark

`test_bench_deermem_scope_isolation.py` exercises production admission and storage.
Check persisted facts and user/history summaries for semantic safety, but only
agent-local facts for routing. Failed updates must not become sealed observations;
report and resume must reject failed or old-schema rows. Stub live models so these
tests never need credentials or network access.

## MCP claim fencing

Notification recovery tests advance to the persisted `next_notification_at` after
dispatch. It records completion time; a fixed scan-time offset can query before
the notification is due on a slower runner. Preserve delivery and counter assertions.

`test_mcp_task_repository.py` covers same-worker reclaim during an in-flight
release, poll/cancel snapshot, or notification completion. Use explicit events
to pause the old operation at the persistence boundary, reclaim via the real
repository, then verify the entire new row remains unchanged. Reclaiming before
the old operation starts does not catch SQLite SELECT/ORM-flush races. Keep the
old completion timestamp within its original lease so expiry cannot mask a
missing token fence; always drain paused tasks and restore session patches.

## Project document cache

`test_project_document_char_cache.py` pauses real file-IO workers between cache
lookup and promotion to exercise concurrent eviction. Keep cache metadata
operations atomic, and verify that a full scan does not hold the cache lock.

## Executor starvation tests

`test_executor_starvation.py` covers the deterministic starvation semantics from RFC #4560:

- default-executor saturation and queueing;
- cancellation of an awaiter while an already-started synchronous worker continues;
- isolation between the asyncio default executor and DeerFlow's dedicated file-I/O executor.

Use explicit synchronization such as `threading.Event` rather than sleep-based timing thresholds for worker lifecycle assertions. Every test must release blocked workers and restore any process-global monkeypatches so teardown cannot leak threads or state into later tests.

Stress/soak testing, AnyIO worker instrumentation, Uvicorn multi-process behavior, and broad production executor redesign are separate concerns and should not be folded into these deterministic regressions.

## Managed DeepSeek compatibility

`test_managed_deepseek.py` exercises real SDK request serialization and SSE parsing
with an HTTP double; do not replace the provider classes with successful stubs.
`test_managed_deepseek_live.py` uses the same production probe/model configuration
against DeepSeek only with `DEER_FLOW_RUN_LIVE_TESTS=1` and
`DEEPSEEK_TEST_API_KEY`, never in CI. Keep credentials and provider payloads out of
committed evidence. A passing connectivity probe does not establish full agent
compatibility; distinguish protocol assertions from observed live behavior.

## Readability fallback regressions

`test_web_fetch_relative_links.py` simulates degraded extraction without requiring
Windows. The link-free article companion must retain the extractor's exact HTML
and exclude resolved navigation links from Markdown; checking for raw `href=`
attributes after Markdown conversion cannot detect an unwanted fallback.

The authority-tag census also scans parser regexes as raw source. Classify
Readability's ordinary HTML body tag as a reviewed test exemption, with markup
preservation and nested framework-tag neutralization covered in
`test_input_sanitization_middleware.py`.

## Classification request packing

Pin the Jev example's outbound byte budget against captured HTTP request bodies.
Cover UTF-8 text and JSON embedded in chat messages, including escaping-heavy
inputs that fit the host's separate input limit. Use offline transports.
Check the host's JSON output bound with the maximum item count and keys that
expand when escaped; preserving raw UTF-8 limits alone does not cover it.

## Advisory fetched-content screening example

Load the example through `load_extensions()` and the host isolation wrapper, as a
`plugins:` entry is loaded; never instantiate the middleware directly.
`test_jev_result_screening_extension.py` checks install/config diagnostics, bounded
requests, the task-store handover between the tool wrapper and `before_model`, copy
semantics, fail-open provider errors and isolation diagnostics for local bugs. It
also pins per-message classification: a benign first message in a `Command` must
not take the flag of an injected later one, and a malformed, compressed or slow
answer for one message must not cancel or hide another's flag. The loopback test
uses a real local server behind an unreachable proxy variable.
`test_jev_screening_pipeline.py` runs real lead/subagent graphs to a recording model
with a task store bound under the runtime-context key the Gateway worker uses; cover
the redacted excerpt, error metadata/receipts, budget, one-time warnings including
valid empty-string message IDs, and runs without a task store. Only `None` awaits ID
assignment. Use offline transports. Detection is not behavioral defense; do not infer
the final model input from an isolated hook test. `test_jev_screening_policy.py` uses
the host descriptor builder to pin policy identity; hash endpoint/prompt text and
never project credential values.

## Business report numeric contracts

Use independently specified amounts or `csv` + `Decimal` expectations for report
parsing and reconciliation regressions. Cover currency evidence across all files
and rows, including explicit currency columns and preference conflicts. Report
JSON and XLSX cells retain numeric precision; rounding belongs to display formats.
Check actual XLSX formula operands and cached values, including grouped totals,
averages and fractional cents, rather than comparing two rounded code paths.

## Sandbox CI base

The restricted sandbox smoke build must use the release workflow's mirrored
base at the Dockerfile's digest. Its contract test executes the build step with
an offline Docker double; keep the separate vendor compatibility pulls intact.

## Direct-fetch worker capacity

HTML extraction and Markdown conversion run in one worker, and retain the fetch
slot until that worker exits even after repeated cancellation. Network and queued
request cancellation remain prompt. Use explicit worker events and slot-acquisition
signals in regressions, release every held worker in `finally`, and verify the
four-worker bound against replacement requests while cancelled work is still held.

## Account export completeness

Use real owner-filtered project/document/preference repositories with two owners.
Exports include active and archived project definitions, conversation membership,
live originals and existing converted Markdown; deleted projects, trash, staging,
orphans and arbitrary preference keys stay out. Keep SQL-only accounts exportable.
Restored documents can retain their old project storage namespace. Plan lexical
paths and let the existing descriptor-based copy verify ancestors and immutable
original hashes; never resolve symlinks first. Document `files` lists only ZIP
entries actually copied, including after quota skips or copy rollback. The export
is a best-effort readable snapshot, not an automatic restore format or a global
transaction. Preserve worker draining, disk limits, splitting and manifest hashes.

## AI employee consumer qualification

Consumer qualification lives in `test_work_consumer_journeys.py` (production
Work tools/worker with deterministic graph and reopened SQL/services),
`test_consumer_working_data.py`, and `test_work_input_example.py` (actual optional
package over the canonical human facade). Keep business formats in consumer
packages. Native proof stays in the mandatory no-skip storage tier; deterministic
fixtures and model smoke are separate evidence. No consumer path selects host
identity or bypasses native containment.


## Shipped Compose qualification

The mandatory native tier's scheduled-trim regression: [details](../docs/tests-guidance-details.md#native-tier-scheduled-trim-regression).

`test_compose_instance_acceptance.py` (opt-in live, `HARTMESH_REQUIRE_BUNDLE_INSTANCES=1`, all 27 cases, zero skips):
[details](../docs/tests-guidance-details.md#compose-instance-acceptance).

Cases 18–27 prove direct live/closed uploads, streamed 40 MiB digest equality,
Stop without assignment cancellation, count refusals and same-kind slot reuse,
denied-site artifacts, supervisor requests and operator/container-loss settlement.
The scripted Work branch rejects `ask_clarification` or a missing system question
rule with HTTP 422. Full suites and large fixtures must use task-owned disk temps.

Channel execution tests separate admitted human/checkpoint provenance from final
AI output completeness. Exercise real manager, owned HTTP/SSE and Gateway paths,
both SQL dialects, strict read acknowledgements, actual binding authority,
post-await scope fences and retained reads during shutdown. No receipt, cached
success or fabricated AI identity proves a complete final answer. Preserve legacy
channel defaults; never run native/Compose suites locally.

Output-evidence tests use real isolated SQLite/PostgreSQL for atomic seals,
ambiguous commit recovery, cancellation/version/lease fences and authoritative
snapshots. Pure projection and mock HTTP tests prove protocol validation only,
not actual worker-source provenance. Opt-in creating admission requires the
actual run/event capability pair. Keep missing API scaffolds distinct from
behavioral regressions.

Run-read ownership tests cover actual SQLite cursors in DELETE/WAL modes and
both SQL dialects for owner, error and timeout behavior. Measure independent
COMMIT/readback before red-only diagnostic cleanup; WAL writer success does not
prove cursor closure. A cancelled terminal read must not continue its status CAS.

Independent storage startups need fresh app state and the actual storage
lifecycle owner. Drain each lifetime before disposing its SQL engine or dropping
its schema; never reuse a completed cleanup task for a new maintenance generation.

## Deploy home permission tests

`test_deploy_home_writability.py` covers the production deploy permission
preflight. It runs the complete script with a recording Docker stub and
isolated runtime paths. Permission cases remove directory write bits or secret
file read bits and restore them during teardown; they skip on Windows, as root,
or when the filesystem does not enforce those bits. Cover both persisted
secrets, shell/Compose dotenv overrides, readable read-only secrets, and teardown
without secret exports. `down` must not probe, read, or generate secrets.
Writable-directory cases remain portable.

## Workspace snapshots and text cache

`test_workspace_changes.py` and `test_run_worker_delivery.py` remove real files
at the hash-read boundary. A vanished source must not discard other file records
or produced-output detection; permission and other hash I/O errors still propagate.

`test_scanner_text_cache_atomic.py` verifies complete publication and preservation
of an existing entry on failure. A cleanup failure must preserve the original
publish exception, including interruption. Import the scanner during fixture
setup, after the autouse fixtures initialize runtime, to avoid the package cycle.

### Web model provider presets

`test_managed_provider_presets.py` covers encrypted compatibility, auto IDs, key switching, snapshot isolation and native/compatible protocols. Its HTTP double is below actual LangChain/vendor SDK serialization; do not replace these adapters with successful stubs. `test_managed_presets_live.py` requires explicit opt-in and vendor credentials, and remains skipped in CI.

## Helm Redis credentials

`test_helm_redis_existing_secret.py` renders the chart and executes its startup
and both probe commands with argument-recording Redis stubs through
`support.shell.require_posix_sh`. Keep missing/empty password keys, stale
`redis.auth.password` values, Secret precedence, external mode and shell
metacharacters covered.
Render tests skip when Helm is unavailable; they need no Kubernetes cluster.
A falsy `existingSecret` (`false` is how people spell "not set") must fall
through to the chart-managed Secret, not name the Secret after the value.

## Helm postgres Secret precedence

`test_helm_database_secret_precedence.py` pins that the gateway's `DATABASE_URL`
and the postgres StatefulSet's `POSTGRES_PASSWORD` resolve the same Secret in
every branch of `deer-flow.databaseUserSecret`, and that only the chart-managed
branch renders a Secret. Teaching one consumer a new Secret source without the
other has to fail here. The falsy cases exist because `or` returns its last
operand even when falsy, which would name the Secret `false`.

## Multi-instance harness

Multi-instance harness tests use `support.shell.require_script_bash()` for shell
smoke checks. Keep config content and secret stability checks portable; isolate
POSIX owner-only permission assertions so Windows skips only those checks.
Nginx path expectations must include quoted-string escaping for native paths.

## Launcher dotenv parsing

`serve.sh` never sources root `.env`: stdlib-only `scripts/dotenv_exports.py`
(host Python, before `uv sync`) tokenizes it like python-dotenv, keeps `.env`
over shell exports and the `$NAME`/`${NAME:-x}`/leading-`~` expansion that
`support_bundle.py` mirrors (keep both expanders in step), and prints
shell-quoted exports for `eval`. `test_serve_dotenv.py` and the launcher-block
cases in `test_support_bundle.py` run the real block; harnesses that copy
`serve.sh` with a `.env` must copy the parser too.

## Channel artifact turn sequencing

`test_channels.py` models consecutive artifact-producing turns by awaiting an
async outbound callback before sending the next inbound message. Keep the real
worker-pool concurrency, preserve per-turn artifact assertions, and stop the
manager in `finally` so a failure cannot leave tasks using a closed test loop.

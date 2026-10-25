# Backend Tests

Backend tests must preserve the runtime invariants they exercise without changing production execution topology.

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

Repository tests use `_telegram_repository_test_support.py` with isolated real
SQLite/PostgreSQL databases, disk-backed fixtures and no runtime bootstrap.
Group SQLite fixture DDL in one explicit transaction; retain normal production
writer semantics during tests. Normalize PostgreSQL `sslmode=disable` into the
asyncpg connect option, matching CI. Exercise concurrent first-owner/capacity
admission, borrowed-session rollback, account ownership, binding generations,
durable Start replay, code epochs, cleanup and immutable replacement receipts.
The tracked-token scan reports locations only; construct credentials at runtime
and run it after staging so newly added source files are included.
Revision 0043 preserves retained inbox records while adding chat to their primary
key. Test duplicate chat-local message IDs across account changes and refuse a
downgrade that would collapse retained identities. Keep 0042 DDL/metadata/ancestry
and downgrade guards unchanged; its one exact successor-shape validation path
must preserve cross-chat records and reject drift during older-stamp replay.
Current ORM parity is measured after 0043; historical 0042 tests use its frozen
table metadata.

Presentation tests use explicit route-table expectations, both locale catalogs,
UTF-16 boundaries and canonical preview reconstruction. Provider tests use fake
clients or actual SDKs over socket-free mock transports, including timeout,
cancellation, endpoint/key capture and context-scoped payload-log suppression.
Keep recorded credentials synthetic and test artifacts on disk.

Ingress tests run the real Application and recorded SDK through the ordered gate.
Prove swallowed handler/SQL failures retain PollTask's suffix, while fixed replies
commit their route and delivery before acknowledgment. Token/stranger input must
reach no inbox, hash, bus or file consumer. Ledger churn, removal/re-add and
rotation retain bounded unexpired allowances independently of LRU recency.
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

## Router auth fixtures

For owner-scoped route assertions, pass a stable `user_factory` and
`bind_current_user=True` to `make_authed_test_app`. The default stub stamps
request state but leaves the ambient user context unchanged; the opt-in binding
uses the stub user during the request and restores the previous context after it.

## Lark CLI blocking-I/O fixtures

`blocking_io/test_integrations_router.py` uses a real local CLI stub: a `.cmd`
script on Windows and an executable shell script on POSIX. Resolve it through
the production PATH lookup, including a directory containing spaces. Seed fake
app credentials so auth completion reaches the CLI instead of returning early,
and keep fixture filesystem work behind `asyncio.to_thread`.

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

`test_mcp_task_repository.py` covers same-worker reclaim during an in-flight
release, poll/cancel snapshot, or notification completion. Use explicit events
to pause the old operation at the persistence boundary, reclaim via the real
repository, then verify the entire new row remains unchanged. Reclaiming before
the old operation starts does not catch SQLite SELECT/ORM-flush races. Keep the
old completion timestamp within its original lease so expiry cannot mask a
missing token fence; always drain paused tasks and restore session patches.

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

The mandatory native tier also runs the scheduled-trim regression on isolated,
disposable loop images. Only a task-owned nonempty fstab table is passed to
`fstrim --listed-in`: its unprotected positive control must actually lose physical
allocation and fail qualification; the protected image must retain allocation,
files and qualification. Never run this native test locally or trim the host's
global table. Compose failure evidence includes bounded fstrim service/timer facts.
After deliberate workspace destruction/replacement, the bundle journey waits for
public `agent_instances.execution` readiness before the next independent journey.
The shared wait retains a hard 90-second deadline and bounded GETs; it never
retries an activation or substitutes Gateway liveness for resident qualification.
Cases 11 and 12 also capture the exact current attachment, personal AI assistant
and Home scope, primary and paired relay before each deliberate removal. The
read-only retirement barrier requires that exact host's `container-absent` fenced
record, both containers' ID AND name absence, then fresh public readiness within
one 90-second budget. SQL closure alone does not prove native finalization.
Keep the standard-library helper's manual-clock tests offline and inspect native
wiring through AST only; case 27 must still kill its in-flight attempt.

`test_compose_instance_acceptance.py` is opt-in live, gated by
`HARTMESH_REQUIRE_BUNDLE_INSTANCES=1`; missing prerequisites fail when requested.
The `bundle-instances` CI job requires all 27 storage/agent cases in order
with zero skips and disk-backed `TMPDIR`/pytest basetemp. Never run or collect this
suite locally. Ordinary coverage reads its AST/workflow through
`test_compose_instance_acceptance_contract.py`; receipt tests use synthetic files
and do not qualify a tenant host. Case four stops the Gateway and takes the pool
runtime ownership lock for isolated orphan-fixture cleanup; it preserves the new
allocation and unrelated bindings. Case seven proves automatic reclamation of an
used account after two allowlist chats. It authenticates that account through the
public setup flow, captures both actual paired sets, rechecks all eight resources
before disablement and requires original ID AND name absence before reuse.
Case nine exercises exact-ID Undo sharing and explicit republication,
matching existing product behavior; removal has no Undo. Keep CI stack evidence
separate from the complete human-performed tenant qualification receipt.
The missing-role observation holds the production allocation advisory lock on a
separate test-owned PostgreSQL session. Drain that exact session and any owned
cleanup one-off before allowing maintenance or restarting the Gateway; failure
to confirm drainage leaves the Gateway stopped and fails qualification.
The offline contract pins runtime-stage provisioning of the bundle's `nsenter`
and `setpriv` tools; builder-only packages do not reach the final slim image.
Each synthetic run preserves bounded SSE and terminal JSON evidence before its
success assertion. Exclude cookies and auth headers. Skill category binds may
come from caller/global projections or operational `chat-runtime` projections;
neither changes the six admitted data/ACP binds or caller-owned integration binds.
The scripted model listens on a runner-host port outside the shipped sandbox's
allocation range; Gateway-container socket checks cannot observe host listeners.
Failure artifacts include bounded numeric image/root facts and role/backing
mappings. Keep file contents and credentials out; physical failures must remain
diagnosable without relaxing admission or repairing an image during the test.
Full-files HTTP 507 uses the central root-level `reason` and fallback `detail`
sentence; feature unavailable/busy responses have separate nested envelopes.
Stage-three cases share one AI assistant and use authenticated Space WRITE before
execution. They require idle-restart CID reuse, workspace loss recovery, actual
runtime/probe facts, Work outcomes/capacity and graceful/forced restart settlement.
The Docker event observer starts before preparation and drains on every exit.
Native resident cases use a real provider, registry and run manager on a separate
4096-inode backing; keep the 256-inode exhaustion fixture unchanged. Drain SDK
clients, confirm attachments contained, close the registry/provider, then dispose
SQL. Both named resident cases are mandatory with zero skips; never import or
collect the native suite locally. Generic native backing proof is distinct from
managed Gateway ownership, proved by the bundle cases.

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

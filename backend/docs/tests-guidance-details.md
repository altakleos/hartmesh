# Backend test guidance details

Detail moved out of `backend/tests/AGENTS.md` so it stays within the budgets in `scripts/check_agent_guidance.py`. These rules still apply; the test guide links each section.

## Telegram repository tests

Repository tests use `_telegram_repository_test_support.py` with isolated real
SQLite/PostgreSQL databases, disk-backed fixtures and no runtime bootstrap.
Group SQLite fixture DDL in one explicit transaction; retain normal production
writer semantics during tests. The SQLite fixture sets production's WAL and
`synchronous=NORMAL`, so checkpoint before copying its file. Normalize PostgreSQL `sslmode=disable` into the
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
Current ORM parity is measured after 0044; historical 0042/0043 tests use their
frozen table metadata. Revision 0044 widens only the nullable, nonindexed Telegram
assistant-message ID to Text. Exercise actual legal long SDK IDs through worker
events/sealing and primary planning on both dialects, old-to-new migration and a
downgrade refusal before DDL when longer IDs are retained.

## Native tier scheduled-trim regression

The mandatory native tier also runs the scheduled-trim regression on isolated,
disposable loop images. Only a task-owned nonempty fstab table is passed to
`fstrim --listed-in`: its unprotected positive control must actually lose physical
allocation and fail qualification; the protected image must retain allocation,
files and qualification. A second native case restores a really trimmed image and
reads its file through a fresh mount. Never run these locally or trim the host's
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

## Compose instance acceptance

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

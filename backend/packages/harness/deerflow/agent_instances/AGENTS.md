# Persistent agent instances

Definitions remain in `persistence/agents`; adopted documents, resident
identities and delegated access live in `persistence/agent_instances`. Never
substitute a resident principal for human `user_id`, create login users, or
inherit private creator memory/credentials as ambient agent context.

`InstanceDirectory` rechecks active nonhuman identities. Existence does not
authenticate caller-selected IDs. Active personal actors need a live custodian;
company actors do not depend on creator login. Only the exact host-owned
provisioning ContextVar validates a pending grant target; reset it on every exit.
Ordinary lookup rejects pending/suspended/archived/deleted identities.

Creation drains enrollment, atomic home binding, canonical membership grants
and activation across cancellation. Creation UUIDs bind creator plus exact
request. Pending retries retain home/revision identity; ready retries never
restore revoked grants or recreate backing. Use Storage Spaces' provisioning
transaction and membership API, never direct grant inserts or directory creation.
Unknown outcomes retain inspectable intent.

Creation applies current human/company provisioning authority before exact retry;
new intents serialize allocation → kind → personal count locks on PostgreSQL,
or the SQLite writer. Bundle allowance lookup and human validation borrow the
owning session. Per-user limits also apply to administrators; unfinished,
suspended and archived rows count. Exact retries bypass counts, never current
authority. Homes use assistant/employee slots by custody; refresh authority before
binding effects and active publication. Generic `space_owner` is metadata only;
the caller must admit current Space ADMIN before an ownership refusal.
Only `CompanyProvisioningDenied` maps company copying to 403; hidden or revoked
source authority retains 404. Capacity reads use the Gateway config wrapper's 503.

New reply/Work admission checks current grants → execution → storage, repeating
the decision immediately before durable admission and after capacity reservation.
Exact Work retries validate their recorded intent without another readiness
decision. Preserve uncertain committed launch promises. Lifecycle delete/abandon
alone admit `space-unverified`. Account release locks allocation → seats → role
→ sorted Space parents → instances; no parent-locked reclamation callback may
acquire allocation. Delete only grants plus an exact personal provisioning row
with no Home; unexpected referenced history rolls back. Bound assistants and
their Spaces become deleted together. The release receipt counts currently
matched assistants, so a retry excludes a removed unfinished creation.

SQLite reserves its writer before instance reads; PostgreSQL locks the parent.
Resource locks precede instance locks during home binding/activation. Confirm
initial grants in the shared transaction before active publication. Host grant
admission also fences delayed initializers under resource-then-instance locks:
a ready peer must not cause a stale creator to restore later revoked grants.
This additional condition never replaces mandatory storage checks. Generation
checks fence rename. SQL filters current grants before pagination. Provider
route permissions are additional ceilings; optional authorization settings
never bypass mandatory instance/storage checks.

Hash exact captured config/SOUL/lookup namespace, without adding future defaults
to old revisions. Validate stored revision bytes on reads and retries. Business
configuration does not grant ADMIN, credentials, plugin trust or wider mounts.
Append migrations after `0034`; preserve published fixtures and register models
in `persistence/models/__init__.py`. No creator/user deletion cascade may erase
company identity or homes.

Discovery projections live in `app.gateway.agent_discovery`: bindings come from
one durable batch read; metadata grants no identity. Reuse conversation authority
before pagination, retaining Inspect-required protected context. Running details
and Home state require Inspect; quota bytes never leave the detail projection.
Administrator inventory adds four summary fields without granting access.
`abandon_creation` locks Home before instance, retains bound deletion evidence and
never resolves humans or performs file effects under that lock. Gateway maintenance
expires 24-hour intents before release, including when no adapter is available;
per-row failure cannot prevent other cleanup. Keep it in the owned maintenance task.
Host orphan deletion repeats live owner/supervisor/Manage-holder checks inside
Home→instance locks, including after dry-run. Atomically tombstone both rows while
retaining the binding, slot accounting and pending intents; Gateway owns release.
Host Home-grant callbacks borrow that transaction and require current human Inspect.

`test_agent_instances.py` covers both SQL backends and actual storage APIs with
injected directories/backings; these fixtures never qualify native quotas. The
mandatory Storage Spaces Linux Docker tier owns native filesystem proof.
`AgentConversations` owns durable bindings, retry keys and deleted-chat tombstones.
Never infer binding from JSON metadata or requester ownership. SQL audience filters
precede pagination; current Inspect or original requester Use admits unprotected reads.
Memory-bearing/cross-requester copies persist Inspect-required provenance; carry it
through references/branches and honor it on all reads and atomic execution admissions.
Compare source/destination conversation recipients as well as writable Home readers.
Manage
controls mutations, while Use controls execution and stopping one's own run.
Worker bookkeeping locks thread then instance, matching repository mutations.

`AgentExecution` is host-only and carries human requester separately from resident,
adopted revision, Home and thread incarnation. Reject client copies and never persist
the object. Inspection projections cannot execute. Validate on the original host
loop before model/tool calls, including isolated child loops and worker threads.

Workspace refusal types in `contract.py` use the existing `Refusal` HTTP envelope.
All eight `ResidentUnavailable` subclasses default to 409, including workspace
`files-full`, distinct from the personal-chat 507. Preserve cause metadata and
stable fallback sentences. `RESIDENT_INTERRUPTED` names a frame, never an exception.

Managed Gateway startup installs `residents.py`; preparation/worker hooks use the owning loop;
provider-locked slots include exact preparation claims through uncertain outcomes.
Restoration retains each durable attachment; incomplete snapshots never open admission.
Private `bootstrap.py` retains startup tasks through timeout/cancellation,
checks exact census evidence before atomic callback/readiness publication, and seals
before draining workers, initialization and registry effects. See the registry guide.
Expiry/cancellation never implies containment. Timed-out eviction stays owned
through drainage. Restore durable claims and coordinate admission before installation;
see [registry rules](../../../../docs/RESIDENT_REGISTRY.md).

`app.gateway.resident_runtime` owns the registry and complete native collector.
Resident capacity deduplicates retained SQL and v5 journal claims only by exact
attachment/CID/daemon/incarnation/full resource scope. An unknown CID stays charged.
The collector freshly qualifies each original resident graph before explaining its
paired resources, preserving partial cleanup and rejecting changed/foreign graphs.
Capture that exact set and resident revision in the memory-only publication proof.
Work activation's refusing preflight authorizes an exact replay without writing
a new attempt; physical capacity stays outside SQL, with pure exact binding
before commit. Retain live launch tasks through cancellation and never expire
their promises. Confirmed rollback may cancel; uncertain commit retains charge.
Serialize runless replay recovery under the Home lock, releasing before launch.
Drain sealed admissions before RunManager shutdown snapshots workers. Periodic
orphan callbacks request an exact deferred recycle; maintenance owns closure.
An unacknowledged preparation cannot become absence from an empty census.
Startup recovery freezes the original record set and rotates at most four records
per pass before the final census. Require exactly one supported writable Home
mount, current Home ownership, unchanged missing/fenced SQL facts, and no local
holders or borrowed complete durable inflight matches. One absolute deadline
belongs to the owned initializer; only a new explicit retry gets a new budget.
Completion drops only the exact attachment claim after durable original removal.

Opt-in Work readiness/host-settlement seams require exact Home/actor/host facts.
Reload attempts after Work locking; preserve terminal outcomes and candidates.
Commit before journal output. Readiness checks current generation; settlement
accepts historical containment. Caller-owned live-run snapshots require coordination.

`runtime.prepare_environment` consumes exact admitted native attachments and the
existing AIO SDK. Address immutable Docker IDs, not container IPs. Drain preparation
and closure across cancellation; unknown outcomes retain native intents. SDK closure,
leases and ordinary completion never fence or replace physical writers. Children
inherit the full Home mount; tool filtering is not a narrower filesystem boundary.
Qualification requires the inventory and probe image. Cache the adapter only after
successful qualification; reuse still proves current host/owner/evidence. Residents
use restricted topology without host ports, with exact RO configured mounts and
per-plan Shared projections. Fresh Company role/binding/root qualification precedes
prepare and resume; opaque revisions refuse reuse after directory replacement.
The exact-container transport establishes fixed compatibility aliases using
isolated Python and protected directory descriptors. This bounded root setup
never opens Home targets; the nonroot SDK creates Home children and confirms cwd.
Reject layout conflicts/failures before publishing a provider. Instance bash
does not execute a command when changing to Home fails.
Output presentation reads only metadata through confined output descriptors on
the admitted Home backing, with current resident READ and captured generation.
It requires the exact prepared execution; container paths are never host paths.
References remain mutable and confer no human retrieval permission.

`agent_files.py` owns direct host Home mutations through existing `SpaceFiles`:
resident actor, human provenance, bounded bytes and immutable operation receipts.
Borrow current human/conversation admission before receipts and inside each mutation;
`context_admission_in_session` shares the admitted Home transaction, never a checkout.
No workspace acquisition or containment is needed. Upload context stats regular,
no-follow Home uploads only under the exact prepared execution. Drain file/metadata
workers before scope cleanup; never inspect requester paths or upload contents.

Public skill scope is read-only and excludes private/legacy/integration storage,
including late activation and primed caches. Capture bounded public bytes and record
their native revision separately from live host activation. Home copies are mutable
data, not authority or immutable execution evidence. Instance DeerMem facts and
all summaries use SQL through the portable storage port, with explicit current
Inspect/Home READ and execution Use. Never substitute user IDs or disk indexes.
Resource→thread→instance locks admit snapshots/commits; release before extraction.
Capture epoch when queued and retain it across retries. Clear/import retire old
epochs atomically; lifecycle generations and conversation tombstones reject stale
writes. Use-only/disabled/unsupported scopes have no memory fallback. Queues stay
owned through completion and shared-budget shutdown; do not prune live run queues.
Scheduling and a separate storage ACL engine stay outside this module.

`change(host_admission=...)` adds a trusted borrowed-session condition under the
Home→instance locks during withdrawal and finalization; it never replaces current
human, Manage or ADMIN checks. The operator CLI uses it for orphan recovery and
retains immutable intent/handoff events in the Home's existing event stream.
Reserve generation room before publishing recovery grants. Exact partial retries
must preserve target, host, original facts, grants and lifecycle attribution.

Lifecycle uses the host-only callback inside qualified attachment retirement:
resource→attachment→instance, capture exact intent/IDs and withdraw generation
before effects. Pending never proves containment. Empty/completed retries never
retire replacements; finalization proves captured IDs fenced under current ADMIN
and Manage before reconciling Home generation. Delete commits both tombstones and
the false-retention event atomically; Gateway then makes one guarded host release.
Deleted agents cannot restore. Preserve current rights on other restores.
Company delegates resolve exact intents with original
actor plus resolver attribution; personal intents remain actor-bound. Fresh adoption
requires current ownership; retry uses the committed immutable snapshot. Rename
must reject pending lifecycle intents. Explicit company targets copy approved
business data, then use existing resource/memory transfer; no private inheritance.

Manage/Home ADMIN may abandon only after captured and current attachments are fenced.
Keep original attribution and suspended status. Grant actions map Inspect to17,
Inspect+Manage to27, otherwise0; write Home access before instance membership.
Abandon resets from current membership, removing retired recipients. Fence caller
generation before compensation, then recheck membership/file grants under Home→instance
locks. Failure stays pending. Borrow human reads; never nest pool checkouts.
`only_if_idle` checks actual registry run IDs first in withdrawal, never stored
identity or broad reservations. Generic Space grants cannot bypass live ownership.
Existing-chat legacy actions need a positive unbound lookup.

The authority middleware adds the current AI employee display name only as
sanitized transient human-role data; never interpolate editable identity into
system text or persist this projection in checkpoints. Scope disclosures do not
include principal IDs, grants, credentials or host paths.

`agents/runtime_scope.py` builds an immutable per-assembly description after tool
filtering. Instance file facts require the exact host `AgentExecution` and its
matching prepared `InstanceSandboxProvider`. Inspection/unprepared assemblies
say unavailable, never advertise requester files. Ordinary conversations retain
their own layout. Lead and child prompts share runtime layout, actual actions and
memory scope; never persist this projection or accept it from a client. The
bounded operational-facts disclosure exception excludes prompts, SOUL, credentials,
authority objects, internal host details and membership lists.

`AgentWork` owns obligations and human commands; `WorkExecution` owns host attempts. Current Inspect
filters before pagination; delegation/factual input adds Use, management adds
Manage, and HTTP read/write ceilings remain independent. Never add status,
outcome, attempt or activation writes to the human command DTO. `work_policy`
is human-managed and becomes effective only through existing definition adoption;
model self-update cannot change it or synthesize defaults into old snapshots.

`stop` requires Inspect and Manage, or Use by the named attempt's requester,
plus `runs:cancel`, including receipt replay. Only starting/running attempts
accept new stops. Both HTTP and human-input plugin callers pass the validated
attempt id to cancellation dispatch; never cancel a newer attempt on replay.
Stopping preserves the assignment. Confirmed nonsuccess becomes cancelled;
uncertain cleanup stays uncertain and success may win.

Runtime network rules live in `runtime.py`; sandbox middleware calls its three
helpers. Work/unattended/subagent calls deny and mark their own tool result;
only interactive human managers receive an agent card, with temporary access.
The instance provider delegates network mode, TTL and decisions to its baseline.
Work's server-owned autonomous mode follows caller-key scrubbing, and its prompt
adds the question rule only with `report_work`, outside delegated scopes.

Mutations reserve the SQLite writer or lock sorted source resources → instance
→ Work. Capture persisted decision sources before locking and compare them again
under the parent/Work locks. Idempotency binds instance/actor-kind/actor/Work/exact
request and rechecks current authority before returning a redacted receipt.
Sources are bounded Space locators, admitted against the current Inspect audience;
every current/history/retry projection rechecks source READ. No automatic source
content copying or file-version claim. Plain-text human input is an explicit
shared disclosure, never a canonical decision by implication.

Assignment changes supersede blockers; typed blocker/outcome/review payloads bind
exact assignment revisions. Outcome acceptance requires a matching reconciled
attempt and exact evidence-set revision, with explicit statement-only review.
Current contents remain unchecked. Changes requested/Reopen clear current bindings
and retain event history. Historical projections do not graft on a newer attempt.
Policy removal preserves authorized history and management. The additive0038
migration registers Work/attempt/event tables. Explicit activation reserves attempts
before existing run admission. See `docs/AGENT_WORK.md`.

`HumanInput` qualifies only Work as its source. One live request per Work matches
its single blocker/outcome. Manual creation atomically adds an attributed blocker;
responses append against request/assignment revisions, while decisions fence the
row and exact response set. All canonical Work commands synchronize closure inside
their transaction, including legacy endpoints. Linked blockers reject legacy input.
Decision locks include original and response-attached sources before instance/Work.
Read state is separate. Current lifecycle/grants gate responses and inbox counts;
recipient eligibility never grants access or silently follows supervisor changes.
History remains inspectable. No request path invokes run or filesystem effects.
See `docs/HUMAN_INPUT_REQUESTS.md`; additive0039 owns the four new tables.

Work attempts bind the exact host run before native dispatch. Current Use+Inspect,
mandate, assignment, source and destination audience checks apply before every
model/tool call; row-only progress does not retire the attempt. Mark protected
context independently of memory. `read_work_context` pages replies; `report_work`
is host-capability bound, with nonhuman attribution and exact idempotent intent.
Factual assessment/revision acknowledges every response. Derived Work is bounded
by adopted policy and awaits separate activation. No human authority is model-writable.

Candidate promotion follows durable terminal success and confirmed owned command
cleanup, SDK closure and no unresolved transport/session evidence. Refresh locked
Work rows after pre-lock snapshots. SDK close or a lease alone does not prove native
containment. Late/reused terminal observations remain uncertain. Qualified recovery
requires current Home ADMIN, exact completed lifecycle scope and captured IDs fenced;
it preserves effects/candidates without success. Additive0040 owns candidate/settled
columns and request/event attribution; never rewrite0038/0039.

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

`runtime.prepare_environment` consumes exact admitted native attachments and the
existing AIO SDK. Address immutable Docker IDs, not container IPs. Drain preparation
and closure across cancellation; unknown outcomes retain native intents. SDK closure,
leases and ordinary completion never fence or replace physical writers. Children
inherit the full Home mount; tool filtering is not a narrower filesystem boundary.
The exact-container transport establishes fixed compatibility aliases using
isolated Python and protected directory descriptors. This bounded root setup
never opens Home targets; the nonroot SDK creates Home children and confirms cwd.
Reject layout conflicts/failures before publishing a provider. Instance bash
does not execute a command when changing to Home fails.
Output presentation reads only metadata through confined output descriptors on
the admitted Home backing, with current resident READ and captured generation.
It requires the exact prepared execution; container paths are never host paths.
References remain mutable and confer no human retrieval permission.

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
No scheduling, background tasks or separate storage/file-manager API is introduced.

Lifecycle uses the host-only callback inside qualified attachment retirement:
resource→attachment→instance, capture exact intent/IDs and withdraw generation
before effects. Pending never proves containment. Empty/completed retries never
retire replacements; finalization proves captured IDs fenced under current ADMIN
and Manage before reconciling Home generation. Retain Home/grants and preserve
current rights on restore. Company delegates resolve exact intents with original
actor plus resolver attribution; personal intents remain actor-bound. Fresh adoption
requires current ownership; retry uses the committed immutable snapshot. Rename
must reject pending lifecycle intents. Explicit company targets copy approved
business data, then use existing resource/memory transfer; no private inheritance.

Managers with current Home ADMIN can abandon an obsolete pending lifecycle operation only after its exact captured environments are confirmed fenced. The intent and original actor remain recorded; grants are unchanged and the agent stays suspended until explicit restore. Existing-chat legacy uploads, browser and sidecar actions require a positive unbound lookup.

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

`AgentWork` owns records-only obligations and human commands. Current Inspect
filters before pagination; delegation/factual input adds Use, management adds
Manage, and HTTP read/write ceilings remain independent. Never add status,
outcome, attempt or activation writes to the human command DTO. `work_policy`
is human-managed and becomes effective only through existing definition adoption;
model self-update cannot change it or synthesize defaults into old snapshots.

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
migration registers Work/attempt/event tables; attempts remain empty until a
qualified host execution adapter ships. See `docs/AGENT_WORK.md`.

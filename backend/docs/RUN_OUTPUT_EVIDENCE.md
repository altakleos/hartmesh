# Durable run output evidence

Run creation accepts an explicit `output_capture: {"version": 1}` capability.
It requires the actual SQL run repository and durable database event store;
Memory, JSONL, legacy custom stores and a database factory's Memory fallback do
not qualify. Omission preserves ordinary behavior. Managed Telegram publication
and model activation remain separate work.
New captured admissions persist their server-owned disconnect mode in
`output_capture_on_disconnect`; caller kwargs cannot override it. Cold peer
hydration restores that mode, and captured idempotent reuse requires the same
intent and disconnect policy before other request compatibility checks.

## Generic contract

`runtime/runs/output.py` defines strict immutable capture intents and version-1
receipts. Capture version 2 adds a separately versioned
[terminal inventory](RUN_OUTPUT_INVENTORY.md), atomically sealed with that same
receipt; receipt version 1 remains unchanged. A receipt contains the exact raw
owner, thread and run. Text evidence
also names a genuine assistant message, its positive durable event sequence and
the SHA-256 of its exact UTF-8 text. Never trim, normalize or stringify the text.
Message identities are opaque; never manufacture an assistant or human ID.
The genuine input-message ID is required but nullable for autonomous platform
runs. The Harness knows nothing about Telegram publications or SMB business rules.

An unavailable receipt contains one closed reason and no synthetic output
identity. Text and selector-refusal receipts require success; failed receipts
require error or timeout; cancelled receipts require interrupted or the rollback
error tail. The stored row supplies status, rather than a duplicated receipt field.
Malformed present intent or evidence fails closed; only an absent key is absence.

## Durable authority

Only the actual SQL `RunRepository` advertises `supports_output_receipts`.
Memory and legacy custom stores preserve their ordinary behavior and refuse
the new proof APIs. `RunManager.finalize_output` requires an opted-in locally
owned run and uses terminal recovery's bounded conditional write. New admissions
discard caller-supplied `output_evidence`; idempotent reuse preserves stored proof.

The SQL writer locks the change clock before the run row. It merges only the
reserved receipt slot from authoritative stored kwargs and conditions the write
on owner, change sequence, source status, cancellation and current lease. A text
seal cannot overwrite an already observed cancellation, even with a matching CAS
action. Status and receipt commit together. Ambiguous writes are read back before
any recovery attempt; only exact status plus exact receipt confirms output.
A terminal row cannot acquire a new receipt.
Only an already identical cancelled receipt can survive interrupted-to-error
cleanup, and the stored cancellation action must be rollback. Proof operations
retain started SQL reads, cursor cleanup and transactions through cancellation;
tests verify drainage with an independent writer commit.

`get_output_snapshot` always reads the actual store, validates observed scope and
returns detached, deeply frozen data. A cached success is not durable authority.
An observed wrong owner is a scope error; absence requires a completed supported
lookup. The `local_finalizing` observation is never a durable completion seal.
An ACTIVE row belonging to this exact locally fenced worker yields fixed
uncertainty, even after its stream-notification flag clears. A peer-owned row
or a valid terminal seal follows its own durable authority. Local fence knowledge
survives ordinary cache eviction while execution or cleanup remains unresolved.
Only confirmed, fully drained terminal associations can be released. Process
restart loses that local fence knowledge. Eligible expired ownership
can instead be closed by the SQL recovery path described below.

## Prestart and abandoned ownership

Locally admitted runs have a host-only startup decision, serialized with
`try_start` by its start lock. A conclusive prestart cancellation prevents graph
execution and seals unavailable/cancelled. A known startup or task-attachment
failure seals error with unavailable/failed. The frozen outcome remains a failure
when the attached worker later observes its abort flag; it cannot be reclassified
as human cancellation or acquire a different receipt. Started closure operations
remain owned through repeated cancellation and committed-result cleanup.
Each admission retains its own creation/reuse evidence; a shared retry flag cannot
decide compensation. After a lost SQL result, recovery reads the original host
UUID and verifies current owned scope before retaining that same constructed
record and closing its known startup failure. Missing or uncertain reads never
authorize a new row or an invented outcome; the original exception is preserved.

A missing or completed task after startup does not prove cancellation cleanup.
Live workers retain their active durable row until their actual cleanup seals
the outcome. Unknown ownership or cleanup stays uncertain.

SQL recovery locks the change clock and actual row, then applies the existing
lease/grace predicate. Eligible expired or null-lease captured runs become error
with unavailable/failed in the same transaction. Stored cancellation requests,
owner identity, disconnect policy and other kwargs are preserved. A request to
cancel is not proof that the lost owner stopped or rolled back its execution.
Lease recovery fences run ownership; it does not prove native/tool containment.
Malformed intent or evidence refuses recovery rather than becoming an ordinary
unsealed row. Existing terminal-unsealed rows are never retrofitted.

Interrupt/rollback replacement refuses a live-lease captured predecessor and any
local captured predecessor still active or finalizing. Exact idempotent reuse
is resolved first. Eligible expired predecessors receive the failed seal in the
same transaction as successor admission; a failed insert rolls both back. The
confirmed finalizer's narrowly authorized goal-checkpoint handoff is preserved.

## Captured I/O lifetime

The manager registers a host-only lifetime before the first captured admission
query. It binds the original run object and is never serialized into kwargs,
metadata, checkpoints or API projections. Metadata setup, direct worker entry
and later callbacks reuse that original lifetime; a reconstructed or foreign
captured record cannot acquire a replacement owner.

At shutdown the Gateway seals admission and uses one cooperative deadline for
manager retirement and already admitted resident preparation. A later ordinary
worker attachment receives another task snapshot under that same deadline.
Retirement refuses new finite operations, pages, batches, retries and stream
publication. Started delegates keep their complete native, codec, session and
transaction lifetime. The host joins them and registered resource closes before
provider teardown, including through repeated cancellation. A blocked native
operation can exceed the cooperative grace. Arbitrary graph continuations,
external tools and container containment remain governed by their own contracts.

Factories expose explicit public saver/Store facades over the existing objects,
optional cache and pools. Supported builtin async methods retain their complete
delegate call; actual native list iterators remain registered between yields and
close on retirement. Captured synchronous calls on async-backed providers refuse
before invoking a delegate, even from a worker thread. Atomic Memory synchronous
operations remain supported. Ordinary public calls preserve delegation; private
SDK layout and concrete-type inspection are not part of the facade contract.
The dormant private-layout checkpoint-retention utility has no qualified capture
adapter. Embedded captured workers require supported facades and the host cleanup
callback, and refuse unsupported profiles before model execution.

Each journal batch, progress report, source page and terminal read/CAS has fresh
admission. Normal open quiescence can flush its buffered tail; retirement cannot.
Idle native iterators and exact temporary snapshot caches have explicit cleanup
owners. Checkpoint reservation release is a preauthorized, single-attempt close
conditioned on the original raw owner, thread, worker, operation kind and change
sequence. A heartbeat-only lease renewal does not change that reservation identity.
Failed cleanup stays retained and reports a fixed error without blind replay.

The run's producer view never closes the shared bridge or request subscribers.
The application owns stream retention separately: the existing serving delay is
outside the finite delete operation. Shutdown cancels unstarted delays, joins
started deletes once and rejects later scheduling before creating tasks. Redis
closure is irreversible even for a wrapper borrowing its client. Retiring I/O
does not authorize a new failure/cancellation seal, cleanup proof, goal deletion
or completion hook; confirmed evidence stays fixed and uncertainty stays active.

## Worker source capture

The worker registers a fresh host graph UUID per invocation and overrides the
caller's tracing ID. The initial qualified shape is a completed model callback
directly under that graph's actual `model` node. Nested graphs, middleware and
auxiliary model calls cannot gain authority by copying lead tags or answer IDs.
The first completed callback is canonical; usage replays cannot replace it.
The actual completed root suffix must match the same genuine AI identity, exact
text and shared eligibility rules. History IDs, manual answers, trailing tools,
hidden/fallback/refusal output and multiple graph epochs are never promoted into
replacement text. Completed `AIMessageChunk` callbacks are supported when the
independently completed root matches; token frames alone prove nothing.

The exact host-normalized human input object receives its real SDK reducer ID.
Capture matches that identity and the restored original consumed input; it never
invents an ID or selects a convenient historical human. Autonomous AI-employee
runs can have no human input. Exact visible text may include separate auxiliary
reasoning, as metadata or as provider blocks, which is never returned, hashed or
used as answer fallback. Provider bookkeeping keys on a text block do not change
it, and `content_block_role` is the one reading of a block. Any other nontext
block still makes the message unsupported as a whole.

The journal pairs immutable callback facts with actual committed batch results
to obtain the durable event sequence. It stops producers and drains admitted
event/progress writes before freezing completion/delivery summaries. An unknown
event write is never blindly appended again. The worker audits the exact stored
source before the existing atomic status-and-receipt CAS. A genuine execution
failure or completed cancellation can seal a matching unavailable receipt.
Cancellation winning the text CAS completes interrupt/rollback cleanup while the
row remains active, then seals that cancelled result atomically.

Flush, decode, source or seal uncertainty fences locally, retains the unsealed
durable state, and skips status-only finalization, goal deletion and completion
hooks. Already admitted SQL and cleanup remain owned through repeated host
cancellation. No model retry or second creating request repairs proof failure.
After a confirmed compatible seal, only the exact registered finalizing run on
its own executing task may reserve its goal checkpoint write. The original
cleanup barrier stays held; other local finalizers and peer SQL reservations
continue to block that write. Completion hooks cannot admit a replacement run
before cleanup finishes.

## Private channel read protocol

The host-only `ChannelExecutionFacade.capture_output` option defaults to literal
`False`. Explicit `True` adds the typed intent to a detached creating request;
provider metadata cannot switch it. Unsupported responses never trigger a second
creating POST. `GET /api/threads/{thread_id}/runs/{run_id}/output` and the
retained `get_run_output` client require the existing version-1 strict read
request/acknowledgement and exact original scope. Generic capture admission is
independent of channel publication; the private route additionally requires the
separate server-owned channel admission receipt and human publication marker.

Its closed projection keeps channel admission and generic output evidence
separate. Complete private-channel text requires a genuine human input identity,
matching terminal status and the exact source digest. A sealed unavailable result
may have no input identity when execution failed before input capture; it exposes
no model source and retains the server-owned publication admission scope. Source
loss never rewrites the historical receipt. Unknown versions, malformed proof,
scope changes and unacknowledged absence remain uncertainty. Supported active
unsealed runs return pending. An authorized supported run miss returns404 with
the acknowledgement; unsupported stores, malformed proof, unsealed terminal
rows and backend uncertainty return fixed503 without it. Foreign scope and
authorization failures carry no supported-absence acknowledgement.

Post-response authorization remains inside the existing retained HTTP owner.
Shutdown drains started codec and SQL cleanup before releasing ownership. These
read boundaries enable reading independently sealed output. Managed-bot model
execution and durable publication recovery remain separate work.

No schema or configuration version changes are required: the receipt uses the
existing run kwargs JSON. Schema head remains `0044_telegram_assistant_ids`;
configuration remains version 55.

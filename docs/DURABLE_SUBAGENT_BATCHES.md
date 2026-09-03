# Evidence-bound durable subagent batches

Durable subagent batches let an accepted lead invocation submit many independent
items through the explicit `batch_task` tool. They are disabled by default and
require SQL persistence. Submission, observation, control, and result export are
scoped to the server-owned tenant, owning user, and parent thread.

## Admission and evidence

Only an active `batch_task` attempt inside an `InvocationRuntime`-accepted run
can submit a new batch. Tenant, parent, or tool-attempt evidence is never
accepted from tool arguments. One acceptance transaction writes the immutable
batch commitment and every item request digest before any item can be claimed.

| Record | Purpose | Payload policy |
| --- | --- | --- |
| `AcceptedBatchV1` | Commits the tenant, parent invocation and assembly, active tool receipt, subagent catalog and definition, skills, extensions, model constraints, tools, item root, and limits | Safe, bounded evidence; no prompts, results, credentials, provider handles, or user identifiers |
| `ParentBoundBatchExecutionV1` | Protected material used to reconstruct the selected child | Owner-protected; no item prompts, results, credentials, or live Python objects |
| `AcceptedBatchItemV1` | Stable item ID, ordinal, key, and operational-input digest | Safe evidence; no prompt |
| `BatchAttemptEvidenceV1` | Lease epoch, attempt state, safe terminal code, and optional result digest | Safe, bounded evidence; no result or exception text |

Idempotency is exact. A retry returns the existing batch only when tenant,
parent tool receipt, submission key, and the full acceptance digest match. Any
different accepted material returns `batch_admission_conflict`.

`InvocationRuntime` remains the durable admission authority. Batch code consumes
the accepted parent material and the catalog produced by
`runtime/subagent_snapshot.py`; it neither discovers live subagents nor creates a
second agent revision. The same seam captures each allowed tool's name plus a
digest of its schema, description, source, and MCP transport identity.

## Execution, recovery, and retries

The SQL repository owns scheduling state. A claim creates an append-only attempt
row and a monotonically increasing lease epoch. Preflight validates the persisted
acceptance and protected execution record; `started` is recorded only after a
real process execution slot has been acquired and before model work begins.
Renewal and terminal publication compare batch, item, attempt ID, owner, epoch,
and database time. A stale worker can finish locally, but cannot publish a
result, evidence, or terminal state.

Delivery is **at least once at the attempt boundary**, with one accepted terminal
publication per attempt epoch. Queue-capacity rejection occurs before model work,
is recorded as `queue_rejected`, and does not consume the execution-attempt
budget. It does consume one of the separately accepted attempt-record slots;
exhausting that evidence ceiling stops the item as `evidence_limit_exhausted`.
Execution failure and a lease that expires after start do consume the execution
budget. Manual retry preserves that cumulative count and returns `409` once the
accepted `max_attempts` limit is exhausted; it cannot create a fresh budget.
After a process crash, an expired item may execute again with the same stable
item key.

External side effects can therefore repeat. HartMesh fences accepted database
state, not the external world. Batch prompts tell workers to use the stable item
key as an idempotency identity, but tools and downstream systems must implement
their own idempotency where duplicate effects matter.

Recovery uses only the accepted material. The service retains the accepted skill
projection and material leases through terminal batch cleanup. Empty-skill
execution material can be reconstructed from the protected record after a
restart. A nonempty skill snapshot or another process-local adapter that is no
longer available fails closed with `execution_material_unavailable`; recovery
never refreshes it from current live configuration. Tool construction resolves
only the accepted names, then requires their persisted schema, description,
source, and transport contract digests to match. Model, tool, catalog, skill,
extension, authorization configuration, or constraint drift fails closed as
`provider_not_qualified` or `execution_material_unavailable`.

The regression matrix replaces the live catalog after admission, removes or
changes accepted skill material, and changes each extension generation,
artifact, and configuration anchor. Recovery either uses the protected accepted
record and retained snapshot or fails closed; it never consults those changed
live sources as a substitute.

## Cancellation policy

The initial policy is deliberately non-cascading. Accepted batches persist
`parent_cancellable=false`, and admission rejects `true` until a durable parent
cancellation event can enforce that promise. Cancelling or rolling back the
parent run does not cancel its accepted batch.

Use `cancel_batch` or
`POST /api/threads/{thread_id}/subagent-batches/{batch_id}/cancel`. Batch
cancellation increments a durable cancel epoch, terminalizes active attempts,
clears their leases, and fences late completion. Cancellation and successful
completion race under the same database lock order; whichever transaction wins
becomes the only accepted terminal outcome.

## Observation and results

Owner-scoped routes expose:

- batch and item projections under
  `/api/threads/{thread_id}/subagent-batches`;
- bounded payload-free attempt history at `/{batch_id}/attempts`;
- additive `batch.accepted`, `batch.item_attempt`, and `batch.terminal`
  observations at `/{batch_id}/observations`;
- protected results at `/{batch_id}/results.jsonl`.

Portable invocation consumers can opt into the same durable projections with
`include_subagent_batches=true` on
`GET /api/runtime/v1/invocations/{parent_run_id}`. That independently paged
view is authorized only after the parent run is visible and carries the
parent-receipt link plus bounded lifecycle observations, never results.

Lifecycle evidence contains stable IDs, digests, counts, timestamps, and safe
reason codes only. Raw prompts, model output, tool arguments, exception text,
credentials, worker names, and provider handles do not enter evidence or logs.
Results remain in the existing owner-authorized result channel. Acceptance is
hard-limited to 64 KiB, individual attempt evidence to 16 KiB, and API evidence
pages to 100 records. Each item is also limited to at most 128 attempt records
(64 by default), bounding aggregate attempt evidence even when queue rejection
does not consume an execution attempt. `subagent_batches.max_evidence_bytes` can
impose a lower acceptance limit.

## Configuration and qualification

Configuration is startup-only:

```yaml
subagent_batches:
  enabled: false
  poll_interval_seconds: 1
  lease_seconds: 120
  max_items_per_batch: 5000
  default_max_live_items: 100
  max_live_items_per_batch: 1000
  default_max_running_items: 3
  max_running_items_per_batch: 64
  max_attempts: 3
  max_attempt_records_per_item: 64
  max_result_chars: 100000
  result_preview_max_chars: 2000
  max_total_runtime_seconds: 86400
  max_evidence_bytes: 16384
```

This checkout rejects enabled batches in `durable_one_replica`: no passing
artifact exists for the required real-PostgreSQL process-kill and
Gateway/worker restart suite. The checked-in opt-in PostgreSQL repository and
process-kill contracts drive the real batch service after claim, after start,
and at a production-inert fault barrier immediately before `finalize_item`.
Gateway lifespan coverage also proves a restart constructs a new worker against
the shared repository. These remain an unpassed gate when their PostgreSQL
infrastructure is absent. SQLite and local PostgreSQL may be used under
`local_development` for evaluation only. Tenant-bound rows still use
database-owned time on SQLite, but SQLite's concurrency model is weaker than
PostgreSQL's.

`durable_two_gateway_v1` rejects startup when batches are enabled. The exact-two
batch failover gate remains unpassed until a real PostgreSQL and Kubernetes
artifact proves claim takeover, stale-worker fencing, cancellation races, and
continuous observation. Missing infrastructure is an unpassed gate, never a
skip that establishes qualification.

## Legacy rows and rollback

Migration `0032_subagent_batch_evidence` leaves pre-existing rows at schema
writer version 1 and reports them as `legacy_unbound`. It does not invent parent
evidence. Tenant-bound workers and APIs do not claim or expose those rows.

Before cleanup, quiesce the Gateway, back up the database, and inventory legacy
rows:

```sql
SELECT id, user_id, thread_id, status, created_at
FROM subagent_batches
WHERE schema_writer_version = 1
ORDER BY created_at, id;
```

Retain them in a protected offline export if their historical operational data
is required. After explicit operator review, deleting a legacy batch also
deletes its item rows through the foreign-key cascade. There is no automatic
upgrade or rebind path because the missing evidence cannot be reconstructed.

Downgrade is allowed only while no writer-v2 batch or attempt row exists. Once
the new contract has been used, rollback fails closed with
`subagent_batch_evidence_downgrade_blocked`; do not remove evidence-bearing
columns behind an older binary.

## Stable handoff surface

The stable public references for follow-on authorization work are the batch ID
and acceptance digest, plus the attempt ID, lease epoch, terminal code, and
attempt evidence digest. The trusted admission input is
`ParentBoundBatchRequest`, populated from the server-only accepted-parent
runtime context and active durable tool receipt. A future credential-evidence
projection can bind the actor controlling a batch without changing the core
`AcceptedBatchV1` identity.

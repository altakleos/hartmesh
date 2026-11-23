# Native batch dependencies and follow-up

Optional fields extend the existing SQL-backed batch service. Omitting them keeps
independent batch execution. Items remain read-only or idempotent under the existing
retry/recovery policy.

## Dependencies

Use `depends_on` to name prerequisite item keys in the same batch. Unknown keys,
self-references and cycles are rejected before saving. Dependent items consume no
worker slot until every prerequisite succeeds without a guardrail stop reason.
Retryable attempts keep dependents waiting; exhausted failures, cancellation or
terminal capped results fail unstarted descendants. Independent items continue.
Retrying a failed prerequisite by hand does not requeue the descendants it already
failed; retry each blocked item as well.

Item prompts must name their input/output paths. Ordering does not certify factual
accuracy or make advisory acceptance mandatory. Use existing result readers; full
reports are not copied into every dependent's context.

## Requested follow-up

`continuation` describes remaining work the user already requested. After terminal
batch results are saved, the lead performs that work in the original conversation
through ordinary Run admission. Preserve the originating model selection, reasoning,
limits and admitted retrieval scope. Busy conversations wait. While a question is
unanswered the claim is kept until its lease expires, so the conversation is not
reread on every poll.

A stable idempotency key reconciles lost admission replies without another Run.
Pending claims rotate so busy conversations do not starve others. Manual item retries
hold an unadmitted follow-up but do not re-arm one already dispatched. Without a Goal,
a follow-up cannot request another automatic follow-up.

An active Goal wakes only from its own `external_wait`, consuming one existing
continuation allowance, once even after recovery. Other blockers, no-progress limits
and hard stops remain effective. Goal replacement, clearing and thread deletion
suppress pending work; a failed, cancelled or hard-stopped source Run cannot restart.

## Authority, cancellation and recovery

Each Gateway batch item rechecks its owner before tool discovery and again after
acquiring native execution capacity. This applies to the first attempt and to
recovery after a restart. Missing, disabled or reset accounts cannot start work.
A changed role rejects the old item; submit a new batch with current permissions.
Custom roles have no general ordering, so this rule prevents both stale privileges
after demotion and additional privileges after promotion. OAuth identity comes from
the current account; serialized authorization attributes are not restored.

Explicit local auth-disabled mode and unmapped trusted internal owners retain their
existing identity semantics. A deleted local account cannot become an unmapped
internal owner. These checks govern admission, not cancellation of already admitted
work. Standalone harness hosts with mutable accounts can supply the same
`resolve_principal` callback without importing the Gateway.

Follow-up needs a session-authorized Gateway AgentRuns capability or local auth-disabled
mode. Internal/PAT callers and unsupported embedded runtimes retain ordinary batches.
An interactive AI assistant or employee conversation can register one large job when
batches and its helper tools are enabled. Its items run inside the newly admitted
follow-up, with the requester's current permission to use the agent and that agent's
workspace. Work attempts, output capture, packages, channels and schedules cannot
register a large job. A helper cannot recursively submit one. Saved role/OAuth identity must still match at
restoration and each host call; changed identity, account refusal or revocation
suppresses the followup. The original owner check also applies before dispatch.
Customer-management policy stays denied through extension-started descendants.
Persist only the original permission ceiling, principal identity, account token version and auth source;
no cookie, API key or Python capability. Restore checks current account/permissions,
conversation ownership and incarnation. Private continuation data is not returned.

Batch cancellation revokes the existing SQL execution fence and stops an admitted
follow-up, including after a lost launch reply. HTTP and tool cancellation of a batch
with a follow-up also require `runs:cancel`. Trusted internal cleanup retains its existing
path. Claim ownership/expiry lives only in the existing execution-lease table.
Completed item results remain available. The Web panel discovers follow-up Runs
through existing observers; refresh never launches work. Chat Stop remains available.

## Large jobs in agent conversations

The batch owner keeps the item list, results and export. The panel reads the
server's `follow_up` state and `controls`. Recognized scoped records retain
`read_only` for earlier clients. Only the advertised Cancel is available to
the owner with current thread access, Use and the cancellation permission.
An agent manager can stop the running turn through normal Run cancellation.
The owner can see whether the requester or a manager stopped the job. The role
comes from the first accepted cancellation. Historical and operator stops can
have an unspecified role.
A startup cancellation can draw a dismissible notice above the owner's composer.
The notice belongs to the viewer's browser and is outside the conversation history.

The conversation stays busy from submission until the job ends. Jobs have up to
50 checks, 15 minutes of work and one hour to start. A deadline leaves completed
results available for a partial answer and records which checks missed the limit.
After loss, one replacement can finish outstanding checks inside the recovery window.
Once all checks have ended, reconcile a durably completed final turn or stop
as interrupted. A new request from the person can use the retained results.
The final turn can use the agent's connections without the person present.
An interrupted external call has an unknown effect and is never replayed by
restarting that final turn. See the Compose guide for the startup switch and
the pre-disable listing and cancellation command.

## Upgrade and verification

Migration `0052_batch_continuation` adds dependency edges and nullable follow-up data,
status and Run ID. Existing rows need no backfill. Upgrade all batch workers before
using dependencies. Do not downgrade with opted-in work outstanding; downgrade drops
new metadata while retaining items and reports.

Focused regressions cover dependency/retry ordering, SQL claims and recovery,
permission revocation, admission races, Goal limits and page reconnection. PostgreSQL
coverage uses `TEST_POSTGRES_URI`; browser backend timing is simulated.

# Work records

Work is a durable commitment owned by an AI employee instance. An authorized
human delegates an objective and assessable success criteria; the record and
its history persist independently of conversations. Domain procedures belong in
adopted instructions, skills and trusted extensions, not Work schemas.

Saving an assignment, providing input, recording a decision or changing a due
target does not execute the AI employee. **Work on this** and **Resume** explicitly
start an attempt through the existing conversation runtime. The
[Attention inbox](HUMAN_INPUT_REQUESTS.md) supports human and AI employee requests,
attributed human responses and separate factual assessment during execution.
Ordinary conversations remain available independently of Work.

## Enable and use

Enable **Work → Enable tracked Work** in a custom definition’s settings. Review
its human-review requirement, then use the instance’s existing **Adopt
definition** action. Existing adoption and containment checks still apply. A
changed definition does not silently update an existing instance.

Open **Agent instances**, select the AI employee and use **Work**. Delegation
uses the adopted priority and review defaults. Managers may change priority,
review and due targets; an adopted required-review policy cannot be waived.
Dates express expectations and do not schedule execution. Missing/disabled
policy prevents new delegation but preserves authorized history and management.

Work and notes are shared with the instance’s Inspect audience. Use plus Inspect
is required to delegate or supply factual input. Manage plus Inspect controls
assignment changes, cancellation/reopening, binding decisions and review.
Provider `agents:read` is also required, and mutations require `agents:write`.
PATs and client-selected actor identities are unsupported. Work visibility does
not require Home READ; referenced files have their own current access checks.

Optional `work_policy` fields are `enabled` (default false), `default_priority`
(`normal`), `review_required` (true), `responsibilities` (up to 32 unique key/label
pairs), `allow_derived` (false) and `max_derived_per_activation` (1–20, default3).
During an admitted attempt, the AI employee may suggest follow-up Work or register
bounded derived Work when allowed. Derived Work uses the adopted defaults and
requires its own explicit human activation. Human definition APIs accept these fields; model self-update
cannot alter them. Old adopted snapshots retain their exact bytes and hashes.

## States and accountability

Open means a commitment remains. Blocked means input or a decision is needed.
Submitted means **Ready for review**. Completed means either a human accepted
the exact outcome or, where review was not required, **AI employee reported
complete**. Cancelled means a manager withdrew the commitment. A run ending does
not prove an outcome is complete.

New records start Open. A manager can create a linked human-input request, atomically blocking open
Work. An admitted AI employee can also request information or a decision. Human
commands cannot invent a completion report; only the exact current attempt may
report a completion candidate. Submitted assignments
require **Request changes** before editing; Completed/Cancelled assignments
require **Reopen Work**. Both clear current outcome/review bindings and preserve
history. Assignment changes retire the old blocker and advance the assignment
revision. Progress/input changes advance the row revision without silently
changing the commitment.

Factual input is attributed history; it does not approve a decision or resolve a
blocker. A binding manager decision targets the exact question/revision and
requires current access to its source basis. Neither action starts execution or
waives review. A changed adopted mandate requires deliberate manager revalidation
of open Work before any future execution.

Acceptance binds the current assignment, outcome and evidence-set revision to
the authenticated human. This stage supports only explicit acceptance of the
**recorded outcome statement**, acknowledging unchecked or unavailable file
contents. Client-supplied digests are not accepted as proof of file review.
Historical acceptance is distinct from the current mutable file; **Current
contents not checked** is always shown until a qualified observation exists.

## Sources, limits and API

References are bounded `{kind: "space_file", space_id, path}` locators. New
references require current READ for the actor and the Work Inspect audience.
Paths are canonical relative paths, at most 1024 characters; no private-chat
adapter, inline source copy, existence certification or content retrieval is
implied. Current access is rechecked on records, history and exact retries;
unavailable references become opaque placeholders. Spaces owns viewing,
exporting and all filesystem guarantees.

Endpoints below are under `/api/agent-instances/{instance_id}/work`:

| Method/path | Operation |
| --- | --- |
| `GET /` | Current-Inspect-filtered records, `limit`1–100 and bounded `offset` |
| `POST /` | Delegate objective/criteria with UUID-hex `operation_id` |
| `GET /{work_id}` | Current record, attempt and explicit-activation availability |
| `GET /{work_id}/history` | Newest-first attributed events, same pagination |
| `POST /{work_id}/commands` | Typed human command with expected row/assignment revisions |
| `POST /{work_id}/activate` | Explicit activation with exact operation, row/assignment revisions and bound conversation ID |

Command actions: `edit`, `cancel`, `reopen`, `changes_requested`,
`reconcile_mandate`, `reconcile_attempt`, `input`, `decide`, `accept`. Every command rejects fields
belonging to other actions. Reopen/changes/reconciliation require a note.
Input/decision require exact blocker identity and revision; acceptance requires
exact outcome/evidence revision, `basis: "outcome_statement"` and
`acknowledge_unchecked_sources: true`.

Objective, criteria, progress, question, outcome statement and notes are bounded
to4096 characters; each source array has at most16 references. Due timestamps
must include a timezone. Rows and assignments use positive31-bit revisions.
Operation identities bind the exact actor, Work and request. Retry the same body
and identity after an uncertain response. Changed requests conflict; revoked
access is not restored by a receipt. The UI retains uncertain requests and
requires explicit reference replacement when unavailable references would be
removed. Concurrency conflicts require reviewing the current record.

## Persistence and compatibility

Migration `0038_agent_work` follows published `0037_agent_lifecycle` and adds
`agent_work`, `agent_work_attempts` and `agent_work_events`. It validates existing
schema shapes, including the partial unique index allowing at most one
unresolved attempt per Work. Explicit activation reserves an attempt before run
admission. Host reconciliation requires the exact run and confirmed owned cleanup
before declaring that attempt terminal.
Work history has no user/chat cascade; downgrades refuse to erase used tables.
No existing rows, consumer files or adopted definition hashes are rewritten.

Human-input migration `0039_human_input` adds requests, responses, receipts and
read state after0038. Canonical Work transitions synchronize linked requests in
the same transaction; their decision/review commands require exact request and
response-set acknowledgement. See [Attention](HUMAN_INPUT_REQUESTS.md).

## Explicit execution and supervision

Activation adds the current `runs:create` ceiling to Use plus Inspect and
`agents:read`/`agents:write`. Creating a new execution conversation also requires
`threads:write`. The server checks the adopted mandate, exact assignment, current
source access and conversation/Home audience before dispatch. Shared Work context
marks the conversation Inspect-protected even when memory is off.

An attempt records its actual conversation/run identity. Starting, Running,
Stop pending and Outcome uncertain all prohibit a replacement attempt. Retry the
same activation operation and body after an unknown acknowledgement. Recovering
its receipt never creates a second worker for an existing run. An orphaned run or
uncertain cleanup stays unresolved; terminal run status alone is insufficient.

Progress changes the row revision without changing the assignment. A completion
report remains a **candidate** until the matching run succeeds, owned operations
settle and current assignment/access checks pass. Only then does it become Ready
for review, or AI employee reported complete where review is not required.
Required review creates the canonical human-input request. A finished run without
a candidate leaves Work open. Earlier reports remain in attributed history.

Replies may arrive while a run is active. Information assessment names the exact
question revision and every current response; new replies reject stale assessment.
The AI employee can replace an inadequate factual question while retaining a
blocker. It cannot impersonate a human decision or accept its own outcome.

Editing or cancelling active Work also requires `runs:cancel`, records the exact
stop intent and fences stale reports. Cancellation metadata is not physical
containment. For an unresolved attempt, a manager with current Home ADMIN first
completes the existing lifecycle containment operation. **Reconcile contained
attempt** submits that exact operation and attempt, expected revisions and a note;
the server verifies the captured native environments are fenced. Recovery preserves
reported effects and marks the attempt failed, never successful. Inspect effects,
restore/revalidate the mandate as needed, then explicitly Resume. The command API
uses `attempt_id` and `containment_operation_id` for this same operation.

The Work page shows responsibilities and page-scoped current Work, needs attention
and recent outcomes, with separate attempt status and last activity. Follow-up
suggestions can prefill a delegation form but need human submission. Resume can
use a new conversation; receipt recovery always retains its original conversation.
No calendar, event or reply automatically activates Work.

Additive migration `0040_work_execution` adds nullable candidate/settlement columns
to attempts and human/nonhuman attribution to request creation and events. Existing
request/event rows default to human attribution. Used execution or AI employee
attribution blocks downgrade. Existing definition hashes and consumer file formats
remain unchanged.

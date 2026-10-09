# Work records

Work is a durable commitment owned by an AI employee instance. An authorized
human delegates an objective and assessable success criteria; the record and
its history persist independently of conversations. Domain procedures belong in
adopted instructions, skills and trusted extensions, not Work schemas.

This capability is **records only**. Saving an assignment, providing input,
recording a decision or changing a due target does not execute the AI employee.
There is no Work Run/Resume endpoint, background trigger, completion-reporting
endpoint or addressed attention inbox in this stage. Ordinary conversations
remain available independently.

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
Derived-work fields reserve policy for later qualified execution and currently
cause no activity. Human definition APIs accept these fields; model self-update
cannot alter them. Old adopted snapshots retain their exact bytes and hashes.

## States and accountability

Open means a commitment remains. Blocked means input or a decision is needed.
Submitted means **Ready for review**. Completed means either a human accepted
the exact outcome or, where review was not required, **AI employee reported
complete**. Cancelled means a manager withdrew the commitment. A run ending does
not prove an outcome is complete.

New records start Open. Human commands cannot invent a blocker or completion
report. Review/input controls operate only on canonical host records; this
stage has no producer for new AI employee submissions. Submitted assignments
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
| `GET /{work_id}` | Current record and records-only availability |
| `GET /{work_id}/history` | Newest-first attributed events, same pagination |
| `POST /{work_id}/commands` | Typed human command with expected row/assignment revisions |

Command actions: `edit`, `cancel`, `reopen`, `changes_requested`,
`reconcile_mandate`, `input`, `decide`, `accept`. Every command rejects fields
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
unresolved attempt per Work. Human APIs create no attempts. Future host execution
must reconcile exact run/operation outcomes before declaring attempts terminal.
Work history has no user/chat cascade; downgrades refuse to erase used tables.
No existing rows, consumer files or adopted definition hashes are rewritten.

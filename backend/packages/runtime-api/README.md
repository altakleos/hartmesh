# deerflow-runtime-api

`deerflow-runtime-api` is DeerFlow's host-independent durable invocation
contract. Version `0.2.0` exposes strict frozen records under
`api_version="deerflow.runtime/v1"` and depends only on the Python standard
library. It never imports the Gateway application, harness, FastAPI, SQLAlchemy,
or extension API. `DurableInvocationPort` is the complete portable Protocol:
`ensure`, invocation/context `observe`, fenced `control`, and `capabilities`.
It exposes no store, worker, graph, session, framework-response, or deployment
object.

## Surface

- `InvocationEnsureRequest` / `InvocationEnsureReceipt` for idempotent durable
  admission;
- `InvocationQuery` and `ContextInvocationsQuery` /
  `InvocationObservation` for access-filtered lifecycle pages;
- `InvocationSummaryV1` and `InvocationCorrelationReferenceV1` for bounded,
  source-aware accepted evidence joined to those pages;
- `CancelInvocationRequest` / `InvocationControlReceipt` for version-fenced
  cancellation;
- `RuntimeCapabilities` for supported operations; and
- `RuntimeFailure` for safe finite failures.

All records include their exact kind and API version. Construction and parsing
defensively snapshot the complete JSON graph: mappings become read-only
mappings and sequences become tuples recursively. A frozen record therefore
retains no caller-owned mutable JSON container. `to_dict()` recursively thaws
that snapshot into a fresh mutable JSON-compatible wire copy on every call.
Mutating a constructor input or one returned wire copy cannot change the
record, its equality, replay projection, or a later serialization.

`to_dict()`, each record's `from_dict()`, and `record_from_dict()` reject unknown
fields, kinds, versions, non-finite numbers, and unsupported shapes.
Observation snapshots and lifecycle events have fixed field sets. Status
values, lifecycle types, lifecycle/status pairs, and state versions are
validated against the complete v1 state machine; policy reasons and internal
host types are never serialized. The observation itself enforces relational
integrity: every snapshot and event belongs to its top-level thread; singular
pages contain only their top-level run; and snapshot and summary run IDs are
unique. Each summary must join to exactly one snapshot in that page and match
its run, thread, current status, and state version. A singular current snapshot
also matches the top-level current state. Historical events keep their recorded
transition state and are not required to equal the latest snapshot. Empty
summary collections and summary subsets remain valid for legacy or intentionally
unsummarized rows.

An observation may include immutable `InvocationSummaryV1` records for normal
runs represented by that bounded event page. A summary contains current
status/version, sealed `source_kind`, bounded safe Origin correlation
references, revision/generation identity, and only digest evidence from
acceptance. It never contains input, private policy reasons, credentials,
secret handles, or arbitrary Origin data. Context queries may filter by the
strict source kinds `http`, `scheduled_task`, `native_channel`, and `service`.
Historical rows that predate sealed Origin remain readable through events and
snapshots but cannot manufacture a summary.

`RuntimeCapabilities` is the same exact strict record over every Adapter. Extension
manifest/health, build provenance, persistence tier, and qualification evidence are
deployment facts and therefore never appear in this package. Gateway exposes an
operator-asserted reference separately through an authenticated administrative Interface;
exact artifact verification is an offline deployment concern, not a portable runtime
capability. Unexpected host Adapter
exceptions may return only an `indeterminate` failure with a bounded correlation ID;
raw exception text and deployment internals remain outside the portable wire contract.

The ensure request deliberately has no property bags. It accepts a service-
supplied external key, thread, optional agent hint, strict graph or resume input,
and `InvocationOptionsV1`. It cannot carry scope, principal, Origin, accepted
digests, revision, raw config/context/metadata, callbacks, delivery settings, or
credentials.

Portable requests enforce the same execution identities as the host. `thread_id` preserves
exact case and matches `[A-Za-z0-9_-]{1,64}`. `agent_hint` accepts
`[A-Za-z0-9][A-Za-z0-9-]{0,127}` and returns the existing lowercase canonical agent
identity; `lead_agent` is the reserved built-in identity. `model_name` is a case-sensitive, non-empty model-profile identity with no ASCII
controls and at most 128 UTF-8 bytes. These are policy identities rather than display
labels; none is truncated, hashed, or routed through a plugin identifier grammar.

Idempotent equality is the canonical caller intent expressed by that complete strict record,
not the host's accepted effective execution projection. Object-key order is insignificant and
array order is significant. Null `agent_hint`, model/thinking, checkpoint, and interrupt values
mean omission; `multitask_strategy="reject"` is the explicit wire default. Adding, removing, or
changing any other intent conflicts. HTTP-only delivery metadata cannot participate because it
is not part of this contract. An equal replay returns the retained invocation in any lifecycle
state and reuses its accepted effective execution without rerunning contributors,
authorization, constraints, default resolution, agent/profile routing, or model execution.
That is the start/admission path; current observe authorization still runs before a retained
invocation is revealed.

## Host adapters

The production adapter is `app.runtime.api.build_in_process_runtime_api(app,
authenticated_service_id=...)`. It implements `DurableInvocationPort`; callers
can type against that Protocol rather than application code. The application
authenticates that service ID before construction. The adapter derives a
domain-separated canonical scope from `["service",
authenticated_service_id]` and routes every operation through the existing
`InvocationRuntime`; it does not implement policy, persistence, or execution
independently. This application-hosted in-process Adapter is distinct from the
local synchronous `DeerFlowClient`, which runs a graph directly and does not
provide durable admission, lifecycle observation, cancellation, or recovery.

Embedded and HTTP service principals are owner-scoped by default. An operator may configure a
finite host-side observation grant for a specifically authenticated service, but no portable
request carries that grant. It bounds which run/thread/owner/source rows the host may search;
the current `invocation:observe` authorization decision is still mandatory. Revocation is
checked on the next poll, grant-invisible and nonexistent targets share the same public failure,
and cancellation never inherits an observation grant.

Lifecycle cursor tokens are opaque. Callers persist `next_cursor`, handle
`cursor_gap` by resuming at `minimum_available_cursor`, reject `cursor_ahead`,
and may safely repeat a page by deduplicating stable event IDs/cursors. Reads are
at least once. Page limits are 1–500. Event payloads are limited to 4 KiB,
individual summaries to 16 KiB, and the complete portable observation to
12 MiB of canonical JSON. Context summary/snapshot rows are loaded only for
distinct run IDs in the returned event page, so observation work is bounded by
the requested page rather than total thread history. Events, cursor metadata,
and joined summaries come from one database snapshot; filtered empty pages
still advance to the captured global fence.

Polling `DurableInvocationPort.observe` is the supported durable evidence path. Cursor polling
of transactional lifecycle rows is authoritative in v1. The transactional journal and
authoritative snapshot do not depend on an event sink or broker; push delivery can only be
optional at-least-once acceleration and is not a correctness requirement. A clarification
result is a successful completion of its invocation, and the caller's answer is a new
invocation on the same DeerFlow thread, reusing its checkpoints, memory, workspace, and
conversation context. V1 deliberately has no `input_required` lifecycle state.

Gateway's authenticated `/api/runtime/v1` routes consume the same Protocol and
records. The transport-neutral conformance suite runs against both the embedded
adapter and an HTTP adapter. HTTP authentication supplies the principal and
scope; request bodies cannot override them. The package remains
transport-independent and still imports no HTTP framework.

# deerflow-runtime-api

`deerflow-runtime-api` is DeerFlow's host-independent durable invocation
contract. Version `0.1.0` exposes strict frozen records under
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
host types are never serialized.

The ensure request deliberately has no property bags. It accepts a service-
supplied external key, thread, optional agent hint, strict graph or resume input,
and `InvocationOptionsV1`. It cannot carry scope, principal, Origin, accepted
digests, revision, raw config/context/metadata, callbacks, delivery settings, or
credentials.

Idempotent equality is the canonical caller intent expressed by that complete strict record,
not the host's accepted effective execution projection. Object-key order is insignificant and
array order is significant. Null `agent_hint`, model/thinking, checkpoint, and interrupt values
mean omission; `multitask_strategy="reject"` is the explicit wire default. Adding, removing, or
changing any other intent conflicts. HTTP-only delivery metadata cannot participate because it
is not part of this contract. An equal replay returns the retained invocation in any lifecycle
state and reuses its pinned effective projection without resolving defaults or executing again.

## Host adapters

The production adapter is `app.runtime.api.build_in_process_runtime_api(app,
authenticated_service_id=...)`. It implements `DurableInvocationPort`; callers
can type against that Protocol rather than application code. The application
authenticates that service ID before construction. The adapter derives a
domain-separated canonical scope from `["service",
authenticated_service_id]` and routes every operation through the existing
`InvocationRuntime`; it does not implement policy, persistence, or execution
independently.

Lifecycle cursor tokens are opaque. Callers persist `next_cursor`, handle
`cursor_gap` by resuming at `minimum_available_cursor`, reject `cursor_ahead`,
and may safely repeat a page by deduplicating stable event IDs/cursors. Reads are
at least once.

Gateway's authenticated `/api/runtime/v1` routes consume the same Protocol and
records. The transport-neutral conformance suite runs against both the embedded
adapter and an HTTP adapter. HTTP authentication supplies the principal and
scope; request bodies cannot override them. The package remains
transport-independent and still imports no HTTP framework.

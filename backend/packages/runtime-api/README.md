# deerflow-runtime-api

`deerflow-runtime-api` is DeerFlow's host-independent durable invocation
contract. Version `0.1.0` exposes strict frozen records under
`api_version="deerflow.runtime/v1"` and depends only on the Python standard
library. It never imports the Gateway application, harness, FastAPI, SQLAlchemy,
or extension API.

## Surface

- `InvocationEnsureRequest` / `InvocationEnsureReceipt` for idempotent durable
  admission;
- `InvocationQuery` and `ContextInvocationsQuery` /
  `InvocationObservation` for access-filtered lifecycle pages;
- `CancelInvocationRequest` / `InvocationControlReceipt` for version-fenced
  cancellation;
- `RuntimeCapabilities` for supported operations; and
- `RuntimeFailure` for safe finite failures.

All records include their exact kind and API version. `to_dict()`, each record's
`from_dict()`, and `record_from_dict()` reject unknown fields, kinds, versions,
non-finite numbers, and unsupported shapes. Observation snapshots and lifecycle
events have fixed field sets. Status values, lifecycle types, lifecycle/status
pairs, and state versions are validated against the complete v1 state machine;
policy reasons and internal host types are never serialized.

The ensure request deliberately has no property bags. It accepts a service-
supplied external key, thread, optional agent hint, strict graph or resume input,
and `InvocationOptionsV1`. It cannot carry scope, principal, Origin, accepted
digests, revision, raw config/context/metadata, callbacks, delivery settings, or
credentials.

## Host adapters

The production adapter is `app.runtime.api.build_in_process_runtime_api(app,
authenticated_service_id=...)`. The application authenticates that service ID
before construction. The adapter derives a domain-separated canonical scope
from `["service", authenticated_service_id]` and routes every operation through
the existing `InvocationRuntime`; it does not implement policy, persistence, or
execution independently.

Lifecycle cursor tokens are opaque. Callers persist `next_cursor`, handle
`cursor_gap` by resuming at `minimum_available_cursor`, reject `cursor_ahead`,
and may safely repeat a page by deduplicating stable event IDs/cursors. Reads are
at least once.

Gateway's authenticated `/api/runtime/v1` routes use the same application
adapter and records. HTTP authentication supplies the principal and scope;
request bodies cannot override them. The package remains transport-independent
and still imports no HTTP framework.

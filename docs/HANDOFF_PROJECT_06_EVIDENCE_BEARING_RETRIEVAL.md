# Project 06 Handoff: Evidence-Bearing External Retrieval

Project 05 publishes the safe retrieval references Project 06 may aggregate
into a portable run evidence bundle. Consume these contracts through their
strict decoders and bounded projections; do not reopen provider payloads,
queries, tool results, or private selectors to make a bundle appear complete.

## Canonical identity and join

The deep module is
`backend/packages/harness/deerflow/retrieval/`. The event type is
`retrieval.observation.v1` in run-event category `tool`.

`RetrievalObservationV1` has these stable identities:

- `receipt_id`: the existing `DurableToolReceiptV1.receipt_id`;
- `attempt`: the receipt's exact durable attempt;
- `idempotency_key`: `<receipt_id>:retrieval`;
- `observation_id`: `ro_<observation_digest>`;
- `observation_digest`: SHA-256 over the domain-separated canonical terminal
  projection (`hartmesh/retrieval-observation/v1`);
- `draft_digest`: SHA-256 over the domain-separated safe draft projection;
- `result_projection_digest`: the exact authoritative digest copied from the
  terminal receipt after result sanitization and output budgeting.

Always decode with `RetrievalObservationV1.from_event_body()` and join with
`validate_retrieval_pair(receipt_body, observation_body)`. Do not reproduce the
canonicalization or derive a second result commitment in the bundle code.
Validation binds run, tenant, receipt, attempt, phase, result kind/digest, and
safe terminal reason. The parser also rejects non-canonical source references,
unsafe field additions, bad bounds, and observation/draft digest disagreement.

## Safe bundle projection

`RetrievalObservationV1.to_public_projection()` is the maximum safe detail
projection currently authorized for a run owner. It contains:

- observation/receipt IDs, attempt, provider/tool kind and adapter capability;
- policy and accepted tool-plane base/overlay/projection/effective digests;
- provider/terminal status, safe reason, timestamps and bounded duration;
- result/source counts, truncation/partial flags, and closed safe constraints;
- normalized HTTP(S) source references or tenant-scoped pseudonymous
  `ragflow-doc:` references;
- exact receipt result digest/kind and observation/draft digests;
- optional accepted sandbox execution/operation and explicit MCP evidence
  references.

The projection intentionally omits tenant digest/ref because API authorization
already establishes scope. If the bundle manifest needs its server-owned
tenant public reference, obtain it from the accepted invocation snapshot and
verify it against the decoded observation internally; do not add it to the
public retrieval API ad hoc.

Never include a raw query, query hash/embedding/length or other stable
query-derived identifier, credential or selector, private endpoint/dataset/
document selector, result title/snippet/body, header, provider response, raw
error, or unnormalized URL. Do not include operational metric state.

## Pagination and completeness

The authorized route is:

```text
GET /api/threads/{thread_id}/runs/{run_id}/retrieval-observations
    ?limit=100&after_seq=<thread-global-seq>
```

The page contains `items`, `next_after_seq`, and `invalid_event_count`.
`limit` is 1–100. Each item carries its `event_seq`; `after_seq` is exclusive.
The route applies current run visibility and observation authorization before
reading, validates run/tenant envelope agreement, and returns only the closed
safe projection.

That HTTP route is useful to clients but is **not** a coherent cross-section
bundle snapshot. Project 06 should read through an injected snapshot/store port
under its terminal high-water mark or repeatable-read transaction, decode every
observation, and compute its own bounded ordered page/root commitments. Order
by store `seq`, with observation ID as a defensive stable tie-breaker only if a
snapshot adapter needs one. Do not infer completeness from a short page.

Completeness rules:

- a supported terminal retrieval receipt requires exactly one matching
  observation for that attempt;
- a receipt-only terminal is incomplete/corrupt for replay even though the
  paired append API can reconcile it when the original safe draft remains in
  memory;
- an observation without its receipt, a duplicate key, or a cross-link/digest
  mismatch is an integrity error;
- each retry is a different immutable receipt attempt and observation; changed
  sources never overwrite history;
- a run with no explicitly supported retrieval attempt needs no fabricated
  retrieval section;
- pruned, legacy, unsupported-provider, invalid-event, and unavailable-store
  states must remain distinct section-level completeness reasons;
- a skipped or absent live-provider qualification is unqualified, not passing.

The current user API counts invalid selected events instead of returning their
unsafe bodies. A complete durable evidence bundle should normally fail closed
on any invalid required observation rather than exporting that count as if it
were evidence.

## Storage and fencing

`RunEventStore.append_retrieval_pair()` is implemented by memory, JSONL, and SQL
stores. It appends the terminal receipt and observation under one active
owner/lease fence and one backend-specific atomic operation. SQL uses one
transaction; JSONL prepares and atomically replaces the run file while holding
the execution fence; memory performs a no-await transaction with rollback on
publication failure. The generic `run_events` schema required no migration.

Reservation replay returns the matching observation with the terminal receipt
and revalidates their pair. Operational counters are emitted only when the
observation row is newly created, so idempotent recovery does not alter bundle
content or counts.

## Claims and qualification

Retrieval evidence proves HartMesh's bounded observation of provider use and
policy application for the linked tool attempt. It does not prove source truth,
completeness, copyright status, provider integrity, freshness at export time,
or deterministic replay. A bundle must repeat those limitations and must not
archive retrieved content merely because the source is public.

Implemented adapters are RAGFlow, DuckDuckGo, Serply, and Tencent WSA. Their
live status is deployment-specific. The opt-in gate and required environment
are documented in
`backend/docs/EVIDENCE_BEARING_RETRIEVAL.md`; no passing artifact is committed
by Project 05.

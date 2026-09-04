# Evidence-Bearing External Retrieval

Evidence-bearing retrieval records a bounded, provider-neutral observation for
supported external search tools during an accepted durable run. It answers
which durable tool attempt contacted which provider, under which accepted
constraints, which safe source references were returned, and which exact final
tool result the agent received.

It does **not** prove that a source is true, complete, unchanged, copyrighted
appropriately, or reproducible. External results are mutable and no provider
response body is archived for replay.

## Supported paths

The durable evidence adapter is currently installed on these built-in tools:

| Provider | Tool | Safe sources | Recency | Server-owned scope |
| --- | --- | --- | --- | --- |
| RAGFlow | `knowledge_search` | tenant-scoped pseudonymous `ragflow-doc:` references | unsupported | explicit `datasets` allowlist required for durable runs |
| DuckDuckGo | `web_search` | normalized HTTP(S) origins | day/week/month/year | fixed DuckDuckGo HTML endpoint, no-redirect HTTPS transport, configured domain and size ceilings |
| Serply | `web_search` | normalized HTTP(S) origins | no portable recency claim | fixed Serply endpoint, configured vertical/domain/size ceilings, resolved API key |
| Tencent WSA | `web_search` | normalized HTTP(S) origins | unsupported | fixed WSA endpoint, configured domain/size ceilings, resolved service API key |

These rows describe implemented adapters, not live qualification. A provider is
qualified for a deployment only after its opt-in live gate passes against that
deployment. Missing network access, datasets, or credentials is an unpassed
gate, never evidence of a pass.

Other search tools continue to work normally but do not emit
`retrieval.observation.v1`. Direct/local tool invocation without an accepted run
also retains its compatibility behavior and does not claim durable evidence.

## Trust and commit boundary

`deerflow.retrieval.EvidenceBearingRetrievalService` is the single normalization
boundary. It receives an active `DurableToolReceiptV1`, the verified actor and
tenant, server-resolved credential, accepted tool-plane digests, server policy,
and caller-requested narrowing. Policy is intersected before the provider port
is called. A caller may reduce domains, collections, recency, result count,
bytes, timeout, redirects, schemes, or partial-result acceptance; it cannot
widen any of them.

The service returns the candidate result and publishes a host-owned observation
draft through typed per-call context. Tool return metadata cannot publish or
replace that draft. The outer `ToolReceiptMiddleware` runs after result
sanitization and output budgeting, computes the existing authoritative
`result_projection_digest` over exactly the model-visible result, and atomically
appends the terminal receipt plus observation under the active run owner/lease
fence.

A successful supported retrieval with no draft is converted to a failed paired
terminal. Duplicate, wrong-attempt, forged, status-disagreeing, or
digest-disagreeing observations fail closed. Recovery accepts a terminal
retrieval attempt only when its matching observation is present and validates
against the same receipt. A retained receipt-only write may be reconciled when
the original safe draft is still available; it is never treated as complete by
replay. Each retry receives a new durable attempt and immutable observation.

Memory, JSONL, and SQL stores implement the paired append. The existing generic
`run_events` table already stores typed event content and idempotency keys, so no
database schema migration is required.

## Privacy and normalization

Portable evidence, ordinary logs, the observation API, and metrics never carry:

- a raw query, query hash, embedding, byte length, or other stable
  query-derived identifier;
- result titles, snippets, document bodies, provider response bodies, or
  headers;
- credentials, secret selectors, raw provider errors, or unsafe provider
  request IDs;
- private RAGFlow dataset or document selectors.

The accepted tool receipt replaces every declared protected argument with one
fixed marker, so even low-entropy query dictionaries and query lengths cannot
be tested against receipt fields. The portable policy digest commits only to a
closed safe projection; exact private configuration remains bound by the
accepted deployment/user/projection/effective tool-plane digests.

Web source references lowercase and IDNA-normalize the host, remove user info
and default ports, and retain only the origin. Paths, query strings, and
fragments are discarded wholesale because any of them can reflect query or
tenant data. The adapter enforces the accepted scheme/domain policy before
this coarse origin is recorded, and decoded events reject non-canonical
references. Portable safe constraints expose only a `provider_default` or
`restricted` domain category; literal allow/deny selectors remain private and
are bound by accepted tool-plane digests.
RAGFlow references expose a server-created collection reference and a
tenant-scoped digest of the private document selector. They do not expose the
dataset ID, document ID, title, or text.

Provider failures use only these safe categories: policy denied, unavailable,
timeout, rate limited, authentication failed, configuration error, unsafe
response, oversized response, internal error, and cancellation. The provider
adapter may keep protected diagnostics under an existing restricted operational
logging policy, but it must never copy them into a model-visible error or
portable event.

HTTP adapters require an application JSON media type and reject a raw response
body over the accepted aggregate-byte ceiling before JSON decoding or candidate
normalization. Provider headers and rejected bodies are never copied into the
observation.

## Configuration

The evidence-bearing web adapters recognize these optional server ceilings on
their selected `tools:` entry:

```yaml
max_results: 5
allowed_domains: [example.com]
denied_domains: [private.example.com]
max_item_bytes: 16384
max_total_bytes: 65536
timeout: 30
```

`allowed_domains` includes the named domain and its subdomains. A deny rule
wins. Provider endpoints are fixed by the adapter and redirects are disabled.
DuckDuckGo durable retrieval bypasses the SDK aggregator, verifies the pinned
HTML endpoint before I/O, and installs an HTTPS-only, no-redirect transport;
an SDK endpoint change fails closed. The caller's
`max_results` and recency choice can only narrow these values. Timeout includes
waiting for the per-tenant/provider concurrency slot and is capped below the
enclosing run deadline. There is no automatic provider retry in version 1;
run/tool retry creates a separately evidenced attempt.

Serply, Tencent WSA, and DuckDuckGo use cancellation-aware blocking offload.
If the enclosing deadline fires, the tenant/provider concurrency permit stays
held until the client's bounded network operation has actually stopped, so
timed-out worker threads cannot escape the concurrency ceiling.

For RAGFlow, configure a non-empty operator-owned `datasets` list for durable
runs. Omitting it preserves the legacy direct/local tenant-wide catalog search,
but the evidence-bearing path rejects that mutable scope before retrieval.
RAGFlow remains read-only.

Configuration and credentials are resolved once into the accepted in-memory
request from the captured admitted tool-plane material before provider I/O.
Later mutable config or environment changes cannot change that request. Serply
and Tencent durable runs therefore require an explicit `api_key: $...` entry;
their environment-only fallback remains available only to legacy direct/local
calls.
The observation binds the accepted tool-plane base, user overlay, projection,
and effective digests; it does not expose the secret selector material.

## MCP and sandbox linkage

MCP tools are never classified as retrieval by name, description, result shape,
or server prefix. A trusted adapter must add explicit
`deerflow_retrieval_v1` metadata containing version, provider, tool kind,
adapter capability version, and the complete protected-argument field list. It
may also add a bounded `mcp_evidence_ref` that refers to independently retained
MCP evidence. Arbitrary MCP tools remain ordinary MCP calls.

When an accepted sandbox bridge is present, the observation may carry its safe
execution-evidence reference and an explicitly accepted operation reference.
It never copies the sandbox backend handle, lease capability, credential, or
operation payload.

## API and metrics

An authorized run owner can page observations with:

```text
GET /api/threads/{thread_id}/runs/{run_id}/retrieval-observations?limit=100&after_seq=123
```

`limit` is 1–100. The response contains `items`, `next_after_seq`, and an
`invalid_event_count`. Items are closed safe projections with provider/status,
times, counts, flags, safe constraints, normalized references, accepted
evidence links, and result/observation digests. Raw run events are not returned
through this endpoint. Existing run visibility and observation authorization
apply before the event store is read.

Committed observations increment in-process operational counters exactly once.
Metric labels are restricted to the closed provider category
(`ragflow`, `duckduckgo`, `serply`, `tencent_wsa`, `mcp`, or `other`) and safe
status. Tenant/user identifiers, queries, URLs, document IDs, and credential
selectors are neither labels nor metric values.

## Live qualification

Run the selected provider gate explicitly:

```bash
cd backend
DEER_FLOW_RUN_LIVE_TESTS=1 \
DEER_FLOW_RETRIEVAL_QUALIFICATION_PROVIDER=duckduckgo \
uv run pytest tests/test_retrieval_provider_live.py -v -s
```

Supported selectors are `duckduckgo`, `serply`, `tencent_wsa`, and `ragflow`.
Serply requires `SERPLY_API_KEY`; Tencent requires
`TENCENTCLOUD_WSA_APIKEY`; RAGFlow requires `RAGFLOW_BASE_URL`,
`RAGFLOW_API_KEY`, and comma-separated `RAGFLOW_DATASETS`. An explicitly
selected provider with missing material fails. A skipped or never-run gate is
unqualified and must not be represented as passing deployment evidence.

The live test holds raw results only in memory, checks the normalized draft,
and does not write provider fixtures or result text into evidence.

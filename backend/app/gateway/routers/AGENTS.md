# Gateway routers

Artifact `report_preview=true` projects output report JSON behind the existing
thread read/owner gate. Read at most 16 MiB through one no-follow descriptor;
return at most 1 MiB of card fields with the original bytes' SHA-256 and size.
Keep parsing/serialization off-loop and drain it on cancellation. No projection
cache or stored report mutation; `download=true` always returns the original.
Malformed/oversized/unsupported projections retain a bounded source-preview
fallback in the UI; ownership and path denials do not. Platforms without safe
descriptor-relative reads return 501 so source preview remains available.

Workspace prewarm checks `sandbox:execute` before provider lookup/scheduling,
using the shared request authorization helper off-loop. Denial returns 202
with `scheduled: false, reason: forbidden`; thread ownership still applies.

`POST /api/threads/{id}/history` hydrates reopened chats with the rendered
`artifacts`, `todos`, and non-null `goal` channels in the newest returned
checkpoint alongside title and messages.
Keep that projection explicit and serialized through the API channel serializer;
do not expose internal sandbox state. Empty lists are meaningful, while absent
or null goals are omitted to preserve the frontend's local override semantics.

Artifacts, Files and Shared must stream from one safely opened descriptor, including MIME
sniffing, response metadata and range reads. Preflight paths only determine HTTP
errors; they must never become permission to reopen a path through symlinked
parents. Use `deerflow.files.store.open_regular_source` for source reads/copies,
and retain ownership until every offloaded operation has drained on cancellation.
Conversation keep/publish sources remain lexical paths within uploads/outputs.
Artifact reads use `resolve_thread_read_path`; never resolve away link segments
before no-follow opens. Small editable artifacts capture bounded immutable bytes
for SHA-256/ranges; large artifacts stay descriptor-streamed without content hashing.
Skill archive detection/extraction shares one source descriptor and drains off-loop.
`SafeFileAccessUnavailable` means HTTP 501 for descriptor reads and archives;
never turn an unsupported host into a missing/invalid/changed file.
`files.store.copy_into` uses private hidden staging and descriptor-relative
exclusive hard links: no placeholders or overwrites of racing names. Verify
staging ownership/mode before writing; the worker owns cleanup. Unsupported
atomic publication fails closed.
Shared publication drains copying, its database record and failure rollback as
one operation under the deduplication lock before propagating cancellation.

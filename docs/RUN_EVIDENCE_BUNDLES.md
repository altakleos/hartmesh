# Portable Run Evidence Bundles

HartMesh can export one authorized terminal durable run as a bounded ZIP. The
ZIP contains the exact presented artifact bytes and one canonical manifest at:

```text
hartmesh-evidence/manifest.v1.json
```

The bundle provides **internal integrity**, not authenticity. Its manifest and
artifact bytes are digest-bound, but HartMesh does not sign the bundle, use a
timestamp authority, or independently attest that an external system reported
the truth. A successful verification result therefore always reports
`"authenticity":"not_signed"`.

## Bundle, ordinary archive, and partial export

- An **evidence bundle** is created by the dedicated evidence-bundle endpoint.
  It is terminal-only, requires all evidence selected by the
  `complete_durable` profile, and contains the canonical manifest.
- An **ordinary artifact archive** is the existing ZIP returned by
  `.../artifacts/archive`. It remains available under its existing semantics
  and must not be described as HartMesh evidence.
- There is no partial evidence export in V1. Required legacy, pruned,
  unavailable, or inconsistent evidence fails closed with a safe error code.
  Optional capabilities that were neither accepted nor attempted are recorded
  as `absent_by_design`. An accepted durable MCP task surface with no attempts
  is a complete empty section, not a fabricated omission.

## HTTP API

Both routes require the current principal to have `runs:read` and own the
existing thread. PAT use is explicitly allowlisted with the same scope and is
otherwise denied by the PAT route matrix.

```http
GET /api/threads/{thread_id}/runs/{run_id}/artifacts/evidence-bundle
POST /api/threads/{thread_id}/runs/{run_id}/artifacts/evidence-bundle
```

`GET` validates the same terminal evidence snapshot used by export and returns
a bounded status projection with `Cache-Control: private, no-store`:

```json
{
  "available": true,
  "schema": "hartmesh.run-evidence-bundle",
  "schema_version": 1,
  "canonicalization_version": 1,
  "profile": "complete_durable",
  "run_ref": "run-…",
  "thread_ref": "thread-…",
  "terminal_status": "success",
  "artifact_count": 2,
  "sections": [
    {
      "name": "accepted_invocation",
      "state": "complete",
      "required": true,
      "item_count": 1,
      "reason_code": null
    }
  ],
  "limitations": [
    "artifact_content_is_not_sanitized",
    "bundle_copy_outlives_server_retention",
    "internal_integrity_only_not_signed"
  ],
  "authenticity": "not_signed"
}
```

`POST` repeats snapshot validation, validates and copies artifact files, and
returns `application/zip` with a server-created filename. The response uses
`Cache-Control: private, no-store`, `X-Content-Type-Options: nosniff`,
`X-HartMesh-Evidence-Bundle`, and
`X-HartMesh-Evidence-Authenticity: not-signed`.

Status and download reserve the existing per-thread artifact-archive
operation and conflict with active mutations. Download also shares the process
archive concurrency limit and keeps that slot until an off-thread ZIP worker
exits even when the request is cancelled. Both operations have a 60-second
whole-generation deadline and monitor request disconnects; cancellation is
propagated through the snapshot/archive task and partial output is drained and
closed before its slot is released. A status response establishes
snapshot eligibility; filesystem safety and exact artifact bytes are
necessarily checked again while the POST archive is built.

## Snapshot and consistency contract

`RunEvidenceSnapshotService` is runtime-owned and independent of FastAPI, ZIP,
and filesystem types. The Gateway adapter supplies current tenant and owner
context and reads the owner-scoped run, event, MCP-task, and subagent-batch
repositories.

A V1 snapshot:

1. requires a terminal `success`, `error`, `timeout`, or `interrupted` run;
2. requires current accepted-invocation V4 credential/tool-plane material,
   accepted subagent catalog and skill scopes, V3 assembly evidence, terminal
   evidence, and complete durable tool receipts;
3. validates tenant, run, parent receipt, assembly, catalog, sandbox, retrieval,
   MCP, and batch cross-links before reducing them to safe digests;
4. pages the run journal up to 100,000 events and records its last sequence as
   the lifecycle high-water mark;
5. derives required MCP, batch, and retrieval coverage from accepted tool-plane
   task declarations and terminal tool attempts, including an explicit safe
   terminal-failure reference when submission ended before a child row existed;
6. uses only payload-free bounded MCP and batch lifecycle projections, recording
   MCP public lineage plus private-request-commitment presence/version without
   exporting the HMAC digest or key ID;
7. re-reads the terminal run fence and external terminal projections; and
8. retries once when that fence changes, then returns
   `evidence_snapshot_changed`.

The endpoint surrounds this read with the RunManager's thread-operation
reservation, so no accepted application mutation can overlap it. The archive
builder then opens each artifact without following links, hashes bytes while
copying them from that descriptor, and revalidates the same descriptor and path
components. It never reopens a path to populate the manifest. A replacement or
same-size mutation fails as `artifact_changed`; an unsafe path, link, special
file, or collision fails as `artifact_unsafe`.

## Manifest V1

The runtime contract is
`deerflow.runtime.run_evidence.RunEvidenceBundleManifestV1`. Its top-level
fields are fixed:

| Field | Meaning |
| --- | --- |
| `schema`, `schema_version` | `hartmesh.run-evidence-bundle`, version `1` |
| `canonicalization`, `canonicalization_version` | `utf8-nfc-sorted-json`, version `1` |
| `bundle_ref`, `tenant_ref`, `thread_ref`, `run_ref` | bounded public references; raw run/thread/user/tenant IDs are excluded |
| `terminal` | status, safe stop reason, and UTC accepted/completed timestamps |
| `admission` | accepted invocation/context/agent digests and version; catalog/scope counts and digests; extension and credential evidence references; accepted tool-plane base, overlay, projection, and effective digests |
| `assembly` | bound assembly evidence digest and fingerprint |
| `lifecycle` | high-water mark, terminal-event digest, event count, and coarse counts |
| `evidence_sections` | fixed section set with state, requirement, bounded item count, safe references, and recomputable root |
| `evidence_links` | sorted digest-only MCP/batch/retrieval-to-receipt edges whose endpoints must occur in the declared section roots |
| `artifacts` | canonical relative ZIP path, byte size, SHA-256, and optional safe media type |
| `qualification` | exact projection of the qualification section; never synthesized as passing |
| `completeness` | profile, complete state, and expected section count |
| `limitations` | the three mandatory limitations shown above |
| `manifest_digest` | domain-separated digest over the canonical manifest with this field set to JSON `null` |

All fields are present; optional values use JSON `null` rather than field
omission. Strings are NFC-normalized. Objects use lexicographically sorted
keys, arrays use their contract-defined order, JSON is compact UTF-8 without
ASCII escaping, and non-integer numbers are forbidden. Timestamps use UTC `Z`
with seconds or exactly six fractional digits when needed. Section references
and artifacts are sorted. Paths use `/`, are portable across Windows/POSIX,
and the reserved `hartmesh-evidence` namespace cannot be supplied by a user
artifact.

Digest domains are versioned and terminated by a zero byte:

```text
hartmesh.run-evidence-bundle.manifest.v1\0
hartmesh.run-evidence-bundle.reference.v1\0
hartmesh.run-evidence-bundle.public-reference.v1\0
hartmesh.run-evidence-bundle.section-root.v1\0
hartmesh.run-evidence-bundle.safe-leaf.v1\0
hartmesh.run-evidence-bundle.snapshot-fence.v1\0
```

`bundle_ref` is derived from the canonical manifest without `bundle_ref` and
with `manifest_digest: null`; `manifest_digest` is then derived from the same
projection with `bundle_ref` present. This avoids a recursive self-digest.

### Evidence sections

V1 always contains these section names:

```text
accepted_invocation  actor_credential       assembly
subagent_catalog     skill_material         extension_material
tool_plane           lifecycle              tool_receipts
mcp_tasks            subagent_batches       sandbox_execution
retrieval_observations                     qualification
```

Section states are `complete`, `absent_by_design`, `unsupported`, `legacy`,
`pruned`, `unavailable`, and `unqualified`. A complete section has sorted safe
references and a domain-separated root over those references. An omitted
section has no root or references and carries a safe reason code. Required
sections must be `complete`; V1 does not serialize a partial bundle. Admission,
assembly, and lifecycle section references must exactly equal their declared
top-level anchors rather than merely containing them.

`evidence_links` makes child/parent joins checkable without a database. Every
MCP task, subagent batch, and retrieval observation reference has exactly one
edge to an included durable tool-receipt reference. The Gateway validates the
underlying typed records before reducing them to these digest-only edges; the
offline verifier then rejects missing, duplicate, dangling, or wrong-section
edges. The edges expose no raw task, receipt, run, provider, or user identifier.

No prompts, messages, tool arguments/results, retrieval text, credentials,
friendly PAT names, user IDs, raw tenant names, provider handles, request
headers, trace headers, or unsafe exception text enter the manifest. Artifact
filenames and contents are intentionally exported and can themselves be
sensitive.

## Limits

| Resource | V1 limit |
| --- | ---: |
| Artifact files | 50 |
| One artifact | 50 MiB |
| All artifact bytes | 100 MiB |
| One artifact path | 1,024 UTF-8 bytes |
| Manifest | 1 MiB |
| References in one section | 4,096 |
| Cross-section evidence links | 4,096 |
| Run events in one snapshot | 100,000 |
| Concurrent archive workers per Gateway process | 4 |
| Generation deadline | 60 seconds |

The ZIP is stored rather than compressed. Manifest and central-directory
overhead are additional to the 100 MiB artifact limit and are independently
bounded. ZIP64 and encrypted entries are not used.

## Offline verification

The verifier uses only the Python standard library and needs no HartMesh
configuration, database, credentials, or network:

```bash
python -I scripts/verify_run_evidence_bundle.py path/to/bundle.zip
```

It emits one compact JSON object. Exit `0` means ZIP paths, bounds, canonical
manifest, self-digest, bundle reference, section roots/cross-links, declarations,
and streamed artifact bytes are internally consistent. Exit `1` emits a stable
failure `code`. In either case, the output states `"authenticity":"not_signed"`.

Example success:

```json
{"artifact_count":2,"authenticity":"not_signed","bundle_ref":"bundle-…","manifest_digest":"…","schema_version":1,"status":"valid"}
```

The verifier rejects unknown versions instead of guessing forward semantics.
It also rejects compression, encryption, duplicate/case-colliding paths,
special-file metadata, undeclared or missing members, traversal and nonportable
names, oversized archives, and any digest or cross-link mismatch.

## Safe failures and operations

The HTTP surface returns bounded codes such as `run_not_terminal`,
`run_not_found`, `run_operation_active`, `evidence_incomplete`, `evidence_pruned`,
`evidence_legacy_unbound`, `evidence_snapshot_changed`,
`evidence_cross_link_invalid`, `artifact_changed`, `artifact_unsafe`,
`bundle_limit_exceeded`, and `bundle_generation_busy`. Cross-owner requests
remain generic `404` responses. Detailed filesystem, provider, credential, and
evidence payloads are not returned.

`bundle_generation_timeout` is returned as `504` when the bounded whole-request
deadline expires. A client disconnect cancels the operation and is recorded as
cancelled rather than returning a response to the closed connection.

Gateway process metrics count requested, completed, refused, cancelled, and
failed operations without raw resource identifiers. Bounded events use only
the verified actor digest and the bundle public reference when one exists;
authorization middleware separately audits refusals that occur before the
endpoint handler.

## Retention, qualification, and trust

After download, the ZIP is a user-controlled copy. Deleting or expiring the
server-side run or artifacts does not remove that copy; store and share it as
sensitive data.

Qualification is included only through already accepted administrator-created
sandbox qualification evidence. A run without that evidence is explicitly
`unqualified`. The field does not elevate a candidate, missing, expired, or
operator-asserted deployment into a passing release claim. This checkout still
ships no passing exact-two deployment artifact.

# Project 07 handoff: portable run evidence bundles

Project 06 owns evidence selection, consistency, ZIP construction, and offline
verification. Project 07 may render the bounded status projection and initiate
the download. It must not query raw run events, reconstruct roots, inspect
internal database rows, or assemble a manifest in the browser.

## HTTP surfaces

Both endpoints are additive and use the same path:

```text
/api/threads/{thread_id}/runs/{run_id}/artifacts/evidence-bundle
```

- `GET` returns eligibility and section status as JSON.
- `POST` returns the completed ZIP as `application/zip`.
- Browser sessions and PATs both require `runs:read` plus current ownership of
  an existing thread. Never treat a run ID, public reference, prior status, or
  downloaded bundle as authority.
- Cross-owner requests are generic `404`. Active thread work or evidence
  inconsistency returns a bounded error response.
- Both operations reserve `ThreadOperationKind.artifact_archive`; do not poll
  GET aggressively while a run or artifact edit is active.

The exact GET response model is:

```ts
type RunEvidenceBundleSection = {
  name:
    | "accepted_invocation"
    | "actor_credential"
    | "assembly"
    | "subagent_catalog"
    | "skill_material"
    | "extension_material"
    | "tool_plane"
    | "lifecycle"
    | "tool_receipts"
    | "mcp_tasks"
    | "subagent_batches"
    | "sandbox_execution"
    | "retrieval_observations"
    | "qualification";
  state:
    | "complete"
    | "absent_by_design"
    | "unsupported"
    | "legacy"
    | "pruned"
    | "unavailable"
    | "unqualified";
  required: boolean;
  item_count: number;
  reason_code: string | null;
};

type RunEvidenceBundleStatus = {
  available: true;
  schema: "hartmesh.run-evidence-bundle";
  schema_version: 1;
  canonicalization_version: 1;
  profile: "complete_durable";
  run_ref: string;
  thread_ref: string;
  terminal_status: "success" | "error" | "timeout" | "interrupted";
  artifact_count: number;
  sections: RunEvidenceBundleSection[];
  limitations: [
    "artifact_content_is_not_sanitized",
    "bundle_copy_outlives_server_retention",
    "internal_integrity_only_not_signed",
  ];
  authenticity: "not_signed";
};
```

The POST response supplies:

```text
Content-Type: application/zip
Content-Disposition: attachment; filename="run-evidence-{public-run-ref}.zip"
Cache-Control: private, no-store
X-Content-Type-Options: nosniff
X-HartMesh-Evidence-Bundle: bundle-…
X-HartMesh-Evidence-Authenticity: not-signed
```

Use `Content-Disposition` for the download filename. The bundle reference is
known only after exact artifact bytes are copied and the manifest is created;
the GET status intentionally has no speculative `bundle_ref`.

## UI semantics

Recommended flow:

1. Show the evidence action only for a terminal run known to the UI.
2. Fetch GET on demand and render the server's ordered section projections.
3. Explain `absent_by_design` as a capability that was not accepted, and
   `unqualified` as no verified deployment qualification—not an export error.
4. On user confirmation, POST and save the returned blob using the response
   filename.
5. Display the mandatory warning that artifacts may be sensitive, server
   deletion cannot recall the downloaded copy, and digest verification is not
   signature/authenticity verification.

Do not label the ordinary `.../artifacts/archive` download as evidence. Do not
offer a partial-mode control: V1 exposes only `complete_durable`.

The current backend does not stream progress events. Treat GET as a bounded
eligibility check and POST as one cancellable request. Project 07 can expose
idle/checking/downloading/succeeded/failed client states, but must not infer
server progress percentages. Cancelling the fetch disconnects the request; the
Gateway retains its archive slot until the off-thread worker has exited and
cleaned up.

## Safe failures

The UI may branch on these stable `detail` values without displaying internal
payloads:

```text
run_not_terminal
run_operation_active
evidence_incomplete
evidence_pruned
evidence_legacy_unbound
evidence_snapshot_changed
evidence_cross_link_invalid
artifact_changed
artifact_unsafe
bundle_limit_exceeded
bundle_generation_busy
bundle_generation_cancelled
manifest_version_unsupported
```

Use generic, retry-oriented text for `evidence_snapshot_changed`,
`artifact_changed`, `run_operation_active`, and `bundle_generation_busy`.
Legacy, pruned, incomplete, cross-link, and unsupported-version outcomes need
operator-facing guidance rather than a retry loop. Preserve a generic 404 for
ownership/not-found and do not distinguish those cases in telemetry or UI.

## Backend ownership

- Runtime manifest/snapshot types:
  `backend/packages/harness/deerflow/runtime/run_evidence.py`
- Gateway repository adapter:
  `backend/app/gateway/run_evidence.py`
- ZIP integration and exact-byte hashing:
  `backend/app/gateway/artifact_archive.py`
- HTTP models/routes:
  `backend/app/gateway/routers/thread_runs.py`
- PAT method/path policy:
  `backend/app/gateway/auth/pat.py`
- Offline verifier:
  `scripts/verify_run_evidence_bundle.py`
- Full contract and limits: `docs/RUN_EVIDENCE_BUNDLES.md`

Project 07 should add only frontend API/types/state/rendering tests against
these public responses. Any desired new section, partial export, progress
protocol, signing claim, or manifest field is a backend contract change and
requires additive versioning rather than browser-side interpretation.

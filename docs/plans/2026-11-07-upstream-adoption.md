# Upstream adoption: November 7, 2026

## Scope

Integrate DeerFlow `d0007090c262cf3806d78f39dfcb4f5aca8226ea` into HartMesh
`3b49cef50ad0320fd50c664080d34c9aabe3d3cd`. The common ancestor is
`c373f8491d70a2908efe3c10c99a14a9f8dbb8f4`: 47 upstream commits (41 fixes,
five features, one documentation change). Preserve upstream ancestry and the
complete reference frontend. Product UI changes belong in `frontend-hm/`.

## Integration plan

1. Resolve the upstream merge, retaining HartMesh's admission, managed storage,
   session ownership, output capture, resident shutdown and presentation contracts.
   Review automatically merged intersections as well as the 22 textual conflicts.
2. Adopt reliability fixes with their upstream regressions: input validation,
   tool/result pairing, memory and compaction, off-loop construction, MCP claims,
   channels, sandbox cleanup, knowledge formatting and platform compatibility.
3. Reconcile the complete-generation analysis cache with HartMesh's private
   workspace cache, read-only SQL, stable input keys and retention behavior.
4. Integrate native batch dependencies and requested continuations through the
   existing owner checks. Retain cancellation, account revocation, Goal budgets
   and thread incarnation; unsupported execution scopes must fail explicitly.
   Qualify migration `0052` from the published migration head.
5. Adopt extension API `0.2.11` and optional Agent Teams versioned artifacts,
   preserving HartMesh's extension resources, authorized Spaces access and
   execution/output authority. Keep team policy inside the extension.
6. Adopt HarnessGuard's advisory recovery hints and optional syntax validation,
   preserving existing middleware order, sandbox authorization and delivery.
7. Port applicable chat UI fixes and batch follow-up protocol handling. Keep
   account lifetimes, strict product/workspace presentation and managed-chat
   controls. Model catalogue and gallery pages remain subject to the existing
   product scope; backend availability alone does not expose a new surface.
8. Update source provenance, guidance, configuration/migration references and
   user documentation. Keep HartMesh release versions and published artifact
   pins separate from this source integration.
9. Run focused backend, skill, frontend and blocking-I/O regressions; check
   formatting, types, guidance, migration ancestry and frontend isolation.
   Native/Compose qualification runs in the existing required CI workflows.
10. Obtain independent review, address findings, commit and publish the PR.
    Watch all applicable checks on the exact head, repair failures, and merge
    only after they pass. Record validation and review in the PR.

## Progress

- Both remotes refreshed; HartMesh main was already current.
- Integration branch: `sync/deerflow-2026-11-07`.
- All 22 textual conflicts resolved. The upstream reference frontend remains
  an exact snapshot; applicable changes were ported into the HartMesh app.
- Semantic review retained account and owner checks, managed-storage boundaries,
  native/resident sandbox ownership, output-capture proof and shutdown behavior.
  Batch follow-ups carry revocable authority through delegated capabilities;
  they cannot regain management rights through a descendant extension.
- Artifact handoffs refuse unsupported storage before new admission and still
  reconcile recorded idempotent runs. Goal finalization preserves the live
  reservation until authoritative terminal acknowledgement. HarnessGuard keeps
  raw receipts, ordered transforms and output-inventory qualification.
- Analysis caches keep private immutable generations, HartMesh's input identity
  and read-only SQL. Shared lifetime locks allow parallel readers/builders;
  cleanup retains the three newest complete entries only when the cache is idle.
- Focused compatibility regressions and static checks passed, and independent
  review approved the implementation. [PR #377](https://github.com/altakleos/hartmesh/pull/377)
  records commands, outcomes, reviewed revisions and CI repairs. All 33 applicable
  checks passed on `15fcf73ca5efec4853004bfb082a4a259c08fddd`; the integration
  merged as `d72eb9235737490b90695c0b1602aa9b35ab00c2` on November 7, 2026.

## What upstream is improving

Most of this interval hardens existing behavior: providers must not block the
agent event loop while loading TLS/model resources; interrupted sandbox creation
must retain ports until cleanup is proven; memory and tool results must survive
malformed or partial provider responses; channels must preserve conversation and
delivery boundaries. These fixes fit the existing HartMesh runtime directly,
with adaptations at its stronger ownership and output-verification boundaries.

The new batch and Agent Teams features make delegated work explicit: dependencies
determine when tasks can start, a requested follow-up is admitted once, and file
handoffs retain the exact version that another agent consumes. HartMesh adopts
these capabilities only in execution and storage scopes their contracts support.

HarnessGuard helps agents recover from repeated tool failures and can validate
Python/JSON edits before writing. Model discovery reduces manual operator setup;
video options expose provider capabilities. These are generic configuration or
extension capabilities. Upstream gallery/catalog presentation stays in the
reference frontend; HartMesh retains its existing product surfaces.

## Verification boundary

Source adoption and successful CI do not establish release delivery. Publishing
new immutable images and qualifying a release are separate operations.

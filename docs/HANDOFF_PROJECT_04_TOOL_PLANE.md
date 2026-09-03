# Project 04 Handoff: Accepted Tool-Plane Anchor

Project 03 makes the governed tool plane available through one read interface:

```python
effective = await ToolPlaneRevisionService.effective_for_actor(actor)
```

`actor` is Project 02's immutable `VerifiedActorContextV1`. The service derives
the user overlay scope from its frozen tenant and effective subject; callers do
not provide a user reference. It reads the deployment and overlay generations
before and after composition, retries a bounded race, verifies projection
digests, and fails closed in durable mode for bootstrap, recovery, drift, or an
incoherent pair.

The result is `EffectiveToolPlaneRevisionV1`. Its `to_json()` projection is
stored as `decision_evidence.tool_plane_revision` in
`AcceptedInvocationContextV1` and contains these digest anchors:

- `base_revision_digest`: promoted deployment-base revision identity.
- `user_overlay_digest`: promoted verified-user overlay identity, or
  `EMPTY_OVERLAY_MARKER_V1` when that user has no nonempty overlay. The service
  verifies the live absence of custom/legacy packages, nonempty user state,
  credentials, and unexpected pointers before admitting that marker.
- `projection_digest`: canonical digest of the observed base and overlay
  projection digests used for this composition.
- `effective_digest`: canonical digest of the complete effective V1 projection,
  including both source digests, generations, projection digest, effective MCP
  server structure/tool ceilings, global skill states, managed-integration IDs,
  and governance state.

The same `effective_digest` is also supplied as
`governed_tool_plane_digest` while building the accepted agent revision. It
replaces live extension-config identity in that agent-revision projection; the
ordinary accepted skill snapshot and its package/projection digests remain
separate accepted material.

Admission resolves secret selectors only into process-local MCP configuration,
captures the resulting MCP tool objects and skill bytes, then rereads the
effective revision. Recovery reconstructs runtime configuration from the
accepted selector-safe `tool_plane_revision`, verifies MCP tool-contract and
accepted-agent-revision digests, and never consults a newer promoted revision.

Project 04 may add this already accepted tool-plane anchor to its existing
accepted-material execution evidence. It must not introduce a second
tool-plane read path, create a parallel sandbox lease authority, or make
sandbox ownership responsible for tool-plane governance. Sandbox lease and
ownership fencing remain independent of base/overlay revision authority.

# HartMesh governance extension template

This standalone package demonstrates tenant-aware restrictive policy and bounded audit
contributions using only `deerflow-extension-api`. It is a reference package, is not enabled
automatically, and does not replace HartMesh authorization, tenant resolution, evidence,
lineage, or run fencing.

It contributes:

- a v2 invocation constraint that can only reduce the host's subagent limit;
- task-lifecycle and assembly audit events containing IDs and digests only;
- MCP preparation evidence containing a safe decision reference, never call arguments;
- a finite health probe and a service that fails closed when its audit dependency is required;
- a stdlib HTTPS audit adapter with a bounded non-blocking queue.

No raw prompts, tool payloads, memory content, credentials, readable tenant IDs, or private
tenant configuration are exported. The only tenant fact is `TenantReferenceV1.public_ref`.

## Install and require

Review and test your copied package, then install it from the repository root:

```bash
make extension-install SOURCE=examples/hartmesh-governance-extension
deerflow extensions verify
```

Configure only non-secret values under the plugin's `config`. Put credentials in existing
Secret/env mechanisms; this template intentionally accepts no audit credential. To make its
authority mandatory, add both `invocation_constraints.v2` and
`mcp_interceptor:hartmesh.governance.mcp` to the host's `required_capabilities`. A failed or
timed-out audit health check then blocks readiness, and `fail_closed_startup: true` also makes
the service reject startup.

`artifact_fixture.py` is a publisher-side review aid. HartMesh's committed
`backend/extensions.lock.json`, image-built `/app/hartmesh/extension-artifacts.json`, and
`deerflow extensions verify` remain the authoritative provenance chain.

Run the independent tests with:

```bash
uv run --project examples/hartmesh-governance-extension --extra dev pytest \
  examples/hartmesh-governance-extension/tests
```

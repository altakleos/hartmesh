# OpenSandbox accepted-material feasibility

## Decision

The OpenSandbox immutable accepted-material profile is **unavailable**. The
bounded Phase 0 probe targets OpenSandbox server `0.1.14` and Python SDK
`0.1.15` (wheel SHA-256
`992b01490551f4d8e3f99caa25e34cb9d1690f0c5027eeebab912738291957d1`,
source tag `python/sandbox/v0.1.15` resolved to
`e9d0a63919739b1bed05914373acbacb11e37d43`).
The pinned contract definitively lacks two prerequisites:

- a server-reported resolved OCI image digest;
- compare-and-set metadata or an equivalent atomic ownership lease.

The lifecycle schema does expose read-only volumes and the execd schema exposes
per-command UID/GID selection. Those are candidates for a trusted setup/final
principal split, not proof that the deployed backend enforces one, so that
scenario remains `not_run` rather than being reported as unsupported.

The absence is detectable from the exact SDK and server schemas, so no credentials
or live service dump are needed to reach the no-go decision. The probe does not
claim that a service was provisioned or that any live mutation scenario passed.
The committed canonical result is
[`opensandbox_feasibility_0_1_15.json`](../tests/fixtures/opensandbox_feasibility_0_1_15.json).

OpenSandbox support for ordinary execution is distinct from HartMesh-qualified immutable accepted material. Nonempty durable skills are supported only for the exact live-qualified profile and artifact.

No such OpenSandbox profile or artifact exists in this release. OpenSandbox
therefore remains `empty_only` for accepted durable material, and selecting any
non-disabled accepted-materialization profile fails before remote or model work.

## Phase 0 matrix

| Required primitive | Pinned surface result | Consequence |
| --- | --- | --- |
| Create by full OCI digest and read back the resolved digest | Unsupported. `SandboxInfo.image` carries the requested image specification; there is no resolved-image digest field. | `opensandbox_image_digest_readback_unsupported` |
| Persist metadata and rediscover by exact metadata | List filters, metadata patching, get, and reconnect surfaces are present. No live restart test was run after the hard blockers were found. | Surface only; not qualification evidence |
| Atomic compare-and-set ownership/lease | Unsupported. Metadata PATCH accepts only the patch body and has no resource version, ETag, expected epoch, or `If-Match` input. | `opensandbox_accepted_claim_cas_unsupported` |
| Renew expiry without losing metadata | Separate renewal and metadata surfaces are present. The live preservation scenario was not run. | Surface only; not qualification evidence |
| Trusted setup separate from final UID/GID 1000 | Candidate surfaces exist (`Volume.readOnly`, plus `RunCommandRequest.uid`/`gid`), but their combined backend enforcement has not been live-proven. | `opensandbox_trusted_setup_live_probe_not_run` |
| UID 1000 mutation and positive-read probes | Not run because trusted publication cannot first be established. | `opensandbox_read_only_live_probe_not_run` |
| Destroy and reconcile labeled expired/orphaned instances | List and destroy surfaces are present. The live reconciliation scenario was not run. | Surface only; not qualification evidence |

OpenSandbox's lifecycle specification explicitly describes metadata PATCH as
having no optimistic-locking semantics. The probe verifies vendored lifecycle
and execd specifications byte-for-byte from server tag `server/v0.1.14`, commit
`ef13d88f8479089ab6773556d7782b5d92fea53f`, before inspecting them. See the
exact upstream [sandbox lifecycle specification](https://github.com/opensandbox-group/OpenSandbox/blob/ef13d88f8479089ab6773556d7782b5d92fea53f/specs/sandbox-lifecycle.yml).
The missing primitives are tracked upstream in
[OpenSandbox issue #1690](https://github.com/opensandbox-group/OpenSandbox/issues/1690).

## Reproduce and verify

The documented backend `make test` recipe selects the exact optional SDK from
the lock so this offline probe is reproducible after the default `make install`.
That test-only selection does not promote OpenSandbox into the harness or
production dependency set. To run the probe directly from `backend/`:

```bash
uv run --project packages/harness --extra opensandbox \
  python tests/support/opensandbox_feasibility.py
```

The command emits one bounded canonical JSON object. Exit status `1` means the
probe reached a deterministic `no_go`; exit status `2` means the evidence is
invalid or merely unpassed. To parse the committed evidence without probing the
installed SDK:

```bash
uv run --project packages/harness --extra opensandbox \
  python tests/support/opensandbox_feasibility.py \
  --verify tests/fixtures/opensandbox_feasibility_0_1_15.json
```

The parser rejects unknown fields, missing primitives, version drift, digest
tampering, malformed timestamps, and a decision inconsistent with its blockers.
The active probe additionally verifies the exact SDK wheel's 336 package files
(1,662,982 bytes) against its RECORD hashes and a canonical aggregate digest,
plus both pinned server-specification SHA-256 digests. The vendored specifications
and provenance are recorded in
[`opensandbox_server_0_1_14/README.md`](../tests/fixtures/opensandbox_server_0_1_14/README.md).
It records no API keys, headers, provider bodies, or uploaded content.

## Implemented boundary

`deerflow.sandbox.accepted_material` owns the provider-neutral request, manifest,
lease, execution-evidence, capability, and adapter protocol. The existing
Kubernetes/AIO `rwx_verified_copy_v2` path is translated by
`AioAcceptedMaterializer` without changing its qualified tuple. OpenSandbox's
control-plane port keeps SDK objects and synchronous calls behind a bounded
async/redaction boundary. Its SDK adapter deliberately rejects `claim`, fenced
renewal/deletion, and trusted setup instead of emulating them in Gateway memory.
The stateful control-plane fake exists for contract testing only and is not
qualification evidence.

OpenSandbox consequently supplies no `AcceptedMaterializerSelection`, capability
profile, qualification, or `AcceptedSandboxSession` for durable nonempty work.
Ordinary OpenSandbox tools retain their existing lifecycle, but durable admission
does not fall back to that weaker route. The cross-provider capability matrix and
check-then-call semantics are documented in
[tenant-bound accepted sandbox execution](ACCEPTED_SANDBOX_EXECUTION.md).

Enabling the future scope
`durable_one_replica_opensandbox_immutable_skills_v1` requires a new Phase 0
artifact for exact supported versions, implementation of all live scenarios,
an independently verified canonical qualification artifact, startup/render
gates, and a release-policy update. A code-only fake or SDK surface result can
never satisfy that gate.

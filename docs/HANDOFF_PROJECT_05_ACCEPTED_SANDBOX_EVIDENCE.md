# Project 05 Handoff: Accepted Sandbox Evidence

Project 04 publishes the sandbox-side references Project 05 may add to retrieval
observations. They are links only; neither grants execution, cleanup, or replay
authority.

## Safe references

The deep module is
`backend/packages/harness/deerflow/sandbox/accepted_material.py`.

- `accepted_execution_evidence_reference(evidence)` returns
  `accepted-execution-<canonical-evidence-digest>`. A host-owned
  `AcceptedSandboxSessionBridge.execution_evidence_reference` exposes the same
  value without exposing the evidence object or provider resource.
- `AcceptedSandboxOperationV1.operation_ref` is a fresh
  `accepted-operation-<32-lowercase-hex>` identifier. Operation arguments are
  process-local and intentionally have no portable serializer.
- `current_accepted_sandbox_bridge()` resolves the bridge behind the executing
  context's declared accepted session, or `None` when the execution declared
  none. The declaration is the only carrier: nothing placed in a runtime
  context dict can stand in for it, so there is no value to forge. Project 05
  may read the evidence reference from the bridge. If retrieval itself submits
  a sandbox operation through the bridge, construct one
  `AcceptedSandboxOperationV1`, retain its `operation_ref`, and call
  `bridge.execute(operation)`; do not reach through the facade to recover a
  provider ID.

The ordinary `Sandbox` facade creates operation envelopes internally, so a
caller that needs an operation link must capture the envelope at its typed
per-call host seam. Do not infer an operation reference from tool name,
arguments, command text, a provider request ID, or runtime state.

## Evidence bindings available in V2

`AcceptedExecutionEvidenceV2` is canonical, strictly decoded, bounded, and
handle-free. Its relevant fields are:

- accepted run/attempt and tenant reference;
- provider kind and tenant-bound `provider_resource_commitment`;
- provider ownership epoch;
- runtime image, accepted skill snapshot/scope, materialization, verifier, and
  read-only proof digests;
- accepted invocation reference/digest;
- governed tool-plane base, user-overlay, projection, and effective digests;
- optional Project 01 batch-child attempt reference;
- capability-profile and qualification-evidence digests;
- qualified isolation facts and evidence digest.

Project 05 should normally store only the accepted-execution reference and
optional operation reference. It already receives the accepted invocation,
actor, tool-plane, and durable receipt anchors directly; copying the full
sandbox evidence would duplicate authority-adjacent facts and consume its
bounded observation budget.

V1 remains readable under its original contract and includes a raw provider
instance field. Never project that field into retrieval evidence. If a safe V2
link is required and only V1 is available, omit the optional retrieval link or
fail at the documented Project 05 boundary—do not hash or silently upgrade the
V1 provider handle yourself.

## Capability vocabulary

`AcceptedSandboxCapabilityProfileV1` separates declaration from
`AcceptedSandboxQualificationV1`. Its fields are:

- `material_capability` (`empty_only` or `immutable_read_only`);
- `atomic_provider_ownership_fencing`;
- `atomic_provider_operation_fencing`;
- `authoritative_shared_expiry`;
- `resolved_immutable_image`;
- `restricted_non_root_isolation`;
- `recoverable_resource_lookup`;
- `durable_one_replica`;
- `exact_two`;
- canonical profile digest.

Project 05 may record the existing profile digest or a coarse capability
version when useful, but must not reinterpret a declaration as live
qualification. Current AIO/Kubernetes is a baseline check-then-call adapter:
atomic operation fencing, process-loss lookup, and exact-two are false. One
operation in the validation/delegation gap may start; later calls and stale
terminal publication are refused once loss is observed.

The runtime qualification companion binds a portable topology-policy digest.
The provisioner separately resolves the current namespace UID, ServiceAccount,
and each PVC UID plus its bound PV name on every live sample; it does not claim
a PV UID. Those deployment-specific identifiers do not enter the companion or
Project 05 observations. Accepted execution
evidence may retain the exact current runtime-topology digest, but Project 05
should continue linking only its safe accepted-execution reference.

## Boundaries Project 05 must preserve

- Retrieval evidence is an external observation linked to the existing
  `DurableToolReceiptV1`; it is not accepted invocation material.
- The outer tool-receipt middleware remains the sole owner of the final
  sanitized/budgeted result digest.
- A sandbox evidence or operation reference is optional context, not a second
  receipt, query identity, provider lease, or cleanup capability.
- Do not store a provider sandbox ID, namespace/Pod/container name, endpoint,
  token, capability Secret, renewal handle, command, query, file content,
  provider body, or tool output.
- Do not treat sandbox qualification as evidence that a retrieved source is
  true, complete, current, or reproducible.
- Use the session bridge for indirect sandbox execution. Direct provider lookup
  bypasses the run/materializer checks and is forbidden in durable mode.

The full contract, race limitation, provider matrix, configuration, and
lifecycle semantics are in
`backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`.

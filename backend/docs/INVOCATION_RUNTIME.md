# Durable Invocation Runtime

`app.runtime.InvocationRuntime` is the single in-process application boundary for durable
HTTP, Scheduled Task, and authenticated native-channel launches. Scheduling and channel
delivery remain source-owned; normalization, accepted-fact sealing, durable admission, and
one worker attachment belong to the runtime. Checkpoint and artifact reservations are
auxiliary thread operations, not accepted invocations.

## Trust and sealing

Every source constructs an internal launch intent, but caller thread, assistant, agent,
body context, headers, queries, and metadata remain hints. Before admission the host:

1. authenticates the principal and, for channels, authenticates the provider event, looks
   up the connection, and revalidates its current owner;
2. resolves the thread, agent, source facts, normalized input, and execution-significant
   options;
3. creates a bounded base `InvocationOrigin` containing only independently authenticatable
   source/correlation evidence;
4. runs trusted Origin and run-context contributors; and
5. seals an immutable `AcceptedInvocation` and admits it with the normal active-thread
   conflict rule before attaching one worker.

The accepted object contains a minimal principal projection, sealed Origin, bound
thread/context references, resolved agent revision, normalized input/options, immutable
extension generation, and versioned principal, base-Origin, accepted-context,
runtime-identity, and contributor-execution digests. Contributors cannot replace host-owned
principal, thread, agent, source kind, or base-source fields.

## Contributor data and redaction

Public contributor contracts live in `deerflow-extension-api`, which remains independent
of the harness and Gateway. Contributor factories are startup-only, loader-attributed
capabilities. Calls are concurrent, each has a two-second timeout, and successful results
are composed in stable contribution-ID order.

Results contain only bounded namespaced scalar/list references. Persistable safe Origin
facts may enter `origin_json`; runtime-only values never do. Execution references and stable
secret-handle identifiers affect the accepted-context digest, correlation references do
not. Raw credentials, arbitrary objects, nested maps, and plugin exception text are never
persisted or reflected in diagnostics. Optional failures are omitted; a configured required
capability fails startup when absent/broken and fails an invocation closed when it times out
or returns invalid data.

## Persistence

Normal `runs` rows have nullable, backward-compatible columns for Origin and principal
projections and digests, accepted-context and agent-revision digests, safe agent revision
metadata, extension generation, and versioned empty decision evidence. Full resolved agent
material, credentials, runtime-only references, and normalized request content are not
serialized. Historical null rows remain readable. Auxiliary checkpoint/artifact operation
rows carry none of these facts.

## Pinned agent construction

Acceptance resolves `ResolvedAgentMaterialV1` once. Its versioned projector covers agent
storage source/version, validated agent configuration, SOUL bytes, resolved non-secret model
execution settings and opaque secret-handle IDs, effective tool groups/tools, enabled skill
manifest/content digests, and thinking/reasoning/planning/subagent defaults. Guard tests make
new graph-factory configuration fields choose explicit inclusion or exclusion.

The accepting worker receives and uses that exact captured object; lead and subagents inherit
its revision digest and extension generation for audit. After restart, the worker resolves
current material once. Only an equal digest allows that exact newly resolved object to become
the pinned factory input. A mismatch sets terminal error state with
`stop_reason=agent_revision_drift` immediately before construction, so no graph or model work
occurs. The runtime never reconstructs historical material or performs a compare followed by
a mutable-state reread.

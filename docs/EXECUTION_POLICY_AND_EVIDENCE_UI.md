# Execution policy and evidence UI

HartMesh resolves an `ExecutionBudgetV1` before every new durable invocation.
The server policy is the ceiling: callers may request stricter values in
`context.execution_budget`, and scheduled runs receive the stricter configured
scheduler profile, but neither can broaden server limits. The canonical budget
and digest are part of accepted invocation identity. Configuration edits affect
later admissions only; recovery reads the original budget. Older rows are shown
as `legacy`, not retroactively claimed as policy-controlled.

## Circuit breakers

`ExecutionPolicyEvaluator` is a pure transition over the accepted budget,
compact durable state, and one normalized observation. It covers turns,
total/category tool attempts, equivalent repeated calls, subagent batch
aggregates, retrieval, and sandbox operations, which are enforced by counting
sandbox-category shell/exec tool attempts against `max_sandbox_operations`. A
fenced compare-and-set stores compact state under the current run owner and
epoch. Only a successful threshold transition emits `policy.decision.v1`;
stale workers fail closed. Recovery validates state, budget, key ID, and
normalizer before work.

Three accepted budget fields are declared but inert in this checkout: no
runtime producer emits `no_progress` observations (accepted runs replace the
loop-detection heuristics with the durable turn counter), nothing measures
`max_sandbox_runtime_seconds`, and `max_delegation_depth` is bounded
structurally by the one-batch-layer contract rather than a counter. The
evaluator enforces them the moment an adapter observes them, but until then
they cannot warn or stop a run and must not be described as enforced. Batch
admission observes its worst-case attempt cost (`items × max_attempts`) after
acceptance, so a cumulative batch budget can be exceeded by at most one batch
admission before the stop lands.

Stable reasons include `turn_budget_exhausted`,
`tool_attempt_budget_exhausted`, `repeated_tool_loop`, `no_progress_loop`, the
`batch_*_budget_exhausted` family, `retrieval_budget_exhausted`,
`sandbox_operation_budget_exhausted`, `sandbox_runtime_budget_exhausted`,
`policy_equivalence_key_unavailable`,
`policy_equivalence_normalizer_unavailable`, and `policy_state_inconsistent`.
A stop prevents later controlled work but cannot undo an already-issued
external side effect.

## Private equivalence commitments

Repeated-call equality never uses the public receipt projection, which hides
values and can collapse different same-shape paths or queries. The policy module
normalizes transient arguments and computes a domain-separated HMAC-SHA-256
over tenant, run, tool, normalizer, and salient values. `read_file` ranges use
200-line buckets. Secret-shaped or unclassifiable inputs are excluded.

Durable deployments configure the startup-frozen keyring only through secrets:

```text
EXECUTION_POLICY_HMAC_KEYS={"2026-09":"<unpadded-base64url-32+-byte-key>"}
EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID=2026-09
```

Rotate additively: install the new key everywhere, restart the quiesced
topology, switch the active ID, and retain old keys until no accepted run needs
them. Exact-two topology identity includes only a non-secret confirmation.
Local mode uses a process-ephemeral key and is restart-unqualified. Key bytes,
normalized values, and commitments stay out of events, logs, metrics, model
context, evidence APIs, and bundles.

## Evidence summary and panel

Authorized clients use
`GET /api/threads/{thread_id}/runs/{run_id}/evidence`. The versioned
`hartmesh.run-evidence-summary` V1 response supplies public refs, a server-
ordered policy timeline, safe counters, and bounded admission, assembly/tool
plane, tool, batch, sandbox, retrieval, MCP, artifact, and export sections.
Section states are `available`, `not_applicable`, `unsupported`, `legacy`,
`pruned`, `unqualified`, or `error`.

The chat Evidence panel consumes only this projection. It supports native
keyboard-expandable sections, public-reference copy, active-run refresh,
retryable errors, and Project 06 bundle download. `qualified` requires a
relevant passing artifact; `unqualified` means a gate failed; `unverified`
means no qualifying evidence was supplied; `legacy` and `unsupported` are
explicit compatibility states. This checkout has no passing external
qualification artifact, so it must not render a green production claim.

Bundles remain unsigned and may contain sensitive user artifacts; download
requires current run-read authorization. Trace IDs are correlation aids only
and never affect admission, policy, authorization, public refs, or digests.

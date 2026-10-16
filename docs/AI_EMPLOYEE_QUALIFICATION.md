# AI assistant and AI employee consumer qualification

HartMesh supplies Work, explicit execution, human-input requests, current access
and attributed history. Skills own business reasoning and working-file formats;
optional extensions own specialized human-facing controls. No reporting,
procedure or supplier fields are added to core tables, dispatch or authorization.

The reporting skill now validates all saved preferences before applying them.
It offers read/validate/save/patch/reset with revisions and exact retry identity,
and temporary overrides that do not change saved bytes. Managed v1 preferences
add optional `_mutation` metadata. Reset retains an empty v1 document with a new
revision. Valid legacy v1 documents remain readable without rewriting. Unknown
fields, versions or invalid fields are rejected as a whole. Report and passive
view schemas are unchanged; retained bundles include a hashed
`preferences-used.json` snapshot through later prose/render operations.

The standalone [procedure and supplier examples](../examples/skills/README.md)
have independent working-data formats and executable operations. Their files
follow current workspace scope: a persistent Space of its own for a qualified
AI assistant (a person’s own) or AI employee (the company’s), conversation scope
otherwise. Working files are mutable data, not host policy,
authenticated provenance or approval. Existing quota, ACL, native attachment and
file rules remain owned by Storage Spaces. Files reach an agent through its file
routes at any time, including while its workspace runs, by one direct Gateway
write; there is no exclusive write window.

The optional [quote response plugin](../examples/deerflow-extension-work-input/README.md)
checks its own domain fields and submits human-attributed facts through the public
facade. Core Attention remains complete without it. It cannot accept an outcome,
resolve a decision or activate Work by interpreting supplied text.

## Reproducible evidence tiers

| Tier | Coverage | Limits |
| --- | --- | --- |
| Consumer files | Reporting and both independent examples: validation, lasting vs temporary choices, output inspection, exact retries, conflicts, reset, concurrent processes and failed publication | Ordinary disk fixtures do not establish native quota or containment |
| Deterministic lifecycle | `test_work_consumer_journeys.py`: four consumers use production tools, run worker, Work/input/review services, new conversations, a reopened SQL/service stack and two successive HTTP Gateway-router processes; human answers affect final output, intermediate artifacts remain, current source revocation is applied | Graph/model, process identity and filesystem backing are injected; production startup/authentication have separate Docker acceptance coverage |
| Native storage | Required no-skip `test_storage_spaces_volume_native.py` uses the pinned shipped AIO image, a qualified Space, two human requesters, all three consumer writers, real resident admission, host containment and replacement; Docker admission is a nonce probe of pool/bound-view bytes | Proves filesystem/runtime behavior, not model judgment; instance Work activation is a separate lifecycle |
| Compose bundle | Required `bundle-instances` starts the shipped profile with the built backend and scripted model; 27 journeys prove prepared mounts, accounts/seats, ordinary chat, My Files/Shared/Projects, host-attributed publication, full-files refusal/recovery, AI assistant persistence, Work outcomes/capacity and restart containment; live/closed file transfers, 40 MiB hashes, Stop, count refusals and same-kind slot reuse, denied sites, supervisor questions, operator closure and workspace loss | runc under an alias; no model judgment or tenant-class proof; seat reclamation uses two allowlist chats and verifies full paired-set absence |
| Tenant class | A separately recorded per-release `tenant_class` receipt covers the actual VM class under `runsc`: preflight, default first start, runtime placement, real browser transfers, cold start, full storage, egress, two conversations, hold, reboot and snapshot restore | Requires a person’s qualified run; GitHub runc/scripted results cannot stand in for it, and this document does not claim it has passed |
| Browser | `work-input-plugin.spec.ts` loads the actual optional module and checks current request binding, invalid input, exact uncertain retries, no implicit run and generic fallback; existing Work/Attention journeys cover resumption | Deterministic HTTP responses; real SQL is qualified separately |
| Actual model | Bounded reporting and independent-workload runs require approved test inference | Pending when no approved inference capability is available; deterministic results are not actual-model evidence |

Existing Work tests additionally cover required human decisions versus factual
responses, memory-disabled protected history, two-instance separation, derived
work policy, source audience, cancellation, cleanup uncertainty and exact recovery.
No result promises idle monitoring, external delivery, procurement or automated
activation. Actual-model smoke, when recorded, is a bounded observation rather
than a reliability percentage.

Run the consumer/lifecycle tests from `backend/` with the existing environment:

```bash
uv run --no-sync pytest tests/skills/business_report tests/test_consumer_working_data.py tests/test_work_consumer_journeys.py tests/test_work_input_example.py -q
```

Reporting requires the document libraries supplied by the sandbox image. The
skill-script CI tier asserts their presence. PostgreSQL cases use a disposable
`TEST_POSTGRES_URI`. Full suites use a task-specific disk-backed `TMPDIR`, bounded
memory and monitored logs. Native cases belong to their required qualified CI
runner and may not be represented as passing when skipped locally.

Schema impact: **no application SQL migration or definition-policy change**.
Consumer managed-file metadata changes are described above, independently of SQL.
No release or background activation is introduced by this qualification work.

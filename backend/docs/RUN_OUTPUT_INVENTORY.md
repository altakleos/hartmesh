# Terminal output inventory

Run creation can request `output_capture: {"version": 2}` to seal a generic
inventory of terminal presentations alongside the existing version 1 output
receipt. Consumers can account for files and structured output before publishing
a reply. Skills and extensions retain their business interpretation and UX;
the Harness contains no SMB business rules.

Omission and version 1 preserve existing behavior. Version 2 requires the actual
SQL run repository and the same durable database event store with explicit
inventory capabilities. Unsupported stores refuse before execution. Idempotent
reuse cannot change capture versions. No table, migration, configuration version
or existing receipt format changes.

## Coverage

The actual first-party SDK assembly issues a host-only profile bound to its
graph, state schema, normalized middleware and standard `StructuredTool`
adapters. Unknown middleware, state roles or tool adapters refuse qualification
before model execution. No observers are required, and unused assembly
descriptors still skip their expensive hashes. Refusal after startup records
unsupported/failed evidence; it cannot masquerade as an unstarted run.

The worker observes current raw tool/Command presentation fragments, retained
root output, the server runtime presentation registry and regular output-file
changes. An unchanged old path explicitly offered again counts. Inherited
artifact lists and old controls alone do not become new offers. Runtime file
presentation can update an existing AI-message ID; that witness remains separate
from the original canonical model event. Provider tags alone establish no host
handover. Auxiliary reasoning, usage and telemetry stay outside fragment hashes.

Scans cover created or modified regular files under `/mnt/user-data/outputs`,
using actual frozen internal/spill exclusions. The same bounded confined walk
reports roots as opened, missing or unavailable. Empty maps do not prove an
examined empty root. Missing/disabled/truncated scans and chats without a
qualified personal file view cannot claim complete-empty enumeration.
Cleanup invalidates scans taken before its effects.

| Document coverage | Meaning |
| --- | --- |
| `complete` | One completed qualified root, accounted declared carriers and complete before/final scans. `items: []` is positive empty evidence. |
| `incomplete` | A closed reason and `observed_items`, only a known subset. An empty subset does not prove absence. |
| `unsupported` | The profile or surface could not be qualified; observations remain a subset. |

File descriptors carry exact virtual references, observed surfaces and witnesses.
Structured descriptors carry a presentation-fragment digest and witness. Neither
grants retrieval, action execution or immutable bytes. The claim excludes arbitrary
filesystem contents, remote effects, native containment and business completion.
The whole canonical document is bounded to 256 KiB and 256 descriptors, including
all witnesses. Fragment traversal also bounds depth to 12 and nodes to 8,192.
Live overflow is explicit incomplete evidence; identities are never shortened.
Malformed stored evidence or an unrepresentable required envelope refuses proof.

## Persistence and cancellation

One `run.output_inventory` event is appended and its actual returned row audited.
Status, `output_evidence` and `inventory_evidence` then seal in the same terminal
CAS. The companion binds the full output-receipt digest, inventory sequence and
document digest. A lost append reply permits only exact read-only recovery;
it never triggers another append. Unknown CAS replies must confirm both proofs.
Terminal rows cannot acquire missing proofs later; progress preserves both slots.

Four actual lifecycle transitions can seal unavailable inventory without an
event: unstarted closure, locked lease takeover, atomic predecessor replacement,
and completed cancellation after an audited inventory append. These are host
facts, never DTO-selected permissions. The last retains the old event unchanged
and attests non-enumeration after cleanup, even if the cancelled receipt SHA is
unchanged. A host-only continuation binds the original record, actual decision,
exact completed action and version/owner/lease fences. Failed or uncertain cleanup
cannot mint that outcome. None of these forms proves empty output or containment.

## Authorized opt-in reads

The existing output endpoint accepts
`X-DeerFlow-Require-Output-Inventory: 1` and acknowledges supported reads with
`X-DeerFlow-Output-Inventory-Version: 1`, alongside the existing channel read
acknowledgement. Its version 2 result nests the unchanged version 1 `output`
projection and `inventory: {state, evidence, document, reason}`.

`matched` proves event integrity, independently of coverage or feature handling.
`pending` requires actual active authority and both proofs absent. `unavailable`
has no document and preserves the exact lifecycle or successfully observed
missing/changed-source reason. Both sources are audited, followed by a fresh
read of status, scope, admission, target and both proofs, then current
authorization before acknowledgement. Unsupported versions, malformed/duplicate/
half proofs, terminal-unsealed rows and uncertain reads refuse acknowledgement.
Authorized supported misses return acknowledged 404; foreign/revoked scope does not.

`ChannelExecutionFacade(capture_output=True, capture_inventory=True)` uses this
mode through the same HTTP owner and `get_run_output` method. Both options are
strict readonly booleans. Unsupported responses cause no downgrade, second POST,
second client or model retry. Default reads remain version 1, even for a version 2 run.

This prerequisite activates no Telegram dispatcher, sender, feature consumer or
restart reconciler. Schema stays `0044_telegram_assistant_ids`; config stays 55.

# Durable run output evidence

This is an inactive runtime foundation. Worker capture, its creating-request
admission, the server output endpoint and managed Telegram publication follow
separately. The current Gateway rejects `output_capture` as an unknown creating
field before preparation or model execution. No ordinary run enables capture.

## Generic contract

`runtime/runs/output.py` defines strict immutable version-1 capture intent and
receipts. A receipt contains the exact raw owner, thread and run. Text evidence
also names a genuine assistant message, its positive durable event sequence and
the SHA-256 of its exact UTF-8 text. Never trim, normalize or stringify the text.
Message identities are opaque; never manufacture an assistant or human ID.
The genuine input-message ID is required but nullable for autonomous platform
runs. The Harness knows nothing about Telegram publications or SMB business rules.

An unavailable receipt contains one closed reason and no synthetic output
identity. Text and selector-refusal receipts require success; failed receipts
require error or timeout; cancelled receipts require interrupted or the rollback
error tail. The stored row supplies status, rather than a duplicated receipt field.
Malformed present intent or evidence fails closed; only an absent key is absence.

## Durable authority

Only the actual SQL `RunRepository` advertises `supports_output_receipts`.
Memory and legacy custom stores preserve their ordinary behavior and refuse
the new proof APIs. `RunManager.finalize_output` requires an opted-in locally
owned run and uses terminal recovery's bounded conditional write. New admissions
discard caller-supplied `output_evidence`; idempotent reuse preserves stored proof.

The SQL writer locks the change clock before the run row. It merges only the
reserved receipt slot from authoritative stored kwargs and conditions the write
on owner, change sequence, source status, cancellation and current lease. A text
seal cannot overwrite an already observed cancellation, even with a matching CAS
action. Status and receipt commit together. Ambiguous writes are read back before
any recovery attempt; only exact status plus exact receipt confirms output.
A terminal row cannot acquire a new receipt.
Only an already identical cancelled receipt can survive interrupted-to-error
cleanup, and the stored cancellation action must be rollback. Proof operations
retain started SQL reads, cursor cleanup and transactions through cancellation;
tests verify drainage with an independent writer commit.

`get_output_snapshot` always reads the actual store, validates observed scope and
returns detached, deeply frozen data. A cached success is not durable authority.
An observed wrong owner is a scope error; absence requires a completed supported
lookup. The `local_finalizing` observation is never a durable completion seal.

## Prepared channel protocol

The host-only `ChannelExecutionFacade.capture_output` option defaults to literal
`False`. Explicit `True` adds the typed intent to a detached creating request;
provider metadata cannot switch it. Unsupported responses never trigger a second
creating POST. The prepared `get_run_output` reader requires the existing strict
acknowledgement and exact original scope. No server output route is shipped yet.

Its closed projection keeps channel admission and generic output evidence
separate. Complete private-channel text requires a genuine human input identity,
matching terminal status and the exact source digest. A sealed unavailable result
may have no input identity when execution failed before input capture; it exposes
no model source and retains the server-owned publication admission scope. Source
loss never rewrites the historical receipt. Unknown versions, malformed proof,
scope changes and unacknowledged absence remain uncertainty.

Post-response authorization remains inside the existing retained HTTP owner.
Shutdown drains started codec and SQL cleanup before releasing ownership. These
client and data primitives do not establish worker-source provenance or enable
managed-bot model execution, publication recovery or orphan-run sealing.

No schema or configuration version changes are required: the receipt uses the
existing run kwargs JSON. Schema head remains `0043_telegram_inbox_chat_scope`;
configuration remains version 55.

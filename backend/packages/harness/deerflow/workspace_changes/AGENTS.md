# Workspace change evidence

`scanner.py` enumerates only run-owned workspace/outputs roots. Open files
relative to the walked directory descriptor with no-follow/nonblocking flags;
derive hashes, text and metadata from that same verified regular file. Never
read sensitive paths or follow symlinks. Outputs-only delivery snapshots have
their own file/walk budget, independent of scratch workspace saturation.

`WorkspaceChangeLimits.max_total_text_bytes` defaults to 1 MiB per snapshot,
shared across roots and text caches. Reserve a conservative UTF-8 bound before
decoding, refund unused bytes only after successful decoding, and retain the
reservation on invalid text. UTF-16 reservations can omit a near-budget file
whose actual UTF-8 would fit. Binary samples, sensitive paths and unselected
files bypass text capture. Small eligible files still receive full hashes after
the text budget is exhausted; omissions have reason `truncated` and cannot hide
same-size/same-mtime edits.

Text exhaustion is distinct from `WorkspaceSnapshot.truncated`, which describes
incomplete enumeration and invalidates delivery evidence. A text omission marks
the affected diff and change summary truncated. Keep the independent diff-size,
file-size, file-count and directory-walk caps. `recorder.py` does filesystem work
off the event loop; cancellation drains text scans before removing their cache.
Preserve that ownership when changing capture or cleanup. Regressions live in
`backend/tests/test_workspace_changes.py` and the strict blocking-I/O suite.

`handoff.py` offers the worker's complete metadata-only outputs baseline to
RuntimeDeliveryMiddleware through a private ContextVar capability during the
first graph stream. Claim/close are atomic and single-use, bound to effective
owner, thread, run and exact spill exclusions. Reject delegated command scopes
before claiming; reject partial, cached or non-output evidence. A mismatch
invalidates the offer. Stream exit closes it before resetting the ContextVar,
including exceptions/cancellation, so copied child contexts cannot reuse it.
Never put this capability in runtime config, graph state or public metadata.
Continuation baselines, middleware after scans and terminal worker scans remain
fresh. Setup files created after the worker baseline are part of the same
delivery interval and can now be handed over. `abefore_agent` clears any old
same-run baseline before capture; failed resumes must not reuse an interrupted
snapshot. `test_output_snapshot_handoff.py` counts actual scanner traversals
through a compiled graph and compares receipts with independent capture.

# Workspace change evidence

Inventory capture optionally returns exact same-walk root observations and frozen
exclusions. Only actually opened before/final roots with complete enumeration
prove empty output; missing roots and no-files adapters do not. Ordinary snapshot
calls retain their defaults. Never add a preflight open or a second scanner.

Native ordinary chats use `spaces/chat_snapshots.py`: refresh current User
admission on the owning Gateway loop before and after scanning, and require the
same role/root incarnation. Open roots, every walked directory and every regular
file through the pinned confined filesystem. A directory device check alone
cannot reject same-device bind mounts or file mounts; retain `NO_XDEV` at every
entry and compare walked/admitted directory inodes. The scanner's optional
descriptor callbacks keep these checks independent of its default host-root
behavior. Native text evidence uses the existing memory budget, with no legacy
disk cache. Cancellation drains the complete native scan before closing the
volume. A chat without personal files returns empty evidence without filesystem
work; a lost admission cannot publish a partial successful delivery scan.

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

## Snapshot cancellation (`recorder.py`)

The scanner opens the lexical root with `files.store.open_directory_source`,
uses descriptor-relative `fwalk`, and opens each regular file once with no-follow.
Metadata, sample, digest and text come from that file; per-file reads stay bounded
and size/mtime changes make enumeration incomplete. An entry that is gone between
the listing and its open is skipped, not truncation; any other read failure still
truncates. Symlinks remain metadata-only stubs. Unsupported safe reads must not be reported as complete delivery evidence.

After `_prepare_capture()` hands off roots, cancellation must drain text scans
(`include_text=True`) before removing the cache the worker may still access.
Metadata scans (`include_text=False`) own no cache: cancel promptly, let the worker
continue, and consume/log its outcome in a completion callback. Prepare-stage
cancellation retains its handoff/reclaim path. Regressions in
`tests/blocking_io/test_workspace_changes_cancellation.py` must cover prompt
metadata cancellation and text-cache drain/cleanup.

## Gateway workspace change review

`packages/harness/deerflow/workspace_changes/` captures a pre-run and post-run snapshot of the thread-owned `workspace` and
`outputs` directories. `runtime/runs/worker.py` performs the filesystem scan via
`asyncio.to_thread` and writes a `workspace_changes` event with category
`workspace` when changes exist. Uploads are intentionally excluded. Text diffs
are size-limited; binary, large, and sensitive-looking paths are persisted as
metadata only. Internal process-feedback directories never count as changes:
the scanner's `EXCLUDED_DIR_NAMES` drops `BROWSER_FRAMES_DIRNAME` (transient
browser screenshots) and `TOOL_RESULTS_DIRNAME` (the tool-output budget
middleware's default externalization subdir, `constants.py` is the shared
source of truth for both writers and the scanner), and the worker threads the
configured `tool_output.storage_subdir` through the snapshot capture as an
extra excluded dir name so custom storage locations stay excluded too.

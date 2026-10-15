# AIO Sandbox

`owner_admission.py` shares only the provider metadata lock. Distinct live borrows
retain growing actual slot sets; prewarm holds a separate reference through actual
Future completion, with callbacks registered outside that lock. Reserve owner AND
target slot before native retirement's first await; pending evidence matches owner
OR slot. Cache, warm, discovery, orphan and final publication retain/recheck admission.
No provider lock encloses Docker/journal/store IO or the global lease holder gate.
Positively unrelated starts and provider-owned mountless clients remain usable.
`NativeRetirementBusy` refuses reservation before publication or physical effects.
Gateway reclamation may wait up to five seconds and retry only scope entry;
unknown failures and errors after entry must never replay containment.

`native_retirement.py` retains reservation through caller work and drained final
proof; expired scopes refuse. Strict all-prefix/all-state inventory is correlation,
never cleanup authority. Reuse canonical holder/local/store/heartbeat teardown.
Qualified open sets use full-ID stop/removal and positive CID+name absence, including
retries after lost proof. If auto-removal finishes between post-stop inventory
reads, require fresh CID AND name absence with current host proof and the same
deadline; send no additional destructive command from an ambiguous inventory.
Restricted sets use `native_resource_set.py` to inspect original primary, relay
and networks, labels and exact membership. Persist the complete set before any
effect; survivor recovery never derives identities from current names or policy.
Resident census equality excludes only `ExecIDs` and `State.Health`. Compare all
other inspection facts and qualify the paired graph against the fresh primary.
Normalize mount list order using full records; retain all fields and duplicates.
Mismatch logs expose only known top-level field names, never unknown names/values.
Absence replies require return code 1, empty output and an exact known template
bound to the inspected reference, including Docker 29's lowercase object error.
Unexpected replies log bounded structural facts, never names or payload contents.
`native_teardown_execution.py` binds cleanup to the exact live canonical holder,
provider reservation, lease, runtime and optional owner/target reservation.
Observed lease loss and scope exit invalidate copied capabilities. Reserve before
closing a client or untracking it. Remove primary, relay, internal and egress in
that order, by full ID, with fresh proof before each effect. Journal-only survivors
remain discoverable across restart; all original IDs AND names must be absent
before journal fsync, index removal, tracking and port release. Completed handoffs
repeat absence-only proof without publication or effects. Unknown tracking,
held/foreign owners and untransferred results stay
reserved. Process-local slot/containment facts never serialize. Host eligibility
belongs outside this generic provider and no provider callback runs under SQL.

External capacity callbacks are synchronous memory-only slot reads/changes under
the metadata lock. Physical eviction runs outside it, after warm-chat eviction
and fresh admission checks. Drain async effects across cancellation; prewarm
never evicts externally. See [registry rules](../../../../../docs/RESIDENT_REGISTRY.md).

Optional `ChatStorageHost.admission` is a private, generic exact-provider
lifecycle capability. Capture it before constructor reconciliation/threads;
explicitly bind cached providers with `_bind_runtime_admission`. Absence leaves
existing admission unchanged. The host installs complete accounting callbacks
and opens the capability in one `external_changed` critical section; opening
it does not certify qualification or restoration. Close/seal through
`_close_runtime_admission` to serialize publication and wake capacity waiters.
Check before starting/reserved shortcuts, speculative submission and every
cache/warm/discovered/new-client publication. Deferred orphan adoption retains
its actual identity. Closed admission must still permit qualification and cleanup:
the private `_get_for_cleanup` hook is for lease command-scope release only,
with no activity refresh or acquisition. Drain already submitted physical work
before opening; preserve starting, warm, active and native evidence accounting.
Lease binding and existing-holder return also check the capability; those checks
are memory-only and never acquire the provider metadata lock under the holder lock.
Regressions: `test_provider_lifecycle_admission.py`.

Gateway bootstrap accounting uses an opaque permit pinned to the exact
provider, admission transition revision, pool owner and native bridge. It is
valid only while that lifecycle stays closed and the native binding remains
qualified. Open/close cannot revive a permit. Parking retains the original
`SandboxInfo` in the existing warm pool, keeps ownership/grace, retirement and
native evidence checks, and constructs no client. Its memory-only accounting
marker prevents warm SDK reuse; ordinary discovery must qualify endpoint/policy.
Later registration cannot replace that charge with a different immutable CID.
Ending a permit never drops retained charges or opens admission.
`LocalContainerBackend._bootstrap_primary_inventory` is bounded and read-only,
includes all states/prefixes of labelled primaries, and checks exact actual IDs,
owner, slots, duplicate names/SIDs, resource facts and the native bridge before
returning. Explicit NoUser facts permit zero slots only with the exact production
operational mount projection; trusted integration config/locks/data modes remain
exact. Missing and foreign attribution refuse completeness;
never fabricate `mountless_provider=self` for an old container. These seams
do not certify a complete platform budget alone. Gateway additionally correlates
all-state resident and paired inventories with full retained SQL mounts; unknown
secondary resources or preparations without an original CID keep admission closed.
Tests: `test_bootstrap_sandbox_accounting.py`, `test_bootstrap_primary_inventory.py`.

Full acquire/acquire_async operations register producers before their first await
or shortcut. Bootstrap stops the existing reaper and drains actual acquisitions,
prewarm Futures, owner references, starting and teardown work before restoration.
Cancellation of operational projection/thread-lock offloads drains their worker;
capture a late file descriptor inside that worker so finalization still closes it.
Normal prewarm restarts only after complete capacity publication.

Private `resident_preparation.py` uses committed attachment tokens and the exact
pool-owner runner; never invent chat/human labels or use the chat journal for it.
Expire capabilities after drained workers, consume once, and retain every bound
slot through CID commit. Residents stay stopped/unpublished; chat defaults remain.
Before CID return, capture and publish a complete v5 actorless original graph.
`native_resident_index.py` restores its separate revision/accounting. Prepared
records permit unrelated chat creation and RO Company sharing. Activation/reuse
require exact prepared record, current plan/roots/policy and complete fresh graph,
under a retained opaque resident owner/slot reference. Use preparation's common
network-target normalization. Retiring/missing records grant no SDK authority.
`resident_containment.py` consumes original records only: qualify, publish retiring,
effect exact IDs, retain evidence through SQL commit, then fresh absence/fsync.
Uncertain journal transitions restore accounting even while closing, preserving
the original exception and charge. Incomplete resident records block every touched
backing's retirement, including RO companions. They never enter chat cleanup.

Native bridge qualification/current proof restores all typed indexes from one
mixed snapshot before publication. `native_teardown.py` retains bounded pending
accounting, exact member ID/typed-name collision guards and restore tombstones.
The journal revision closes readiness until the restored set exactly matches.
Only the bridge's typed `_NativeAccountingStale` identifies this snapshot mismatch.
Gateway reconciliation may requalify that same bridge/provider/owner once, after
resume has released SQL admission and the Home lock. Retry complete fresh row,
run, authority and graph reads under the original deadline; never retry unknown
graph replies or classify errors by message. Failed refresh retains the charge
and closes accounting. Requalification never occurs under metadata or SQL locks.
An absent primary still consumes capacity; deduplicate only its exact known ID.
Matching owner/name/explicit-target admission refuses unresolved evidence.
Independent same-owner sets remain separately counted and can be cleaned under
exact record capabilities; incidental Shared correlation is never an inferred
retirement target. No metadata lock encloses journal, Docker or store IO.
Ordinary reconciliation resumes same-daemon `target=None` records before running
primary enumeration. Limit each pass to four rotating attempts and the inherited
60-second Docker deadline, including initial proof. Retain all actual slot
admission through canonical cleanup; recheck exact record retention after admission
and inside the canonical holder reservation before claiming or inserting tracking
to prevent stale-snapshot resurrection. Held entries never starve later records;
foreign and targeted records remain counted without ordinary cleanup authority.
Cleanup invoked during another owner's acquire validates that inherited reference
against this provider's live gate, retains a separate admission for the actual
cleanup owner/slots and restores the original context on every exit. Never borrow
a different owner or relax general admission; stale or foreign-gate references
still refuse before publication or effects.
Canonical cleanup restores mixed accounting with `qualify(teardown=True)` during
closing, including uncertain-publication and terminal-sync retries. It requires
prior successful installation and fresh owning-runtime/daemon proof. Normal
qualification stays refused and memory readiness remains closed to new work.

Managed native chat primaries require installed `native_creation.py` accounting and the
exact owner-held journal. Persist pending before submission; preserve the existing
run/create timeout policy. Strict actual daemon/ID/name/attempt/slot inspection
precedes verified handoff then durable ACK. Opaque worker reservations fence
starting/count transfer across sync, async and prewarm cancellation. No metadata
lock encloses journal/Docker/store IO. Partial journal publication restores pending
accounting or closes admission. Pending denies guessed adoption/replacement/removal;
known exact returned or live tracked IDs may use canonical fenced cleanup.
Confirmed absence retires acknowledged handoffs only; unacknowledged evidence stays
reserved. SDK-less quarantine retains actual identity, never a guessed client.
Unconfirmed create warnings expose bounded cause types, source locations, errno,
return code and retained attempt/submission facts. Never log exception messages,
subprocess argv/output, environment or relay tokens at this boundary.

With managed storage off, `_get_thread_mounts` retains the release-45 binds and
does not resolve a Space. Storage-on ordinary chats use a Gateway-owned
`ChatStorageHost` callback before acquire, prewarm, cache/warm/discovery adoption,
lease reuse and tool-client return. Exact actual Docker mounts, immutable
container identity, owner/thread and physical slot labels gate each candidate.
No qualified User Space means no user-data or ACP bind; Company alone unavailable
omits Shared. Extra mounts cannot overlay those destinations or expose any pool
slot, including unassigned slots; check planned mounts before creation and actual
mounts before return. Without a scoped native projection, reject storage-on mounts
before resolving default paths; `peek_scoped_paths()` never creates legacy roots.
Policy skill views stay in the operational area.
First-conversation capacity is checked before any sandbox build; existing thread
directories keep history readable, and unassigned User storage stays mountless.

A known mount-profile mismatch may retire a client only through the canonical
no-holder cleanup reservation and ordinary ownership fencing, then reacquire
once after absence proof. Held clients, unknown inspection/identity and admission
failures cannot authorize replacement. Keep provider and lease metadata locks
short; Docker/SQL work runs outside them. Bound-host reset/shutdown cannot join
prewarm on the owner loop: the Gateway drains them off-loop while admission is
still alive, then closes the bridge before SQL. Resident topology has a separate
storage admission and containment lifecycle.

`LocalContainerBackend._start_container(start=False)` prepares a stopped Docker
environment with the existing builder. Only host storage admission may activate
it after durable exact-ID registration and mount/containment verification. This
adds no thread identity or lease-based writer proof; see `deerflow/spaces/AGENTS.md`.

Instance SDK clients may use host-owned immutable-container control transport and
explicit admitted download roots. Legacy clients retain their existing root guard.
Keep the same HTTP protocol and SDK; no IP fallback or requester mounts are allowed.

Prewarming admits at most one speculative create per provider, before executor
submission. Busy requests skip immediately; the dedicated one-worker executor
has no admission queue and consumes neither default nor real-acquire workers.
In-process and cross-process locks are nonblocking for speculation. Cancellation
signals the existing readiness stop and drains creation/cleanup; reset and
shutdown close admission and drain the worker before serializer/store teardown.
Capacity, ownership, failed-destroy quarantine and unclaimed reaping still use
the ordinary provider lifecycle. Regressions: `test_sandbox_prewarm.py`.

## Stdin contracts per transport

AIO has two distinct stdin contracts: the persistent `/v1/shell` transport is a
PTY, while `/v1/bash` uses a subprocess pipe that stays open for writes. Keep
persistent shell commands unchanged: the Lark broker shim already ignores TTY
input. Only fresh `_run_bash_exec` calls prefix the original command with
`exec < /dev/null` so default stdin gets immediate EOF; explicit
pipes/heredocs/files still override fd0. Those sessions are created and
released per invocation, so the prefix never closes a reusable terminal or
alters its state. Avoid brace-group/eval wrappers: they shift top-level
parsing (alias/extglob timing), disturb `PIPESTATUS` across calls, can fire an
`ERR` trap an extra time, or append a delimiter a trailing backslash consumes.

The broker shim forwards pipe/file input only after EOF: an idle pipe exits
124 without a broker request, and read errors fail rather than becoming empty
input. Window overrides must be finite positive seconds, at most 600. Broker
INFO logs contain argc/exit/elapsed only, never argument values.

Regressions: `tests/test_aio_sandbox.py`, `tests/test_lark_broker.py`.

## Persistent shell cancellation

Use an explicitly created default shell session from the first call; omitting
`id` creates a fresh AIO session and loses cwd, exports and cancellation targeting.
Only the first command exports a session token and records the shell PID plus
Linux start ticks in a private marker. Later commands stay unchanged. Abort
shortens the session idle wait, kills the matching parent shell before sweeping
its current descendants, then discards the session. Killing the foreground
child first allows trailing statements to run. Before each later call, snapshot existing token-bearing PID/start-tick
identities on a separate control shell. The abort sweep preserves those jobs
and their descendants, including jobs started just before the cancelled call.
Age rounding cannot distinguish adjacent calls. An aborted in-flight command fences recovery,
including missing-session 404s, and an already aborted call cannot dispatch.
Every abort RPC has a timeout and disables retries. Warm-pool release detaches
the drained default shell before closing host sockets; reclaim restores its
id and token from a process-local `SandboxInfo.default_shell_state` that never
appears in persisted metadata. Scoped shells still close with their execution.

Regression: `tests/test_sandbox_command_abort.py` uses real Bash processes for
persistence, trailing statements, old jobs and recovery; `test_aio_sandbox.py`
pins the session lifecycle and unchanged-command contract.

Shell exec is submitted asynchronously and polled within the command deadline;
the final console record supplies this command's output (view output includes
older commands). This lets a cancelled caller drain even when an image's hard
timeout ignores idle changes. The HartMesh image also makes the vendor executor
loop observe its closed flag, so deleted sessions release their server workers;
the real-image smoke check covers a pending command with a hard timeout.

Shell capacity reserves `2 * subagent_runtime.max_running + 3`: persistent
lead/subagent shells, their temporary snapshot controls, and an abort control.
Accepted asynchronous commands never replay on a polling error; missing-session
recovery applies only before dispatch was accepted.

Cancelled callers wait for the abort control's bounded completion before normal
session cleanup. Cleanup itself kills foreground processes and must not overtake
the parent-shell kill, or Bash can execute the rest of the cancelled command.

`execution_outcome_uncertain` is monotonic per SDK client. Lost command/mutating-file
replies and failed session cleanup prevent Work completion confirmation even when
socket close returns. `requires_container_recycle` also covers pending/ambiguous
session creation. Neither flag grants permission to retire a native storage writer.

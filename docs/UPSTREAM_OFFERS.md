# Upstream offers

HartMesh carries its sandbox session model behind the provider seam so that
upstream DeerFlow's lease manager, middleware, tools, and providers can be
taken verbatim. A few small seams would let HartMesh delete the hunks it still
patches into upstream files and would serve upstream's own roadmap (the
per-user quota RFC, prewarm #5002, lease work #5128, tool receipts #4651). Each
seam is prepared as a standalone patch against `deerflow/main`, with tests, on
an `upstream-offer/*` branch of this repository. Nothing here has been
submitted; submission is a maintainer decision.

| Offer | Status | Branch | Upstream files |
| --- | --- | --- | --- |
| Capability negotiation | patch ready | `upstream-offer/capability-negotiation` | `sandbox/sandbox_provider.py`, new `sandbox/capabilities.py` |
| Declared-sandbox resolver | patch ready | `upstream-offer/declared-sandbox-resolver` | new `sandbox/resolution.py`, `sandbox/middleware.py`, `sandbox/tools.py` |
| Admission before acquire | patch ready | `upstream-offer/acquire-admission` | `sandbox/sandbox_provider.py`, `sandbox/exceptions.py`, `sandbox/lease.py`, `sandbox/middleware.py`, `sandbox/tools.py` |
| Lease observer | patch ready | `upstream-offer/lease-observer` | `sandbox/lease.py` |
| Acquire-epoch and lost-sandbox guards | already upstream | none | `community/aio_sandbox/aio_sandbox_provider.py` |
| The egress verdict belongs to the command | proposal | none | `sandbox/middleware.py`, `sandbox/sandbox.py`, providers |

Each branch is one commit on top of `deerflow/main` (`27b2b676` at the time
of writing), formatted with upstream's ruff configuration and tested with the
upstream suites that cover the touched modules. None of them edits upstream's
sandbox guide, because that guide sits 185 bytes under upstream's own local
soft budget; a submission that must touch it has to trim it first.

## How to submit one

1. `git fetch deerflow && git rebase deerflow/main upstream-offer/<name>` and
   rerun the tests named in the commit message from a clean upstream checkout.
2. Push the branch to a fork of `bytedance/deer-flow` and open the pull request
   with the description below; drop the `Claude-Session` trailer if the
   maintainers prefer plain commits.
3. Once merged, take the upstream commit on the next sync and delete the
   HartMesh code listed under "HartMesh drops".

## Capability negotiation

`SandboxProvider.capability(protocol)` answers the object through which the
provider offers an optional contract, or `None`; `sandbox_capability(provider,
protocol)` is the caller side, which also accepts duck-typed doubles and
refuses an answer that does not declare the contract. The required provider
surface stays `acquire`, its async twin, `get`, and `release`. Upstream benefit:
today the provider base class carries eight network-policy hooks and two
skill-sync methods every third-party provider must fake; each can become a
contract offered through negotiation, and the base class can shrink back to the
four verbs without breaking subclasses.

HartMesh drops: the `capability` method and `sandbox_capability` function
(`sandbox/sandbox_provider.py`, `sandbox/capabilities.py`); the two HartMesh
contracts stay.

Pull request description:

> Add `SandboxProvider.capability(protocol)` and
> `deerflow.sandbox.capabilities.sandbox_capability`. A provider offers an
> optional contract by inheriting it (the default answers the provider itself)
> or by answering a companion object that inherits it; callers fail closed on
> `None` instead of probing attribute names, and an answer that does not
> declare the contract is a `TypeError`. The required provider surface is
> unchanged. This is the seam for moving the network-policy hooks and skill
> sync off the base class one contract at a time. Tests:
> `tests/test_sandbox_capabilities.py`.

## Declared-sandbox resolver

`deerflow.sandbox.resolution.set_sandbox_resolver(fn)` installs a process-wide
resolver; `resolve_declared_sandbox()` asks it. The sandbox middleware binds a
declared sandbox's id into state and never acquires or releases on its behalf,
the three tool-side initialization helpers return it before consulting state
or the provider, and the `sandbox:execute` gate still applies. With no
resolver installed nothing changes. Upstream benefit: prewarm can hand a
warmed container to a run, tests can declare an in-memory double, and an
embedding runtime can bind a fenced handle over a container it provisioned and
will release itself, none of which needs a middleware or tools patch.

HartMesh drops: the seven `declared_sandbox()` hunks in
`sandbox/middleware.py` and `sandbox/tools.py`; HartMesh installs
`declared_sandbox` as the resolver where it installs the session provider.

Pull request description:

> Let an embedding runtime declare the sandbox an execution must use. A
> resolver installed through `set_sandbox_resolver` is consulted by
> `SandboxMiddleware.before_agent`/`abefore_agent` (which bind the declared id
> into state and skip acquisition), `after_agent`/`aafter_agent` (which skip
> release, because the declaring runtime owns it), `sandbox_from_runtime`,
> `ensure_sandbox_initialized`, and `ensure_sandbox_initialized_async`. The
> authorization gate is unchanged; a declared sandbox for a denied role is not
> bound. With no resolver installed every path is unchanged. Tests:
> `tests/test_sandbox_resolution.py`.

## Admission before acquire

`SandboxProvider.admit(thread_id, user_id=...)` and `admit_async`, no-op by
default, are called with the arguments about to be passed to `acquire` on
every acquisition path: `SandboxLeaseManager._acquire_and_bind[_async]` and
the direct fallbacks in the middleware and tools. A provider raises
`SandboxAdmissionRefused(reason=...)` to deny, before it has spent anything.
Upstream benefit: the per-user quota RFC needs exactly one place that every
acquisition crosses before a container exists; this is that place, and it is a
provider concern rather than a lease-manager concern.

HartMesh drops: nothing by itself, but the session provider's refusal of an
ordinary acquire on a mount scope held by an open accepted session moves from
its `acquire` override into `admit`, and a future quota policy lands in the
same method.

Pull request description:

> Add `SandboxProvider.admit`/`admit_async` and call them immediately before
> `acquire`/`acquire_async` in the lease manager and in the direct fallbacks
> of the sandbox middleware and tools. The default admits everything;
> `SandboxAdmissionRefused` denies before anything is provisioned, so a refusal
> leaves nothing to release. Motivation: a per-user quota, and an embedding
> runtime that must refuse an ordinary acquisition of a thread it holds under
> another regime. Tests: `tests/test_sandbox_admission.py`.

## Lease observer

`SandboxLeaseObserver` with `lease_bound(owner_id, sandbox_id, thread_id,
user_id, borrowed)` and `lease_released(owner_id, sandbox_id, last_holder)`,
installed through `set_sandbox_lease_observer`. The manager reports a bind
when an owner is bound to a sandbox it did not hold before, and a release when
a binding is removed, saying whether that holder was the last and a provider
release follows. The observer runs under the metadata lock, must return
promptly, and its exceptions are logged and swallowed; observation never
changes the lifecycle. Upstream benefit: eviction and TTL work (#5178, #5104)
needs holder facts, and receipts (#4651) want to know who held the sandbox
during a tool attempt.

HartMesh drops: the executor's `_record_scope_release` hook; `scope.released`
and any future holder diagnostics come from the observer.

Pull request description:

> Add `SandboxLeaseObserver` and `set_sandbox_lease_observer`. The lease
> manager reports holder transitions (bind with the thread key and borrower
> flag, release with a last-holder flag) to an installed observer, under the
> metadata lock, with exceptions logged and swallowed so observation can never
> change the lifecycle. Tests: `tests/test_sandbox_lease_observer.py`.

## Acquire-epoch and lost-sandbox guards

Already upstream. The `_acquire_epoch`, `_acquire_inflight`,
`_reserve_local_teardown`, and `_held_teardown_lease` guards in the AIO
provider are present on `deerflow/main` with the same names and semantics
HartMesh runs; HartMesh's remaining delta in that file is accepted material
only. Nothing to offer.

## The egress verdict belongs to the command

Proposal, not a patch. Today the sandbox middleware surfaces a blocked egress
as a Human Input card from `wrap_tool_call`, after the tool handler has
returned, by consuming trusted-proxy events keyed by sandbox id. Three things
follow. The verdict is not part of the command's result, so anything that
records or digests tool results sees the result before the verdict and has to
be ordered by construction; HartMesh pins the sandbox middleware inner of its
receipt middleware for exactly this reason. A grant is keyed by sandbox id, so
a temporary grant binds to the container rather than to the execution that
asked, which is wrong for containers shared across executions or destroyed at
the end of a run; HartMesh denies accepted sessions unasked until an approval
can be run-bound. And a non-interactive execution learns of a denial only
through a side effect.

Proposed shape: the proxy's decision becomes an outcome of `execute_command`.
The sandbox returns a result that carries the blocked request ids (or raises a
typed `EgressBlocked`), the tool renders it into the tool message and the
Human Input card, and decisions are applied by the execution that asked, with
grants scoped to a lease owner and expiring with it
(`decide_network_policy_request(sandbox_id, request_id, decision, *,
owner_id)`). The middleware keeps only the Human Input plumbing. The change is
additive: providers keep the current hooks, and the middleware path remains
the fallback for a provider that does not report the verdict inline. Once
adopted, HartMesh's ordering-constraint entry becomes structural and its
`egress.blocked` diagnostic moves into the command path.

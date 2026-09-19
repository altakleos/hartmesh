# Changelog

All notable changes to DeerFlow are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0+hartmesh.26] — 2026-09-19

- hartmesh#116 — a plan is Ultra's alone, and the chat mode is read in one place. On the `.25` tenant a one-command report cost five model calls and two of them were todo bookkeeping: the model wrote a plan, executed it, and revised it, on a skill whose whole job is one command. Nothing had asked for that. `is_plan_mode` binds the `write_todos` tool, Pro sets it, and Pro is what every thinking-capable model resolves to when nobody chooses — so the plan tax was on every ordinary turn, and the todo prompt already telling the model not to bother with trivial tasks did not stop it. A tool a model is given is a tool a model uses. Plan mode now belongs to Ultra, the mode that also divides work between subagents, where a visible plan has something to track. The same change removes the four private spellings of the mode dial — one in the composer, one in the sidecar, one at each of the two places a run starts — and replaces them with a single module that answers the three questions that exist: which modes a model can offer, which mode a stored choice resolves to on it, and what a mode turns on in the agent. A mode can no longer mean one thing in the menu, another on the first message and a third on a follow-up. The third question had been answered wrong for this tenant: Reasoning and Pro are the same request but for `reasoning_effort`, and the model factory drops that field for a model that does not support it, so on `inclusionai/ling-3.0-flash-vl` the two rows sent byte-identical bytes. The menu offered more time for more accuracy and delivered neither. The row is now derived from the model's own capability and a stored Reasoning choice resolves to Pro where the two are the same request.

This one is visible in the product, not only in the timing. On a model that
ignores effort the Reasoning row is gone from the picker, and Pro no longer
produces a plan. The mode copy and both locales moved with it.

- hartmesh#117 — the prewarmed container now arrives with its skill view already published. `.24` built the sandbox while the person typed and `.25` proved it on a tenant: the new chat's container was ready 3.5 s ahead and reclaimed in 14 ms. The first turn still spent 2.2 s in skill projection, because the container was parked and the *view* it mounts was not. Binding a thread's accepted view stages an fsync'd copy of the snapshot the first time that thread sees it — capture, write, two re-captures — and verifies the published tree in place on every bind after. So the prewarm publishes the view for the snapshot the first turn is most likely to bring: the default agent, no subagents, every enabled skill, which is the whole of what the default turn's snapshot depends on and all anyone can know before a word is typed.

The identity never authorizes the reuse; the bytes do. A turn that brings a
different snapshot fails the in-place verification and takes exactly today's
slow path, which is the entire cost of guessing wrong, and a view that cannot
be published costs the turn nothing either — the container is parked
regardless. The prewarm is handed a *resolver* rather than a snapshot, so the
ordinary "built nothing" answers on a two-slot tenant spend no tree passes,
and the lease is taken and released by the same party.

What the prewarm deliberately does not take is ownership. It publishes the
tree and immediately compare-and-pops its own record, leaving bytes and no
claim, because a guess recorded as an owner wedges the thread: when a turn's
own bind fails after it has claimed the coordinator — a tenant disk filling
during staging is the realistic cause, and staging is the very cost this
change exists to remove — the unwind's compare-and-clear carries the run's own
identity and cannot match a guess, and its fallback refuses any recorded view
outright. Neither path reaches the release that clears the coordinator, so
every later turn on that thread is refused, permanently. Publishing bytes and
keeping nothing leaves precisely the state the unwind knows how to finish.

- hartmesh#118 — that wedge is recorded as a known defect where the code is. It is older than the prewarm and reachable without it: any failed first bind on a thread whose view map holds an identity the unwind cannot match leaves the coordinator clearing and the thread refused. The note carries the mechanism, the in-process probe showing that emptying the view map does not recover the thread, and one rejected repair that will otherwise be proposed again — ignoring generation 0 in the unowned release, which a real binding can legally carry, and which would therefore empty a view a live container is mounting.

Both timings in this release were taken on the released `.25` profile, and
neither change has run on a tenant. The arithmetic says the 39.793 s report
turn should lose two model calls and the 2.2 s of staging, which subtracts to
about 27 s; that is a subtraction and not a measurement, and it is offered as
one. The two calls that disappear were short-output calls and were plausibly
cheaper than the five-call average the subtraction uses, while the three that
remain now run on a shorter history and should be faster than they were —
both effects are unmeasured and they point opposite ways. The next
tenant-class run is what has standing to say.

Nothing in this release touches what every model call carries before any
content: about 12.4k tokens of system prompt and tool schemas, with the
report skill's own instructions re-sent on each later call. That is the
remaining measured lever and it is untouched on purpose. Nor is the first
container after a guest boot explained — it still costs more than a later one
(19.3 s, against 11.6 s once the image is present) with no attribution
between image unpack, the sandbox runtime's first start, a cold daemon and
plain boot contention. Prewarm hides it wherever it has lead time; the first
chat after a boot is where it is still paid.

## [2.1.0+hartmesh.25] — 2026-09-18

- hartmesh#115 — a report offers a download only while its file is still there. A tenant built a report with PDF, Word and Excel, revised it in the same chat with a prose change that rendered nothing, and reopened on a fresh login: the card advertised all three formats and all three returned 404. The revision had deleted those renders and written a manifest with an empty file list, and the durable presentation for that turn names only the report JSON — the dead links were entirely UI state. What the card read is the thread's cumulative presented-files list, and that list is *history*: it appends and dedupes, `/history` restores it correctly on a fresh browser, and both the artifact panel and the file chips on earlier messages need it. Its own source comment already recorded the consequence — a rebuild deletes the previous draft's renders and their paths stay in the list. So the card now asks two questions and needs both. Presentation still decides **eligibility**: the delivery fence is unchanged, a format nobody presented is never offered however live its file is, and nothing globs the report directory or infers the three conventional names. A bounded probe then decides **availability**, asking for one byte (`Range: bytes=0-0`) over the same authenticated, owner-checked artifact route the download link itself uses, with the body dropped unread; 200 and 206 are live, and 400, 403, 404 and a network failure are all fail-closed. Proving a 40 MiB PDF is still there costs a byte on the wire and nothing on the Gateway, which skips its content hash above 2 MiB and caches it below. Two races are closed by identity rather than guarded against: every draft rewrites the same filenames while the cumulative list stays byte-for-byte identical, so the report body's own digest is part of the question being asked and a slow probe from the previous draft cannot resolve into this draft's view and restore a link to a file that draft deleted; and a run settling is deliberately *not* part of that identity — it is an event that makes the answer worth re-asking, so settling re-asks the same question and the confirmed links stay on screen instead of blinking out whenever an unrelated turn in the thread ends. While the verdict is unknown the card shows neither links nor the "No file to download yet" notice, because a false empty is worse than the stale link it replaces.

Nothing else moves. Charts, the report schema and its parser, the KPI layout,
*Save to my files* (which now offers exactly the renders the card is showing),
history, todo and goal hydration, archive and download authorization and the
durable delivery receipts are all unchanged, and a visible link is still the
ordinary `download=true` URL it was. There is no backend change in this
release: the repair is entirely in how the card decides what to offer.

This is source repair with deterministic tests, not field acceptance. The
defect was reproduced by the estate on a disposable clone of the `.23` golden
image with a real model through released nginx and gVisor; the fixed behaviour
has been proved against unit, DOM and route-mocked browser tests, and those
mocks exercise the card's decision and the URLs it asks for rather than real
authentication, the released filesystem or the Gateway's own path resolution.
Whether every visible link on a real tenant now downloads a current file is
the next tenant-class run's to say.

## [2.1.0+hartmesh.24] — 2026-09-18

- hartmesh#114 — a new chat's sandbox is built while the person types. On a cold turn the wait before the first model request is the container, not the skills: across six cold starts on the released profile, `sandbox_create` measured 3.8 to 4.6 s and `sandbox_readiness` 5.3 to 9.7 s, against about two seconds for the projection itself, and the `.23` tenant-class run put cold materialization at 21.8 s against 152 ms warm. None of that depends on what the person is about to say. An accepted container is shaped by `(user, thread)` and the configured mounts and never by the binding — its own reuse fingerprint says so for local backends — so it can be built before the message exists. The web client already mints a thread id the moment a new chat opens, seconds before the first send; it now asks the Gateway to prewarm that thread, and the provider runs the accepted acquisition **minus the hand-out** — one shared preflight, the same deterministic id, the same fingerprint, the same create — and parks the result. The first turn's own warm reclaim finds it: `acquisition=accepted_warm_reclaim`, no `sandbox_create` span. There is no second identity and no second lifecycle to keep in step: the prewarm records the fingerprint the acquisition computes for itself and parks through the acquisition's own terminal. Three rules keep a speculative build from ever costing a real turn — it never evicts (it takes a free slot or builds nothing, and containers still in their readiness wait count as taken, so builds seconds apart stay inside the slot budget), it never stands in for an active sandbox or replaces a parked one, and one no turn claims is stopped past `sandbox.prewarm_claim_timeout` (300 s, checked every 30 s) by its own reaper thread — independent of `idle_timeout`, because `idle_timeout: 0` is a supported configuration that starts no idle checker. The unclaimed mark is the parked container *object*, never its reused id, so a container rebuilt under that id after an eviction is never mistaken for the prewarm. The remote backend refuses, since there the binding shapes the Pod at creation.

The accepted-run evidence contract is untouched. Materialization still
completes before `try_start`; it binds into a container that is already ready.
Deferring `execution_evidence` past the first model call would have removed
more of the wait and was rejected: that evidence is about *placement* — a
lease, a runtime image digest, a file manifest — and moving it trades a safety
property for latency.

What this does not address is stated because the numbers above would otherwise
read as more than they are. The first container created after a guest boots
took 19.3 s and 11.6 s in two runs, with the image already on the disk, so it
is not a pull and the cause is not known; the prewarm's own build goes through
the same path, so the first person after a restart can still meet it. And on a
research turn the tool work after the first model reply — 51 s of the 96 s
measured in `.22` — remains the larger share of the wait and is untouched here.
Warm follow-up turns were already fast and are unchanged.

## [2.1.0+hartmesh.23] — 2026-09-18

- hartmesh#113 — a malformed tool name that recovers, a fetch that installs nothing, and two questions answered before they are asked. **Tool names**: a model emitted a 129-byte shell command where a tool *name* belongs, carrying only a description in its arguments. Nothing executed it, and the run still died with an opaque runtime error after nine model calls, because two boundaries disagreed about what a tool name is: the durable receipt — which is the outermost tool wrapper and writes evidence before any inner code runs — required a syntactically safe one, and everything upstream accepted any nonblank string. The receipt's rule is right and stays; it is now asked earlier, while the answer is still recoverable. One predicate states it, in the module that owns it, and both boundaries call it; registration comes from the tool graph's own answer rather than a name list that could drift. A call failing either test is answered with one bounded result saying it was not executed and naming the tools that run actually has, and the model's string never becomes a message field, an evidence key or a log line — the log carries its length and a digest. **Fetch**: every successfully retrieved page was invoking readabilipy's runtime `npm install`, which cannot succeed on a non-root immutable image, so each fetch logged a permission error and a traceback before the pure-Python extractor did the work it was always going to do. That path is now chosen deliberately: no Node probe, no install attempt, no package management during a request, and the same output as before. Writing its test found a second defect — a page with an empty body raised from inside the parser and failed the whole fetch, which is an ordinary thing for a fetch to meet — so that one case is reported as empty while every other error still surfaces. **Avoidable work**: measured from durable receipts rather than assumed. Across three captured runs there are no duplicate calls at all — every search query and every fetched address is distinct — so nothing here suppresses retrieval; breadth is not redundancy, and deciding that two differently worded queries share an intent is not a judgement this can make correctly. What was avoidable were two questions the runtime could already answer: the model spent a call asking which document libraries exist (the same question whose malformed call ended the run above), and followed a successful PDF write with three more calls checking the file was there and how big it was. The image asserts its libraries at build time, so the prompt now states them where the model plans, with a test reading that assertion out of the image build so the promise cannot drift from what the build proves. And a delivered file now carries the runtime's own byte count, which is what a verification command would have reported.

No behaviour in the delivery fence, the direct-fetch network guards, provider
withdrawal, sandbox isolation or the profile's memory budget changes here. The
fetch guards in particular — scheme, DNS pinning and rebinding, per-hop
redirect re-validation, private and link-local screening, content type, the
2 MiB cap and the single timeout — all run before extraction sees any bytes.

Cold-start latency is unchanged and remains the headline wait. On a research
turn measured at 96.4 server seconds, everything before the first model request
took 31.1 s — and the phase records say what that is made of, because the
sandbox phases nest inside the projection rather than adding to it:
`sandbox_create` 19.3 s and `sandbox_readiness` 9.7 s sit inside
`skill_projection`'s 31.0 s, leaving about 2 s for projection itself. So the
cold wait is creating and booting the sandbox, not preparing skills. The same
guest's next cold start took 3.9 s to create and 6.3 s to become ready, a
spread this release does not explain. Nothing here addresses any of it, and the
instrumentation is preserved so the next measurement reads the same fields.

## [2.1.0+hartmesh.22] — 2026-09-18

- hartmesh#110 — keyless `web_fetch` that answers, a provider refusal that stops, and a turn that hands over what it made. Three failures in one release, all found by a tenant. **Fetch**: the default keyless `web_fetch` was a hosted reader that answers a tenant's server address with HTTP 401 for every page, so a research turn made three refused calls and a report turn thirteen, each to a different address, each refused identically, before answering from search snippets alone. The Gateway now reads the page itself: `http`/`https` only, every resolved address screened against the same never-allowed set as the sandbox egress policy, the connection made to the address it checked with the name on `Host` and TLS SNI so the certificate is still verified and a resolver that answers differently the second time gains nothing, redirects followed by hand with each hop re-screened and re-pinned to eight at most, HTML/XHTML/plain text only, 2 MiB streamed, one budget across the chain, four fetches at once, no cookies, no credentials, no retries. Measured on the seventeen addresses those two turns actually asked for: fourteen answer a plain GET with `200 text/html`, two are gated to automated readers from any address and stay that way, one timed out. Jina remains the keyed upgrade, no shared secret entered the public profile, and source attribution is unchanged. **Repetition**: tool results gained a typed `error_scope`, and a provider-scope refusal withdraws that tool from the model's bound tools for the rest of the run, so thirteen identical refusals become one. There is no counter, no threshold and no tool-name registry — the stamp is the entire contract, and the same sentence without it withdraws nothing. **Delivery**: an ordinary "create a PDF about…" request produced a valid PDF and still ended the run in an error, because the agent never presented the file and the delivery fence correctly refused to call that success. Three releases had already improved the *report* of that failure without removing its cause, which is a model judgement. The runtime already knows the answer at the moment it decides to fail — it computes the exact set of files the turn produced — so it now hands that set over instead. Files the model named still stand and only omissions are added; a delegated task presents nothing; no stdout is parsed, no path authorization is widened, nothing under `workspace` is touched, and a stranger asking for the same file still gets a 404. **Search sizing**: the profile's SearXNG sat at its 192 MiB ceiling with 170 reclaim events and no OOM kill under two ordinary turns. `deploy/compose/scripts/measure-searxng.sh` replays those turns' own queries through the pinned image under the profile's limits; 90 queries peaked at 144–149 MiB and never reached the ceiling, so that pressure is real and was **not** reproduced. The limit is therefore set against the observed ceiling rather than against the replay: 256 MiB, funded by 64 MiB of the Gateway's stated headroom (1152 → 1088 MiB), with the profile line still exactly 5120 MiB and every term of it written out.
- hartmesh#111 — a same-chat follow-up drives the fetch a second time through the real Gateway stream, with the thread reloaded, the artifact re-fetched and the run archive pulled.

Everything above is proved by source tests and by direct measurement from a
development host. **No real model has yet run against these repairs**: the
failures they fix were captured on a tenant-class host, and the fixed behaviour
has not been. Call counts, token totals, phase timings and the search service's
own cgroup peaks under a real turn are therefore owed by the next tenant-class
run, not claimed here. The development-host fetch measurement is the same class
of address as a tenant's, not the same address.

Cold-start latency is untouched and remains the headline wait, unchanged from
`.21`: a new thread's first message still reaches the model at about 16.5 s, and
a warm turn still spends about 1.8 s projecting accepted skills.

## [2.1.0+hartmesh.21] — 2026-09-17

- hartmesh#105 — say why a search failed when retrying cannot fix it. A refused provider came back to the model as a bare failure, so it tried the same query again, and again, before giving up with nothing to tell the person. The guidance now distinguishes a fault worth retrying from a provider that has declined, and a declined one ends the turn in a sentence rather than in silence.
- hartmesh#107 — keyless search that answers, through the profile's own SearXNG. The tenant profile's default `web_search` reached DuckDuckGo, which answers server addresses with an anti-bot challenge, so a tenant without a search key got a failure instead of an answer; `image_search` returned nothing at all. Every keyless engine was measured in isolation from a server-class host, and the default is now the profile's own SearXNG on a sixth service — private to the tenant network, nothing published, pinned by digest, read-only with a minted secret and three tmpfs, in 192 MiB. Web search reaches Google's search element weighted above Yahoo, with Bing at half weight as the availability floor after it returned unrelated results live; image search has its own measured engines. Both tools now degrade into a sentence the model can act on, and the person's question stays out of the logs entirely: the query moved from the URL to a POST body, and a failure records a status code rather than the request it came from.
- hartmesh#108 — close the four items the `.19` tenant-class upgrade left open. **Eviction**: a parked sandbox took 22.8 s to stop in the foreground of somebody's next message, because the container runtime's ten-second SIGTERM grace was paid twice for containers that never handle the signal — both already died by SIGKILL, and the grace only decided how long the person waited for it. Measured on the released images at the profile's limits, the pair now stops in 3.4 s instead of 21.6 s, so a turn that has to make room reaches the model near 16 s instead of 35.7 s. **Memory writers**: a background extraction that finished 34 s after the last foreground turn invalidated a byte-exact baseline, with nothing published to warn against taking one; `GET /api/memory/writers` now answers whether background memory work has finished, and the operator guide says to check it before snapshotting. Its contract default is *unknown*, not idle, because a caller acts on idle by taking a snapshot. **Schedules**: a task read "enabled, next run September 9" on September 17 on a Gateway whose scheduler had never started; `GET /api/scheduler` reports whether one is actually running, and the page says so above the create form, marks a next run that has already passed, and notes that triggering by hand still works. Nothing starts, resumes or reschedules anything. **Report card**: monetary KPI values printed across the tile beside them in the artifact panel, because the tiles were sized by viewport breakpoints that cannot tell a full page from a 480 px side panel; they size by container now.

This release closes two of the three gaps `.20` named: the synchronous eviction
on a full warm pool, and the memory-writer drain boundary. The keyless search
failure `.20` reported is closed too, by replacing the provider rather than by
working around its refusal.

Cold-start latency is untouched and remains the headline wait: a new thread's
first message still reaches the model at about 16.5 s, of which 4.8 s is
creating the container and 9.7 s is the sandbox booting under gVisor. Neither
is addressed here, and the accepted-material ordering that puts both before the
first model request is deliberate. A warm turn also spends about 1.8 s
projecting the accepted skills before its first model request, which nothing
has yet explained; it is the next thing to look at, because unlike the cold
start it is paid on every follow-up message.

## [2.1.0+hartmesh.20] — 2026-09-17

- hartmesh#101 — tell the person what the wait is for on the turn where it is longest. An upload turn spent its first seconds with a stale spinner and a generic "Working…", although the stage frames `.19` added were already on the wire at 1.6 s: the client mints a placeholder message for the upload, and the activity row read that placeholder as model output, so it drew nothing until the first real token at 13.8 s. The placeholder is now identified by what it is rather than by where it sits, so the row shows "Preparing your workspace…" when the workspace is what the turn is waiting on. Resolving an upload also stopped erasing the sidecar context a message carried.
- hartmesh#102 — measure the interval the turn journal could not see. Everything before worker admission — the identity lookup, the admission fence, sealing the accepted invocation, authorization, constraints, preparation and the run row — sat outside `total=` and every `@` offset, so a reader adding up the phases was missing it. `launch=` now reports that interval with a per-step breakdown, stamped at all four entry points (HTTP, scheduler, IM channels, the embedded runtime). It sits *outside* `total=`: acknowledgement is `launch=` plus `first_stream_text@`, which the compose README now states, because reading the offset alone under-reported a warm turn by seconds.
- hartmesh#103 — stop re-staging material that had not changed. Every warm turn staged the accepted skill snapshot twice, once at launch and once at the bind, with an `fsync` per file, and deleted both copies when the run ended — for a tree that is content-addressed, read-only, and re-verified by digest before any use. Both are now retained and verified in place. On a development host with the 13 seeded packages: the snapshot 2.0–2.4 s and 45 `fsync`s becomes 23 ms, and the view 3.2 s and 43 `fsync`s becomes 13 ms. Nothing new authorizes a reuse — the bytes do, at bind, as before — and what removes the material is unchanged apart from the container going away, which is now what bounds a parked thread's view. The disk this keeps, and the fact that a Gateway restart is the reclaim, are stated beside the tenant's disk sizing.
- hartmesh#104 — let a deployment that has not adopted governance run its own configured tools. The released tenant profile enables the governed tool plane and has no promoted revision until an administrator adopts one — a supported state both contracts describe. Execution did not agree: admission recorded that state only by *omitting* a revision, and a missing revision also means "the governed material this run needs is missing", which must fail closed. So every turn where the model reached `web_search` failed before the tool ran, the public deep-research skill with it, and the search provider was never contacted. Admission now seals the decision it actually made, and retrieval reads it: a governed run is unchanged, an ungoverned one executes and its observation says so (`tool_plane.mode: "unmanaged"`, no digests substituted), and an admission that made no statement still fails before dispatch. The mode cannot be chosen by anything but admission — the seal refuses a durable profile structurally, is bound into the run's identity, and is not honoured by a process whose own profile is durable. The same change makes every `accepted_*` runtime-context key server-owned by prefix, closing a path by which a caller could have supplied one.

`.19` measured the acknowledgement gap and this release closes the part of it
that was ours: on the development host a warm turn's `launch=` is 81 to 102 ms
and its accepted-skill projection 157 to 165 ms, against 9.3 s for the same
shape before. None of that is measured on the tenant class, whose next Part A
reads these fields.

What this release does not settle: the keyless DuckDuckGo `web_search` now
reaches its provider and is refused there — the endpoint answers the adapter's
request with a challenge page, which the adapter reports as
`provider_unavailable`. A search turn therefore still fails on the default
profile, after dispatch, with a receipt and an observation recording why
instead of nothing at all. Cold-start latency, the synchronous eviction on a
full warm pool, and the memory-writer drain boundary are untouched.

## [2.1.0+hartmesh.19] — 2026-09-17

- hartmesh#95 — make the call that produces a file the call that hands it over. `.18` shipped the skill's handover as a `Present:` line the report script printed and the bash tool parsed, which keys a behaviour to a wording: change the phrase and delivery silently regresses, and every new flow has to learn it. The tool now takes a typed `present` argument the model fills, and nothing is read from output. Each named path is checked against the filesystem rather than against text — inside the caller's scope, a regular file, and modified during this call (with a two-second tolerance) — and paths that fail are reported back in the result instead of dropped. What the run produced is then one signal end to end: a tool result tagged `presented_files`. The delivery fence, the archive route and the run's evidence receipt all read that tag, so the registry of tool names they used to consult is gone; a file that reaches the artifact panel as a side effect, such as a browser screenshot, is no longer mistaken for a delivery. A new producing tool adopts delivery by accepting the argument and returning one helper, and a new skill by one documentation line. `present_files` keeps working and now tags its own result, so no flow is asked to call twice. The report script's output name is derived from the directory the caller chose, so the model can name the paths before the run rather than learn them from it.
- hartmesh#96 — verify the published skill view in place instead of re-staging it on every bind. The accepted snapshot is immutable, so a warm turn was re-materializing a view it already had; it is now verified where it stands. The bind step of a warm turn falls from the 5 to 9 s `.16` measured to roughly 25 ms — the single largest item in the pre-model budget that `.16` and `.17` kept naming.
- hartmesh#97 — two 1 GiB sandbox slots at two CPUs on the same 5.0 GiB line. The measured composition is 436 to 484 MiB idle plus 100 to 114 MiB on the first shell exec, so the four 512 MiB slots the profile carried had no headroom under a large report — the memory finding `.17` and `.18` left open. Four slots become two with twice the memory and twice the CPU, the line total unchanged; `deploy/compose/scripts/measure-sandbox-boot.sh` and the compose README carry the figures.
- hartmesh#98 — say what the run is doing while the person waits. A turn spends seconds before its first token on work that had no outward sign, so the composer simply sat. Three ordered stages — `preparing`, `workspace_starting`, `thinking` — are published as advisory `custom` frames the moment each begins, once per run, and the browser keys them per thread so a second conversation cannot borrow the first one's label. The stream contract is unchanged: `metadata` is still the first frame, and the publisher holds progress until the worker opens it after that frame, so a run refused before it publishes nothing.

None of the four is a tenant-class qualification. What this release does not
settle: whether the model fills `present` unprompted on a real tenant turn is
an empirical question the next Part A run answers, and the fence remains the
backstop until it does. `write_todos` — 60.3 s across seven calls in the `.17`
report turn, and the largest remaining item in that budget — is untouched.

## [2.1.0+hartmesh.18] — 2026-09-16

- hartmesh#90 — tell a reopened chat what it has already presented. `.17` found a report card offering no downloads after the chat was reopened, although its three files were presented, persisted and downloadable. A client that merely opens a conversation never sees a `values` stream frame: its one state read is `POST .../history`, whose `values` carried `title`, `thread_data` and `messages` and none of the whole-thread channels. So the cumulative presented-files list was empty and everything drawn from it read as nothing — the artifact panel opened empty, inline relative images broke, the todo list vanished, and an active goal was invisible while it went on driving hidden continuation turns, so the chat answered the next message under a standing instruction with nothing on screen saying so. That response now projects `artifacts`, `todos` and `goal` from the same checkpoint it already reads, todos with the statuses they were last written with and nothing re-deriving one. The card also stops asserting an answer it does not have: messages and state arrive on different requests, so a new tab could show "No file to download yet" before the state read landed. `tests/unit/core/threads/history-contract.test.ts` pins the projection from both sides — every key the app declares it renders must be in it, and the mocked backend may not answer with a key outside it — because the e2e mock had been answering with keys the Gateway never sent, which is how this passed a release.

- hartmesh#91 — make each intention in the business-report skill one run. The `.17` tenant class spent 20 model calls on one report and 13 on a one-sentence revision, and its retained receipts say where: 50.5 s of the revision's 102.8 s tool span went on six Python probes hunting for a summary the `prose` step had written but never printed, and 45.3 s of the report's 178.3 s went on two retries after the three documented render commands were collapsed into one backgrounded line that dropped `$SKILL_DIR`, a separate `show` of figures the build had already computed, an `ls` of the output directory, and two calls to a tool that does not exist. `render --to` takes a list, `build` and `prose` take `--render`, both print the figures `show` prints — `prose` out of the draft it wrote, so a sentence the number check dropped is not echoed back as if it stood — and each run ends with a `Present:` line naming the files to hand over, one absolute path per line, the report first. `prose` now also removes the renders it invalidates, bounded to what the skill recorded writing in `renders.json`: its report path comes from its caller, so a name-based rule would delete a user's own copies out of a re-uploaded bundle. Spreadsheet text reaching the digest has its control characters stripped, because a category cell carrying a newline could open a line at column 0 and forge the handover list. On the 5,000-row fixture the report flow falls from 7 script runs to 3 and the revision from 4 to 1.
- hartmesh#92 — say where the accepted preparation's own time goes. `.17` measured roughly 5 to 6 s before the first model request on a warm turn — most of the budget for a one-sentence revision — inside a `skill_materialization` phase that reported one figure and named nothing in it. Three spans nest inside it now: `accepted_authorization`, `skill_projection` (which `sandbox_lookup`, and on a cold turn `sandbox_create` and `sandbox_readiness`, nest inside) and `skill_snapshot_bind`. Instrumentation only; the residual between them is the isolation assertions, and a turn where that residual is not small is itself a finding.

Neither change is a tenant-class qualification. What `.17` left open and this
release does not move: `write_todos` cost 60.3 s across seven calls in that
report turn — 34% of its tool span, more than everything #91 removes together —
and the memory budget of a 512 MiB sandbox under a large report.

## [2.1.0+hartmesh.17] — 2026-09-16

- hartmesh#86 — show a built report as a report card rather than as JSON. A `*.report.json` artifact that parses against the report contract is drawn as the document it describes — KPIs, tables, charts and the checks line — with a download for each render the thread has presented; a file the app cannot draw stays JSON, and the panel's existing code/preview toggle switches between the two. `formatValue` is a port of the skill's own `format_value`, down to rounding the decimal spelling of a number rather than the binary double, so a figure reads the same on the card as in the PDF, Word and Excel renders.
- hartmesh#87 — let a deployment say what a workspace opens on and who is offered the rest. `ui.starters` is Home's starter grid: choosing one fills the composer, focuses it and sends nothing, because the first moment is "pick the thing, drop the file, say the month". `ui.profile: business` stops offering skills, tools, subagents, integrations and the scheduled-task recipes to someone who is not an administrator, and drops the product blurb; channels and memory stay, because the phone someone messages it from and what the agent remembers about them are theirs. One rule governs the unknown: a control someone might need stays offered, and copy the deployment authors waits. Hiding is presentation, not authorization — the routes are unchanged and `authorization` has no permission covering them; `system_role` is what limits a person.
- hartmesh#88 — end the skill-path dead end that cost 341 s of a 522 s report. The `.16` tenant class produced its August report in 522.218 s, of which **341.3 s** was one `find / -name "report.py"` in a 512 MiB sandbox, for a script that was mounted and readable throughout. The model read the skill at the right snapshot path, then ran that file's own first command example, written against a live tree a durable invocation does not mount — and every listing root above the snapshot is refused by the same fence, so the skills tree was the one place it could not look. The fence is unchanged and nothing new is reachable; what changes is that a refusal now names the snapshot root and the re-rooted path, carries that through `ls`/`glob`/`grep`/`read_file` instead of flattening it to `Permission denied`, and that neither the skill index nor the legacy skills section states a root any more — `describe_skill` reports each skill's `Directory` beside its `Location`. Six public skills address their own files through a guarded `$SKILL_DIR` instead; four still carry an absolute path, recorded with the reason, behind unrelated skill-review debt.
- hartmesh#89 — give the delivery notice back to a reader who reloaded. `.16`'s advisory frame is page-local state, so the `.16` tenant class found both fenced turns keeping their stored `error` and stop reason across a reload while the notice, and the way to the files it offered, were gone. The verdict now has one owner (`runs/delivery.py`) shared with the worker, and `GET .../runs/{run_id}/delivery` projects it from the terminal receipt — only when `stop_reason` says the fence fired, and reporting nothing rather than an empty correction when that best-effort receipt is missing, duplicated or malformed. The browser reads the stream while the page that heard it is open and the receipt afterwards, through the same parser, at one request per thread: the thread's runs name the fenced turns, and only a named turn asks for the paths it withheld.

The two repairs answer the `.16` tenant-class rerun directly and are what it
asked for before another release. Neither is a tenant-class qualification: the
next Part A rerun is what closes DF14 and DF15, and the report's remaining
latency and memory-budget findings — a 512 MiB sandbox under severe
reclaim during a large report — are untouched and still open.

## [2.1.0+hartmesh.16] — 2026-09-16

- hartmesh#84 — report a turn's own acquisition origin, and name the time it spends before the model. A turn acquires its sandbox in stages, and `set_acquisition_source` took the last word, so a later "already active" observation overwrote the origin the field exists to disclose: the `.15` tenant class read `acquisition=accepted_active` on all seven turns, including the cold one that also carried `creates=1` and a measured `sandbox_create` span. An origin now wins whenever it is known and a later active-reuse observation rides beside it as `reused=`. The same run left 2.6 to 3.4 s of every turn unattributed between the sandbox lookup ending and the binding starting; four phases now cover that window (`skill_materialization`, `agent_build`, `checkpoint_preflight`, `graph_start`), and the rendered phase list keeps its tail so a turn with goal continuations can no longer truncate away `model_completion` and `terminal`. Reproduced live on the released profile shape, where a warm turn spends 143 ms finding its container and 8.5 s of a 9.3 s turn projecting and binding the accepted skill snapshot — the cost that was invisible. `deploy/compose/README.md` gains the eight acquisition sources, split into origins and observations.
- hartmesh#85 — tell an operator, and the person in the chat, when a turn produced files it never handed over. The delivery fence runs after an ordinary graph completion, so a run that wrote outputs and skipped `present_files` was `error` in SQL and in the journal while the browser showed confident prose and an ordinary end marker, with the files offered nowhere (found by the `.15` tenant class). Both fence branches now set a `stop_reason` — `artifact_delivery_incomplete` and `delivery_receipt_failed`, registered in the store's lifecycle vocabulary — which were the only terminal-error branches in the worker leaving it unset, so a fenced run had read as a generic failure over HTTP; that is the half of the verdict that outlives the connection. Live clients additionally get one advisory `custom` frame, and `frontend-hm` renders the withheld paths under the run's own last assistant bubble. Deliberately **not** an `error` frame: that frame asserts the stream carries no valid turn, which is false here, and the SDK acts on it — measured in a real browser as a spurious reconnect, an aborted follow-up-suggestions request, and a send button pinned as an X for the rest of the thread.

Both changes were reproduced live on a development host against the released
compose shape, and the delivery verdict was measured end to end in a real
browser against a real Gateway. Neither is a tenant-class qualification; the
next Part A rerun is what closes the two `.15` findings. The delivery notice is
browser-only (IM surfaces still show the uncorrected prose) and does not yet
survive a reload, since it rides the stream rather than being rehydrated from
the run's delivery receipt.

[2.1.0+hartmesh.26]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.26
[2.1.0+hartmesh.25]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.25
[2.1.0+hartmesh.24]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.24
[2.1.0+hartmesh.23]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.23
[2.1.0+hartmesh.22]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.22
[2.1.0+hartmesh.21]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.21
[2.1.0+hartmesh.20]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.20
[2.1.0+hartmesh.19]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.19
[2.1.0+hartmesh.18]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.18
[2.1.0+hartmesh.17]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.17
[2.1.0+hartmesh.16]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.16

## [2.1.0+hartmesh.15] — 2026-09-15

- hartmesh#83 — make a turn's timing readable in a deployment's own log, and stop dropping its completion data. `TurnPhaseJournal.emit` put the whole journal in `extra=`, which neither the default text format nor `JsonTraceFormatter` renders, so every released deployment printed the bare words `turn phase timings` and none could see where a turn's time went; the journal now renders its reading into the message (`@` start offsets, `+` measured durations, unobservable phases with their reasons, bounded independently of the record cap) and the JSON formatter carries the structured field. The per-turn `Refused to recreate missing authoritative lifecycle row … during completion persistence` was not noise: a durable store stamps a row's terminal projection whether or not lease heartbeats run, while `RunManager.update_run_completion` named that projection only when `run_ownership.heartbeat_enabled` was true — the default on every single-Gateway profile — so each turn silently lost its token counts, message count and message previews, and the refusal was then misreported as a missing row. Completions now name the projection on both paths and a refusal over an existing row is reported as one; a compatibility store keeps its write-through recovery. Operators on those profiles should note that `first_human_message` and `last_ai_message` (up to 2 KB each) are now stored where they had always been NULL, in the same tenant database as the rest of the row. `deploy/compose/README.md` gains "Reading a turn's timing".

Verified live on a development host against the released compose shape
(PostgreSQL, Redis, heartbeats at their default, `SANDBOX_RUNTIME=runsc`): the
counters and preview are stored, the per-turn ERROR is gone, and a reclaimed
chat reads its sandbox-to-first-text span off one line. Offline: the full
backend suite, with each new test proven to fail with its fix reverted. Not a
tenant-class qualification, not token counters against a real provider (the
scripted probe reports no usage), and not the heartbeat-enabled path, which was
exercised offline only.

[2.1.0+hartmesh.15]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.15

## [2.1.0+hartmesh.14] — 2026-09-15

- hartmesh#82 — run an accepted skill snapshot through the accepted-skills projection on a non-durable deployment profile. Since 48dd3aa7 the worker reached that branch only when a run had no record, which never holds for a Gateway run, so on `local_development` the first turn of any deployment whose effective skills were non-empty failed before a sandbox existed with `AcceptedSkillSandboxBindingError`; the guard first mattered in 2.1.0+hartmesh.13, which is the first release that gives a deployment a seeded skill library. The two durable profiles keep refusing without a qualified materializer, and the opaque boundary error is now preceded by a log line naming the reason code.

Verified live on a development host against the released compose shape
(cold and reclaimed chats, the snapshot's packages visible under
`/mnt/skills/.accepted/<digest>/` during a turn and empty between turns) and
offline by a scripted-Gateway stream test over a seeded skill. Not a
tenant-class qualification, and not a turn that runs a tool from the
projected skill through a real model.

[2.1.0+hartmesh.14]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.14

## [2.1.0+hartmesh.13] — 2026-09-15

- hartmesh#78 — sandbox image: ship the document libraries skill scripts import (`duckdb`, `python-docx`) and pre-build matplotlib's font cache; `data-analysis` reads `.xls` and installs nothing at runtime.
- hartmesh#79 — add the `business-report` skill: a tabular export becomes a branded PDF, DOCX and XLSX management report from one `report.json`, with a number verifier for model prose and a checks line; the smoke job builds and renders one on the image.
- hartmesh#80 — tenant profile: the sandbox image's slim services profile (six `DISABLE_*` switches), four 512 MiB slots at 256 pids on the same 5.0 GiB line with the Gateway at 1152 MiB, a measurement script for boot and idle under those limits, and a smoke step that runs the image under them.
- hartmesh#81 — ship the public skill library in the backend image and seed it onto the tenant data disk at every start, minus the skills the profile's own skill review refuses or its policy excludes (13 of 24 seeded); an older Gateway image without the library still starts.
- frontend-hm (042e044d, be53b015, e3e0a2be) — isolate the Hartmesh UI from the pinned upstream snapshot: the frontend image now builds from `frontend-hm/Dockerfile` and `make check-frontend-isolation` guards the snapshot.

Verified on a development host under runsc (boot, four concurrent renders,
the seed and projection on a gateway-only stack) and offline against the
real skill tree and the profile's own tool-plane policy. Not a tenant-class
boot or render measurement (the slim projection there is 40 to 50 s at one
CPU and the readiness budget stays 120), not package installation at
512 MiB, not the Gateway's peak at four concurrent turns, and not a sandbox
opened through a chat on the released images.

[2.1.0+hartmesh.13]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.13

## [2.1.0+hartmesh.12] — 2026-09-14

- hartmesh#73 — reuse a compatible accepted warm sandbox instead of destroying and recreating it, and bound turn-phase timing so the phase a turn reports is the phase it is in. Source evidence only: no warm-acquire percentile, cold-start or browser-timing claim.
- hartmesh#74 — report creation provenance from the backend, cancel by origin, replace a sandbox whose input configuration drifted, and cover authenticated model-to-HTTP streaming with regressions.
- hartmesh#75 — make a local destroy mean confirmed absent, keep a set tracked and quarantined until cleanup confirms absence, and account teardown failures separately from refusals (journal wire version 3).
- hartmesh#76 — arbitrate orphan reconciliation against acquisition, refuse lookups on a quarantined set, retry warm entries by generation identity, and validate identity before an accepted cleanup retry.
- hartmesh#77 — refuse active lookups once teardown has reserved the set, treat accepted reuse alone as activity, and stop the active-idle pass from retrying a resource it just quarantined in the same tick.

Verified over fake Docker and the in-memory ownership store, with a real
authenticated Gateway/worker/graph and synthetic inference. Not a Redis or
distributed-ownership qualification, a fully admitted live chat, a cold-start
or warm-acquire percentile result, a hard capacity admission change, or a
tenant rollout qualification.

[2.1.0+hartmesh.12]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.12

## [2.1.0+hartmesh.11] — 2026-09-13

- hartmesh#72 — make the local sandbox readiness deadline a validated effective setting used by both the synchronous and asynchronous acquisition paths, give the Compose tenant profile a 120-second initial candidate with the bounded `SANDBOX_READY_TIMEOUT` override (whole seconds, 60 through 600), enforce it against real monotonic deadlines rather than an approximate polling duration, and own and mark a new sandbox before waiting so neither reconciliation nor a peer can adopt it during the longer startup window. 120 seconds is an initial candidate, not a qualified fleet-wide bound.

[2.1.0+hartmesh.11]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.11

## [2.1.0+hartmesh.10] — 2026-09-10

- hartmesh#69 — validate every rendered model with the Gateway schema while preserving unrelated configuration state.
- hartmesh#70 — prevent operator-controlled model values and keys from appearing in renderer refusal diagnostics.
- hartmesh#71 — build refusal locations from trusted structure so operator-controlled keys cannot forge or leak through paths.

[2.1.0+hartmesh.10]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.10

## [2.1.0+hartmesh.9] — 2026-09-09

- hartmesh#68 — restore public DNS for open-mode gVisor sandboxes using the VM's validated read-only upstream resolver.

[2.1.0+hartmesh.9]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.9

## [2.1.0+hartmesh.8] — 2026-09-09

- hartmesh#62 — clarify the tenant Compose profile's consumer and egress documentation.
- hartmesh#63 — key login lockouts by account and provide an administrative unlock path.
- hartmesh#64 — move the tenant application bridge away from the operator's pod network.
- hartmesh#65 — move the development bridge away from the operator's routed network.
- hartmesh#66 — size the tenant profile's source-volume guard for an office retry budget.
- hartmesh#67 — let operators supply the tenant's model list independently of provider keys.

[2.1.0+hartmesh.8]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.8

## [2.1.0+hartmesh.7] — 2026-09-06

- fix: close the #56–#59 review findings in the compose profile and the local backend (#60)
- fix: sandbox 1 GiB and the gateway trimmed to 1344 MiB (P-s) (#61)

## [2.1.0+hartmesh.6] — 2026-09-06

- fix: adopt step preserves the pinned digest and never adopts on a dispatch (R-10 fix-up) (#59)
- release: supersedes 2.1.0+hartmesh.5, whose image tags were re-encoded by the adopt step and which received no release manifest

## [2.1.0+hartmesh.5] — 2026-09-06

- feat: tenant VM compose profile (P-r) (#56)
- fix: gVisor sandboxes resolve their network proxy (P-r follow-up) (#57)
- fix: sandbox memory 768 MiB and a release-pin guard (P-r fix-up) (#58)

## [2.1.0+hartmesh.4] — 2026-08-23

- fix: harden sandbox probe budgets (#17)

## [2.1.0+hartmesh.3] — 2026-08-22

- fix: resolve P-h running defects (#16)

## [2.1.0+hartmesh.2] — 2026-08-21

- feat: build a restricted-compatible sandbox image (#14)
- feat: consume the hardened sandbox image (#15)

## [2.1.0+hartmesh.1] — 2026-08-21

- feat(runtime): add durable invocation runtime (#1)
- fix(runtime): close durable production verification gaps (#2)
- fix(runtime): close durable runtime release gaps (#3)
- fix(runtime): retain post-commit ownership obligations (#4)
- fix(runtime): close post-commit ownership gaps (#5)
- fix(runtime): harden post-commit obligation integrity (#6)
- feat: harden Kubernetes sandbox runtime (#7)
- feat: configure database pool overflow (#8)
- feat: support split sandbox namespaces (#9)
- feat: scope Redis bridge keys by tenant (#10)
- fix: fail closed on incomplete sandbox PVC configuration (#11)
- P-n: harden Helm contracts and tenant deployment follow-ups (#12)
- R-1: add fork release tooling (#13)

## [Unreleased]

This section accumulates work toward the **2.1.0** milestone
([milestone 2](https://github.com/bytedance/deer-flow/milestone/2)).

### Added

- Added accepted, digest-bound execution budgets with durable fenced policy
  state, secret-keyed tool-equivalence detection, stable stop evidence, and an
  authorized thread evidence panel with bounded public summaries and portable
  terminal bundle download.

### ⚠ Breaking changes

- **deployment tenancy:** The Gateway now resolves one operator-owned
  `TenantIdentityV1`; `durable_production` and Helm `durable_one_replica`
  require an explicit non-`local` value. Each database schema binds to one safe
  tenant digest, and every covered Redis key uses the canonical
  `hm:v1:<tenant-public-ref>:redis` namespace. Before upgrading a nonempty
  deployment, stop writers, back up database/Redis, dry-run and then execute
  `deerflow deployment bind-tenant --tenant-id <id>
  --expected-nonempty-schema`, copy retained Redis keys offline, and verify
  readiness. Rollback after binding requires compatible code or restoring the
  backup; never delete the singleton identity row. During this feature release,
  the binding command can record exact legacy Redis component projections;
  config/Helm may select only those recorded values or canonical projections.
  This legacy compatibility path is removed in the following feature release.
  See `backend/docs/TENANT_IDENTITY.md`.
- **gateway:** Request trace ids are now issued unconditionally, and every
  Gateway HTTP response carries an `X-Trace-Id` header. Previously both were
  gated behind `logging.enhance.enabled`, which now controls **log output
  only** — whether records carry a `trace_id` field, and in which format. The
  header cannot be turned off; installations running the default
  `enabled: false` will start seeing it after upgrading. Scheduled tasks, MCP
  task notification runs, IM channel messages, and the embedded
  `DeerFlowClient` bind an id per unit of work, so the id also reaches the run
  record, the checkpoint metadata, and Langfuse traces that previously had
  none. A `deerflow_trace_id` supplied in a run request's `metadata` or
  `config.context` is now ignored and overwritten so the response header, the
  logs, and the persisted run cannot disagree — send the `X-Trace-Id` request
  header to pin a correlation id across services. `logging` remains
  restart-required. No config keys were added or removed. ([#5119])
- **skills:** Sandboxes now reserve `/mnt/skills` for managed enabled-only
  projections. `DEER_FLOW_HOST_SKILLS_PATH` and `SKILLS_HOST_PATH` are no longer
  used; Docker/AIO and hostPath deployments derive projection paths from
  `DEER_FLOW_HOST_BASE_DIR`. E2B operator mounts targeting `/mnt/skills` or any
  child path are skipped with a warning so they cannot shadow the managed
  projection; move extra E2B content to a different container path. User
  projections re-read global enable state from disk so toggles propagate across
  Gateway workers on the next sandbox acquire. Existing E2B sandboxes retain
  their creation-time snapshot until they are recreated. PVC-backed provisioner
  deployments still mount the operator-supplied PVC snapshot directly, so
  disabled-skill filesystem isolation does not apply in PVC mode until dynamic
  PVC materialization is implemented. ([#4178])
- **sandbox:** E2B now enforces `sandbox.replicas` as a process-local capacity
  limit. The default `wait` policy waits for `acquire_timeout`, then fails the
  agent turn. DeerFlow does not retry the turn automatically. Use `burst` with
  `burst_limit` to permit bounded extra VMs. The `reject` policy can remove one
  warm VM before it returns a capacity error. ([#4391])
- **skills:** A directory containing `SKILL.md` is now a runtime package
  boundary. Nested `SKILL.md` files inside that package are supporting data and
  are no longer registered as independent skills; unusual custom layouts must
  move independently loadable skills under a namespace directory without its
  own `SKILL.md`. ([#4098])
- **memory:** The memory system is now pluggable (`memory.manager_class` selects
  a backend; default `deermem` is self-contained). DeerMem-private settings moved
  from the top level of `memory:` into `memory.backend_config`, and the
  `/memory/config` response (and `client.get_memory_config()`) changed shape.
  ([#4122])
- **memory:** `/memory/config` and `client.get_memory_config()` no longer return
  flat DeerMem fields (`storage_path`, `max_facts`, `debounce_seconds`,
  `token_counting`, `guaranteed_*`, `staleness_*`, ...). They return
  `{enabled, mode, injection_enabled, manager_class, backend_config}` where
  `backend_config` is an opaque dict the active backend self-interprets. Memory
  *data* responses (`/memory`, `/memory/status` data) are unchanged. External
  API/SDK clients reading the old flat fields must read `backend_config` instead.
  ([#4122])
- **memory:** Custom `memory.storage_class` moved: the old default path
  `deerflow.agents.memory.storage.FileMemoryStorage` no longer exists (now
  `deerflow.agents.memory.backends.deermem.deermem.core.storage.FileMemoryStorage`).
  Custom `MemoryStorage` subclasses must accept `config` in `__init__` (was
  no-arg). A broken/old `storage_class` logs an error and falls back to
  `FileMemoryStorage` (won't crash) -- update the path + signature to restore it.
  ([#4122])
- **memory:** `storage_path` semantics changed from a FILE path to a root
  DIRECTORY. Pre-abstraction, an absolute `storage_path` was the shared memory
  file (opting out of per-user isolation) and a relative value was the global
  file under the data base_dir. Now `storage_path` (absolute or relative) is the
  root directory; per-user memory lives at `{storage_path}/users/{uid}/memory.json`.
  An upgrade keeping the old default `storage_path: memory.json` (a relative file
  name) would orphan per-user memory or hit `NotADirectoryError` on save, so the
  legacy migration **drops file-style `storage_path` values (ending in `.json`)
  with a warning** and the factory **raises** if `storage_path` resolves to an
  existing file. Set `memory.backend_config.storage_path` to a directory for a
  custom root. ([#4122])
- **memory:** `memory.mode: tool` with a backend that does not implement
  `search()` now fails fast at Gateway startup with a `ValueError` from the
  `MemoryManager` invariant, instead of starting successfully and silently
  returning empty results on every `memory_search` call. Both shipping backends
  implement `search()` (DeerMem retrieves; `noop` returns `[]`), so this only
  affects a custom backend that onboards without overriding `search()`. It is
  intentional -- silent empties are worse than a loud startup error. Fix: switch
  to `mode: middleware` or override `search()` (and set `supports_search=True`).
  ([#4324])
- **config:** `database.checkpoint_delta_snapshot_frequency` moved to
  `database.checkpoint_delta.snapshot_frequency` and its default changed from
  `1000` to `10`. A legacy top-level value is still honored with a deprecation
  warning and mapped onto the nested key (an explicitly set nested key wins).
  Deployments that relied on the old default now snapshot 100x more often in
  delta mode -- set `database.checkpoint_delta.snapshot_frequency: 1000`
  explicitly to keep the previous cadence. ([#4516])
- **docker:** The published entry port now binds to loopback (`127.0.0.1`) by
  default in both compose files, matching the documented local-trust deployment
  model. Deployments that relied on the old `0.0.0.0` binding must set
  `BIND_HOST` to expose the stack on other interfaces. ([#4618])

### Added

#### Authentication
- **auth:** Personal access tokens (PAT) for programmatic API access:
  `POST/GET/DELETE /api/v1/auth/pats` manage tokens (shown once, stored as
  SHA-256 digests); a default-deny route policy admits only the thread/run
  lifecycle routes, narrowed further by the token's `threads`/`runs` scopes,
  and any request dimension that carries cancel capability (`?action=`,
  `multitask_strategy`) additionally requires `runs:cancel`. ([#5041])

#### Agents & runtime

- **evidence:** Add terminal-only portable run evidence bundles with a
  canonical safe manifest, exact copied-artifact digests, explicit section
  completeness derived from persisted accepted capabilities and terminal attempts,
  prefix-aware MCP lineage/receipt validation, offline-checkable parent/child
  links, current-owner/PAT authorization,
  bounded cancellation-aware generation, and a stdlib-only offline verifier.
  Bundles prove internal digest integrity and explicitly remain unsigned.
- **runtime:** Accepted durable lead runs now bind a bounded fingerprint of the
  actual assembled model, prompt, authorized tools, middleware, skills, and
  policy before checkpoint or graph execution; recovery must match it, and
  authorized lifecycle summaries expose only the revalidated digest projection.
- **middleware:** New `TokenBudgetMiddleware` enforces a per-run token budget,
  shared additively across the lead agent and subagents. ([#3412])
- **middleware:** Structured tool-result metadata and a tool-progress state
  machine give the runtime first-class visibility into multi-step tool flows.
  ([#3601])
- **context:** Record the effective memory identity per run and persist durable
  context (system messages, memory, and tool state) across summarization,
  emitting it as structured runtime metadata so compaction no longer drops it.
  ([#3556], [#3887], [#3906])
- **runtime:** Goal continuations let a run resume toward a goal across multiple
  agent turns, with `continuation_count` tracked and capped. ([#3858])
- **subagents:** A system-maintained delegation ledger prevents redundant
  re-delegation of an in-flight task, and a total delegation cap bounds fan-out
  per run. ([#3877], [#4115])
- **subagents:** Persist and display subagent step history in the thread.
  ([#3845])
- **tools:** Structured synopses replace raw oversized tool output in previews.
  ([#3377])
- **files:** Deterministic read-before-write version gate for file tools
  prevents clobbering concurrent edits. ([#3912])
- **gateway:** Cache-aware cost accounting attributes token costs to cached vs.
  uncached paths; a Redis stream bridge enables distributed event streaming; and
  manual context compaction is exposed to the user. ([#3920], [#3191], [#3969])
- **gateway:** The stream-bridge heartbeat interval is configurable via
  `stream_bridge.heartbeat_interval_seconds` (default 15s), so deployments
  behind aggressive proxy idle timeouts can tune SSE, `/wait`, and internal
  subscribers together. ([#5017])
- **runtime:** Dual-mode checkpoint storage with LangGraph `DeltaChannel` cuts
  thread storage from O(N²) to near-linear for long research/coding runs.
  ([#4292])
- **runtime:** Delta-mode checkpoint history cache (memory/redis) with O(1)
  incremental composition, configured via `database.checkpoint_cache`. ([#4638])
- **agent:** Config-declared lead-agent middlewares let deployments add custom
  `AgentMiddleware` classes without patching the runtime chain. ([#3964])
- **agents:** Per-agent model and generation settings (`temperature`,
  `max_tokens`, `thinking_enabled`, `reasoning_effort`) override the shared
  model profile. ([#4347])
- **runtime:** Record terminal artifact-delivery receipts so runs expected to
  `present_files` no longer report success when delivery fails. ([#4365])
- **uploads:** Lazy-load historical files via a `list_uploaded_files` tool
  instead of injecting the full manifest. ([#4174])
- **scheduler:** `scheduler.recursion_limit` in `config.yaml` sets the LangGraph
  super-step cap for scheduled runs (default 1000, matching the web UI's
  interactive budget, clamped by `max_recursion_limit`). ([#4848])
- **runtime:** Every tool call now carries a runtime-stamped, tamper-evident
  tool receipt, and a bounded receipt ledger is injected into the model
  context so agents can cite execution evidence in their reports. Enabled
  by default via the new `verification` config section. ([#4659])
- **subagents:** Subagent delegations are now verifiable, layering RFC #4651:
  every subagent's report contract requires citing tool receipts (e.g.
  `[r3 write_file]`) and attaching a verifiable handle to each deliverable,
  the lead agent cross-checks those citations against the subagent's actual
  execution record, and `acceptance_criteria` on a `task` delegation are
  checked deterministically parent-side (file existence/non-emptiness,
  recorded test-command exit status) with anything undecidable reported
  UNVERIFIED instead of silently passed. ([#5076], [#5090], [#5109])
- **clarification:** Human-input (clarification) cards support structured
  form fields, so an agent can request exactly the input it needs instead
  of free text only. ([#4406])
- **subagents:** Built-in subagents now receive the current-date context
  anchor, so delegated tasks involving relative dates behave like tasks the
  lead agent handles directly. ([#4797])
- **subagents:** A Settings page manages a deployment-level Subagent catalog
  (admin-managed worker definitions alongside built-in and `config.yaml`
  ones), and Custom Agents can restrict delegation to an explicit worker
  allowlist enforced at both prompt and execution time. ([#4887])
- **subagents:** Subagent concurrency is now governed by one process-wide
  capacity controller, and an opt-in `batch_task` tool runs large
  collections of independent items as durable, resumable SQL-backed batches
  with leases, bounded retries, pause/resume/cancel, and a chat panel for
  tracking progress. ([#4998])

#### Memory

- **memory:** Memory consolidation synthesizes fragmented facts, and a staleness
  review prunes silently-outdated facts using LLM-assigned per-fact
  `expected_valid_days` / `staleFactsToExtend`. ([#3996], [#3860], [#4143])
- **memory:** Guaranteed injection of correction facts (with graceful fallback)
  so user corrections always reach the model. ([#3592])
- **memory:** Slim the pluggable `MemoryManager` interface for backend
  onboarding - new backends no longer implement unused abstract methods, and
  DeerMem-specific hook injection moves out of the shared factory. ([#4326])
- **memory:** Incremental agent-scoped Markdown fact storage isolates per-agent
  facts and updates a single fact without rewriting or reindexing the whole
  collection. ([#4279])
- **memory:** Memory message processing adds a conversation watermark,
  trivial-turn filtering, and a durable queue so extraction no longer re-feeds
  the full conversation every turn. ([#4447])
- **memory:** A built-in FTS5/BM25 retrieval adapter provides full-text
  search over stored memories without an external retrieval service.
  ([#4360])
- **memory:** New pluggable memory backends: OpenViking and mem0 over HTTP,
  plus Honcho as a user-model memory provider. ([#4509], [#4528], [#4730])
- **memory:** A hybrid fact eviction policy blends multiple signals when
  deciding which stored facts to drop as memory fills. ([#4789])

#### Skills

- **skills:** Native SkillScan (phase 1) statically analyzes skill packages at
  load, and `describe_skill` enables deferred discovery so the model fetches a
  skill's schema on demand instead of loading all skills up front. ([#3033],
  [#3775])
- **skills:** Per-user custom skill isolation with sandbox mounting. ([#3889])
- **skills:** The skill list reopens after a skill is selected, so several
  skills can be attached in a row. ([#4639])
- **skills:** Install local `.skill` archives directly from the Skills
  settings page, reusing the existing per-user installer and security scan.
  ([#5039])

#### Models & integrations

- **community:** New web search/fetch engines - GroundRoute, Crawl4AI
  (`web_fetch`), and a fastCRW provider - plus a Browserless `web_capture`
  screenshot tool and Brave `image_search`. ([#3675], [#3821], [#3585], [#3881],
  [#3866])
- **mcp:** Per-server `tool_call_timeout` for MCP tool calls, and routing hints
  that guide the model to the right server. ([#3843], [#4004])
- **mcp:** Add an official OpenViking `/mcp` example that exposes the native
  tool set through DeerFlow's generic MCP client. ([#4745])
- **community:** Agentic browser control as a first-class thread capability -
  Playwright-backed browser sessions the agent operates while the user observes
  or takes over from the workspace. ([#4187])
- **community:** Lark/Feishu CLI integration bundles the runtime install, the
  official `lark-*` skill pack, and an interactive auth flow so the integration
  is no longer environment-dependent. ([#3971])
- **integrations:** Lark/Feishu app credentials can be switched per user
  from Settings > Integrations: new App ID/Secret values are validated
  before anything is committed, and the previous OAuth token is revoked
  after a successful switch. ([#4703])
- **acp:** MiniMax Code (`mcode acp`) is supported and documented as a
  native external coding agent, and ACP thought chunks are no longer
  concatenated into tool results. ([#4846])
- **models:** A Z.AI GLM-5.3-Flash profile keeps thinking permanently enabled
  and stops generic reasoning-effort forwarding, since the model rejects
  disabled thinking and only accepts its own effort values. ([#5074])
- **community:** New web search providers - Serply (with news and scholar
  verticals) and Tencent Cloud WSA - plus native recency filters
  (day/week/month/year) shared across DDGS, Brave, Tavily, and SearXNG.
  ([#5023], [#5057], [#5099])
- **knowledge:** Opt-in read-only RAGFlow retrieval exposes a
  `knowledge_search(query)` agent tool over configured RAGFlow datasets, with
  a dataset-ID allowlist and credential/dataset-id redaction on error paths.
  ([#4955])

#### MCP

- **mcp:** A durable task runtime for MCP: long-running tool tasks survive
  Gateway restarts through a durable driver, and their progress and
  completion notifications surface in the chat UI. ([#4665], [#4690],
  [#4833])
- **mcp:** Shared MCP servers can inject per-user credentials: a single
  server entry authenticates each DeerFlow user with their own header
  value, unmapped users are denied by default, and stored credentials are
  masked in Gateway API responses. ([#4868])
- **mcp:** Per-server `tool_name_prefix` option lets servers that already
  namespace their own tools keep their original tool names; the default
  behavior is unchanged. ([#4624])
- **mcp:** Settings > Tools can add, edit, and delete MCP servers through
  targeted Gateway endpoints, with a copy-paste JSON workflow that preserves
  advanced fields and masked secret placeholders. ([#5022])
- **mcp:** Shared HTTP/SSE servers can map request-scoped secrets to headers
  via `headers_from_context`: callers supply per-request values in
  `config.context.secrets`, the config stores only key names, and missing
  values deny by default. ([#5010])

#### Channels

- **channels:** Expose the IM `channel_user_id` to sandbox commands as
  `DEERFLOW_CHANNEL_USER_ID`. ([#3926])
- **channels:** Queue rapid same-thread messages and preserve topic-card
  previews across batches. ([#3988])
- **channels:** Inbound webhook deduplication moves to Postgres, so several
  Gateway pods can serve the same IM channel without double-processing
  events. ([#4210])
- **channels:** DingTalk inbound messages support file and image
  attachments. ([#4423])
- **channels:** New Buzz (Nostr) channel connector, including the frontend
  experience for the channel. ([#4649], [#4727])

#### Auth & guardrails

- **auth:** Generic OIDC/SSO authentication with Keycloak support. ([#3506])
- **guardrails:** Authenticated runtime context is exposed in `GuardrailRequest`,
  and security interventions are persisted as run events. ([#3665], [#3837])
- **auth:** "Keep me signed in" login option with a centralized session-cookie
  policy (persistent `Secure` cookies on HTTPS, session cookies on public HTTP).
  ([#4255])
- **auth:** Deployments can close local self-registration to restrict new
  accounts to SSO/OIDC provisioning. ([#4311])
- **authz:** Built-in RBAC authorization provider with a unified factory, plus
  tool-authorization enforcement at both assembly (tools removed before the
  model sees them) and runtime (denied calls blocked). ([#4260], [#4370])
- **authz:** Gateway route permissions are derived from the configured
  AuthorizationProvider rather than a fixed table. ([#4439])
- **authz:** Model authorization is enforced at Gateway routes and again in
  the agent runtime, and `sandbox:execute` is checked when a sandbox is
  acquired - users can no longer reach models or sandboxes they are not
  authorized for. ([#4540], [#4911])

#### Sandbox & provisioner

- **sandbox:** New E2B and BoxLite (micro-VM) sandbox providers; BoxLite ships
  with a warm pool. ([#3883], [#3940], [#3951])
- **provisioner:** ClusterIP Services and scoped per-skill PVC mounts, plus a
  configurable sandbox container port. ([#4016], [#3928])
- **sandbox:** New cloud sandbox providers: Tenki and OpenSandbox.
  ([#4382], [#4877])
- **sandbox:** An optional lark-cli credential broker sidecar (K8s
  provisioner mode) keeps Lark app secrets and OAuth tokens out of the
  sandbox filesystem entirely - the sandbox sees only a shim that forwards
  commands to a loopback broker in the pod. Off by default. ([#4501])
- **sandbox:** The E2B mount-upload wall-clock deadline is configurable via
  `mount_upload_deadline_seconds` (default 120s). ([#4876])

#### Extensions & plugins

- **extensions:** An out-of-tree Python extension system: extensions can
  contribute middleware, task-lifecycle and system-model observers, Gateway
  services, and HTTP routers, and are managed with `deerflow extensions`
  install/enable/disable/remove. ([#4636], [#4684], [#4780])
- **extensions:** Extensions can observe what the agent did - message
  provenance, middleware policy declarations, agent-assembly fingerprints,
  context-compaction records, guardrail decisions, and the MCP origin of a
  tool. `deerflow-extension-api` moves to 0.2.0; extensions written against
  0.1 are refused at startup with an install hint. ([#4863])

#### Persistence

- **persistence:** A custom PostgreSQL schema can be selected via
  `postgres_schema`; ORM, LangGraph checkpointer, and store tables are all
  created there, and the schema is created automatically at startup.
  ([#3442])

#### Frontend

- **frontend:** Branching support for assistant turns and side conversations for
  quoted follow-ups. ([#3950], [#3934])
- **frontend:** Regenerate the latest answer. ([#3637])
- **frontend:** Citation-sources evidence panel, workspace change review for
  agent runs, and a visualized `ask_clarification` card. ([#3907], [#3945],
  [#3956])
- **frontend:** Voice dictation, prompt-history recall with arrow keys, composer
  input polishing, and a "(thought for N seconds)" thinking-duration chip.
  ([#4036], [#3718], [#3986], [#3627])
- **frontend:** Feature-gate the agents UI behind the `agents_api` flag, and
  persist AI turn duration in backend and UI. ([#3769], [#3663])
- **frontend:** Render slash-skill activations as inline chips. ([#3981])
- **frontend:** Localized AI-assistance disclaimer. ([#4374])
- **frontend:** Pin recent chats. ([#4442])
- **frontend:** Validate `/goal` objective length in the composer. ([#4337])
- **frontend:** Real-time context window usage is shown as a conversation
  grows. ([#3183])
- **frontend:** The latest user turn can be edited and rerun in place.
  ([#4377])
- **frontend:** Replies can be typed and sent while a clarification card is
  pending. ([#4530])
- **suggestions:** The number of follow-up suggestions is configurable via
  `suggestions.max_suggestions` (default 3). ([#4533])
- **artifacts:** Text artifacts can be edited inline in the artifact panel.
  ([#4596])
- **artifacts:** Markdown artifacts open rendered in a new-window reader (with
  "View source" and "Download" fallbacks), and all files presented in a run
  can be downloaded as one zip archive derived from the run's delivery
  receipt. ([#5056], [#5117])
- **frontend:** Browser Live is available in Custom Agent chats. ([#4719])
- **frontend:** A conversation outline navigates long chats: past 5 user turns,
  a compact side menu lists the conversation's questions and jumps between
  them, tracking the current section. ([#5025])
- **frontend:** Scheduled tasks can be duplicated into an editable draft that
  carries over the configuration but not the run history. ([#5064])
- **threads:** Branched conversations get distinguishing titles
  (automatic `Title (2)`, `Title (3)` sibling numbering) and the
  recent-chats list shows parent-child lineage with tree connectors.
  ([#4983])

#### Observability & tooling

- **observability:** Trace-id correlation with enhanced logging and agent
  observability via Monocle. ([#3902], [#4024])
- **tooling:** A Hermes-like terminal workbench (`deerflow` CLI) backed by
  `DeerFlowClient`, plus a redacted community support-bundle generator. ([#3760],
  [#3886])
- **setup:** The setup wizard now asks whether OpenAI-compatible gateway models
  support thinking, and a Volcengine Coding Plan quick-setup path was added.
  ([#3428], [#4141])
- **tui:** `clear` command. ([#4306])
- **tui:** The TUI supports a transparent terminal background. ([#4631])

### Changed

- **tenant identity:** Accepted/trusted context, run/event/lifecycle storage,
  assembly evidence, durable tool receipts, extension contributor requests,
  recovery, deployment reports, support diagnostics, and Redis factories now
  consume the same immutable server-owned tenant reference. Recovery stops
  before ownership/model/tool work on a mismatch.
- **frontend performance:** Keep the public root and localized docs static;
  lazy-load closed workspace panels and editor/highlighter dependencies;
  incrementally derive streamed message state; bound streaming Markdown work;
  virtualize long message and chat lists; pause offscreen decorative effects;
  and enforce representative route JS/CSS budgets.
- **browser:** Negotiate binary Browser Live JPEG frames, retain the legacy
  JSON/base64 protocol for older clients, coalesce presentation to the latest
  frame per refresh, and revoke replaced object URLs.
- **artifacts:** Stream regular text artifacts with HTTP byte-range support and
  limit the initial Web UI preview to 1 MiB until the user explicitly loads the
  complete file.
- **sandbox:** The Helm chart now defaults per-sandbox Services to `ClusterIP`
  instead of `NodePort`, so the code-execution sandbox is reachable only inside
  the cluster via Service DNS (`http://sandbox-<id>-svc.<ns>.svc.cluster.local`)
  and is no longer bound on every node's interfaces - including the
  externally-reachable ones on GKE/EKS/AKS. Existing chart installs flip
  NodePort -> ClusterIP on upgrade. To preserve the old reachability (an
  external probe hitting the 30xxx port, or the Docker-Compose/hybrid path
  where the gateway is not in K8s), set `provisioner.sandboxServiceType: NodePort`
  (with `provisioner.nodeHost` if needed). The provisioner itself is unchanged
  (mode-aware since #4016). ([#4190])
- **skills:** An active restrictive skill must explicitly list `task` in
  `allowed-tools` to delegate to a subagent. Read-only discovery infrastructure
  (`tool_search` and `describe_skill`) remains available, but cannot grant schema
  visibility or execution for a denied business tool. ([#4098])
- **memory:** Pre-abstraction top-level `memory.*` DeerMem fields
  (`storage_path`, `max_facts`, `debounce_seconds`, `model_name`,
  `token_counting`, `staleness_*`, `consolidation_*`, ...) are **auto-migrated
  into `backend_config`** on load with a warning, so an upgrade does NOT silently
  revert customized settings to defaults (`model_name` ->
  `backend_config.model.model`). Move them under `memory.backend_config` in
  `config.yaml` to silence the warning. ([#4122])
- **memory:** Added `memory.mode` (`middleware` | `tool`); `tool` mode registers
  memory tools (`memory_search`/`add`/`update`/`delete`) the model calls directly
  instead of passive per-turn summarization. `manager_class` resolution is now
  fail-fast (raises `ValueError` on an unknown backend instead of silently
  falling back). ([#4023])
- **middleware:** Declarative layered middleware builder; `ThreadData` now runs
  before `Uploads`. ([#3809])
- **sandbox:** The host->virtual output-masking regex now has a single owner,
  eliminating duplicated pattern compilation. ([#4108])
- **docs:** `AGENTS.md` is now the source of truth for agent guidance, imported
  by `CLAUDE.md` via `@AGENTS.md`; module guides refreshed. ([#3770])
- **memory:** The OpenViking memory backend now uses the official OpenViking
  adapter; the old trusted-mode `auth_mode`/`account` fields are rejected in
  favor of a credential-bound USER API key. ([#4707])
- **gateway:** Threads created before the run-event journal have their
  checkpoint history backfilled as seed events before the first new run, so
  legacy conversations stay visible and correctly ordered after an upgrade.
  ([#4590])
- **agents:** Subagent delegation is now routed by net benefit: the lead agent
  defaults to direct execution unless parallel latency, specialist capability,
  or context isolation clearly pays off. ([#4384])

### Fixed

- **gateway:** Stop persisting a caller-supplied `deerflow_trace_id` on the run
  record. `body.metadata` reaches both the live run config, which the run
  worker restamps, and the run record echoed verbatim by the runs API; only the
  first was covered, so a client could make the most durable surface of a run
  disagree with the `X-Trace-Id` and the log lines from the same request. The
  id is now stamped once at the trust boundary, `config.context` is closed off
  the same way, and a thread's own metadata is no longer seeded with the
  run-scoped id of whichever run created it. ([#5119])
- **gateway:** Expose `X-Trace-Id` in `Access-Control-Expose-Headers`. It is not
  CORS-safelisted, so split-origin browser clients — the ones that cannot read
  the Gateway's logs either — could not read the correlation id they are meant
  to quote in a bug report. ([#5119])
- **gateway:** Keep `X-Trace-Id` on unhandled-exception 500s. Starlette's
  `ServerErrorMiddleware` emits those through the raw send outside every user
  middleware, so the 500 for a server bug — the response most in need of
  correlation — was the only one shipped without the id. `TraceMiddleware` now
  sends its own 500 carrying the header before re-raising; the server's
  exception logging is untouched and mid-stream failures propagate unchanged.
  This fallback is emitted outside `CORSMiddleware` and stays CORS-opaque, so
  split-origin browser clients cannot read the id on this one response — same
  as the `ServerErrorMiddleware` 500 it replaces. ([#5119])
- **gateway:** Strip a forged `deerflow_trace_id` from the persisted request
  echo. `body.config` is stored verbatim as `runs.kwargs_json` and served back
  by the runs API, so a forged id in `config.metadata` or `config.context`
  survived on that one surface while every other carried the real id.
  `redact_config_secrets` now drops the key from both containers, and
  `build_run_config` merges run metadata onto a copy so the server-stamped id
  can no longer be written through into the caller's request body. ([#5119])
- **artifacts:** Keep explicit full-file loading scoped to the source thread, so a same-path artifact in another conversation keeps its 1 MiB preview. ([#4634])
- **sandbox:** `SandboxAuditMiddleware` no longer blocks ordinary command
  substitution that only captures output. The rule now judges *position* instead
  of matching any `$(`: `x=$(curl url)`, `echo $(curl url)`, an argument, and a
  `for` word list all run normally, while a substitution in command position
  (`$(curl url)`, after a `|`/`&&`/`;`, behind leading assignments or an
  `env`/`nohup`/`time` style wrapper, or as an `eval`/`source` argument) still
  blocks because it executes fetched content. An interpreter's code-string flag
  (`bash -c`, `python -c`, `perl -e`, `node -p`, `php -r`, and the `<<<`
  here-string) is treated as an execution context wherever it appears, so
  `bash -c "$(curl url)"` blocks; `source <(curl url)` and the backtick spelling
  of `eval`/`source` now block too, neither of which was detected before. An
  unquoted newline separates statements like `;`, so `echo hi` followed by a
  new line starting `$(curl url)` blocks as well, while heredoc bodies are
  consumed as data — writing a file whose content happens to start a line with
  `$(curl url)` is not a command.
  Variable expansions whose name merely starts with a risky executable
  (`$shell`, `$bashrc`, `$python_version`) and lookalike binaries
  (`shellcheck`, `shasum`) are no longer false positives.
  ([#4611], [#4623])
- **mcp:** Isolate Settings > Tools enable/disable updates to one MCP server, so
  an unrelated disallowed stdio command no longer blocks every switch; allow
  disabling a disallowed target while still rejecting its re-enable, preserve
  the raw extensions config, honor the MCP-spec `transport` alias when enabling
  SSE/HTTP servers, surface backend validation details in the UI, and atomically
  replace the shared config for MCP, skill, and embedded-client updates so
  interrupted writes cannot leave it truncated.
  ([#4574], [#4577])
- **runtime:** Thread metadata now switches to `running` only after the run passes
  the startup barrier, so pending-cancelled runs no longer briefly project
  `running`; clients may observe the prior thread status during worker startup.
  ([#4450])
- **runtime:** Re-check orphan candidates through an atomic, lease-aware takeover
  claim so a successful heartbeat after the scan keeps the run active and only
  one reconciler reports recovery. ([#4424], [#4434])
- **skills:** Apply `allowed-tools` only to slash-activated or actually loaded
  lead-agent skills, preventing passive enabled skills and evaluation fixtures
  from removing MCP, web, file, and delegation tools from every run. ([#4095],
  [#4098], [#4192])
- **models:** Honor `api_base` on every `BaseChatOpenAI` subclass (`VllmChatModel`,
  `MindIEChatModel`, `PatchedChatMiMo`, `PatchedChatStepFun`, `PatchedChatMiniMax`),
  not just `ChatOpenAI` / `PatchedChatOpenAI`. Those five previously dropped the
  configured endpoint silently and then failed every request with an opaque
  `unexpected keyword argument 'api_base'`; the unknown-config-key warning was
  disabled for them as well. Both now gate on `issubclass(BaseChatOpenAI)`.
  ([#4146])
- **agents:** Coalesce `SystemMessage`s before the LLM request; ensure a visible
  response after tool runs; avoid a default LLM title call before stream end;
  reserve ellipsis room so the local title respects `max_chars`; and snap the
  tool-output tail forward so fallback truncation respects `max_chars`. ([#3711],
  [#4033], [#3885], [#4052], [#4017])
- **agents:** Skip dateless reminders in the dynamic-context date scan; load
  `SOUL.md` from agent dirs without `config.yaml`; require `config.yaml` in
  `update_agent`'s legacy-agent guard; and refuse empty `SOUL.md` updates.
  ([#3685], [#4136], [#4166], [#4219])
- **middleware:** Window the loop-detection tool-frequency counter so long runs
  no longer false-trip; prevent the title middleware from streaming tokens;
  fix positional fallback consuming an unrelated todo when the same-content list
  is exhausted; acquire the token-budget lock across `_apply`, `before_agent`,
  `_clear_run_state`, and `_drain_pending_warnings`; drop orphan `ToolMessage`s
  so strict providers don't 400; sanitize invalid tool-call arguments; and
  recover from empty tool-call names and malformed tool-call ids in dangling
  repair. ([#4072], [#3566], [#3709], [#3714], [#4080], [#4193], [#4008],
  [#4246])
- **subagents:** Inherit `LoopDetectionMiddleware` and summarization middleware
  so tool loops break and steps are captured; surface the turn-budget cap as
  `MAX_TURNS_REACHED` with a partial result; unify guardrail caps on the additive
  `stop_reason` + `token_budget`; inject durable context before compaction;
  preserve the parent checkpoint namespace; prohibit the `task` tool in the
  general-purpose system prompt; re-buffer subagent events on flush failure to
  avoid losing steps; and fix the lost `loop_capped` stop reason when a
  subagent's `run_id` is `None`. ([#3931], [#4009], [#3949], [#3980], [#4040],
  [#4215], [#4161], [#4082], [#4059])
- **memory:** Harden against null/empty edge cases - skip whitespace-only facts;
  coerce null `confidence` / `source.confidence` in updates, searches, and the
  three remaining raw reads; treat explicit `null` `backend_config` values as
  omitted; fix `KeyError` / `UnboundLocalError` when a fact has no id or the
  facts list is empty; stop the busy-spin in the debounced update queue; and
  flush the memory queue on graceful shutdown to prevent loss. ([#3719], [#4074],
  [#4076], [#4034], [#4217], [#3993], [#3992], [#4073], [#4181])
- **runs:** Close multi-worker ownership gaps in run atomicity; fail-stop local
  execution when lease renewal cannot be confirmed before its deadline and
  fence late completion writes after peer takeover; degrade cancel to lease
  takeover for multi-worker; keep `create_thread` idempotent when the insert
  loses a race; read `stop_reason` from runtime context; and persist run duration
  in checkpoints for history reads. ([#4003], [#4064], [#4414], [#3800], [#4188],
  [#4118], [#4431])
- **runtime:** Serialize SQLite event-store writes to prevent per-thread
  sequence collisions; skip hidden human messages in the journal; and drop the
  silent delta-discard in `_merge_stream_text`. ([#4077], [#3698], [#4085])
- **gateway:** Attach thread-message feedback by real `event_type`; offload
  blocking filesystem IO in artifact serving, gateway uploads, and the Discord
  channel; limit the uploaded-file context manifest; and live-tail malformed
  Redis reconnect ids. ([#3651], [#3551], [#3935], [#3927], [#3917], [#4012])
- **uploads:** Claim the converted-Markdown companion filename before writing
  it, so two convertible uploads sharing a stem (or a convertible plus a
  same-stem `.md` upload) no longer silently clobber each other within one
  request. When `uploads.auto_convert_documents` is on, the companion `.md` now
  gets a unique name (e.g. `a_1.md`); `POST /threads/{id}/uploads` and
  `DeerFlowClient.upload_files` both report the actual name in `markdown_file`.
  ([#4288])
- **config:** Coerce null object config sections to their defaults; honor the
  unified database configuration in the store and sync checkpointer; and have
  legacy DB backfill create missing `Index` objects on existing tables. ([#3573],
  [#3904], [#3994], [#4090])
- **models:** Apply the `stream_chunk_timeout` default to all `BaseChatOpenAI`
  subclasses; and normalize `api_base` -> `base_url` for `ChatOpenAI` with a
  warning on unknown config keys. ([#4102], [#3790])
- **mcp:** Isolate tool-discovery failures per server; synchronize the
  session-pool singleton lifecycle; invalidate the tools cache on config content
  + path (not just newer mtime); validate MCP tool names at load so deferred
  prompts stay inert; and route tools by source server, not name prefix. ([#3772],
  [#3797], [#4124], [#4154], [#3812])
- **skills:** Activate a slash skill once per run, not per model call; close the
  skill-install security-scan coverage gap; recognize fully deleted skill
  packages in review CI and remaining `requests` / `httpx` methods as network
  sinks in SkillScan; reuse the resolved app config in the no-arg skills prompt
  section; and reload mounted skills without restarting the Gateway. ([#4103],
  [#3924], [#4169], [#4130], [#4160], [#4264])
- **sandbox:** Guard the reverse path-translation and output-masking regexes
  with segment boundaries; handle one-sided line ranges and empty files in
  `read_file` / `str_replace`; align the AIO bash working directory; use
  `os.sep` in the reverse-resolve containment check on Windows; normalize
  Windows backslash paths in bash commands; stop `glob` / `grep` / `ls` from
  surfacing disabled skills' files; and allow valid heredoc commands in the
  sandbox audit. ([#4035], [#4053], [#4078], [#4079], [#4051], [#4058], [#3869],
  [#4096], [#3786])
- **sandbox:** Synchronize the sandbox provider singleton lifecycle (with
  concurrency regression tests) and keep k8s calls off the event loop in the
  provisioner. ([#3730], [#3941])
- **sandbox:** Align sandbox artifact mounts with the channel user; fix
  local-dev (`make dev`) on non-root / NFS hosts; reap macOS nginx processes on
  stop; and fix production Postgres UV-extras detection in Docker. ([#3729],
  [#3590], [#3828], [#3897])
- **channels:** Validate the channel provider before resolving its config;
  dedupe GitHub webhook redeliveries and drop redundant GitHub review-comment
  webhook fan-out; scope the slash-skill whitelist check to the run's owner;
  batch Feishu file messages into one thread and dispatch Feishu group commands
  prefixed with a bot @mention; accept leading @mentions before `/connect` bind
  codes and don't treat a bare "connect" as a bind command; stop Feishu from
  creating thread topics and throttle card updates; let the UI runtime channel
  config win over `config.yaml`; fix `require_mention` gating on
  whitespace-only `bot_login` / `mention_login`; guard null quote fields in
  WeCom; and key inbound dedupe on chat-scoped workspaces so Telegram, Feishu,
  WeChat and DingTalk redeliveries stop re-running the agent on a default
  (unbound) configuration, releasing the dedupe key on transient failures so a
  redelivery can still recover. ([#4100], [#4104], [#4131], [#4129], [#3753],
  [#4229], [#4222], [#4251], [#3810], [#3674], [#4055], [#4069], [#4287])
- **frontend:** Preserve messages and durable context across summarization;
  preserve artifacts and stabilize artifact paths during streaming; resolve
  relative artifact image paths; retain presented artifacts in the header
  dropdown; keep orphan tool messages visible; show assistant text during tool
  steps; reset new chat on client-side navigation; prevent stream cancellation
  on concurrent submit; fix stale-run reconnect and cancel handling; fix chat
  math rendering, single-tilde markdown, double reasoning rendering, UTF-16
  markdown binary classification, and `<memory>` tags in Streamdown; make
  recent-chat rows fully clickable; validate attachment limits before upload and
  fix uploaded-file metadata in message copy; fix mobile workspace and
  accessibility blockers, the card tool-message bug, and side-chat toolbar /
  panel-button behavior; block unresolved suggestion-template placeholders;
  refresh notification permissions; show the branch action only for completed
  turns; enable regenerate in custom agent chats; and generate a fallback title
  for interrupted first-turn runs. ([#3826], [#3791], [#4094], [#4038], [#3854],
  [#3880], [#4114], [#3673], [#3878], [#3908], [#3557], [#4245], [#3870], [#3966],
  [#4209], [#3733], [#3900], [#3944], [#3740], [#3976], [#3959], [#3961], [#3764],
  [#3768], [#4147], [#3967], [#3874], [#3644])
- **tui:** Interrupt an active run before `/quit` exits. ([#4235])
- **harness:** Don't flag the outline as truncated at exactly `MAX_OUTLINE_ENTRIES`
  headings. ([#3856])
- **tracing:** Attach Langfuse trace metadata to the goal evaluator. ([#4202])
- **context:** Resolve the context-compress bug. ([#4065])
- **threaddata:** Fix `AttributeError` when `runtime.context` is `None`. ([#3989])
- **goal:** Stop `continuation_count` double-bump during stand-down. ([#4199])
- **circuit-breaker:** Stop wedging after a non-retriable half-open probe. ([#3991])
- **github:** Match `allow_authors` logins case-insensitively. ([#4218])
- **community:** `image_search` now returns the full-resolution image URL. ([#3990])
- **skills:** Offload blocking filesystem IO in the skill-history endpoint.
  ([#3563])
- **skills:** Don't treat a lazily evaluated PEP 695 type alias as a network
  sink in SkillScan. ([#4315])
- **tracing:** Resolve the Langfuse trace user from runtime context. ([#3794])
- **guardrails:** Propagate internal owner attribution into the guardrail
  context. ([#3839])
- **subagents:** Clamp the subagent limit consistently with
  `MIN_SUBAGENT_LIMIT`. ([#4081])
- **subagents:** Load user-scoped skills. ([#4356])
- **mcp:** Per-server fail-soft OAuth priming, and persist rotated refresh
  tokens. ([#4084])
- **mcp:** Ignore malformed path-like text. ([#4456])
- **auth:** Resolve email accounts case-insensitively. ([#4101])
- **auth:** Recover from setup-status timeouts. ([#4371])
- **scheduler:** Close a dispatch race that could launch two runs for one
  scheduled task. ([#4105])
- **channels:** Buffer and drain GitHub comments queued during a busy run.
  ([#4133])
- **channels:** Escape Slack reserved characters before mrkdwn conversion.
  ([#4197])
- **channels:** Check `response.success()` on Feishu card/reaction SDK calls.
  ([#4234])
- **channels:** Drop inbound DingTalk messages that carry no conversation
  identity. ([#4316])
- **channels:** Receive inbound Telegram attachments. ([#4392])
- **memory:** Consolidated facts inherit `expected_valid_days` from their
  sources. ([#4225])
- **config:** Sync `_memory_config` with AppConfig auto-reload. ([#4208])
- **postgres:** Harden the async engine with `pool_recycle` and
  `command_timeout` to stop stale-connection 504s. ([#4230])
- **harness:** Add a timeout to `invoke_acp_agent` to prevent indefinite hangs.
  ([#4238])
- **community:** Surface the target-page error status in `web_fetch`
  (Browserless). ([#4239])
- **sandbox:** Widen the BoxLite/AIO tenant hash and verify identity on reclaim.
  ([#4171])
- **sandbox:** Make an empty `old_str` a no-op in `str_replace` on any file.
  ([#4256])
- **sandbox:** Serialize E2B release transitions. ([#4355])
- **sandbox:** Bound E2B output-synchronization resources. ([#4364])
- **sandbox:** Unwrap `Overwrite`-wrapped sandbox state in `after_agent`.
  ([#4381])
- **sandbox:** Bypass proxies for local AIO traffic. ([#4444])
- **models:** Surface length-capped model responses instead of dropping them.
  ([#4309])
- **streaming:** Keep large file generation responsive. ([#4354])
- **streaming:** Expose custom events to `astream_events`. ([#4403])
- **streaming:** Signal replay history gaps. ([#4426])
- **summarization:** Summarize with the run model and fall back on
  summary-provider failure. ([#4361])
- **runtime:** Remove transient image context after model calls. ([#4267])
- **runtime:** Stop subgraph stream frames from impersonating root frames.
  ([#4407])
- **runtime:** Reject unsupported run options and stream modes. ([#4430])
- **runtime:** Serialize checkpoint writes with active runs, linearize
  delta-mode checkpoint resume, and accept the SDK's default
  `stream_resumable=false` to avoid resume races. ([#4437], [#4460], [#4468])
- **checkpoint:** Unwrap `Overwrite` first writes into empty channels. ([#4383])
- **nginx:** Allow long chat prompts through `/api/langgraph/` without a raw
  500. ([#4277])
- **gateway:** Prefer `X-Trace-Id` over `metadata.deerflow_trace_id` when the
  header is set. ([#4283])
- **gateway:** Seed branch run-events so inherited history survives forking.
  ([#4385])
- **gateway:** Scope branch-history seed run ids per inherited turn. ([#4459])
- **frontend:** Harden artifact and markdown rendering. ([#4117])
- **frontend:** Classify a symlink replacing a file distinctly from deleted in
  workspace-change review. ([#4170])
- **frontend:** Offload blocking filesystem IO in the workspace-change
  text-cache lifecycle. ([#4268])
- **frontend:** Encode artifact URL path segments. ([#4278])
- **frontend:** Clarify run-duration display. ([#4348])
- **frontend:** Preserve regenerate state in branched threads. ([#4358])
- **frontend:** Default the reasoning-effort label to Medium when unset.
  ([#4373])
- **frontend:** Strip and parse the `<current_uploads>` upload-context tag.
  ([#4402])
- **frontend:** Keep leading orphan tool messages visible. ([#4408])
- **frontend:** Keep completed subtask cards stable after reload. ([#4432])
- **frontend:** Apply message-image `maxWidth` via inline style. ([#4446])
- **frontend:** Restore resizing for the artifacts and sidecar panels. ([#4469])
- **frontend:** Allow dev-server access from non-localhost hosts. ([#4471])
- **safety:** Backfill empty content-filter responses so they don't poison the
  thread. ([#4394])
- **tools:** Exclude injected runtime from the `list_uploaded_files` schema.
  ([#4376])
- **mcp:** Bound MCP server bring-up — tool discovery (subprocess spawn +
  `initialize` + `tools/list`) and persistent stdio session initialization —
  with a new per-server `session_init_timeout` (default 60s, `null` disables),
  so a hung stdio server can no longer block agent construction, or the whole
  Gateway event loop, indefinitely. `tool_call_timeout` still bounds individual
  stdio tool calls. ([#4657])
- **runtime:** Tool-output budget externalization no longer trips run delivery
  verification. The default `.tool-results` storage dir (and any custom
  `tool_output.storage_subdir`) is excluded from workspace-change snapshots and
  produced-artifact detection, so a run that only externalized oversized tool
  outputs succeeds instead of failing as an error. ([#4657])
- **frontend:** Hide stale follow-up suggestion chips while a turn is still
  streaming. ([#3396])
- **frontend:** Fix streaming render glitches: stop the word animation from
  replaying, keep step text stable, preserve message order during long runs,
  and keep reasoning above the answer. ([#4266], [#4510], [#4513], [#4578])
- **frontend:** Encode thread IDs in chat routes so IDs with special
  characters no longer break navigation. ([#4302])
- **frontend:** Render citation links from React children. ([#4486])
- **frontend:** Localize conversation export failure messages. ([#4493])
- **frontend:** Sync side panel state when a drag collapses the panel. ([#4556])
- **frontend:** Render one workspace-change card per run instead of
  duplicates. ([#4559])
- **frontend:** Refresh the active artifact's content when it changes. ([#4584])
- **gateway:** Reject non-positive read limits in API requests. ([#4284])
- **gateway:** Handle a null `config.configurable` when resolving the thread
  id instead of failing. ([#4301])
- **gateway:** Unify thread id validation across API routes. ([#4589])
- **gateway:** Merge concurrent thread metadata updates instead of letting
  them silently overwrite each other's changes. ([#4489])
- **gateway:** Expose the run metadata response header to cross-origin
  clients, so a split-origin frontend learns new run ids instead of staying
  stuck on the new-thread placeholder route until reload. ([#4535])
- **gateway:** Replay edit and rerun from a settled checkpoint so the edited
  prompt actually runs (previously a first turn's edit replayed the original
  prompt and vanished after reload), and keep a manual rename through the
  rerun. ([#4534], [#4539])
- **runtime:** Cancel a run from any live gateway worker, not only the one
  that owns it, so the stop button no longer depends on request routing.
  ([#4500])
- **runtime:** Close a replacement run when interrupt or rollback admission
  is cancelled mid-flight, instead of stranding an unseen active run on the
  thread. ([#4472])
- **runtime:** Regenerating a response now preserves the thread's current
  title and supports the latest interrupted response whose partial message
  never reached a checkpoint. ([#4480], [#4524])
- **agents:** Classify web_fetch error pages such as 404s as errors rather
  than successful evidence, so retries and stagnation guards can react.
  ([#4314])
- **agents:** Handle XML-to-dict option shapes when normalizing
  clarification choices. ([#4527])
- **subagents:** Run delegated subagents with isolated callbacks and lazy
  skill activation, fixing cross-event-loop failures and passive skills
  stripping baseline tools like `write_file`. ([#4497])
- **sandbox:** Handle overwrite-wrapped state when ensuring the sandbox is
  initialized. ([#4429])
- **sandbox:** Reconcile E2B sandboxes safely: pick the first healthy
  candidate, adopt the canonical instance per user and thread, defer a
  peer's live duplicates, and reap orphans after a grace window. ([#4443])
- **sandbox:** Claim ownership before destroying a sandbox that failed its
  readiness check, so a peer gateway can no longer adopt the not-yet-ready
  sandbox and kill a live turn. ([#4505])
- **sandbox:** Allow grep to search a single file. ([#4512])
- **sandbox:** Enforce the E2B capacity limit deployment-wide when sandbox
  ownership uses Redis, so multiple gateways cannot create past it. ([#4575])
- **skills:** Activate managed integration skills from the managed
  integrations root on slash invocation. ([#4570])
- **skills:** Offload blocking filesystem IO when updating a skill and
  serialize concurrent writes. ([#3565])
- **mcp:** Ignore oversized path-like text. ([#4582])
- **memory:** Harden long-term memory: reject duplicate facts inside the
  create critical section, truncate injected mem0 context on entry
  boundaries, and keep task-scoped instructions such as "inspect only" out
  of long-term memory. ([#4599], [#4600], [#4604])
- **scheduler:** Keep a successfully launched scheduled run's slot and run
  id when post-launch bookkeeping fails, preventing a later dispatch from
  launching a duplicate run. ([#4504])
- **config:** Treat a deleted extensions config file as absent instead of
  raising, so tool and skill config resolution keeps working. ([#4275])
- **config:** Normalize the `postgres://` short scheme for the async ORM
  engine. ([#4293])
- **console:** Disable cost reporting when model pricing mixes currencies
  instead of reporting a meaningless cross-currency total. ([#4564])
- **browserless:** Accept the `timeout` config key and harden its coercion.
  ([#4519])
- **docker:** Send `Connection: upgrade` only when the browser requests it,
  fixing login-page refresh loops when the Docker dev stack is accessed via
  a remote host. ([#4250])
- **runtime:** Group JSONL batch event writes by run, so a batch covering
  several runs no longer lands all events in the first run's file and makes
  later runs unreadable through per-run APIs. ([#4938])
- **runtime:** Restore standalone LangGraph Studio compatibility: the graph
  entrypoint and file-based app load again, the Studio identity can discover
  system assistants, and the documented `langgraph dev` workflow works.
  ([#4760], [#4838])
- **gateway:** Stamp `turn_duration` on a run's last AI message only in
  `/messages/page`, so multi-step turns no longer repeat the same run
  lifetime as thinking latency on every intermediate message. ([#4755])
- **gateway:** Preserve exact history attribution beyond the event page
  limit, so older AI messages on long-lived threads are no longer credited
  to a later turn's run and duration. ([#4953])
- **gateway:** Reject MCP task cancellation with HTTP 503 when the task
  worker is stopped, instead of acknowledging a cancellation that would
  never run. ([#4963])
- **middleware:** Correct four context-handling defects: fallback
  dynamic-context injection targets the latest user message instead of
  resurrecting an old prompt as the current turn; bare string blocks in
  list-form user content are sanitized like all other user text; duplicate
  placeholders are no longer emitted for the same invalid tool call; and
  summarization no longer compresses away the current request's user message
  while leaving the previous turn's behind. ([#4667], [#4668], [#4693],
  [#4882])
- **middleware:** Restore the system-prompt injection that teaches the model
  about the `write_todos` tool, which the todo middleware's model-call
  override had silently dropped. ([#4735])
- **agents:** Make SQL agent-store signatures content-sensitive, so an agent
  update that reuses its previous timestamp no longer leaves the GitHub
  agent registry serving stale webhook routing. ([#4709])
- **tools:** Resolve presented files with the runtime user, so `present_files`
  no longer rejects valid artifacts as outside the outputs directory when
  the request user context is unavailable. ([#4677])
- **tools:** Retain a strong reference to deferred subagent cleanup tasks, so
  garbage collection can no longer destroy a pending cleanup and leak
  cancelled subagent records, locks, and memory. ([#4928])
- **subagents:** Give every background subagent run a server-side execution
  ID, so concurrent runs that reuse a provider tool-call ID can no longer
  overwrite, poll, or cancel each other's state. ([#4758])
- **harness:** Offload ACP workspace creation and MCP config loading from the
  event loop, so invoking an ACP agent no longer raises blocking-IO errors
  or stalls other async work. ([#4965])
- **mcp:** Reject non-finite `poll_after_seconds` values on task snapshots
  when they arrive, so a bad polling interval no longer crashes scheduling
  and persistence after a successful poll. ([#4750])
- **mcp:** Keep the configured `grant_type` authoritative over
  `extra_token_params` during OAuth token exchange, so extra parameters can
  no longer silently switch the configured flow and be rejected by the token
  endpoint. ([#4860])
- **mcp:** Exclude the internal stdio MCP temp directory (`.mcp/tmp`) from
  workspace changes, so MCP temporary and debug files no longer appear
  alongside user deliverables or crowd real changes out of the file budget.
  ([#4898])
- **mcp:** Cancel the remote task when a durable task submission is cancelled
  mid-flight, so an interrupted submission no longer leaves a remote task
  running with no record to poll or stop. ([#4933])
- **sandbox:** Accept the documented E2B reconciliation config fields, so
  valid E2B configuration no longer produces misleading startup warnings.
  ([#4772])
- **sandbox:** Bound E2B mount upload resource use per file, per mount, and
  across the whole upload pass (shared size and file budgets plus a
  wall-clock deadline), so large mounts can no longer spike Gateway memory
  or hold sandbox capacity indefinitely. ([#4812], [#4842])
- **sandbox:** Preserve trailing whitespace in E2B-synced filenames and
  tolerate out-of-range remote mtimes, so output sync no longer re-downloads
  files repeatedly or aborts mid-sync. ([#4861])
- **sandbox:** Reject non-finite Redis lease-timing values in sandbox
  ownership config at parse time instead of crashing with an `OverflowError`
  during startup. ([#4960])
- **sandbox:** Resolve structured skill reads through the sandbox provider's
  path mappings, so `read_file` opens legacy and per-user custom skills
  under the same enabled-state projection as `ls` and shell execution.
  ([#4792])
- **skills:** Parse Responses API content blocks in the moderation scanner,
  so valid skill-management decisions returned as content blocks are no
  longer rejected as unparseable. ([#4936])
- **memory:** Reject non-positive and non-finite timeout and character-limit
  settings in the Honcho and Mem0 backends at config parse time, so a bad
  value fails fast instead of silently truncating stored text or crashing on
  the first HTTP call. ([#4783], [#4823])
- **memory:** Scope custom-agent bootstrap facts to the selected agent's
  bucket, so facts learned during setup no longer leak into the default
  bucket and influence ordinary lead-agent conversations. ([#4804])
- **artifacts:** Support atomic saves on Windows, and serve a SHA-256 ETag on
  artifact reads so inline preview and editing work on non-secure contexts
  such as plain-HTTP LAN origins where `crypto.subtle` is unavailable.
  ([#4629], [#4865])
- **frontend:** Keep conversation order stable around long runs: the
  submitted user message no longer renders twice or sinks below its own
  processing steps, and after a mid-run page reload a turn's steps can no
  longer appear above the user message that started the run. ([#4620],
  [#4660], [#4834])
- **frontend:** Stop matching `<header>` as `<head>` when injecting the base
  href into HTML artifact previews, so relative assets in report fragments
  that begin with `<header>` now load in the sandboxed preview iframe.
  ([#4625])
- **frontend:** Open landing-page case studies on a public read-only
  `/showcase/` route so anonymous visitors are no longer redirected to
  login. ([#4635])
- **frontend:** Sort the chats page by pinned state, so pinned threads no
  longer render below unpinned ones. ([#4643])
- **frontend:** Keep `<think>` pairs written inside markdown inline code in
  the rendered content instead of hollowing them out into the Reasoning
  panel, and restore the copy button for turns that contain only reasoning.
  ([#4647])
- **frontend:** Surface model-loading failures with a workspace error banner
  and retry action instead of a silently empty model list. ([#4840], [#5021])
- **frontend:** Preserve copy and other actions on completed assistant
  messages while a later turn is still streaming. ([#4844])
- **frontend:** Keep the browser live stream connected after a successful
  reconnect instead of tearing down the new socket and immediately creating
  another. ([#4951])
- **frontend:** Reuse the shared clipboard fallback when copying the Lark
  authorization link, so the copy action works in browsers without the
  Clipboard API. ([#4767])
- **frontend:** Use consistent "DeerFlow" casing in the composer disclaimer
  and fix the "What's New" heading on the landing page. ([#4970])
- **channels:** Bound inbound intake with a fixed worker pool and bounded
  admission queues, and await real cross-thread tasks on shutdown, so
  message floods are rejected promptly instead of accumulating and channel
  shutdown no longer tears down transports with work still in flight.
  ([#4800], [#4816])
- **channels:** Offload outbound attachment file IO for Feishu, Telegram, and
  WeCom to worker threads, so sending a large artifact no longer stalls the
  Gateway event loop. ([#4633])
- **channels:** Run Telegram connection-identity lookups on the Gateway event
  loop, so inbound messages and commands no longer crash with a cross-loop
  error when channel connections are enabled. ([#4815])
- **feishu:** Keep file receiving off the event loop and preserve every
  inbound attachment: duplicate provider filenames no longer overwrite each
  other, writes can no longer be redirected outside the thread bucket, and a
  failed attachment no longer blocks the rest of the message. ([#4627],
  [#4903])
- **dingtalk:** Strip leading `@bot` mentions before command classification,
  so slash commands like `/new` sent in group chats are recognized instead
  of treated as plain chat. ([#4724])
- **discord:** Refuse to start typing-indicator loops after the channel
  stops, so shutdown no longer leaves an infinite typing task sending
  events in the background. ([#4752])
- **wecom:** Serialize WebSocket start/stop transitions and await the SDK's
  real receive-task shutdown, so stopping the WeCom channel can no longer
  return before the socket closes or clear a newer connection's state.
  ([#4762])
- **buzz:** Drop replayed events across reconnects using a persistent
  seen-id store, so the agent no longer re-answers the last message in a
  channel after a relay or Gateway restart. ([#4888])
- **lark:** Keep the CLI lock directory writable inside sandboxes while the
  credential-bearing config root stays read-only, restoring Lark API
  commands that previously failed with a read-only filesystem error.
  ([#4701])
- **scheduler:** Enforce the global `max_concurrent_runs` budget for manual
  triggers too, returning HTTP 409 when the cap is reached instead of
  letting manual launches exceed it. ([#4769])
- **scheduler:** Coerce serialized task timestamps on read, so
  scheduled-task operations no longer fail when string-form timestamp values
  reach the database layer. ([#4785])
- **scheduler:** Support safe multi-instance scheduler recovery: startup no
  longer treats live runs owned by peer Gateway instances as local
  leftovers, so a restarting instance cannot interrupt a live run or trigger
  a duplicate execution; multi-instance mode is opt-in via
  `scheduler.multi_instance`. ([#4713])
- **scheduler:** Enqueue busy scheduled task runs instead of skipping them:
  occurrences that hit a busy reused thread now wait in a durable queue
  (bounded by `scheduler.queue_timeout_seconds`) and survive Gateway
  restarts, and the UI explains the queueing behavior. ([#4918])
- **cli:** Add `--recursion-limit` to headless `--print`, `--json`, and
  `--cli` runs, so long-running agent loops are no longer stuck at the
  default recursion limit of 100. ([#4615])
- **dev:** Exclude backend runtime state from the Uvicorn reload watcher in
  the backend `make dev` launcher, so an agent task writing files under the
  runtime tree can no longer restart the Gateway and reset concurrent
  users' requests. ([#4759])
- **dev:** Resolve diagnostic script paths from the script's own location, so
  root diagnostic commands work when invoked from any working directory.
  ([#4736])
- **docker:** Harden local and container startup: `make up` waits for the
  Gateway health probe before declaring the stack ready, Docker startup no
  longer aborts when `.env` is missing, the Gateway can write
  `extensions_config.json` in production, runtime data stays out of the
  image build context, log commands resolve the checkout root correctly,
  and the default loopback origins are allowed so the dev setup page can
  hydrate. ([#4658], [#4806], [#4852], [#4853], [#4956], [#4959])
- **gateway:** Stamp the server-authoritative feed position onto persisted
  messages, so an early user message no longer vanishes or jumps into the
  middle of the step stream once history exceeds one page and context
  compaction has fired. ([#4696])
- **lark:** Preserve the new app secret during managed credential switches by
  clearing the previous app's OAuth data before the replacement is written,
  so the subsequent browser authorization no longer resolves an empty
  `client_secret`. ([#4820])
- **messages:** Drop legacy `<uploaded_files>` tag handling: the backend treats
  the pre-#4174 spelling as ordinary content and strips only
  `<current_uploads>`, while the frontend keeps stripping the legacy tag so
  old threads still render cleanly. ([#4826])
- **skills:** Reject a blank `SKILL.md` description at the write gate, matching
  what the loader already requires, so editing a custom skill with an empty
  description no longer writes a file the loader then rejects - which
  destroyed the skill on disk. ([#4867])
- **sandbox:** Make the model-facing `description` argument optional (empty by
  default) across `bash`, `ls`, `glob`, `grep`, `read_file`, `write_file`,
  `str_replace`, and `task`, so providers that omit it are no longer rejected
  before execution. ([#4878])
- **sandbox:** Bound Windows command execution: host commands run in a new
  process group killed via `taskkill /T /F` on timeout so a descendant cannot
  hold the call open, and output flows through the existing bounded 10 MiB
  capture. ([#4946])
- **sandbox:** Scope the Windows MSYS path-conversion exclusion to safe virtual
  path prefixes instead of disabling conversion globally, so host-native CLI
  launchers that need normal conversion work again. ([#5003])
- **skills:** Rebuild per-user skill storage after an app-config hot reload,
  so it no longer stays bound to paths from the previous config instance.
  ([#4972])
- **skills:** Tokenize portable `allowed-tools` scalars with parenthesis
  awareness, so `Bash(tvly *)`-style entries stay intact, unmatched
  parentheses are rejected instead of silently fragmenting, and
  argument-scoped entries remain literal rather than broadening access.
  ([#4984])
- **agents:** Normalize `ToolMessage`s returned inside `Command` results, so
  error payloads no longer earn a default success receipt and tool-progress
  tracking sees them. ([#4977])
- **mcp:** Tear down the in-flight session owner when `get_session` is
  cancelled during eviction, so a cancelled caller no longer leaks the owner
  task or parks past its timeout. ([#5008])
- **mcp:** Reconnect ordinary stdio tools after a transport disconnect: the
  failed pooled session is evicted (only if still registered), the original
  error surfaces without automatic replay, and a later retry starts a fresh
  subprocess. ([#5018])
- **mcp:** Preserve pooled stdio sessions after protocol timeouts during
  durable MCP task polling - a 408 is not a disconnect - so task state
  survives and the next poll no longer reports `task_not_found`. ([#5027])
- **mcp:** Reject credentials that cannot travel as HTTP header values
  (trailing newline or whitespace, non-ASCII) at the config boundary, so the
  transport's exception - which echoes the full value - can no longer leak a
  secret into model context, checkpoints, and traces. ([#5066])
- **subagents:** Clean up the background-task entry when the poller exits
  unexpectedly and drop a PENDING registry entry when submission fails, so a
  failed or crashed poll no longer leaks the entry or leaves the subagent
  running unattended. ([#5069])
- **subagents:** Stop the zombie PENDING registry entry on the submit-failure
  path, and derive the capacity snapshot's queued count from the waiters'
  length instead of iterating a deque other threads mutate. ([#5086])
- **channels:** Synchronize `ChannelStore` reads with mutations, so
  `get_thread_id()`/`list_entries()` can no longer raise `dictionary changed
  size during iteration`. ([#5083])
- **discord:** Retain strong references to ack-reaction tasks and drain them
  on shutdown, so a GC pass can no longer silently drop a reaction or pin the
  channel across restart cycles. ([#5049])
- **buzz:** Move seen-event persistence off the event loop with coalesced
  atomic writes, preserving dirty generations when events arrive mid-write
  and awaiting the final flush on shutdown. ([#5103])
- **streaming:** Stop an `IndexError` in `MemoryStreamBridge._make_gap` when a
  subscriber reconnects to an empty or drained stream with an expired cursor.
  ([#5047])
- **uploads:** Keep deduplicated filenames within the 255-byte limit by
  truncating the stem on a UTF-8 code-point boundary, so two max-length files
  that differ only by a dedupe suffix upload successfully instead of failing
  the whole batch. ([#5059])
- **frontend:** Format structured upload error details (FastAPI validation
  issues, objects, arrays) instead of showing `[object Object]`. ([#5071])
- **frontend:** Keep a renamed thread's title in sync across the active chat
  header, document title, search results, and metadata caches without a
  reload. ([#5045])
- **frontend:** Truncate selected model names to the selector button width, so
  long model names ellipsize in the composer and sidecar instead of
  overflowing. ([#5050])
- **frontend:** Truncate long subtask card titles to one line with a tooltip,
  so a delegation whose model omitted `description` (falling back to the full
  prompt) no longer overflows the chat layout. ([#5136])
- **dev:** Default the frontend dev server to Webpack on all platforms
  (`DEER_FLOW_DEV_BUNDLER=turbo` opts back into Turbopack), avoiding
  Turbopack's macOS PostCSS worker leak and its Windows runtime panics.
  ([#5036], [#5133])
- **scripts:** Run repo shell scripts through an explicit interpreter
  (`bash scripts/...`), so a lost executable bit - zip/tarball downloads,
  `core.fileMode=false`, non-POSIX filesystems - no longer breaks
  `make docker-start` and friends with `Permission denied`. ([#5031])
- **deps:** Depend on the renamed `tenki` package instead of the PyPI-removed
  `tenki-sandbox` (same `tenki_sandbox` import), so clean checkouts can
  resolve dependencies again on `make dev`/`uv sync`. ([#5087])

### Performance

- **runtime:** Index `MemoryRunStore` by `thread_id` and `MemoryRunEventStore`
  events by `run_id` to avoid O(n) scans. ([#3562], [#3686])
- **subagents:** Deduplicate streamed AI messages via a seen-id set (O(n²) ->
  O(n)). ([#3687])
- **sandbox:** Cache `LocalSandbox` path-rewrite regexes and local-path masking
  patterns per instance instead of recompiling per search match. ([#3648],
  [#3713])
- **messages:** Index tool-call results per group. ([#4411])
- **frontend:** Coalesce streaming renders to a frame budget instead of per
  chunk. ([#4425])
- **frontend:** Stop re-deriving message content on every stream chunk.
  ([#4441])
- **sandbox:** `read_file` reads only the requested line range from the
  sandbox instead of fetching the whole file first. ([#3824])
- **browser:** Encode Browser Live progress frames as JPEG to cut progress
  payload size. ([#4836])
- **middleware:** Inject `view_image` content via `wrap_model_call` instead of
  a checkpointed hidden message, so up to 20 MB of base64 no longer sits in
  two checkpoints per viewed image and an interrupted run can no longer leave
  the payload behind. ([#5014])
- **frontend:** Cache settled copy-data derivation across streaming chunks, so
  each chunk no longer re-derives toolbar/copy text for every settled
  message. ([#5095])
- **runtime:** Bound gateway memory after terminal runs, stopping the post-GC
  low-water mark from creeping upward across completed sessions. ([#5112])

### Security

- **tenant isolation:** Strip tenant-looking client/context aliases, bind
  external-key admission and durable evidence to the process identity, reject
  schema/prefix drift, expose only the pseudonymous reference, and document
  separate-schema plus Redis key/channel ACL boundaries. Tenant identity is not
  a replacement for per-user authorization.
- **prompt-injection:** New input-sanitization middleware defends against
  prompt-injection, forged framework tags in the input guardrail are blocked,
  and system context is injected as a `SystemMessage` for role isolation. ([#3662],
  [#4155], [#3661])
- **prompt-injection:** HTML-escape untrusted content rendered into model prompts
  - memory facts and summaries, `SOUL.md`, subagent descriptions, skill metadata,
  and the conversation block in the memory-update prompt - and neutralize
  prompt-injection tags in `web_capture` tool results. ([#4028], [#4119], [#4137],
  [#4157], [#4162], [#4099], [#4060], [#4097], [#4128])
- **secrets:** Scrub inherited secret environment variables (`MYSQL_PWD`,
  `REDISCLI_AUTH`, abbreviated `*_PASS`, and Postgres `PGPASSFILE`) from the
  skill environment; request-scoped secrets are bound for both slash-activated
  and autonomously-invoked skills. ([#4018], [#4026], [#3871], [#3938])
- **web_fetch:** SSRF guard for self-hosted providers. ([#3942])
- **guardrails:** An empty allowlist now denies all tools instead of failing
  open. ([#4067])
- **authz:** Global skills-management endpoints now require admin; the legacy
  skills mount is gated by user visibility; artifacts honor a trusted
  `owner-user-id` header; and the trusted authorization principal is propagated
  through the runtime. ([#3855], [#3985], [#3982], [#4203])
- **auth:** Persist the `csrf_token` cookie for the access-token lifetime.
  ([#3872])
- **storage:** Stop persisting base64 image data in checkpoint state. ([#4140])
- **mcp:** Reject legacy MCP credentials in run metadata. ([#4448])
- **mcp:** Constrain stdio launcher arguments and environment variables at
  the config API, rejecting launcher flags and env names that could turn an
  allowlisted `npx`/`uvx` server registration into arbitrary code execution.
  ([#4617])
- **auth:** Harden validation of the post-login `next` path. ([#4587])
- **runtime:** Honor the LangGraph Server's authenticated user identity
  across agents, uploads, thread data, memory, and skills, and reject
  client-supplied auth identity fields. ([#4538])
- **frontend:** Send the session cookie on model, workspace-change, and
  ranged artifact reads in split-origin deployments. ([#4827])
- **frontend:** Restore sanitization in custom streamdown rehype chains, so
  artifact markdown previews and the memory settings summary can no longer
  render hostile HTML such as `javascript:` links or `on*` event handlers.
  ([#4987])
- **skills:** Copy projected skill files instead of hardlinking them, so a
  sandboxed write can no longer mutate the canonical skill source, and fail
  closed on a drifted projection namespace on every platform, including
  Windows. ([#4825], [#4830])
- **scripts:** Redact secret-shaped keys (`db_pass`, `signing_key`, ...)
  wherever they appear in bundled config, not only under well-known key
  names. ([#4242])
- **sandbox:** Sanitize MCP-sourced tool results through the same trust
  boundary as the built-in web tools, so a hostile or compromised MCP server
  can no longer hand the model forged `<system-reminder>` or user-input
  boundary tags. ([#4839])
- **sandbox:** Harden local Docker sandbox containers: published ports bind
  the Docker bridge gateway instead of `0.0.0.0` when the sandbox host is
  non-loopback (`DEER_FLOW_SANDBOX_BIND_HOST=0.0.0.0` restores the broad
  bind), Docker's default seccomp profile replaces unconditional
  `seccomp=unconfined` (opt back in with `DEER_FLOW_SANDBOX_SECCOMP_UNCONFINED=1`),
  and containers drop all capabilities, get `no-new-privileges`, and run with
  bounded resources. ([#4986])
- **authz:** Enforce run-create authorization on stateless stream/wait
  endpoints (`runs:create`), and require both `threads:write` and
  `runs:create` for scheduled-task create, update, resume, and manual-trigger
  mutations. ([#5030])
- **authz:** Re-check the authorization policy before reusing a persisted
  sandbox, so a revoked `sandbox:execute` grant takes effect on the next
  sandbox-backed turn instead of outliving the policy in the cached sandbox.
  ([#5006])
- **skills:** Enforce custom-Agent skill allowlists at the sandbox filesystem
  level: an explicit `skills` policy materializes a signed per-user/thread
  skills view, so a custom agent with shell or file tools can no longer read
  skills its policy excludes. ([#5077])
- **runs:** Reject cancel/rollback actions on GET stream joins with
  `405 Method Not Allowed` - cancel-then-stream is a POST operation - closing
  a state change that CSRF middleware deliberately exempted on safe methods;
  action-less GET joins are unchanged. ([#5092])

### Documentation

- **docs:** Clarify how `LocalSandboxProvider` resolves `sandbox.mounts[].host_path`
  under production Docker, with gateway bind-mount and config examples. ([#3833])
- **docs:** Document that Crawl4AI >= 0.9 requires a bearer token. ([#4518])
- **docs:** Document the GitHub inbound-dedupe TTL semantics, including what
  redeliveries are not deduped, and tighten the redelivery tests. ([#4274])
- **docs:** Update the agent AGENTS.md and ARCHITECTURE.md guides. ([#4817])
- **docs:** Document the Honcho memory backend with a dedicated guide and a
  long-term memory section entry in the README. ([#4822])

### Internal

- **tests:** Migrate frontend unit tests to rstest and run hook-level tests in
  a DOM environment. ([#3703], [#4453])
- **tests:** Require explicit opt-in for live client tests. ([#4482])
- **tests:** Rename the LLM-error test stand-in instead of the shared
  FakeError. ([#4744])
- **tests:** Replace the magic unwritable absolute path in tool-output tests
  with a self-constructed failure condition. ([#4722])
- **tests:** Add multi-turn message-stream invariants as graph integration
  tests. ([#3708])
- **tests:** Add trace-based behavioral tests with Monocle Test Tools,
  asserting agent routing, tool calls, and token/duration cost. ([#4025])
- **tests:** Cover passive skill tool visibility in the MCP layer. ([#4247])
- **tests:** Add SQL and concurrent-reconciler coverage for lease-aware orphan
  recovery. ([#4427])
- **tests:** Restore memory updater regression coverage. ([#4490])
- **tests:** Lock in POST logout from the gateway-offline banner. ([#4506])
- **tests:** Document known instance-client false negatives in the SkillScan
  tests. ([#4644])
- **refactor:** Extract frontend placeholder detection into a tested utility.
  ([#3783])
- **refactor:** Consolidate E2B client lifecycle helpers and reuse the kill
  helper during warm-pool eviction. ([#4262], [#4298])
- **refactor:** Name the E2B capacity-ledger meta-field count so the admission
  offset is explicit. ([#4764])
- **dev:** Trace self/cls attribute chains and local aliases in the
  blocking-IO detector's call graph, closing false negatives. ([#4200])
- **ci:** Publish the lark-cli-init and lark-broker images. ([#4558])
- **dev:** Route host-side pnpm consumers through a shared runner with a
  Corepack fallback so local workflows work without a pnpm shim. ([#4405])
- **bench:** Add an isolated checkpoint channel-mode benchmark comparing `full`
  and `delta` across latency, storage, and replay metrics. ([#4395])
- **deps:** Bump `cryptography` 49.0.0 -> 50.0.0, `postcss` 8.4.31 -> 8.5.25,
  `h2` 4.3.0 -> 4.4.1, `langgraph-checkpoint-sqlite` and
  `langgraph-checkpoint-postgres` 3.1.0 -> 3.1.1, and `nanoid` 5.1.6 -> 5.1.16.
  ([#4681], [#4683], [#4737], [#4738], [#4747], [#4748])
- **bench:** Add a reproducible hybrid memory-eviction evaluation under
  `backend/scripts/benchmark/deermem_eviction/` with a deterministic,
  blind-by-construction grader for the #4789 policy. ([#4810])
- **bench:** Measure Postgres checkpoint/blob/write storage growth in the
  checkpoint benchmark alongside memory and SQLite. ([#5051])
- **tests:** Exclude `tests/blocking_io/` from `make test`; the dedicated
  `make test-blocking-io` suite (and its CI workflow) remains the owner.
  ([#5105])
- **refactor:** Share sandbox identity derivation and acquire serialization
  across the five remote sandbox providers (RFC #4741), replacing five
  per-provider lock tables that grew unboundedly with process lifetime;
  derived ids are pinned byte-identical by per-provider golden vectors.
  ([#5089])

## [2.0.0] — 2026-06-15

DeerFlow 2.0 is a ground-up rewrite around a "super agent" harness with
sub-agents, persistent memory, sandbox execution, and an extensible
skills/tools system. It shares no code with the 1.x line, which now lives on
the [`main-1.x` branch](https://github.com/bytedance/deer-flow/tree/main-1.x).

This release closes [milestone 2.0.0](https://github.com/bytedance/deer-flow/milestone/1)
with **180 merged pull requests** since the first 2.0 milestone tag.

### ⚠ Breaking changes

- **harness:** Hydrate runs from `RunStore` and persist interrupted status. Run
  cancellation/multitask semantics now require a working RunStore on the
  worker that owns the run; cross-worker cancels return 409 instead of
  silently appearing successful. ([#2932])

### Added

#### Agents & runtime
- **agent:** Custom-agent self-updates with user isolation — agents can persist
  edits to their own `SOUL.md` / `config.yaml` from inside a normal chat.
  ([#2713])
- **loop-detection:** Make loop detection configurable with per-tool frequency
  overrides; keep configurable on/off switch. ([#2586], [#2711])
- **loop-detection:** Defer warning injection so detector pairs cleanly with
  tool-call lifecycle. ([#2752])
- **run:** Propagate `model_name` from the gateway request through the runtime
  and persistence stack into the SQLite-backed store. ([#2775])
- **subagents:** Stream subagent token usage to the header via terminal task
  events. ([#2882])
- **memory:** Add `memory.token_counting` config to opt out of tiktoken for
  network-restricted deployments. ([#3465])
- **suggest:** Make AI follow-up question suggestions optional. ([#3591])

#### Models & integrations
- **models:** Add StepFun reasoning model adapter. ([#3461])
- **community:** Add Brave Search web search tool. ([#3528])
- **channels:** Enhance Discord with mention-only mode, thread routing, and
  typing indicators. ([#2842])
- **im:** Add user-owned IM channel connections — users can bind their own
  Slack/Telegram/Discord/Feishu/DingTalk/WeChat/WeCom accounts on top of the
  operator-configured bots. ([#3487])
- **models:** Add patched MiMo reasoning content support. ([#3298])
- **models:** Add MiniMax provider for image/video/podcast skills plus a new
  music-generation skill. ([#3437])
- **community:** Add SearXNG and Browserless web search/fetch tools. ([#3451])
- **community:** Add Serper Google Images provider for `image_search`. ([#3575])
- **channels:** Stream Telegram agent replies by editing the placeholder
  message in place. ([#3534])

#### Observability
- **trace:** Set the LangGraph trace name to `lead_agent` (or the custom
  agent's `agent_name`) for cleaner Langfuse/LangSmith traces. ([#3101])
- **frontend:** Refine token usage display modes. ([#2329])
- **defaults:** Enable token usage tracking by default. ([#2841])
- **defaults:** Raise default summarization trigger threshold. ([#3174])
- **trace:** Attribute subagent spans to the parent thread's Langfuse trace.
  ([#3611])

#### Skills
- **skill:** Add `blocking-io-guard` skill for blocking-IO triage and runtime
  anchors. ([#3503])
- **skill:** Add maintainer issue and PR workflow skill. ([#3554])
- **skill:** Strengthen the maintainer orchestrator review workflow. ([#3606])

### Performance

- **harness:** Push thread metadata filters into SQL instead of post-filtering
  in Python. ([#2865])
- **runtime:** Index runs by `thread_id` to avoid O(n) scans in `RunManager`.
  ([#3499])
- **runtime:** Index messages in `MemoryRunEventStore` to avoid O(n) scans.
  ([#3531])
- **persistence:** Cache `Base.to_dict` column reflection per class. ([#3654])
- **sandbox:** Speed up `should_ignore_name` in glob/grep walks. ([#3657])

### Security

- **upload:** Reject symlinked upload destinations. ([#2623])
- **uploads:** Add Windows support for safe symlink-protected uploads.
  ([#2794])
- **mcp:** Mask sensitive values in MCP config API responses. ([#2667])
- **mcp:** Harden the MCP config endpoint against malformed input. ([#3425])
- **auth:** Reject cross-site auth POSTs. ([#2740])
- **gateway:** Cap skill artifact preview decompression to prevent
  zip-bomb-style abuse. ([#2963])
- **sandbox:** Mount the host Docker socket only in aio (DooD) sandbox mode.
  ([#3517])
- **sandbox:** Do not bind-mount host CLI auth dirs by default. ([#3521])

### Fixed

#### Runtime, gateway & persistence
- **runtime:** Rollback restore checkpoint now supersedes newer checkpoints.
  ([#2582])
- **runtime:** Persist run message summaries. ([#2850])
- **runtime:** Bound `write_file` execution-failure observations to keep
  failure traces from blowing out the context. ([#3133])
- **runtime:** Protect the sync singleton's init and reset paths. ([#3413])
- **runtime:** Avoid PostgreSQL aggregate `FOR UPDATE` on run events.
  ([#2962])
- **runs:** Restore historical runs from persistent store after a gateway
  restart. ([#2989])
- **gateway:** Return ISO 8601 timestamps from threads endpoints. ([#2599])
- **gateway:** Make cancel idempotent for already-interrupted runs. ([#3058])
- **gateway:** Split `stream_existing_run` into per-method routes for unique
  OpenAPI `operationId`s. ([#3228])
- **events:** Serialize structured DB event content. ([#2762])
- **persistence:** Emit timezone-aware timestamps from SQLite-backed stores.
  ([#3130])
- **persistence:** Reuse token usage model grouping expression. ([#2910])
- **runs:** Ignore stale run reconnect conflicts. ([#3284])
- **nginx:** Defer CORS to the gateway allowlist instead of double-applying it.
  ([#2861])
- **persistence:** Fix runtime journal run lifecycle events. ([#3470])
- **gateway:** Enforce thread ownership on stateless run endpoints. ([#3473])
- **runtime:** Propagate interrupt through SSE values events for the LangGraph
  SDK. ([#3605])
- **serialization:** Strip base64 image data from streamed values events.
  ([#3631])
- **history:** Strip base64 image data from REST endpoint responses. ([#3535])
- **gateway:** Attribute token usage to the actual models. ([#3658])

#### Agents, subagents & middleware
- **subagents:** Make subagent timeout terminal state atomic. ([#2583])
- **subagents:** Use model override for tools and middleware. ([#2641])
- **subagents:** Consolidate `system_prompt` and skills into a single
  `SystemMessage`. ([#2701])
- **subagent:** Isolate subagents from the parent run's checkpointer.
  ([#3559])
- **agents:** Make `update_agent` honor `runtime.context` `user_id` like
  `setup_agent` does. ([#2867])
- **agents:** Resolve duplicate `todos` channel type conflict in
  `TodoMiddleware`. ([#3200])
- **agents:** Offload blocking filesystem IO in the custom-agent router off
  the event loop. ([#3457])
- **agents:** Keep new agent bootstrap in user scope. ([#2784])
- **loop-detection:** Keep tool-call pairing on warn injection. ([#2725])
- **middleware:** Sync raw tool-call metadata. ([#2757])
- **middleware:** Handle invalid tool calls in dangling pairing middleware.
  ([#2891])
- **middleware:** Prevent todo completion reminder IM-message leak. ([#2907])
- **middleware:** Normalize tool result adjacency before model calls.
  ([#2939])
- **agents:** Require `config.yaml` in `resolve_agent_dir` to skip memory-only
  directories. ([#3481])
- **agents:** Sync `agent_name` across context/configurable and reject empty
  soul. ([#3553])
- **middleware:** Offload the uploads scan in `UploadsMiddleware` off the event
  loop. ([#3311])
- **middleware:** Offload memory injection off the event loop to prevent
  tiktoken blocking. ([#3411])
- **middleware:** Externalize oversized tool output into the sandbox for
  non-mounted sandboxes. ([#3417])
- **middleware:** Preserve the sandbox reducer in middleware state. ([#3629])
- **subagents:** Raise general-purpose `max_turns` to 150 and default timeout to
  30 min. ([#3610])

#### Memory & tracing
- **memory:** Replace short-lived `asyncio.run()` with a persistent event
  loop. ([#2627])
- **memory:** Isolate queued memory updates by agent. ([#2941])
- **memory:** Parse wrapped memory-update JSON responses. ([#3252])
- **tracing:** Propagate `session_id` and `user_id` into Langfuse traces.
  ([#2944])
- **trace:** Decode unicode escape sequences in non-ASCII memory trace info.
  ([#3104])

#### Tools, sandbox & MCP
- **mcp:** Fix env resolution in MCP config lists. ([#2556])
- **models:** Record Codex token usage in `usage_metadata`. ([#2585])
- **sandbox:** Supplement `list_running` in `RemoteSandboxBackend`. ([#2716])
- **sandbox:** Disable MSYS path conversion for Git Bash on Windows.
  ([#2766])
- **sandbox:** Avoid blocking sandbox readiness polling. ([#2822])
- **sandbox:** Uphold the `/mnt/user-data` contract at the `Sandbox` API
  boundary. ([#2881])
- **sandbox:** Scope provisioner PVC data by user. ([#2973])
- **sandbox:** Merge idempotent sandbox state updates. ([#3518])
- **tools:** Introduce `Runtime` type alias to eliminate Pydantic serialization
  warnings. ([#2774])
- **tools:** Preserve `tool_search` promotions across re-entrant
  `get_available_tools`. ([#2885])
- **harness:** Wrap async-only config tools for sync client execution.
  ([#2878])
- **harness:** Wrap all async-only tools for sync clients. ([#2935])
- **tool-search:** Reliably hide deferred MCP schemas by removing the
  ContextVar. ([#3342])
- **search:** Fix DDGS Wikipedia region handling. ([#3423])
- **web_fetch:** Support a proxy for the Jina reader in restricted networks.
  ([#3430])
- **sandbox:** Persist lazily-acquired sandbox state via `Command`. ([#3464])
- **sandbox:** Fix stale AIO sandbox cache reuse. ([#3494])
- **sandbox:** Create a shell session before retrying on a fresh id. ([#3577])
- **sandbox:** Stop flagging string-literal path fragments as unsafe absolute
  paths. ([#3623])
- **sandbox:** Return an actionable hint when `read_file` hits a binary file.
  ([#3624])
- **mcp:** Make stdio MCP-produced files resolvable via virtual sandbox paths.
  ([#3600])
- **mcp:** Surface admin-required state on the settings tools page. ([#3533])
- **mcp:** Add a tools cache reset endpoint. ([#3602])
- **uploads:** Fix the upload file size contract. ([#3408])

#### Skills & channels
- **skills:** Enforce `allowed-tools` metadata. ([#2626])
- **skills:** Harden slash skill activation across chat channels. ([#3466])
- **skills:** Fix custom skill install permissions. ([#3241])
- **channels:** Authenticate gateway command requests. ([#2742])
- **skills:** Surface the offending line and a quoting hint on SKILL.md YAML
  errors. ([#3335])
- **skills:** Keep skill archive installation off the event loop. ([#3505])
- **channels:** Ignore hidden control messages when extracting replies.
  ([#3270])
- **channels:** Reload config on channel restart. ([#3514])
- **channels:** Surface WeCom WebSocket connection failures. ([#3526])
- **channels:** Close the Discord file handle after upload. ([#3561])
- **channels:** Require a bound identity for user-owned IM messages. ([#3578])
- **channels:** Scope IM files and helper commands to the owner. ([#3579])
- **channels:** Make runtime provider state authoritative. ([#3580])
- **channels:** Harden runtime credential management APIs. ([#3581])
- **channels:** Make the channel connect flow deterministic. ([#3582])
- **channels:** Centralize shared channel retry helpers. ([#3583])
- **channels:** Add operational guardrails. ([#3584])
- **channels:** Unsubscribe channel listeners by equality. ([#3608])

#### Auth
- **auth:** Replace setup-status 429 rate limit with a cached response.
  ([#2915])
- **auth:** Persist auto-generated JWT secret so it survives restarts.
  ([#2933])
- **auth:** Align auth-disabled mode with mock history loading. ([#3471])

#### Frontend
- **frontend:** Restore `localhost` fallback for `getGatewayConfig` in prod
  mode. ([#2718])
- **chat:** Prevent the first user message from being swallowed in new
  conversations. ([#2731])
- **frontend:** Use backend thread token usage for the header total. ([#2800])
- **frontend:** Wait for async chat submit before clearing the input.
  ([#2940])
- **frontend:** Resolve login page flickering and the resize-observer loop.
  ([#2954])
- **frontend:** Deduplicate restored thread messages. ([#2958])
- **frontend:** Avoid duplicate optimistic user message. ([#3002])
- **frontend:** Hide the copy button for streaming assistant messages.
  ([#3176])
- **frontend:** Show a new thread in the sidebar immediately on creation.
  ([#3283])
- **frontend:** Isolate new chat thread messages. ([#3508])
- **frontend:** Cap deeply nested list indentation to prevent render crashes.
  ([#3393], [#3570])
- **token-usage:** Dedupe token usage aggregation by message id. ([#2770])
- **frontend:** Fall back to Streamdown clipboard copy. ([#3397])
- **frontend:** Remove the Backspace shortcut for deleting prompt attachments.
  ([#3410])
- **frontend:** Restructure the Memory settings toolbar into two rows. ([#3433])
- **suggestions:** Strip inline `<think>` reasoning before parsing follow-up
  questions. ([#3435])
- **frontend:** Stop fetching follow-up suggestions when they are disabled.
  ([#3599])
- **frontend:** Paginate the workspace chat list beyond 50 threads. ([#3485])
- **frontend:** Prevent user message bubble overflow with long unbreakable
  strings. ([#3488])
- **frontend:** Keep the workspace interactive when the SSR auth probe cannot
  reach the gateway. ([#3495])
- **frontend:** Render user messages as plain text and cap blockquote nesting.
  ([#3502])
- **frontend:** Reset the active chat after deletion. ([#3519])
- **frontend:** Improve the mobile workspace layout. ([#3646])
- **frontend:** Render full content for multi-part AI messages. ([#3649])

#### Build, deploy, scripts & config
- **packaging:** Add `postgres` extra for store/checkpointer support; clarify
  install guidance. ([#2584])
- **harness:** Resolve runtime paths from the project root. ([#2642])
- **docker:** Force nginx to resolve upstream names at request time.
  ([#2717])
- **docker:** Default Gateway to a single worker to prevent multi-worker
  breakage. ([#3475])
- **scripts:** Preserve `uv` extras across `make dev` restarts. ([#2767],
  [#2754])
- **scripts:** Clean up local nginx on stop. ([#3005])
- **deploy:** Fall back to `python` / `openssl` when `python3` is absent for
  secret generation. ([#3074])
- **config:** Make the reload boundary discoverable from code. ([#3144],
  [#3153])
- **replay-e2e:** Key replay fixtures by caller and conversation. ([#3453])
- **setup:** Refresh LLM provider wizard defaults. ([#3421])
- **config:** Coerce null `config.yaml` list sections to an empty list. ([#3434])
- **scripts:** Exclude runtime state from gateway reload. ([#3426])
- **scripts:** Create the backend/sandbox dir before the uvicorn reload-exclude.
  ([#3460])
- **scripts:** Stop next-server correctly after `make start-daemon`. ([#3498])
- **makefile:** Fix per-commit hooks installation. ([#3569])
- **replay-e2e:** Match replay by conversation, not the living system prompt.
  ([#3436])

### Changed

- **provider (refactor):** Share assistant payload replay matching across
  providers. ([#3307])
- **lead-agent (refactor):** Make `build_middlewares` public to drop the last
  cross-module private import. ([#3458])
- **todo (refactor):** Remove the unused completion reminder counter. ([#3530])

### Documentation

- Document blocking-IO detection usage and maintenance. ([#3233])
- Clean standalone LangGraph server remnants from docs. ([#3301])
- Add AI assistance disclosure to the PR template and CONTRIBUTING. ([#3398])
- Document custom AIO sandbox images. ([#3548])

### Internal

- **dev:** Add async/thread boundary detector. ([#2936])
- **runtime:** Add lifecycle end-to-end coverage. ([#2946])
- **windows:** Add `PYTHONIOENCODING` and `PYTHONUTF8` to backend Makefile
  targets. ([#3069])
- **blocking-io:** Fail-loud repo-root resolution and shared detector CLI
  shim. ([#3512])
- **runtime:** Add a Blockbuster runtime anchor for `JsonlRunEventStore` async
  IO. ([#3313])
- **ci:** Consolidate PR/issue labeling and fix the reviewing-job crash and
  label thrash. ([#3455])

[2.1.0+hartmesh.7]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.7
[2.1.0+hartmesh.6]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.6
[2.1.0+hartmesh.5]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.5
[2.1.0+hartmesh.4]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.4
[2.1.0+hartmesh.3]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.3
[2.1.0+hartmesh.2]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.2
[2.1.0+hartmesh.1]: https://github.com/altakleos/hartmesh/releases/tag/v2.1.0+hartmesh.1
[2.0.0]: https://github.com/bytedance/deer-flow/releases/tag/v2.0.0

[#2329]: https://github.com/bytedance/deer-flow/pull/2329
[#2556]: https://github.com/bytedance/deer-flow/pull/2556
[#2582]: https://github.com/bytedance/deer-flow/pull/2582
[#2583]: https://github.com/bytedance/deer-flow/pull/2583
[#2584]: https://github.com/bytedance/deer-flow/pull/2584
[#2585]: https://github.com/bytedance/deer-flow/pull/2585
[#2586]: https://github.com/bytedance/deer-flow/pull/2586
[#2599]: https://github.com/bytedance/deer-flow/pull/2599
[#2623]: https://github.com/bytedance/deer-flow/pull/2623
[#2626]: https://github.com/bytedance/deer-flow/pull/2626
[#2627]: https://github.com/bytedance/deer-flow/pull/2627
[#2641]: https://github.com/bytedance/deer-flow/pull/2641
[#2642]: https://github.com/bytedance/deer-flow/pull/2642
[#2667]: https://github.com/bytedance/deer-flow/pull/2667
[#2701]: https://github.com/bytedance/deer-flow/pull/2701
[#2711]: https://github.com/bytedance/deer-flow/pull/2711
[#2713]: https://github.com/bytedance/deer-flow/pull/2713
[#2716]: https://github.com/bytedance/deer-flow/pull/2716
[#2717]: https://github.com/bytedance/deer-flow/pull/2717
[#2718]: https://github.com/bytedance/deer-flow/pull/2718
[#2725]: https://github.com/bytedance/deer-flow/pull/2725
[#2731]: https://github.com/bytedance/deer-flow/pull/2731
[#2740]: https://github.com/bytedance/deer-flow/pull/2740
[#2742]: https://github.com/bytedance/deer-flow/pull/2742
[#2752]: https://github.com/bytedance/deer-flow/pull/2752
[#2754]: https://github.com/bytedance/deer-flow/pull/2754
[#2757]: https://github.com/bytedance/deer-flow/pull/2757
[#2762]: https://github.com/bytedance/deer-flow/pull/2762
[#2766]: https://github.com/bytedance/deer-flow/pull/2766
[#2767]: https://github.com/bytedance/deer-flow/pull/2767
[#2770]: https://github.com/bytedance/deer-flow/pull/2770
[#2774]: https://github.com/bytedance/deer-flow/pull/2774
[#2775]: https://github.com/bytedance/deer-flow/pull/2775
[#2784]: https://github.com/bytedance/deer-flow/pull/2784
[#2794]: https://github.com/bytedance/deer-flow/pull/2794
[#2800]: https://github.com/bytedance/deer-flow/pull/2800
[#2822]: https://github.com/bytedance/deer-flow/pull/2822
[#2841]: https://github.com/bytedance/deer-flow/pull/2841
[#2842]: https://github.com/bytedance/deer-flow/pull/2842
[#2850]: https://github.com/bytedance/deer-flow/pull/2850
[#2861]: https://github.com/bytedance/deer-flow/pull/2861
[#2865]: https://github.com/bytedance/deer-flow/pull/2865
[#2867]: https://github.com/bytedance/deer-flow/pull/2867
[#2878]: https://github.com/bytedance/deer-flow/pull/2878
[#2881]: https://github.com/bytedance/deer-flow/pull/2881
[#2882]: https://github.com/bytedance/deer-flow/pull/2882
[#2885]: https://github.com/bytedance/deer-flow/pull/2885
[#2891]: https://github.com/bytedance/deer-flow/pull/2891
[#2907]: https://github.com/bytedance/deer-flow/pull/2907
[#2910]: https://github.com/bytedance/deer-flow/pull/2910
[#2915]: https://github.com/bytedance/deer-flow/pull/2915
[#2932]: https://github.com/bytedance/deer-flow/pull/2932
[#2933]: https://github.com/bytedance/deer-flow/pull/2933
[#2935]: https://github.com/bytedance/deer-flow/pull/2935
[#2936]: https://github.com/bytedance/deer-flow/pull/2936
[#2939]: https://github.com/bytedance/deer-flow/pull/2939
[#2940]: https://github.com/bytedance/deer-flow/pull/2940
[#2941]: https://github.com/bytedance/deer-flow/pull/2941
[#2944]: https://github.com/bytedance/deer-flow/pull/2944
[#2946]: https://github.com/bytedance/deer-flow/pull/2946
[#2954]: https://github.com/bytedance/deer-flow/pull/2954
[#2958]: https://github.com/bytedance/deer-flow/pull/2958
[#2962]: https://github.com/bytedance/deer-flow/pull/2962
[#2963]: https://github.com/bytedance/deer-flow/pull/2963
[#2973]: https://github.com/bytedance/deer-flow/pull/2973
[#2989]: https://github.com/bytedance/deer-flow/pull/2989
[#3002]: https://github.com/bytedance/deer-flow/pull/3002
[#3005]: https://github.com/bytedance/deer-flow/pull/3005
[#3033]: https://github.com/bytedance/deer-flow/pull/3033
[#3058]: https://github.com/bytedance/deer-flow/pull/3058
[#3069]: https://github.com/bytedance/deer-flow/pull/3069
[#3074]: https://github.com/bytedance/deer-flow/pull/3074
[#3101]: https://github.com/bytedance/deer-flow/pull/3101
[#3104]: https://github.com/bytedance/deer-flow/pull/3104
[#3130]: https://github.com/bytedance/deer-flow/pull/3130
[#3133]: https://github.com/bytedance/deer-flow/pull/3133
[#3144]: https://github.com/bytedance/deer-flow/pull/3144
[#3153]: https://github.com/bytedance/deer-flow/pull/3153
[#3174]: https://github.com/bytedance/deer-flow/pull/3174
[#3176]: https://github.com/bytedance/deer-flow/pull/3176
[#3191]: https://github.com/bytedance/deer-flow/pull/3191
[#3200]: https://github.com/bytedance/deer-flow/pull/3200
[#3228]: https://github.com/bytedance/deer-flow/pull/3228
[#3233]: https://github.com/bytedance/deer-flow/pull/3233
[#3241]: https://github.com/bytedance/deer-flow/pull/3241
[#3252]: https://github.com/bytedance/deer-flow/pull/3252
[#3270]: https://github.com/bytedance/deer-flow/pull/3270
[#3283]: https://github.com/bytedance/deer-flow/pull/3283
[#3284]: https://github.com/bytedance/deer-flow/pull/3284
[#3298]: https://github.com/bytedance/deer-flow/pull/3298
[#3301]: https://github.com/bytedance/deer-flow/pull/3301
[#3307]: https://github.com/bytedance/deer-flow/pull/3307
[#3311]: https://github.com/bytedance/deer-flow/pull/3311
[#3313]: https://github.com/bytedance/deer-flow/pull/3313
[#3335]: https://github.com/bytedance/deer-flow/pull/3335
[#3342]: https://github.com/bytedance/deer-flow/pull/3342
[#3377]: https://github.com/bytedance/deer-flow/pull/3377
[#3393]: https://github.com/bytedance/deer-flow/pull/3393
[#3397]: https://github.com/bytedance/deer-flow/pull/3397
[#3398]: https://github.com/bytedance/deer-flow/pull/3398
[#3408]: https://github.com/bytedance/deer-flow/pull/3408
[#3410]: https://github.com/bytedance/deer-flow/pull/3410
[#3411]: https://github.com/bytedance/deer-flow/pull/3411
[#3412]: https://github.com/bytedance/deer-flow/pull/3412
[#3413]: https://github.com/bytedance/deer-flow/pull/3413
[#3417]: https://github.com/bytedance/deer-flow/pull/3417
[#3421]: https://github.com/bytedance/deer-flow/pull/3421
[#3423]: https://github.com/bytedance/deer-flow/pull/3423
[#3425]: https://github.com/bytedance/deer-flow/pull/3425
[#3426]: https://github.com/bytedance/deer-flow/pull/3426
[#3428]: https://github.com/bytedance/deer-flow/pull/3428
[#3430]: https://github.com/bytedance/deer-flow/pull/3430
[#3433]: https://github.com/bytedance/deer-flow/pull/3433
[#3434]: https://github.com/bytedance/deer-flow/pull/3434
[#3435]: https://github.com/bytedance/deer-flow/pull/3435
[#3436]: https://github.com/bytedance/deer-flow/pull/3436
[#3437]: https://github.com/bytedance/deer-flow/pull/3437
[#3451]: https://github.com/bytedance/deer-flow/pull/3451
[#3453]: https://github.com/bytedance/deer-flow/pull/3453
[#3455]: https://github.com/bytedance/deer-flow/pull/3455
[#3457]: https://github.com/bytedance/deer-flow/pull/3457
[#3458]: https://github.com/bytedance/deer-flow/pull/3458
[#3460]: https://github.com/bytedance/deer-flow/pull/3460
[#3461]: https://github.com/bytedance/deer-flow/pull/3461
[#3464]: https://github.com/bytedance/deer-flow/pull/3464
[#3465]: https://github.com/bytedance/deer-flow/pull/3465
[#3466]: https://github.com/bytedance/deer-flow/pull/3466
[#3470]: https://github.com/bytedance/deer-flow/pull/3470
[#3471]: https://github.com/bytedance/deer-flow/pull/3471
[#3473]: https://github.com/bytedance/deer-flow/pull/3473
[#3475]: https://github.com/bytedance/deer-flow/pull/3475
[#3481]: https://github.com/bytedance/deer-flow/pull/3481
[#3485]: https://github.com/bytedance/deer-flow/pull/3485
[#3487]: https://github.com/bytedance/deer-flow/pull/3487
[#3488]: https://github.com/bytedance/deer-flow/pull/3488
[#3494]: https://github.com/bytedance/deer-flow/pull/3494
[#3495]: https://github.com/bytedance/deer-flow/pull/3495
[#3498]: https://github.com/bytedance/deer-flow/pull/3498
[#3499]: https://github.com/bytedance/deer-flow/pull/3499
[#3502]: https://github.com/bytedance/deer-flow/pull/3502
[#3503]: https://github.com/bytedance/deer-flow/pull/3503
[#3505]: https://github.com/bytedance/deer-flow/pull/3505
[#3506]: https://github.com/bytedance/deer-flow/pull/3506
[#3508]: https://github.com/bytedance/deer-flow/pull/3508
[#3512]: https://github.com/bytedance/deer-flow/pull/3512
[#3514]: https://github.com/bytedance/deer-flow/pull/3514
[#3517]: https://github.com/bytedance/deer-flow/pull/3517
[#3518]: https://github.com/bytedance/deer-flow/pull/3518
[#3519]: https://github.com/bytedance/deer-flow/pull/3519
[#3521]: https://github.com/bytedance/deer-flow/pull/3521
[#3526]: https://github.com/bytedance/deer-flow/pull/3526
[#3528]: https://github.com/bytedance/deer-flow/pull/3528
[#3530]: https://github.com/bytedance/deer-flow/pull/3530
[#3531]: https://github.com/bytedance/deer-flow/pull/3531
[#3533]: https://github.com/bytedance/deer-flow/pull/3533
[#3534]: https://github.com/bytedance/deer-flow/pull/3534
[#3535]: https://github.com/bytedance/deer-flow/pull/3535
[#3548]: https://github.com/bytedance/deer-flow/pull/3548
[#3551]: https://github.com/bytedance/deer-flow/pull/3551
[#3553]: https://github.com/bytedance/deer-flow/pull/3553
[#3554]: https://github.com/bytedance/deer-flow/pull/3554
[#3556]: https://github.com/bytedance/deer-flow/pull/3556
[#3557]: https://github.com/bytedance/deer-flow/pull/3557
[#3559]: https://github.com/bytedance/deer-flow/pull/3559
[#3561]: https://github.com/bytedance/deer-flow/pull/3561
[#3562]: https://github.com/bytedance/deer-flow/pull/3562
[#3563]: https://github.com/bytedance/deer-flow/pull/3563
[#3566]: https://github.com/bytedance/deer-flow/pull/3566
[#3569]: https://github.com/bytedance/deer-flow/pull/3569
[#3570]: https://github.com/bytedance/deer-flow/pull/3570
[#3573]: https://github.com/bytedance/deer-flow/pull/3573
[#3575]: https://github.com/bytedance/deer-flow/pull/3575
[#3577]: https://github.com/bytedance/deer-flow/pull/3577
[#3578]: https://github.com/bytedance/deer-flow/pull/3578
[#3579]: https://github.com/bytedance/deer-flow/pull/3579
[#3580]: https://github.com/bytedance/deer-flow/pull/3580
[#3581]: https://github.com/bytedance/deer-flow/pull/3581
[#3582]: https://github.com/bytedance/deer-flow/pull/3582
[#3583]: https://github.com/bytedance/deer-flow/pull/3583
[#3584]: https://github.com/bytedance/deer-flow/pull/3584
[#3585]: https://github.com/bytedance/deer-flow/pull/3585
[#3590]: https://github.com/bytedance/deer-flow/pull/3590
[#3591]: https://github.com/bytedance/deer-flow/pull/3591
[#3592]: https://github.com/bytedance/deer-flow/pull/3592
[#3599]: https://github.com/bytedance/deer-flow/pull/3599
[#3600]: https://github.com/bytedance/deer-flow/pull/3600
[#3601]: https://github.com/bytedance/deer-flow/pull/3601
[#3602]: https://github.com/bytedance/deer-flow/pull/3602
[#3605]: https://github.com/bytedance/deer-flow/pull/3605
[#3606]: https://github.com/bytedance/deer-flow/pull/3606
[#3608]: https://github.com/bytedance/deer-flow/pull/3608
[#3610]: https://github.com/bytedance/deer-flow/pull/3610
[#3611]: https://github.com/bytedance/deer-flow/pull/3611
[#3623]: https://github.com/bytedance/deer-flow/pull/3623
[#3624]: https://github.com/bytedance/deer-flow/pull/3624
[#3627]: https://github.com/bytedance/deer-flow/pull/3627
[#3629]: https://github.com/bytedance/deer-flow/pull/3629
[#3631]: https://github.com/bytedance/deer-flow/pull/3631
[#3637]: https://github.com/bytedance/deer-flow/pull/3637
[#3644]: https://github.com/bytedance/deer-flow/pull/3644
[#3646]: https://github.com/bytedance/deer-flow/pull/3646
[#3648]: https://github.com/bytedance/deer-flow/pull/3648
[#3649]: https://github.com/bytedance/deer-flow/pull/3649
[#3651]: https://github.com/bytedance/deer-flow/pull/3651
[#3654]: https://github.com/bytedance/deer-flow/pull/3654
[#3657]: https://github.com/bytedance/deer-flow/pull/3657
[#3658]: https://github.com/bytedance/deer-flow/pull/3658
[#3661]: https://github.com/bytedance/deer-flow/pull/3661
[#3662]: https://github.com/bytedance/deer-flow/pull/3662
[#3663]: https://github.com/bytedance/deer-flow/pull/3663
[#3665]: https://github.com/bytedance/deer-flow/pull/3665
[#3673]: https://github.com/bytedance/deer-flow/pull/3673
[#3674]: https://github.com/bytedance/deer-flow/pull/3674
[#3675]: https://github.com/bytedance/deer-flow/pull/3675
[#3685]: https://github.com/bytedance/deer-flow/pull/3685
[#3686]: https://github.com/bytedance/deer-flow/pull/3686
[#3687]: https://github.com/bytedance/deer-flow/pull/3687
[#3698]: https://github.com/bytedance/deer-flow/pull/3698
[#3709]: https://github.com/bytedance/deer-flow/pull/3709
[#3711]: https://github.com/bytedance/deer-flow/pull/3711
[#3713]: https://github.com/bytedance/deer-flow/pull/3713
[#3714]: https://github.com/bytedance/deer-flow/pull/3714
[#3718]: https://github.com/bytedance/deer-flow/pull/3718
[#3719]: https://github.com/bytedance/deer-flow/pull/3719
[#3729]: https://github.com/bytedance/deer-flow/pull/3729
[#3730]: https://github.com/bytedance/deer-flow/pull/3730
[#3733]: https://github.com/bytedance/deer-flow/pull/3733
[#3740]: https://github.com/bytedance/deer-flow/pull/3740
[#3753]: https://github.com/bytedance/deer-flow/pull/3753
[#3760]: https://github.com/bytedance/deer-flow/pull/3760
[#3764]: https://github.com/bytedance/deer-flow/pull/3764
[#3768]: https://github.com/bytedance/deer-flow/pull/3768
[#3769]: https://github.com/bytedance/deer-flow/pull/3769
[#3770]: https://github.com/bytedance/deer-flow/pull/3770
[#3772]: https://github.com/bytedance/deer-flow/pull/3772
[#3775]: https://github.com/bytedance/deer-flow/pull/3775
[#3786]: https://github.com/bytedance/deer-flow/pull/3786
[#3790]: https://github.com/bytedance/deer-flow/pull/3790
[#3791]: https://github.com/bytedance/deer-flow/pull/3791
[#3794]: https://github.com/bytedance/deer-flow/pull/3794
[#3797]: https://github.com/bytedance/deer-flow/pull/3797
[#3800]: https://github.com/bytedance/deer-flow/pull/3800
[#3809]: https://github.com/bytedance/deer-flow/pull/3809
[#3810]: https://github.com/bytedance/deer-flow/pull/3810
[#3812]: https://github.com/bytedance/deer-flow/pull/3812
[#3821]: https://github.com/bytedance/deer-flow/pull/3821
[#3826]: https://github.com/bytedance/deer-flow/pull/3826
[#3828]: https://github.com/bytedance/deer-flow/pull/3828
[#3837]: https://github.com/bytedance/deer-flow/pull/3837
[#3839]: https://github.com/bytedance/deer-flow/pull/3839
[#3843]: https://github.com/bytedance/deer-flow/pull/3843
[#3845]: https://github.com/bytedance/deer-flow/pull/3845
[#3854]: https://github.com/bytedance/deer-flow/pull/3854
[#3855]: https://github.com/bytedance/deer-flow/pull/3855
[#3856]: https://github.com/bytedance/deer-flow/pull/3856
[#3858]: https://github.com/bytedance/deer-flow/pull/3858
[#3860]: https://github.com/bytedance/deer-flow/pull/3860
[#3866]: https://github.com/bytedance/deer-flow/pull/3866
[#3869]: https://github.com/bytedance/deer-flow/pull/3869
[#3870]: https://github.com/bytedance/deer-flow/pull/3870
[#3871]: https://github.com/bytedance/deer-flow/pull/3871
[#3872]: https://github.com/bytedance/deer-flow/pull/3872
[#3874]: https://github.com/bytedance/deer-flow/pull/3874
[#3877]: https://github.com/bytedance/deer-flow/pull/3877
[#3878]: https://github.com/bytedance/deer-flow/pull/3878
[#3880]: https://github.com/bytedance/deer-flow/pull/3880
[#3881]: https://github.com/bytedance/deer-flow/pull/3881
[#3883]: https://github.com/bytedance/deer-flow/pull/3883
[#3885]: https://github.com/bytedance/deer-flow/pull/3885
[#3886]: https://github.com/bytedance/deer-flow/pull/3886
[#3887]: https://github.com/bytedance/deer-flow/pull/3887
[#3889]: https://github.com/bytedance/deer-flow/pull/3889
[#3897]: https://github.com/bytedance/deer-flow/pull/3897
[#3900]: https://github.com/bytedance/deer-flow/pull/3900
[#3902]: https://github.com/bytedance/deer-flow/pull/3902
[#3904]: https://github.com/bytedance/deer-flow/pull/3904
[#3906]: https://github.com/bytedance/deer-flow/pull/3906
[#3907]: https://github.com/bytedance/deer-flow/pull/3907
[#3908]: https://github.com/bytedance/deer-flow/pull/3908
[#3912]: https://github.com/bytedance/deer-flow/pull/3912
[#3917]: https://github.com/bytedance/deer-flow/pull/3917
[#3920]: https://github.com/bytedance/deer-flow/pull/3920
[#3924]: https://github.com/bytedance/deer-flow/pull/3924
[#3926]: https://github.com/bytedance/deer-flow/pull/3926
[#3927]: https://github.com/bytedance/deer-flow/pull/3927
[#3928]: https://github.com/bytedance/deer-flow/pull/3928
[#3931]: https://github.com/bytedance/deer-flow/pull/3931
[#3934]: https://github.com/bytedance/deer-flow/pull/3934
[#3935]: https://github.com/bytedance/deer-flow/pull/3935
[#3938]: https://github.com/bytedance/deer-flow/pull/3938
[#3940]: https://github.com/bytedance/deer-flow/pull/3940
[#3941]: https://github.com/bytedance/deer-flow/pull/3941
[#3942]: https://github.com/bytedance/deer-flow/pull/3942
[#3944]: https://github.com/bytedance/deer-flow/pull/3944
[#3945]: https://github.com/bytedance/deer-flow/pull/3945
[#3949]: https://github.com/bytedance/deer-flow/pull/3949
[#3950]: https://github.com/bytedance/deer-flow/pull/3950
[#3951]: https://github.com/bytedance/deer-flow/pull/3951
[#3956]: https://github.com/bytedance/deer-flow/pull/3956
[#3959]: https://github.com/bytedance/deer-flow/pull/3959
[#3961]: https://github.com/bytedance/deer-flow/pull/3961
[#3964]: https://github.com/bytedance/deer-flow/pull/3964
[#3966]: https://github.com/bytedance/deer-flow/pull/3966
[#3967]: https://github.com/bytedance/deer-flow/pull/3967
[#3969]: https://github.com/bytedance/deer-flow/pull/3969
[#3971]: https://github.com/bytedance/deer-flow/pull/3971
[#3976]: https://github.com/bytedance/deer-flow/pull/3976
[#3980]: https://github.com/bytedance/deer-flow/pull/3980
[#3981]: https://github.com/bytedance/deer-flow/pull/3981
[#3982]: https://github.com/bytedance/deer-flow/pull/3982
[#3985]: https://github.com/bytedance/deer-flow/pull/3985
[#3986]: https://github.com/bytedance/deer-flow/pull/3986
[#3988]: https://github.com/bytedance/deer-flow/pull/3988
[#3989]: https://github.com/bytedance/deer-flow/pull/3989
[#3990]: https://github.com/bytedance/deer-flow/pull/3990
[#3991]: https://github.com/bytedance/deer-flow/pull/3991
[#3992]: https://github.com/bytedance/deer-flow/pull/3992
[#3993]: https://github.com/bytedance/deer-flow/pull/3993
[#3994]: https://github.com/bytedance/deer-flow/pull/3994
[#3996]: https://github.com/bytedance/deer-flow/pull/3996
[#4003]: https://github.com/bytedance/deer-flow/pull/4003
[#4004]: https://github.com/bytedance/deer-flow/pull/4004
[#4008]: https://github.com/bytedance/deer-flow/pull/4008
[#4009]: https://github.com/bytedance/deer-flow/pull/4009
[#4012]: https://github.com/bytedance/deer-flow/pull/4012
[#4016]: https://github.com/bytedance/deer-flow/pull/4016
[#4017]: https://github.com/bytedance/deer-flow/pull/4017
[#4018]: https://github.com/bytedance/deer-flow/pull/4018
[#4023]: https://github.com/bytedance/deer-flow/pull/4023
[#4024]: https://github.com/bytedance/deer-flow/pull/4024
[#4026]: https://github.com/bytedance/deer-flow/pull/4026
[#4028]: https://github.com/bytedance/deer-flow/pull/4028
[#4033]: https://github.com/bytedance/deer-flow/pull/4033
[#4034]: https://github.com/bytedance/deer-flow/pull/4034
[#4035]: https://github.com/bytedance/deer-flow/pull/4035
[#4036]: https://github.com/bytedance/deer-flow/pull/4036
[#4038]: https://github.com/bytedance/deer-flow/pull/4038
[#4040]: https://github.com/bytedance/deer-flow/pull/4040
[#4051]: https://github.com/bytedance/deer-flow/pull/4051
[#4052]: https://github.com/bytedance/deer-flow/pull/4052
[#4053]: https://github.com/bytedance/deer-flow/pull/4053
[#4055]: https://github.com/bytedance/deer-flow/pull/4055
[#4058]: https://github.com/bytedance/deer-flow/pull/4058
[#4059]: https://github.com/bytedance/deer-flow/pull/4059
[#4060]: https://github.com/bytedance/deer-flow/pull/4060
[#4064]: https://github.com/bytedance/deer-flow/pull/4064
[#4065]: https://github.com/bytedance/deer-flow/pull/4065
[#4067]: https://github.com/bytedance/deer-flow/pull/4067
[#4069]: https://github.com/bytedance/deer-flow/pull/4069
[#4072]: https://github.com/bytedance/deer-flow/pull/4072
[#4073]: https://github.com/bytedance/deer-flow/pull/4073
[#4074]: https://github.com/bytedance/deer-flow/pull/4074
[#4076]: https://github.com/bytedance/deer-flow/pull/4076
[#4077]: https://github.com/bytedance/deer-flow/pull/4077
[#4078]: https://github.com/bytedance/deer-flow/pull/4078
[#4079]: https://github.com/bytedance/deer-flow/pull/4079
[#4080]: https://github.com/bytedance/deer-flow/pull/4080
[#4081]: https://github.com/bytedance/deer-flow/pull/4081
[#4082]: https://github.com/bytedance/deer-flow/pull/4082
[#4084]: https://github.com/bytedance/deer-flow/pull/4084
[#4085]: https://github.com/bytedance/deer-flow/pull/4085
[#4090]: https://github.com/bytedance/deer-flow/pull/4090
[#4094]: https://github.com/bytedance/deer-flow/pull/4094
[#4095]: https://github.com/bytedance/deer-flow/issues/4095
[#4096]: https://github.com/bytedance/deer-flow/pull/4096
[#4097]: https://github.com/bytedance/deer-flow/pull/4097
[#4098]: https://github.com/bytedance/deer-flow/pull/4098
[#4099]: https://github.com/bytedance/deer-flow/pull/4099
[#4100]: https://github.com/bytedance/deer-flow/pull/4100
[#4101]: https://github.com/bytedance/deer-flow/pull/4101
[#4102]: https://github.com/bytedance/deer-flow/pull/4102
[#4103]: https://github.com/bytedance/deer-flow/pull/4103
[#4104]: https://github.com/bytedance/deer-flow/pull/4104
[#4105]: https://github.com/bytedance/deer-flow/pull/4105
[#4108]: https://github.com/bytedance/deer-flow/pull/4108
[#4114]: https://github.com/bytedance/deer-flow/pull/4114
[#4115]: https://github.com/bytedance/deer-flow/pull/4115
[#4117]: https://github.com/bytedance/deer-flow/pull/4117
[#4118]: https://github.com/bytedance/deer-flow/pull/4118
[#4119]: https://github.com/bytedance/deer-flow/pull/4119
[#4122]: https://github.com/bytedance/deer-flow/pull/4122
[#4124]: https://github.com/bytedance/deer-flow/pull/4124
[#4128]: https://github.com/bytedance/deer-flow/pull/4128
[#4129]: https://github.com/bytedance/deer-flow/pull/4129
[#4130]: https://github.com/bytedance/deer-flow/pull/4130
[#4131]: https://github.com/bytedance/deer-flow/pull/4131
[#4133]: https://github.com/bytedance/deer-flow/pull/4133
[#4136]: https://github.com/bytedance/deer-flow/pull/4136
[#4137]: https://github.com/bytedance/deer-flow/pull/4137
[#4140]: https://github.com/bytedance/deer-flow/pull/4140
[#4141]: https://github.com/bytedance/deer-flow/pull/4141
[#4143]: https://github.com/bytedance/deer-flow/pull/4143
[#4146]: https://github.com/bytedance/deer-flow/pull/4146
[#4147]: https://github.com/bytedance/deer-flow/pull/4147
[#4154]: https://github.com/bytedance/deer-flow/pull/4154
[#4155]: https://github.com/bytedance/deer-flow/pull/4155
[#4157]: https://github.com/bytedance/deer-flow/pull/4157
[#4160]: https://github.com/bytedance/deer-flow/pull/4160
[#4161]: https://github.com/bytedance/deer-flow/pull/4161
[#4162]: https://github.com/bytedance/deer-flow/pull/4162
[#4166]: https://github.com/bytedance/deer-flow/pull/4166
[#4169]: https://github.com/bytedance/deer-flow/pull/4169
[#4170]: https://github.com/bytedance/deer-flow/pull/4170
[#4171]: https://github.com/bytedance/deer-flow/pull/4171
[#4174]: https://github.com/bytedance/deer-flow/pull/4174
[#4178]: https://github.com/bytedance/deer-flow/pull/4178
[#4181]: https://github.com/bytedance/deer-flow/pull/4181
[#4187]: https://github.com/bytedance/deer-flow/pull/4187
[#4188]: https://github.com/bytedance/deer-flow/pull/4188
[#4190]: https://github.com/bytedance/deer-flow/pull/4190
[#4192]: https://github.com/bytedance/deer-flow/issues/4192
[#4193]: https://github.com/bytedance/deer-flow/pull/4193
[#4197]: https://github.com/bytedance/deer-flow/pull/4197
[#4199]: https://github.com/bytedance/deer-flow/pull/4199
[#4202]: https://github.com/bytedance/deer-flow/pull/4202
[#4203]: https://github.com/bytedance/deer-flow/pull/4203
[#4208]: https://github.com/bytedance/deer-flow/pull/4208
[#4209]: https://github.com/bytedance/deer-flow/pull/4209
[#4215]: https://github.com/bytedance/deer-flow/pull/4215
[#4217]: https://github.com/bytedance/deer-flow/pull/4217
[#4218]: https://github.com/bytedance/deer-flow/pull/4218
[#4219]: https://github.com/bytedance/deer-flow/pull/4219
[#4222]: https://github.com/bytedance/deer-flow/pull/4222
[#4225]: https://github.com/bytedance/deer-flow/pull/4225
[#4229]: https://github.com/bytedance/deer-flow/pull/4229
[#4230]: https://github.com/bytedance/deer-flow/pull/4230
[#4234]: https://github.com/bytedance/deer-flow/pull/4234
[#4235]: https://github.com/bytedance/deer-flow/pull/4235
[#4238]: https://github.com/bytedance/deer-flow/pull/4238
[#4239]: https://github.com/bytedance/deer-flow/pull/4239
[#4245]: https://github.com/bytedance/deer-flow/pull/4245
[#4246]: https://github.com/bytedance/deer-flow/pull/4246
[#4251]: https://github.com/bytedance/deer-flow/pull/4251
[#4255]: https://github.com/bytedance/deer-flow/pull/4255
[#4256]: https://github.com/bytedance/deer-flow/pull/4256
[#4260]: https://github.com/bytedance/deer-flow/pull/4260
[#4264]: https://github.com/bytedance/deer-flow/pull/4264
[#4267]: https://github.com/bytedance/deer-flow/pull/4267
[#4268]: https://github.com/bytedance/deer-flow/pull/4268
[#4277]: https://github.com/bytedance/deer-flow/pull/4277
[#4278]: https://github.com/bytedance/deer-flow/pull/4278
[#4279]: https://github.com/bytedance/deer-flow/pull/4279
[#4283]: https://github.com/bytedance/deer-flow/pull/4283
[#4287]: https://github.com/bytedance/deer-flow/pull/4287
[#4288]: https://github.com/bytedance/deer-flow/pull/4288
[#4292]: https://github.com/bytedance/deer-flow/pull/4292
[#4306]: https://github.com/bytedance/deer-flow/pull/4306
[#4309]: https://github.com/bytedance/deer-flow/pull/4309
[#4311]: https://github.com/bytedance/deer-flow/pull/4311
[#4315]: https://github.com/bytedance/deer-flow/pull/4315
[#4316]: https://github.com/bytedance/deer-flow/pull/4316
[#4324]: https://github.com/bytedance/deer-flow/issues/4324
[#4326]: https://github.com/bytedance/deer-flow/pull/4326
[#4337]: https://github.com/bytedance/deer-flow/pull/4337
[#4347]: https://github.com/bytedance/deer-flow/pull/4347
[#4348]: https://github.com/bytedance/deer-flow/pull/4348
[#4354]: https://github.com/bytedance/deer-flow/pull/4354
[#4355]: https://github.com/bytedance/deer-flow/pull/4355
[#4356]: https://github.com/bytedance/deer-flow/pull/4356
[#4358]: https://github.com/bytedance/deer-flow/pull/4358
[#4361]: https://github.com/bytedance/deer-flow/pull/4361
[#4364]: https://github.com/bytedance/deer-flow/pull/4364
[#4365]: https://github.com/bytedance/deer-flow/pull/4365
[#4370]: https://github.com/bytedance/deer-flow/pull/4370
[#4371]: https://github.com/bytedance/deer-flow/pull/4371
[#4373]: https://github.com/bytedance/deer-flow/pull/4373
[#4374]: https://github.com/bytedance/deer-flow/pull/4374
[#4376]: https://github.com/bytedance/deer-flow/pull/4376
[#4381]: https://github.com/bytedance/deer-flow/pull/4381
[#4383]: https://github.com/bytedance/deer-flow/pull/4383
[#4385]: https://github.com/bytedance/deer-flow/pull/4385
[#4391]: https://github.com/bytedance/deer-flow/pull/4391
[#4392]: https://github.com/bytedance/deer-flow/pull/4392
[#4394]: https://github.com/bytedance/deer-flow/pull/4394
[#4402]: https://github.com/bytedance/deer-flow/pull/4402
[#4403]: https://github.com/bytedance/deer-flow/pull/4403
[#4407]: https://github.com/bytedance/deer-flow/pull/4407
[#4408]: https://github.com/bytedance/deer-flow/pull/4408
[#4411]: https://github.com/bytedance/deer-flow/pull/4411
[#4414]: https://github.com/bytedance/deer-flow/issues/4414
[#4424]: https://github.com/bytedance/deer-flow/issues/4424
[#4425]: https://github.com/bytedance/deer-flow/pull/4425
[#4426]: https://github.com/bytedance/deer-flow/pull/4426
[#4430]: https://github.com/bytedance/deer-flow/pull/4430
[#4431]: https://github.com/bytedance/deer-flow/pull/4431
[#4432]: https://github.com/bytedance/deer-flow/pull/4432
[#4434]: https://github.com/bytedance/deer-flow/pull/4434
[#4437]: https://github.com/bytedance/deer-flow/pull/4437
[#4441]: https://github.com/bytedance/deer-flow/pull/4441
[#4442]: https://github.com/bytedance/deer-flow/pull/4442
[#4444]: https://github.com/bytedance/deer-flow/pull/4444
[#4446]: https://github.com/bytedance/deer-flow/pull/4446
[#4447]: https://github.com/bytedance/deer-flow/pull/4447
[#4450]: https://github.com/bytedance/deer-flow/pull/4450
[#4456]: https://github.com/bytedance/deer-flow/pull/4456
[#4459]: https://github.com/bytedance/deer-flow/pull/4459
[#4460]: https://github.com/bytedance/deer-flow/pull/4460
[#4468]: https://github.com/bytedance/deer-flow/pull/4468
[#4469]: https://github.com/bytedance/deer-flow/pull/4469
[#4471]: https://github.com/bytedance/deer-flow/pull/4471
[#4516]: https://github.com/bytedance/deer-flow/pull/4516
[#4611]: https://github.com/bytedance/deer-flow/issues/4611
[#4745]: https://github.com/bytedance/deer-flow/pull/4745
[#4574]: https://github.com/bytedance/deer-flow/issues/4574
[#4577]: https://github.com/bytedance/deer-flow/pull/4577
[#4623]: https://github.com/bytedance/deer-flow/pull/4623
[#4634]: https://github.com/bytedance/deer-flow/pull/4634
[#4638]: https://github.com/bytedance/deer-flow/pull/4638
[#4848]: https://github.com/bytedance/deer-flow/pull/4848
[#3183]: https://github.com/bytedance/deer-flow/pull/3183
[#3396]: https://github.com/bytedance/deer-flow/pull/3396
[#3442]: https://github.com/bytedance/deer-flow/pull/3442
[#3565]: https://github.com/bytedance/deer-flow/pull/3565
[#3703]: https://github.com/bytedance/deer-flow/pull/3703
[#3708]: https://github.com/bytedance/deer-flow/pull/3708
[#3783]: https://github.com/bytedance/deer-flow/pull/3783
[#3824]: https://github.com/bytedance/deer-flow/pull/3824
[#3833]: https://github.com/bytedance/deer-flow/pull/3833
[#4025]: https://github.com/bytedance/deer-flow/pull/4025
[#4200]: https://github.com/bytedance/deer-flow/pull/4200
[#4210]: https://github.com/bytedance/deer-flow/pull/4210
[#4242]: https://github.com/bytedance/deer-flow/pull/4242
[#4247]: https://github.com/bytedance/deer-flow/pull/4247
[#4250]: https://github.com/bytedance/deer-flow/pull/4250
[#4262]: https://github.com/bytedance/deer-flow/pull/4262
[#4266]: https://github.com/bytedance/deer-flow/pull/4266
[#4274]: https://github.com/bytedance/deer-flow/pull/4274
[#4275]: https://github.com/bytedance/deer-flow/pull/4275
[#4284]: https://github.com/bytedance/deer-flow/pull/4284
[#4293]: https://github.com/bytedance/deer-flow/pull/4293
[#4298]: https://github.com/bytedance/deer-flow/pull/4298
[#4301]: https://github.com/bytedance/deer-flow/pull/4301
[#4302]: https://github.com/bytedance/deer-flow/pull/4302
[#4314]: https://github.com/bytedance/deer-flow/pull/4314
[#4360]: https://github.com/bytedance/deer-flow/pull/4360
[#4377]: https://github.com/bytedance/deer-flow/pull/4377
[#4382]: https://github.com/bytedance/deer-flow/pull/4382
[#4384]: https://github.com/bytedance/deer-flow/pull/4384
[#4395]: https://github.com/bytedance/deer-flow/pull/4395
[#4405]: https://github.com/bytedance/deer-flow/pull/4405
[#4406]: https://github.com/bytedance/deer-flow/pull/4406
[#4423]: https://github.com/bytedance/deer-flow/pull/4423
[#4427]: https://github.com/bytedance/deer-flow/pull/4427
[#4429]: https://github.com/bytedance/deer-flow/pull/4429
[#4439]: https://github.com/bytedance/deer-flow/pull/4439
[#4443]: https://github.com/bytedance/deer-flow/pull/4443
[#4448]: https://github.com/bytedance/deer-flow/pull/4448
[#4453]: https://github.com/bytedance/deer-flow/pull/4453
[#4472]: https://github.com/bytedance/deer-flow/pull/4472
[#4480]: https://github.com/bytedance/deer-flow/pull/4480
[#4482]: https://github.com/bytedance/deer-flow/pull/4482
[#4486]: https://github.com/bytedance/deer-flow/pull/4486
[#4489]: https://github.com/bytedance/deer-flow/pull/4489
[#4490]: https://github.com/bytedance/deer-flow/pull/4490
[#4493]: https://github.com/bytedance/deer-flow/pull/4493
[#4497]: https://github.com/bytedance/deer-flow/pull/4497
[#4500]: https://github.com/bytedance/deer-flow/pull/4500
[#4501]: https://github.com/bytedance/deer-flow/pull/4501
[#4504]: https://github.com/bytedance/deer-flow/pull/4504
[#4505]: https://github.com/bytedance/deer-flow/pull/4505
[#4506]: https://github.com/bytedance/deer-flow/pull/4506
[#4509]: https://github.com/bytedance/deer-flow/pull/4509
[#4510]: https://github.com/bytedance/deer-flow/pull/4510
[#4512]: https://github.com/bytedance/deer-flow/pull/4512
[#4513]: https://github.com/bytedance/deer-flow/pull/4513
[#4518]: https://github.com/bytedance/deer-flow/pull/4518
[#4519]: https://github.com/bytedance/deer-flow/pull/4519
[#4524]: https://github.com/bytedance/deer-flow/pull/4524
[#4527]: https://github.com/bytedance/deer-flow/pull/4527
[#4528]: https://github.com/bytedance/deer-flow/pull/4528
[#4530]: https://github.com/bytedance/deer-flow/pull/4530
[#4533]: https://github.com/bytedance/deer-flow/pull/4533
[#4534]: https://github.com/bytedance/deer-flow/pull/4534
[#4535]: https://github.com/bytedance/deer-flow/pull/4535
[#4538]: https://github.com/bytedance/deer-flow/pull/4538
[#4539]: https://github.com/bytedance/deer-flow/pull/4539
[#4540]: https://github.com/bytedance/deer-flow/pull/4540
[#4556]: https://github.com/bytedance/deer-flow/pull/4556
[#4558]: https://github.com/bytedance/deer-flow/pull/4558
[#4559]: https://github.com/bytedance/deer-flow/pull/4559
[#4564]: https://github.com/bytedance/deer-flow/pull/4564
[#4570]: https://github.com/bytedance/deer-flow/pull/4570
[#4575]: https://github.com/bytedance/deer-flow/pull/4575
[#4578]: https://github.com/bytedance/deer-flow/pull/4578
[#4582]: https://github.com/bytedance/deer-flow/pull/4582
[#4584]: https://github.com/bytedance/deer-flow/pull/4584
[#4587]: https://github.com/bytedance/deer-flow/pull/4587
[#4589]: https://github.com/bytedance/deer-flow/pull/4589
[#4590]: https://github.com/bytedance/deer-flow/pull/4590
[#4596]: https://github.com/bytedance/deer-flow/pull/4596
[#4599]: https://github.com/bytedance/deer-flow/pull/4599
[#4600]: https://github.com/bytedance/deer-flow/pull/4600
[#4604]: https://github.com/bytedance/deer-flow/pull/4604
[#4615]: https://github.com/bytedance/deer-flow/pull/4615
[#4617]: https://github.com/bytedance/deer-flow/pull/4617
[#4618]: https://github.com/bytedance/deer-flow/pull/4618
[#4620]: https://github.com/bytedance/deer-flow/pull/4620
[#4624]: https://github.com/bytedance/deer-flow/pull/4624
[#4625]: https://github.com/bytedance/deer-flow/pull/4625
[#4627]: https://github.com/bytedance/deer-flow/pull/4627
[#4629]: https://github.com/bytedance/deer-flow/pull/4629
[#4631]: https://github.com/bytedance/deer-flow/pull/4631
[#4633]: https://github.com/bytedance/deer-flow/pull/4633
[#4635]: https://github.com/bytedance/deer-flow/pull/4635
[#4636]: https://github.com/bytedance/deer-flow/pull/4636
[#4639]: https://github.com/bytedance/deer-flow/pull/4639
[#4643]: https://github.com/bytedance/deer-flow/pull/4643
[#4644]: https://github.com/bytedance/deer-flow/pull/4644
[#4647]: https://github.com/bytedance/deer-flow/pull/4647
[#4649]: https://github.com/bytedance/deer-flow/pull/4649
[#4657]: https://github.com/bytedance/deer-flow/pull/4657
[#4658]: https://github.com/bytedance/deer-flow/pull/4658
[#4659]: https://github.com/bytedance/deer-flow/pull/4659
[#4660]: https://github.com/bytedance/deer-flow/pull/4660
[#4665]: https://github.com/bytedance/deer-flow/pull/4665
[#4667]: https://github.com/bytedance/deer-flow/pull/4667
[#4668]: https://github.com/bytedance/deer-flow/pull/4668
[#4677]: https://github.com/bytedance/deer-flow/pull/4677
[#4681]: https://github.com/bytedance/deer-flow/pull/4681
[#4683]: https://github.com/bytedance/deer-flow/pull/4683
[#4684]: https://github.com/bytedance/deer-flow/pull/4684
[#4690]: https://github.com/bytedance/deer-flow/pull/4690
[#4693]: https://github.com/bytedance/deer-flow/pull/4693
[#4701]: https://github.com/bytedance/deer-flow/pull/4701
[#4703]: https://github.com/bytedance/deer-flow/pull/4703
[#4707]: https://github.com/bytedance/deer-flow/pull/4707
[#4709]: https://github.com/bytedance/deer-flow/pull/4709
[#4713]: https://github.com/bytedance/deer-flow/pull/4713
[#4719]: https://github.com/bytedance/deer-flow/pull/4719
[#4722]: https://github.com/bytedance/deer-flow/pull/4722
[#4724]: https://github.com/bytedance/deer-flow/pull/4724
[#4727]: https://github.com/bytedance/deer-flow/pull/4727
[#4730]: https://github.com/bytedance/deer-flow/pull/4730
[#4735]: https://github.com/bytedance/deer-flow/pull/4735
[#4736]: https://github.com/bytedance/deer-flow/pull/4736
[#4737]: https://github.com/bytedance/deer-flow/pull/4737
[#4738]: https://github.com/bytedance/deer-flow/pull/4738
[#4744]: https://github.com/bytedance/deer-flow/pull/4744
[#4747]: https://github.com/bytedance/deer-flow/pull/4747
[#4748]: https://github.com/bytedance/deer-flow/pull/4748
[#4750]: https://github.com/bytedance/deer-flow/pull/4750
[#4752]: https://github.com/bytedance/deer-flow/pull/4752
[#4755]: https://github.com/bytedance/deer-flow/pull/4755
[#4758]: https://github.com/bytedance/deer-flow/pull/4758
[#4759]: https://github.com/bytedance/deer-flow/pull/4759
[#4760]: https://github.com/bytedance/deer-flow/pull/4760
[#4762]: https://github.com/bytedance/deer-flow/pull/4762
[#4764]: https://github.com/bytedance/deer-flow/pull/4764
[#4767]: https://github.com/bytedance/deer-flow/pull/4767
[#4769]: https://github.com/bytedance/deer-flow/pull/4769
[#4772]: https://github.com/bytedance/deer-flow/pull/4772
[#4780]: https://github.com/bytedance/deer-flow/pull/4780
[#4783]: https://github.com/bytedance/deer-flow/pull/4783
[#4785]: https://github.com/bytedance/deer-flow/pull/4785
[#4789]: https://github.com/bytedance/deer-flow/pull/4789
[#4792]: https://github.com/bytedance/deer-flow/pull/4792
[#4797]: https://github.com/bytedance/deer-flow/pull/4797
[#4800]: https://github.com/bytedance/deer-flow/pull/4800
[#4804]: https://github.com/bytedance/deer-flow/pull/4804
[#4806]: https://github.com/bytedance/deer-flow/pull/4806
[#4812]: https://github.com/bytedance/deer-flow/pull/4812
[#4815]: https://github.com/bytedance/deer-flow/pull/4815
[#4816]: https://github.com/bytedance/deer-flow/pull/4816
[#4817]: https://github.com/bytedance/deer-flow/pull/4817
[#4822]: https://github.com/bytedance/deer-flow/pull/4822
[#4823]: https://github.com/bytedance/deer-flow/pull/4823
[#4825]: https://github.com/bytedance/deer-flow/pull/4825
[#4827]: https://github.com/bytedance/deer-flow/pull/4827
[#4830]: https://github.com/bytedance/deer-flow/pull/4830
[#4833]: https://github.com/bytedance/deer-flow/pull/4833
[#4836]: https://github.com/bytedance/deer-flow/pull/4836
[#4838]: https://github.com/bytedance/deer-flow/pull/4838
[#4840]: https://github.com/bytedance/deer-flow/pull/4840
[#4842]: https://github.com/bytedance/deer-flow/pull/4842
[#4844]: https://github.com/bytedance/deer-flow/pull/4844
[#4846]: https://github.com/bytedance/deer-flow/pull/4846
[#4852]: https://github.com/bytedance/deer-flow/pull/4852
[#4853]: https://github.com/bytedance/deer-flow/pull/4853
[#4860]: https://github.com/bytedance/deer-flow/pull/4860
[#4861]: https://github.com/bytedance/deer-flow/pull/4861
[#4863]: https://github.com/bytedance/deer-flow/pull/4863
[#4865]: https://github.com/bytedance/deer-flow/pull/4865
[#4868]: https://github.com/bytedance/deer-flow/pull/4868
[#4877]: https://github.com/bytedance/deer-flow/pull/4877
[#4882]: https://github.com/bytedance/deer-flow/pull/4882
[#4887]: https://github.com/bytedance/deer-flow/pull/4887
[#4888]: https://github.com/bytedance/deer-flow/pull/4888
[#4898]: https://github.com/bytedance/deer-flow/pull/4898
[#4903]: https://github.com/bytedance/deer-flow/pull/4903
[#4911]: https://github.com/bytedance/deer-flow/pull/4911
[#4918]: https://github.com/bytedance/deer-flow/pull/4918
[#4928]: https://github.com/bytedance/deer-flow/pull/4928
[#4933]: https://github.com/bytedance/deer-flow/pull/4933
[#4936]: https://github.com/bytedance/deer-flow/pull/4936
[#4938]: https://github.com/bytedance/deer-flow/pull/4938
[#4951]: https://github.com/bytedance/deer-flow/pull/4951
[#4953]: https://github.com/bytedance/deer-flow/pull/4953
[#4956]: https://github.com/bytedance/deer-flow/pull/4956
[#4959]: https://github.com/bytedance/deer-flow/pull/4959
[#4960]: https://github.com/bytedance/deer-flow/pull/4960
[#4963]: https://github.com/bytedance/deer-flow/pull/4963
[#4965]: https://github.com/bytedance/deer-flow/pull/4965
[#4970]: https://github.com/bytedance/deer-flow/pull/4970
[#4983]: https://github.com/bytedance/deer-flow/pull/4983
[#4987]: https://github.com/bytedance/deer-flow/pull/4987
[#4998]: https://github.com/bytedance/deer-flow/pull/4998
[#4696]: https://github.com/bytedance/deer-flow/pull/4696
[#4810]: https://github.com/bytedance/deer-flow/pull/4810
[#4820]: https://github.com/bytedance/deer-flow/pull/4820
[#4826]: https://github.com/bytedance/deer-flow/pull/4826
[#4834]: https://github.com/bytedance/deer-flow/pull/4834
[#4839]: https://github.com/bytedance/deer-flow/pull/4839
[#4867]: https://github.com/bytedance/deer-flow/pull/4867
[#4876]: https://github.com/bytedance/deer-flow/pull/4876
[#4878]: https://github.com/bytedance/deer-flow/pull/4878
[#4946]: https://github.com/bytedance/deer-flow/pull/4946
[#4955]: https://github.com/bytedance/deer-flow/pull/4955
[#4972]: https://github.com/bytedance/deer-flow/pull/4972
[#4977]: https://github.com/bytedance/deer-flow/pull/4977
[#4984]: https://github.com/bytedance/deer-flow/pull/4984
[#4986]: https://github.com/bytedance/deer-flow/pull/4986
[#5003]: https://github.com/bytedance/deer-flow/pull/5003
[#5006]: https://github.com/bytedance/deer-flow/pull/5006
[#5008]: https://github.com/bytedance/deer-flow/pull/5008
[#5010]: https://github.com/bytedance/deer-flow/pull/5010
[#5014]: https://github.com/bytedance/deer-flow/pull/5014
[#5017]: https://github.com/bytedance/deer-flow/pull/5017
[#5018]: https://github.com/bytedance/deer-flow/pull/5018
[#5021]: https://github.com/bytedance/deer-flow/pull/5021
[#5022]: https://github.com/bytedance/deer-flow/pull/5022
[#5023]: https://github.com/bytedance/deer-flow/pull/5023
[#5025]: https://github.com/bytedance/deer-flow/pull/5025
[#5027]: https://github.com/bytedance/deer-flow/pull/5027
[#5030]: https://github.com/bytedance/deer-flow/pull/5030
[#5031]: https://github.com/bytedance/deer-flow/pull/5031
[#5036]: https://github.com/bytedance/deer-flow/pull/5036
[#5039]: https://github.com/bytedance/deer-flow/pull/5039
[#5041]: https://github.com/bytedance/deer-flow/pull/5041
[#5045]: https://github.com/bytedance/deer-flow/pull/5045
[#5047]: https://github.com/bytedance/deer-flow/pull/5047
[#5049]: https://github.com/bytedance/deer-flow/pull/5049
[#5050]: https://github.com/bytedance/deer-flow/pull/5050
[#5051]: https://github.com/bytedance/deer-flow/pull/5051
[#5056]: https://github.com/bytedance/deer-flow/pull/5056
[#5057]: https://github.com/bytedance/deer-flow/pull/5057
[#5059]: https://github.com/bytedance/deer-flow/pull/5059
[#5064]: https://github.com/bytedance/deer-flow/pull/5064
[#5066]: https://github.com/bytedance/deer-flow/pull/5066
[#5069]: https://github.com/bytedance/deer-flow/pull/5069
[#5071]: https://github.com/bytedance/deer-flow/pull/5071
[#5074]: https://github.com/bytedance/deer-flow/pull/5074
[#5076]: https://github.com/bytedance/deer-flow/pull/5076
[#5077]: https://github.com/bytedance/deer-flow/pull/5077
[#5083]: https://github.com/bytedance/deer-flow/pull/5083
[#5086]: https://github.com/bytedance/deer-flow/pull/5086
[#5087]: https://github.com/bytedance/deer-flow/pull/5087
[#5089]: https://github.com/bytedance/deer-flow/pull/5089
[#5090]: https://github.com/bytedance/deer-flow/pull/5090
[#5092]: https://github.com/bytedance/deer-flow/pull/5092
[#5095]: https://github.com/bytedance/deer-flow/pull/5095
[#5099]: https://github.com/bytedance/deer-flow/pull/5099
[#5103]: https://github.com/bytedance/deer-flow/pull/5103
[#5105]: https://github.com/bytedance/deer-flow/pull/5105
[#5109]: https://github.com/bytedance/deer-flow/pull/5109
[#5112]: https://github.com/bytedance/deer-flow/pull/5112
[#5117]: https://github.com/bytedance/deer-flow/pull/5117
[#5133]: https://github.com/bytedance/deer-flow/pull/5133
[#5136]: https://github.com/bytedance/deer-flow/pull/5136
[#5119]: https://github.com/bytedance/deer-flow/pull/5119

# Upstream adoption: November 16, 2026

## Scope and execution

HartMesh base `fd8408ed7736f22e9941b5bcb76fbfc363877c0e`; fixed DeerFlow target
`ea1833bd7a04eba2a5d6989eaf03790e6a2496b0`; previous upstream ancestor
`10bbcc92de5b8106fb6eceb46946eed34de0d515`. Both remotes were refreshed and
HartMesh main fast-forward checked before integration. No interrupted sync PR
or active-checkout change required recovery. Historical worktrees remain intact.

The interval contains 92 upstream commits. Their main purpose is to keep existing
work dependable: recover ended reservations, fence stale workers, isolate schedule
failures, preserve memory imports, clean up cancelled provider calls, and validate
inputs before delegation or disk effects. New optional capabilities include
search providers, project facts, agent-specific MCP credentials, and checkpointed
subagents. Product-facing additions are adopted deliberately.

One integration PR preserves the complete upstream ancestry and reference frontend.
The related changes share runtime/schema contracts; splitting out the ancestry
without its adaptations would expose unsafe intermediate states. Work proceeds
through focused regressions, independent compatibility review, published CI and
merge. No release is authorized by this synchronization.

## Compatibility plan

- Preserve authenticated owner/account/Space/instance admission, Work settlement,
  cancellation, storage lifetimes and verified output receipts. Review both
  conflicting and automatically merged code against these contracts.
- Retain HartMesh deployment defaults, release versions, curated guidance and
  exact upstream frontend provenance. Port relevant UI changes independently.
- Revisit earlier approval and cross-thread artifact deferrals. Do not expose
  lifecycle or raw-path APIs without current authority and receipt integration.
- Join new upstream schema revisions to the already published HartMesh branch
  with an additive merge revision; never rewrite published ancestry.
- Keep focused local checks separate from mandatory native/Compose CI gates;
  review and qualify the final published revision before merging.

## Decisions and progress

The table accounts for the complete pinned interval. The reviewed integration is
tracked in [PR #550](https://github.com/altakleos/hartmesh/pull/550), which records
published source revisions, focused validation, exact-head CI and merge status.
Ancestry alone is not implementation evidence.

| Upstream commit | Decision | Intention / compatibility |
| --- | --- | --- |
| [`1fbc06279`](https://github.com/bytedance/deer-flow/commit/1fbc06279abd28c7449157f20a9ea81aa53498c5) | Adopt | Compare memory and project context offline through the real Gateway with synthetic data, including original-thread and fresh-thread controls. |
| [`d03f1234f`](https://github.com/bytedance/deer-flow/commit/d03f1234f58b38ed7c9d76d04d1b683779fa1ab7) | Adopt | Recover BoxLite handles on their owning event loop so later SDK operations retain correct lifecycle ownership. |
| [`6d1b12f65`](https://github.com/bytedance/deer-flow/commit/6d1b12f650d4f1b5b83def87d23d076ddf2c5087) | Adopt | Count reasoning text actually replayed to vLLM when deciding whether context needs compaction. |
| [`226d1bc5c`](https://github.com/bytedance/deer-flow/commit/226d1bc5ce2447b423e93e4bf67f5dc5186b27c7) | Defer | Approval rejection history belongs to the still-deferred approval pause/resume lifecycle. |
| [`91f4fb457`](https://github.com/bytedance/deer-flow/commit/91f4fb457059ebed5e837824414e8c1ef3ad1df0) | Adopt | Keep existing retrieval indexes when startup fails operationally instead of treating failure as corruption. |
| [`d49e0f3d7`](https://github.com/bytedance/deer-flow/commit/d49e0f3d7ca721ce9c51e64c8248d70e262c1062) | Adapt | Reject unusable skill references at admission while preserving stripping of server-owned provenance. |
| [`99e395aa9`](https://github.com/bytedance/deer-flow/commit/99e395aa9cdc369e8c80b164be4baa543619d8a8) | Adapt | Recover ended ordinary thread reservations without releasing an unfinished capture or uncertain admission. |
| [`f60e8b1f7`](https://github.com/bytedance/deer-flow/commit/f60e8b1f78ed6394467791dac11cb05a76640865) | Already provided | HartMesh has maintained per-directory guidance budgets; retain those and current ownership instructions. |
| [`42da64a25`](https://github.com/bytedance/deer-flow/commit/42da64a25cf7983eff7b67b636d9f81c46bb6895) | Adopt | Treat unsuccessful DingTalk file-recipient delivery as failure rather than successful publication. |
| [`72fb20d56`](https://github.com/bytedance/deer-flow/commit/72fb20d56d8b027c1f5c05d0a265ddbfb2346930) | Adapt | Keep concise benchmark instructions and HartMesh verification restrictions. |
| [`6fe87b65f`](https://github.com/bytedance/deer-flow/commit/6fe87b65fa2fc224cda66328e8ca92561abfe1af) | Adapt | Validate and serve bounded package-owned usage documentation; product gallery/detail UI remains outside current scope. |
| [`989b20b1d`](https://github.com/bytedance/deer-flow/commit/989b20b1dc1b9d00fdc322bbc37bc8257661b1d8) | Defer | Approval calls without usable IDs cannot pause; no approval runtime is exposed in this adoption. |
| [`cf8c29c6b`](https://github.com/bytedance/deer-flow/commit/cf8c29c6b6920671075bd9010b94d89df2a273a8) | Adopt | Prevent late progress assessments from one graph entry changing a later entry's state. |
| [`8850f8eb7`](https://github.com/bytedance/deer-flow/commit/8850f8eb7ec39b7018a527420070a78349e4743b) | Adapt | Merge assembly metadata only under the worker's current lease, retaining HartMesh capture/settlement fences. |
| [`bf4aa82f0`](https://github.com/bytedance/deer-flow/commit/bf4aa82f09939ba364f43385ddefa7b3619099d7) | Adopt | Retry Telegram-rejected legacy photo sends as documents using the existing delivery result contract. |
| [`3df3b3c63`](https://github.com/bytedance/deer-flow/commit/3df3b3c630922a76efe2f8358a01a464d6e4bf5a) | Adopt | Show source line numbers for short ranged reads while keeping raw bytes and read-before-write qualification independent. |
| [`4a27b133f`](https://github.com/bytedance/deer-flow/commit/4a27b133f81dfa621bf17635f2659bef2eebb7fd) | Adopt | Apply configured image pull credentials to nginx as well as other chart workloads. |
| [`c8d5c108f`](https://github.com/bytedance/deer-flow/commit/c8d5c108f326fbc59c7063417d29f8fe8d7c60dc) | Adapt | Link to a verified conversation snapshot in Company shared on the current origin. [#594](https://github.com/altakleos/hartmesh/pull/594) |
| [`2a97ec3e5`](https://github.com/bytedance/deer-flow/commit/2a97ec3e589821e8435d57ef3f160ef6824b2a45) | Adapt | Retain HartMesh product README and ownership guidance; import upstream reference guides with explicit scope notes. |
| [`d9b2cb4bc`](https://github.com/bytedance/deer-flow/commit/d9b2cb4bcbb27a47b3b3c306a84422dd19b459c1) | Adopt | Require a usable facts collection before replacing imported memory. |
| [`4d95bff55`](https://github.com/bytedance/deer-flow/commit/4d95bff5505fc586dddf0201a0ccd924630c96ba) | Adopt | Exercise Goal-objective and Bash-exit validation boundaries with focused tests. |
| [`218c64180`](https://github.com/bytedance/deer-flow/commit/218c64180e3fb745af12f4f89e7f5cec5bb268b8) | Adopt | Compaction preserves current user requirements, exact approval wording, conditions and later revisions. |
| [`80cb6404b`](https://github.com/bytedance/deer-flow/commit/80cb6404b61c50546381e16db8728d86c3d89167) | Adopt | Reject Browserless timeouts that HTTPX cannot represent instead of failing during a request. |
| [`c7bc8bab6`](https://github.com/bytedance/deer-flow/commit/c7bc8bab6753cd9232900d2d1991694c6ca27d7d) | Adopt | Recognize sandbox configuration despite inline YAML comments without importing runtime dependencies. |
| [`3d826c6d9`](https://github.com/bytedance/deer-flow/commit/3d826c6d95e92d4594c1fb563597ac9a9c7ac026) | Defer | Ask/Auto/Full approvals require complete Work, resident, account, capture and human-input integration first. |
| [`fc7c7e012`](https://github.com/bytedance/deer-flow/commit/fc7c7e012c444b9b8b68cbb3ea6f64ef62ed8fc8) | Defer | The model-initiated handoff remains deferred pending conversation references and authorized tool access. The Gateway supports user-selected files from finished answers. [#570](https://github.com/altakleos/hartmesh/pull/570) |
| [`2baadff44`](https://github.com/bytedance/deer-flow/commit/2baadff4460f724c45de3161ab4363ebc8c7c4cf) | Adopt | Make setup/check help return usage without performing setup effects. |
| [`041d9e375`](https://github.com/bytedance/deer-flow/commit/041d9e375ee1d68b2de2b6a2509d3bd93d924761) | Adopt | Recognize addressed WeCom group commands after the bot mention. |
| [`9699f04cd`](https://github.com/bytedance/deer-flow/commit/9699f04cdc6b65f21576ea5f1cf7d1236e31f50b) | Adapt | Add bounded owner-scoped project facts with explicit extraction opt-in; preserve summaries-off and managed-storage gates. |
| [`6eb483359`](https://github.com/bytedance/deer-flow/commit/6eb48335981e4e4dc2f3ee8bb7ea741a7f7e3cd1) | Adapt | Manage the named ordinary agent's memory bucket while preserving instance-specific memory APIs and authority. |
| [`13e6fc29e`](https://github.com/bytedance/deer-flow/commit/13e6fc29e54201ce650b5f48743f88444b4d46f5) | Adopt | Offer opt-in OpenSandbox endpoint access tokens without changing existing sandbox defaults. |
| [`b883deaab`](https://github.com/bytedance/deer-flow/commit/b883deaab23bcb189ee3e9e1b037babf090983d2) | Adapt | Size HartMesh core shards from measured qualification headroom; derive the total consistently and preserve all tests and diagnostics. |
| [`b80191fad`](https://github.com/bytedance/deer-flow/commit/b80191fad3e0a507ddf06b352e0261b59c6e47d3) | Adopt | Reject duplicate fact IDs before replacing a memory document. |
| [`98e8598ad`](https://github.com/bytedance/deer-flow/commit/98e8598adbcab0d66e8ae2de0faf059585dcddb6) | Adopt | Use Windows extended-length paths when deleting deep thread directories. |
| [`2dc68b242`](https://github.com/bytedance/deer-flow/commit/2dc68b242fd253b32a1106a2b4c213f4b7041e3b) | Adopt | Publish file-backed agent creation exclusively so concurrent creates cannot overwrite a definition. |
| [`7ae36b469`](https://github.com/bytedance/deer-flow/commit/7ae36b469ddb56a4bfc27c8afc338ed9f89a0ee7) | Adopt | Make Gateway init-container images and resource limits configurable for mirrored registries and quotas. |
| [`416a0647a`](https://github.com/bytedance/deer-flow/commit/416a0647a481f2e743215251f95cfd6c24bf7cb8) | Adopt | Resolve inherited subagent model configuration off the event loop. |
| [`51cc489c8`](https://github.com/bytedance/deer-flow/commit/51cc489c8fda100d9e3af6daadb5953d40599d2f) | Adopt | Use WeCom's ten-megabyte image limit for legacy channel uploads. |
| [`95514245e`](https://github.com/bytedance/deer-flow/commit/95514245e2802f62e10fd153bbf5eff879d91818) | Adopt | Require checkpoint libraries with corrected replay ordering and paged ancestor handling; preserve checkpoint ownership wrappers. |
| [`70a4b8390`](https://github.com/bytedance/deer-flow/commit/70a4b839074e7aed3fed81f3922767fc05dac079) | Adopt | Return a safe validation error for malformed SSRF target URLs. |
| [`7d45fea17`](https://github.com/bytedance/deer-flow/commit/7d45fea17ecc6aa14d511d45cc4f9aa19be6ed89) | Adopt | Retain required OAuth beta flags when a Claude caller supplies other beta flags. |
| [`314e2a348`](https://github.com/bytedance/deer-flow/commit/314e2a3489a67c30b802466fcc9344a3d66362eb) | Adapt | Validate delegation before side effects while retaining instance capabilities, admission and current policy. |
| [`7892f79f9`](https://github.com/bytedance/deer-flow/commit/7892f79f9c0b6e155fb74a488f2e5fc2cdbaf6bc) | Adopt | Drop duplicate bound MCP names deterministically instead of silently shadowing a different server. |
| [`06634003a`](https://github.com/bytedance/deer-flow/commit/06634003a54aaeea5d295c12071062d65ca33f23) | Adopt | Apply subagent tool policy to middleware-declared tools as well as ordinary tools. |
| [`eb97327dd`](https://github.com/bytedance/deer-flow/commit/eb97327ddf80a95131065597bac19d64af47cfc4) | Adopt | Reject duplicate imported fact IDs regardless of their entry order. |
| [`53c5195e5`](https://github.com/bytedance/deer-flow/commit/53c5195e5d4ffdf66f34be90251d97fdfe6fba56) | Adopt | Explain the invalid agent selection when run admission rejects it. |
| [`b4410f64d`](https://github.com/bytedance/deer-flow/commit/b4410f64d469c0035f29c3b6bcc34b619a301fe9) | Adapt | Keep delegation occurrence identity exact across reused call IDs; approval transport remains deferred. |
| [`81de0761e`](https://github.com/bytedance/deer-flow/commit/81de0761ed775f9d611a7e5155824dc12f84ff92) | Adapt | The optional [Office package](../../backend/extensions/sources/hartmesh-office-preview) presents ordinary conversation outputs through the bounded, revision-checked worker. It preserves the original download. [Worker PR](https://github.com/altakleos/hartmesh/pull/561) and [Office package PR](https://github.com/altakleos/hartmesh/pull/563) |
| [`8853e6863`](https://github.com/bytedance/deer-flow/commit/8853e68638dcb98152041a381d1e6155d7b6b2a7) | Adapt | Strip server-owned checkpoint state from caller thread metadata while preserving existing HartMesh provenance exclusions. |
| [`b16e19761`](https://github.com/bytedance/deer-flow/commit/b16e19761810092cde3004b97a41a78a91642e03) | Adopt | Close Codex asynchronous HTTP requests promptly on cancellation. |
| [`bce5dc332`](https://github.com/bytedance/deer-flow/commit/bce5dc3327cd2dae6be73da4cd5523334b0b5013) | Adopt | Read Feishu post attachments from the platform's top-level files array. |
| [`bea75f3b2`](https://github.com/bytedance/deer-flow/commit/bea75f3b23e68ecb3d4566ba542f1d98031cb3cb) | Adopt | Verify durable-batch row position before returning an indexed item. |
| [`04a983376`](https://github.com/bytedance/deer-flow/commit/04a9833763f7daae9b3ff19d9c1d45fb6ab21c61) | Adopt | Keep a steered subagent's report in the parent's retained snapshot. |
| [`4d7f770fd`](https://github.com/bytedance/deer-flow/commit/4d7f770fd6e644ae69e52deea102c94bda40deb8) | Adapt | Keep HartMesh release-note and review requirements; source adoption and release publication remain separate. |
| [`2ff88fbb2`](https://github.com/bytedance/deer-flow/commit/2ff88fbb2984feb184af404a50258d8c014b7f83) | Adopt | Offer opt-in reserved DeerMem categories under hybrid-v1 eviction without changing defaults. |
| [`3244d4a8f`](https://github.com/bytedance/deer-flow/commit/3244d4a8ff2d6734c7561021a8805dc06263d70a) | Adopt | Make definition signatures change even for multiple updates in one clock tick. |
| [`94447d12b`](https://github.com/bytedance/deer-flow/commit/94447d12b687357c4d446b46cb9031f17b525865) | Adopt | Preserve existing summaries when memory extraction returns an invalid type. |
| [`ea4aee716`](https://github.com/bytedance/deer-flow/commit/ea4aee716e1ab105834d17a182915130459d9cc2) | Adopt | Read the subagent registry off the event loop. |
| [`067c9736b`](https://github.com/bytedance/deer-flow/commit/067c9736b6c8065f190bb5c47b7944b59de7311d) | Adapt | Synthesize Goals from legacy IM chat with fresh management authorization before inference and persistence. |
| [`161c3503d`](https://github.com/bytedance/deer-flow/commit/161c3503d03314efded83aac3c87b492cc4c458b) | Adopt | Build GitHub App tokens off-loop and reuse the parsed signing key. |
| [`ba4e60dec`](https://github.com/bytedance/deer-flow/commit/ba4e60dec477a8e8bab1a949175d701006a8fe24) | Adapt | Expose an accessible jump-to-bottom control above task cards in the independent product frontend. |
| [`5f6d7c06e`](https://github.com/bytedance/deer-flow/commit/5f6d7c06e0896f4c92aebe7bfb01f5a5d21952ae) | Adopt | Identify placeholder model keys locally instead of reporting example credentials as usable. |
| [`1ba275589`](https://github.com/bytedance/deer-flow/commit/1ba2755899518de2ee6674e2cfcec97922b68d1a) | Adopt | Allow users to repair a malformed persisted schedule interval. |
| [`c263b887b`](https://github.com/bytedance/deer-flow/commit/c263b887b5d4c878d6f31ce0b3f8387e0b07547f) | Adapt | Isolate each scheduled dispatch failure without bypassing HartMesh launch leases, capacity or shutdown holds. |
| [`ae871431a`](https://github.com/bytedance/deer-flow/commit/ae871431a2e38febba5567c37a27b644febbd7d4) | Adapt | Keep sandbox configuration diagnostics credential-safe and map unavailable archive reads to a retryable service error. |
| [`599105e0f`](https://github.com/bytedance/deer-flow/commit/599105e0fb43f706af4188ec03d8b11eecebc1f5) | Adapt | Reject invalid artifact-registry entries at admission without admitting server-owned receipt metadata. |
| [`dba950443`](https://github.com/bytedance/deer-flow/commit/dba950443d2770778fef695215623f863ead0ba1) | Adapt | The Chats page searches conversation titles through the viewer-scoped Gateway API and pages through matches. Message search has an extension API; message results in the product UI are deferred. [PR #567](https://github.com/altakleos/hartmesh/pull/567). |
| [`077773a96`](https://github.com/bytedance/deer-flow/commit/077773a9675fe5467661e8ffa545bd8896b68e72) | Adopt | Match grep against line content without counting newline terminators as trailing whitespace. |
| [`cee54cdd4`](https://github.com/bytedance/deer-flow/commit/cee54cdd496f3870f3e7154ba0393f595ad774a9) | Adapt | Discard exact template credential placeholders through the shared provider rule, preserving safe URL and error handling. |
| [`9cb2bde55`](https://github.com/bytedance/deer-flow/commit/9cb2bde557d5748c546d6fc12e46aa9f4c35dc76) | Adapt | Separate bundled-example behavior into an applicable mandatory CI workflow with PostgreSQL coverage; core coverage remains complementary. |
| [`4f3a8b790`](https://github.com/bytedance/deer-flow/commit/4f3a8b790d33635958cfd1b8a54ff7168a82515a) | Adapt | Publish snapshots under existing Shared permissions. A manager can share an employee conversation. Readers can continue with the same agent when authorized or use the ordinary assistant. Each Home import rechecks the snapshot and file. [#600](https://github.com/altakleos/hartmesh/pull/600) [#594](https://github.com/altakleos/hartmesh/pull/594) |
| [`35d33bea7`](https://github.com/bytedance/deer-flow/commit/35d33bea753c7ed9fadcf72a0c45869041b2ac09) | Adapt | Keep invalid cron fields as custom expressions in the product editor instead of presenting a valid preset. |
| [`f086cce86`](https://github.com/bytedance/deer-flow/commit/f086cce86e6157d63a412f57fdba9bccd826c188) | Adapt | Deliver legacy presented files once in presentation order without replacing managed Telegram delivery custody. |
| [`a68dc2a1a`](https://github.com/bytedance/deer-flow/commit/a68dc2a1a01d776e45cee10b600105beef44f059) | Adapt | Honor the selected app's pnpm pin while preserving absolute executable resolution and the independent product selector. |
| [`1804b7c52`](https://github.com/bytedance/deer-flow/commit/1804b7c5227b7a6b51c0c192331119ee6dcf1031) | Adapt | Read run/auth configuration off-loop while preserving internal authority and managed-storage admission. |
| [`3e2489429`](https://github.com/bytedance/deer-flow/commit/3e2489429ff3061489537d7303240fef3f199f83) | Adapt | Reject unusable deferred-tool promotion records before they can poison checkpoint reducers. |
| [`f582b9a87`](https://github.com/bytedance/deer-flow/commit/f582b9a87ba6ffb8ef03792e74de218701232d21) | Adapt | Preserve Excel literal text, nulls and mixed columns using HartMesh's content-aware offline reader and immutable cache generations. |
| [`b0bca9dcb`](https://github.com/bytedance/deer-flow/commit/b0bca9dcb4bd42d0473bcf2d44371b2830d40683) | Adapt | Bind remote MCP credentials to an operator-approved ordinary agent, definition, owner and destination; recheck revocation on each call. |
| [`085b52b5f`](https://github.com/bytedance/deer-flow/commit/085b52b5f39a2dc6ca23f207584763bc17d3950b) | Adopt | Match referenced conversation URLs against the addressed host instead of an unrelated host. |
| [`453ca834c`](https://github.com/bytedance/deer-flow/commit/453ca834cf268c2d0eb4ae14cdba9e5e4bfa32e4) | Adapt | Combine incoming measured durations with HartMesh-only timings without regenerating the whole local suite. |
| [`e4b2f4cb9`](https://github.com/bytedance/deer-flow/commit/e4b2f4cb9ac0ea5a95e879f9860912c82fa6abd7) | Adapt | Preserve FIFO for scheduled occurrences behind active same-thread work under existing shared capacity fences. |
| [`9134cf26c`](https://github.com/bytedance/deer-flow/commit/9134cf26c6d64f74c446f98911a6beb56b7994fc) | Adopt | Reject artifact archives whose file names collide with implicit directories before writing the archive. |
| [`ec44f2ff3`](https://github.com/bytedance/deer-flow/commit/ec44f2ff3df8a02d6204c4152dd52e2a25360ee2) | Adapt | Web chats keep the admitted human row in view across its server replacement. Reader input releases it, and the jump control follows actual reply content. [PR #568](https://github.com/altakleos/hartmesh/pull/568). |
| [`a1a6866eb`](https://github.com/bytedance/deer-flow/commit/a1a6866ebcc298d8bfc9e3b90004faa9e384d73c) | Adopt | Treat commas as path-list separators so later entries are independently masked and translated. |
| [`778f53a45`](https://github.com/bytedance/deer-flow/commit/778f53a456560aead68fd8f805dafb3b0cc970a0) | Adapt | Exclude hidden output working state from missing-delivery checks while keeping explicit inventory and receipt authority. |
| [`75443ecc2`](https://github.com/bytedance/deer-flow/commit/75443ecc23307c7e1e1a69f9e0f0ac353f500410) | Adopt | Ignore OIDC UserInfo without the ID token's matching subject. |
| [`569022127`](https://github.com/bytedance/deer-flow/commit/5690221272958ef97875895b99171a2974da141c) | Adapt | Do not overwrite an ended scheduled occurrence merely because a newer run occupies the same thread. |
| [`c5d072af9`](https://github.com/bytedance/deer-flow/commit/c5d072af91af11a01d2d4ef3a51fab9b4bd5f560) | Adapt | Offer opt-in SQLite native-subagent checkpoints with loop-owned shutdown and HartMesh saver wrappers. |
| [`54dc5024a`](https://github.com/bytedance/deer-flow/commit/54dc5024a6b8a569e46bf3801e8ce5249923bcd4) | Adapt | Serialize readiness database probes while retaining the richer storage/resident readiness report. |
| [`4b98e4638`](https://github.com/bytedance/deer-flow/commit/4b98e463880b3a7a48233a763a32278b01f45eb3) | Adapt | Revalidate durable batch accounts and roles before execution, retaining delegated capability and Goal fences. |
| [`db02b7783`](https://github.com/bytedance/deer-flow/commit/db02b77837d6209f35959395dd23a20011a3472f) | Adapt | Keep skill evolution in an optional package; require a supported request-scoped evidence reader before staging mutations. |
| [`ea1833bd7`](https://github.com/bytedance/deer-flow/commit/ea1833bd7a04eba2a5d6989eaf03790e6a2496b0) | Adapt | Publish missing receipt-persistence failure on the stream while retaining HartMesh's richer delivery receipt and incomplete-output protocol. |

## Compatibility adaptations and revisited deferrals

- Operator MCP grants remain revocable during active ordinary named-agent runs,
  including after OAuth refresh. Instance employees retain their separate typed
  authority and never inherit those human credentials. Run/checkpoint copies do
  not mint capabilities.
- Instance-bound memory never carries the ambient legacy project-memory hook into
  its portable storage manager or background extraction tail. Legacy project facts
  default to extraction disabled, and plugin-managed projects require an explicit
  supported adapter. Existing document-summary generation stays off by default.
- Durable batch owners are resolved from current accounts and roles; ordinary
  completed reservations, worker assembly metadata and receipts retain their
  stronger HartMesh lifecycle fences. Receipt-persistence failures become visible
  on the stream without replacing the existing incomplete-output protocol.
- New schema head `0056_upstream_adoption` joins published
  `0053_work_operation_indexes` with upstream `0055_thread_shares`. Approval/share
  tables preserve ancestry without enabling unqualified runtime or public routes.
- Previous approval and cross-thread handoff deferrals still apply. Approval
  pauses/resume need current account/instance admission, Work/resident settlement,
  capture lifetimes and web/Telegram human-input integration. Handoffs need an
  authorized storage adapter and exact output-inventory receipts.
- Public conversation snapshots/forks additionally require managed destination,
  export and revocation policy; raw-path routes and authentication exemptions are
  omitted. Existing Files publication is unaffected.
- Product gallery/details, project-memory pages, conversation search controls
  and shared-conversation pages remain unexposed. Office preview support arrived
  after this sync as an operator-enabled package for ordinary conversation
  outputs. It uses the existing revision-qualified renderer and preserves
  immediate uploads and original downloads.
- Sent-turn pinning requires actual product submission identities plus viewport
  geometry, keyboard and cancellation regressions. Only the independent accessible
  jump-to-bottom control is ported; regenerated/history turns keep current scroll
  behavior. Invalid cron fields remain exact custom expressions.
- Excel changes preserve literal NA/NULL/leading-zero text and mixed-column types
  using the existing offline, content-detected reader and immutable cache.
- Product browser/replay CI also triggers on the upstream provenance marker,
  ensuring a large sync still runs product checks when its changed-file list is
  truncated by workflow path evaluation. Existing gates remain required.
- The optional skill-evolution package requires a supported request-scoped source
  evidence reader before staging. Its static evaluations do not grant mutation
  authority or prove general behavior.

## Review and validation progress

Independent agents reviewed separate implementation areas and cross-reviewed the
other implementations. Review found and corrected an accidental deferred-approval
MCP import, mismatched product scrolling assumptions, memory authority crossings,
and a scheduler list/claim cutoff inconsistency. The PR records reviewed source
fingerprints, exact commands and final CI/merge evidence.

Focused checks passed for runtime/peer recovery, input and authorization boundaries,
MCP revocation with real Gateway restarts, provider and path handling, workbook
parsing, the optional evolution package, setup/CI policy, Helm rendering, product
lint/types and scrolling/cron units. Local SQLite-heavy concurrency and benchmark
checks encountered measured filesystem-journal stalls; no deadline or runtime
policy was relaxed. Fresh required CI must qualify these and every applicable
native/Compose workflow on the final published revision before merge.

Core CI uses sixteen duration-balanced shards under the existing 45-minute job
limit. The twelve-shard qualification on `32832bb951e392e144a5c9cfac2f3ffcb14dee57`
passed eleven groups; one continuously progressing group reached 95% before its
deadline, with no failed test assertions. Another group finished with six seconds
left. Stored estimates underpredicted the cancelled group's observed execution
by about 1.9 times. Additional capacity restores headroom while retaining every
case, selection filter, diagnostic and the shared matrix-total command. No test
or job deadline was extended, and no timing measurements were invented.

The reference frontend and marker equal upstream tree
`330ae40e7e41fbf440375cd7d06a50a92fcddf51`. The full isolation ancestry check passed
on integration commit `7f2ad71d756791caa1f56dcbaf399734003c75e3`, whose parents are
the HartMesh base and fixed upstream target. At that source integration commit,
HartMesh's version sources remained `2.2.0+hartmesh.59`.

## Delivery boundary

Implementation evidence remains with [PR #550](https://github.com/altakleos/hartmesh/pull/550).
Release delivery is tracked separately in
[v2.2.0+hartmesh.60](https://github.com/altakleos/hartmesh/releases/tag/v2.2.0%2Bhartmesh.60).
Its `release-manifest.json` and separate `release-acceptance.json` assets bind the
release commit, five immutable image digests and authenticated exact-image
qualification. Only published, successfully verified assets establish delivery;
source adoption must not be reapplied merely to publish them. Deployment and
tenant-class qualification remain separate consumer work.

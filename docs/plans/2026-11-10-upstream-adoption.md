# Upstream adoption: November 10, 2026

## Scope and contributor intent

HartMesh base `bb5deef408419334c7111fc9eeb3a47530aa3cfc`; fixed DeerFlow target
`10bbcc92de5b8106fb6eceb46946eed34de0d515`; common ancestor
`d0007090c262cf3806d78f39dfcb4f5aca8226ea`. Both remotes were refreshed before work.
No open sync PR or interrupted change existed in the active checkout. Unrelated
historical branches/worktrees remain untouched.

The 62 incoming commits mostly make existing operations dependable: cancellation
must finish cleanup, one broken schedule must not stop other schedules, memory
must rank useful evidence and handle multilingual queries, and providers must
not block the event loop or leak credentials through errors. Runtime decisions
move from guessed prose to explicit statuses and receipts. New opt-in providers
extend configuration without changing deployment defaults.

Two enhancements need a HartMesh-specific integration before exposure. Tool-call
approval pauses introduce a new nonterminal run lifecycle, while the proposed
cross-thread file handoff operates directly on legacy disk paths. Their useful
intent is retained below, together with concrete requirements for revisiting them.

## Execution plan and compatibility decisions

1. Merge the complete pinned ancestry; review automatic intersections and textual
   conflicts. Preserve HartMesh versions, operator defaults and existing guards.
2. Adopt independent fixes and their regressions. Keep current Space/instance,
   output receipt, owner/account, admission and shutdown contracts.
3. Retain the exact upstream frontend snapshot, deliberately port applicable
   product changes and update its provenance marker. Existing omitted gallery,
   catalogue and project/trash surfaces remain outside the product UI.
4. Qualify adaptations with focused unit and strict blocking-I/O tests. Native
   and Compose acceptance run only in existing applicable CI workflows.
5. Obtain independent review before commit/publication; monitor all applicable
   workflows on the exact published head, fix failures, and merge preserving
   upstream ancestry. The PR is the single command/review/CI evidence record.

### Adaptations

- Structured tool statuses reach provider requests without rewriting stored
  results or raw receipts. A request-local typed carrier lets RunJournal recover
  the exact original short-circuit ToolMessage; JSON/wire copies grant no trust.
  Declared `capacity` and `not_run` refusals retain their existing recovery rules.
  HarnessGuard preserves immutable callable receipts and one qualified hint tag.
- AIO environment-bearing commands get independent bounded Bash sessions.
  Exact pending admission tokens retain per-call cancellation across queue waits,
  retries and bounded abort-history churn. Uncertain Bash close quarantines
  further creation rather than risking eviction of a live sibling.
  Instance Home clients receive the baseline's validated Bash capacity; existing
  persistent-shell snapshot/control headroom, download roots and abort ownership
  remain. Provisioner metadata participates in reuse compatibility.
- SpecStream uses the shared credential resolver, public-target validation and
  generic credential-safe failure handling. It rejects target URL credentials;
  response bodies/exception text never enter error logs. It is opt-in.
- Shared Weknora retrieval requires the request-bound authenticated human identity,
  with normal role, route, owner and storage constraints retained.
- Scheduled trials no longer infer execution authority from natural-language
  phrases. Chat returns owner-checked instructions for the existing authenticated
  **Run once now** action on `/workspace/scheduled-tasks`; it does not promise an
  upstream-only chat card. Automatic schedules and manual dispatch remain.
- HartMesh already opens the attachment chooser directly and snapshots validated
  artifact generations for streaming. Keep those stronger implementations.
  Port actionable localized chat 5xx errors without logging private server bodies,
  the bookmark Unicode regression and required Node/pnpm versions into the product.
- Deleted thread run records release confirmed drained capture lifetimes through
  the existing safe eviction rules; uncertain/active resources remain retained.
- Docker excludes product-local PEM files as well as upstream-local secrets.
  Configuration stays at version 66; no database migration or release bump.

### Deferred behavior and required next work

- **Tool-call approvals (0d04322):** do not expose the upstream runtime feature
  or its product controls. `waiting_for_input` is incompatible with Work/resident
  settlement and captured-output receipt status; upstream resume writes bypass
  HartMesh's paired output/inventory and worker-fence guarantees. A future change
  must define pause/resume admission, current account/instance authority, Work
  settlement, capture lifetimes and web/Telegram human-input protocols together,
  with real regression coverage. Existing clarification and Attention flows stay.
  The reference frontend still contains the complete upstream implementation.
- **Cross-thread artifact tool (part of 3f9a78e):** do not register or ship the
  legacy `present_shared_artifact` implementation. It lacks current source-thread
  and Space/instance authority and verified output-inventory integration. Add an
  authorized storage adapter and exact-byte receipt qualification first. Existing
  extension-owned artifact handoffs keep their supported-scope restrictions.
- Prior scope decisions were revisited: upstream public pages/model catalogue do
  not gain product routes; durable batch followups and extension handoffs retain
  the explicitly supported execution/storage scopes from the previous adoption.

## Commit accounting

Every incoming commit is listed, including superseded and intentionally deferred
behavior. Ancestry alone is not adoption evidence.

| Upstream commit | Decision | Intention / compatibility |
| --- | --- | --- |
| [`3f9a78e02`](https://github.com/bytedance/deer-flow/commit/3f9a78e02e719896dab7b48e7f856d0cd717b24d) | Adapt | Verify Goal completion; defer legacy cross-thread file handoff until storage authority and receipts are integrated. |
| [`a99cd299f`](https://github.com/bytedance/deer-flow/commit/a99cd299f9633caeacd15e772dae640e87be59ed) | Adopt | trim inherited guidance below hard limit (#7976) |
| [`2bd60d1e6`](https://github.com/bytedance/deer-flow/commit/2bd60d1e6f5167b95da035e2b57605d90d9237ca) | Adopt | build the chat model off the event loop (#7967) |
| [`372baf762`](https://github.com/bytedance/deer-flow/commit/372baf762f69ca05a8bc6de843dea10252ca40f0) | Adopt | prevent todo reminders from resuming runs after hard stops (#7955) |
| [`f68abbb83`](https://github.com/bytedance/deer-flow/commit/f68abbb8314f833929df88cde05d81310da27783) | Adopt | fix the deprecated contextmanager issue (#7974) |
| [`d1238c697`](https://github.com/bytedance/deer-flow/commit/d1238c69740d539f707e9074f55b42c1105b76fd) | Adopt | Refactor tool filtering into a single assembly pass (#7950) |
| [`3a85baba9`](https://github.com/bytedance/deer-flow/commit/3a85baba9fbc6840287ccccbef865560e536d54e) | Adopt | avoid splitting surrogate pairs in automatic labels (#7966) |
| [`c953ab65f`](https://github.com/bytedance/deer-flow/commit/c953ab65f5eb1c43c4231ccc3a8a515021681e19) | Adopt | drain abandoned capacity waiters before cancellation (#7900) |
| [`4e90f5d77`](https://github.com/bytedance/deer-flow/commit/4e90f5d771501007c6b8c1cc8d0605d1dc983547) | Adapt | Exclude local secrets and hand-edited extension credentials while retaining product exclusions and deployed snapshots. |
| [`6c94ecc6c`](https://github.com/bytedance/deer-flow/commit/6c94ecc6cbf1fa05564dbcd21fa977fe81a633a8) | Adopt | strip provider API keys before they reach a header (#7981) |
| [`7d805cd1b`](https://github.com/bytedance/deer-flow/commit/7d805cd1b8daf317370636388cf8828327065bf9) | Adopt | reject Windows device names when creating a skill (#7948) |
| [`1dae5eb36`](https://github.com/bytedance/deer-flow/commit/1dae5eb363ba0043a12216e057849a37d13d64ba) | Adopt | exempt expression division after closers from the host-bash guard (#7448) |
| [`2a8927c5e`](https://github.com/bytedance/deer-flow/commit/2a8927c5e153b8e9b1711a0fce5d72558bba9cad) | Adopt | add StepFun provider preset (#7988) |
| [`b315cf2c9`](https://github.com/bytedance/deer-flow/commit/b315cf2c9e7908f94846d719a374d5ac5802d85f) | Adopt | wait for retry_after before retrying a new message (#7990) |
| [`961262a17`](https://github.com/bytedance/deer-flow/commit/961262a17e4f9e19103d6fad8209f188b850eacf) | Adapt | Declare Node >=22 and pnpm >=10.26.2 in the independent product package too. |
| [`c69e9ef0b`](https://github.com/bytedance/deer-flow/commit/c69e9ef0bc8b7f3d6e6ff6de9b20f59f7ae96887) | Adopt | make doctor validates the selected backend config (#7965) |
| [`3aefc56f5`](https://github.com/bytedance/deer-flow/commit/3aefc56f5f60f51f09badf6082b2e04fcc6c724f) | Adopt | keep batch cancellation running when the scheduler store fails (#7971) |
| [`8924c5844`](https://github.com/bytedance/deer-flow/commit/8924c5844ab7e19441858ce93f1acac814e11b77) | Adopt | pause a task whose schedule no longer computes (#7993) |
| [`5b1bee3d5`](https://github.com/bytedance/deer-flow/commit/5b1bee3d5a1f9006d358bf783779b4923fc5cb47) | Adopt | skip text-less turns when formatting the memory prompt (#7985) |
| [`91f20a782`](https://github.com/bytedance/deer-flow/commit/91f20a782f66fcc0cf779138fd576ec57040c9b0) | Adopt | match repository bindings case-insensitively (#7982) |
| [`284e0d537`](https://github.com/bytedance/deer-flow/commit/284e0d537d1a5115704e71ed741b5ce37bf1fbb9) | Adapt | Exclude local HTTPS keys from both upstream and HartMesh product Docker contexts. |
| [`d8e085630`](https://github.com/bytedance/deer-flow/commit/d8e085630ac04eef9bdd6c95c84d4bae5320617b) | Adopt | keep todo reminders in Goal turns after an earlier provider stop (#8004) |
| [`a73a94a6a`](https://github.com/bytedance/deer-flow/commit/a73a94a6a378e95b10c1b837328971fd1cf02558) | Adopt | keep HarnessGuard from rewriting the result that ends a loop (#8001) |
| [`3920169fb`](https://github.com/bytedance/deer-flow/commit/3920169fb6bd24439685d9c92e6e758ed642bdbd) | Adopt | reject Windows reserved device names in managed subagent creation (#7959) |
| [`f98653649`](https://github.com/bytedance/deer-flow/commit/f9865364924d1af005bbc6a4e19b0b64f37a3965) | Adopt | bump oauthlib from 4.0.0 to 4.0.1 in /backend (#8014) |
| [`aaca3eeae`](https://github.com/bytedance/deer-flow/commit/aaca3eeaebaafe7c4ca0147f83e9738655a39b2d) | Adapt | Use structured runtime evidence while retaining declared refusals, raw receipts, original journal text and explicit manual trials. |
| [`955524a51`](https://github.com/bytedance/deer-flow/commit/955524a51090edd206a21a0331deb7ea940a5bfa) | Adopt | scan PII emails in linear time (#8006) |
| [`2a9e7255a`](https://github.com/bytedance/deer-flow/commit/2a9e7255a7c9bb50b8d0bb5b5b5028095f2ad94d) | Adopt | keep Gateway calls off the environment proxy (#7877) |
| [`fe05090b9`](https://github.com/bytedance/deer-flow/commit/fe05090b969c29fe0e9573dc6c627bd9ac966ffe) | Adopt | fix two stale internal anchor fragments (#7983) (#8007) |
| [`3163a96eb`](https://github.com/bytedance/deer-flow/commit/3163a96ebd4e4f852f796fe521affeb1b00a459b) | Superseded | Intermediate development CLI lock superseded by stable 0.4.42 later in this interval. |
| [`24884e812`](https://github.com/bytedance/deer-flow/commit/24884e8120d13bb28f0a71de7f1f0ddcc102cf1d) | Adopt | count distinct archived search sources (#7956) |
| [`4c00e8d4a`](https://github.com/bytedance/deer-flow/commit/4c00e8d4a91fcda81c36a0c706d4b3f3271dbd63) | Adopt | reject Windows reserved device names in an agent memory scope (#7941) |
| [`3b05ab7dc`](https://github.com/bytedance/deer-flow/commit/3b05ab7dc3a7dc6df00601ab348b6ed691ff273b) | Adopt | remove empty strings from user content (#8038) |
| [`0d04322d4`](https://github.com/bytedance/deer-flow/commit/0d04322d410b39abf78443419fa7f75e6e043124) | Defer | Pause high-risk calls for human approval; requires the lifecycle/authority integration described above. |
| [`97746ad72`](https://github.com/bytedance/deer-flow/commit/97746ad725c1cd57739681e080b0b397c25f43f0) | Already provided | Attachment button already opens the product file chooser directly. |
| [`aa1f376e9`](https://github.com/bytedance/deer-flow/commit/aa1f376e900dceace9f92f9d97570c44f10101fe) | Adapt | Construct Jina/SearXNG clients off-loop while retaining SearXNG query privacy through POST. |
| [`7bb92cea1`](https://github.com/bytedance/deer-flow/commit/7bb92cea10bb45c63b97a2cb7ae9457a576dc75b) | Adopt | keep the owner row when deleting checkpoints fails (#7992) |
| [`7bbccda57`](https://github.com/bytedance/deer-flow/commit/7bbccda575c16925681e97225d0cb43008a295bd) | Adopt | lock langgraph-cli to stable 0.4.42 (#8045) |
| [`c738c5ef8`](https://github.com/bytedance/deer-flow/commit/c738c5ef8bed1bb054c0eba9d862510450276143) | Adopt | give a bracketed literal in the userinfo slot no host on every Python (#8017) |
| [`463361c5b`](https://github.com/bytedance/deer-flow/commit/463361c5bae2850391414434511ad9977437e84f) | Adapt | Allow operator-opted shared retrieval only for verified authenticated humans, retaining existing authorization layers. |
| [`fb094984e`](https://github.com/bytedance/deer-flow/commit/fb094984e3b6c7ab7f4c141cd6f2c20d4d72e656) | Adapt | Classify all backend package trees while retaining frontend-hm triage coverage. |
| [`3e59e22b4`](https://github.com/bytedance/deer-flow/commit/3e59e22b4c341807796459a149df7f10621188ee) | Adopt | keep tests_passed evidence per call when a provider reuses its id (#8012) |
| [`f9767c65d`](https://github.com/bytedance/deer-flow/commit/f9767c65d5db7f436363603ad9508ddd868085af) | Adapt | Run independent scoped Bash commands concurrently; propagate capacity to instance clients and preserve shell/abort controls. |
| [`4e09e63a6`](https://github.com/bytedance/deer-flow/commit/4e09e63a68f55bf5203838598b399fd03b2e80bd) | Adopt | evict in-memory runs when a thread is deleted (#8036) |
| [`3ebd4de99`](https://github.com/bytedance/deer-flow/commit/3ebd4de993d1e50524df03a460565987d2c3cfef) | Adopt | isolate a malformed stored schedule from the poll batch (#7999) |
| [`7adb2062e`](https://github.com/bytedance/deer-flow/commit/7adb2062eb2220e26447f652b0c724285806b071) | Adopt | keep retryable thread inputs ahead of terminal receipts (#7972) (#8008) |
| [`fa8e01502`](https://github.com/bytedance/deer-flow/commit/fa8e015023b937c3251c0a74a68bbc08d4cd1be6) | Adapt | Add optional SpecStream search/fetch with credential-safe errors and public URL admission. |
| [`259894028`](https://github.com/bytedance/deer-flow/commit/2598940283fc3d3595eb939b369089d91a51ce48) | Adapt | Give product users a localized retry message for 5xx stream errors without logging private server bodies. |
| [`31d97fc85`](https://github.com/bytedance/deer-flow/commit/31d97fc85b0b55872a0c752c3c0aa47c1f7cb224) | Adopt | select FTS5 matches by composite score (#8005) |
| [`7a8051c0d`](https://github.com/bytedance/deer-flow/commit/7a8051c0d9211abddc61b1e6031641a6a3c2216e) | Already provided | HartMesh artifact snapshots already pin validated file generations for full and ranged streaming. |
| [`5c5cf24ff`](https://github.com/bytedance/deer-flow/commit/5c5cf24ff24a5e02becee7dca344ca2a4d6e4d42) | Adopt | read undecodable skill state as missing (#7963) |
| [`45c1c22ff`](https://github.com/bytedance/deer-flow/commit/45c1c22ffae12fc61f9f206521700a39aac62566) | Adopt | declare PyYAML so the standalone suite runs as documented (#8067) |
| [`b0b3a8aa2`](https://github.com/bytedance/deer-flow/commit/b0b3a8aa22469b191a66bb918ebfccda7e6cdeab) | Adopt | build the async HTTPX client off the event loop (#7931) |
| [`0dc423605`](https://github.com/bytedance/deer-flow/commit/0dc423605a0431abf54bb70b8479d7e5f68df7cf) | Adopt | resolve the token through the shared credential rule (#8043) |
| [`1647320ad`](https://github.com/bytedance/deer-flow/commit/1647320ad14cc06223c381602ba587f0c68f0515) | Adopt | return 404 on cross-owner thread_id instead of 500 (#8057) |
| [`f746da3a4`](https://github.com/bytedance/deer-flow/commit/f746da3a4c9303d8ec49591d2a482e5a6d2e52f5) | Adopt | count model calls independently of reused message IDs (#8050) |
| [`f17e0504d`](https://github.com/bytedance/deer-flow/commit/f17e0504d0b99d22ddedbde1726a8e4cd4456996) | Adopt | return 409 instead of 500 when feedback is POSTed twice (#8064) |
| [`c61253121`](https://github.com/bytedance/deer-flow/commit/c612531213b2505857bf733a6a332fd7653f84b4) | Adopt | drop empty typed text blocks from user content (#8066) |
| [`ea7bb64f0`](https://github.com/bytedance/deer-flow/commit/ea7bb64f0224fa3479d77f23a1e50b22f46846e5) | Adopt | use checkout-local temp paths for local development (#8075) |
| [`54753636b`](https://github.com/bytedance/deer-flow/commit/54753636b1b6770762f8113a27d57d524851e38f) | Adopt | recall CJK sub-phrases when jieba is not installed (#8022) |
| [`bfa2794c7`](https://github.com/bytedance/deer-flow/commit/bfa2794c7677d22d2a8097c55ffaeb2d00979c24) | Adopt | keep dedupe counter before compound archive suffix (#8042) |
| [`10bbcc92d`](https://github.com/bytedance/deer-flow/commit/10bbcc92de5b8106fb6eceb46946eed34de0d515) | Adopt | delegate max_results coercion to shared owner (#7869) (#7872) |

## Progress and verification boundary

The complete interval is accounted for. Independent compatibility review identified
and drove the adaptations above. Focused regressions and static checks qualify
the implementation before publication. The associated sync PR records exact
review fingerprints, command outcomes, final CI and merge revisions.

Source adoption does not establish release delivery. This run does not authorize
or publish a new release, image set or tenant deployment. The existing published
release remains separate from this implementation.

# AGENTS.md

Product code lives in `frontend-hm/`; `CLAUDE.md` imports this guide. Never import
from the pinned upstream `../frontend/`; port deliberately. HTTP/SSE/WebSocket
and `../contracts/` define the backend boundary; see [isolation](../docs/FRONTEND_ISOLATION.md).

Management UI uses `useCustomerAdministration()` from `/api/features`, keyed by
account ID and role, with missing flags denied. Never derive mutation authority
from admin role alone. Delegated private owners receive the Skills section in
business profile without privileged developer sections. Skill queries and clone
previews are keyed/fenced by account and role. Provided/Private tabs show server
origin; copies default to distinct names and same-name overrides require a
separate explicit choice. Archive override consent is account-local and never
implies replacing an existing private package. Account unmount or capability
retirement fences pending upload results/findings; the server still drains
admitted mutations. Public skills stay read-only; skill drafts can still be
created as artifacts. Local MCP controls require effective delegation, remote
preferences retain their existing behavior, and managed integration installation
is provider-only. EN/ZH notices use configured provider links.

## Project Overview

`/workspace/assistants` and `/workspace/employees` list agents by type.
`/workspace/instances` owns detail tabs. Apply current grants/provider ceilings,
account/role caches and retired-action fences. Retry exact intents; 202 is pending.
Viewer-scoped `/api/search` is cancellable; demos filter locally. Agent files use Spaces and audited writes.
Conversation attachments stay local to chat. The account workspace owns Home uploads and rechecks grants, incarnation and receipts across routes. Pickers send destination references. Browse inspectable agents on demand. Confirm Home copies, omit source titles, reconcile by ID; verify receipt, Home and execution.
Binding discovery hides ordinary artifacts, preserving selections; confirmed bindings retire the panel. Use verified custody in breadcrumbs.
Only Inspect opens produced files. Browser/sidecar/scheduling remain ordinary-chat
controls. Memory uses instance APIs/all summaries only.
Company targets start empty; transfer data explicitly through admitted APIs.
Work controls separate delegation, explicit activation and review. Use+Inspect delegates with adopted
policy defaults; Manage+Inspect changes assignments and reviews exact statements.
Retain uncertain mutation bodies/IDs, retire actions on account/permission changes,
and capture revisions for drafts and acknowledgements. Source projections carry
read-only availability metadata: strip it from command DTOs and require explicit
consent before replacing unavailable references. Definition policy settings do not
change an existing instance until deliberate adoption. Activation requires current
Use+Inspect plus run/agent ceilings. Preserve conversation creation and activation
identities across unknown replies; receipt recovery never changes the bound thread.
Unresolved attempts prohibit replacement. Polling preserves captured draft/review
bases. Candidate reports are separate from submitted outcomes; a response starts no
run. Work Stop captures its attempt/revisions; guarded retries never stop a replacement.
Recovery is collapsed under Advanced for stopping/uncertain work and needs exact
completed containment receipts and current Home ADMIN. Attention recipients come
from eligible grants; directory names never decide authority. Requests and replies
remain available during execution holds and start no work by themselves.

`/workspace/spaces` uses account-scoped queries and host grants. Send captured
generation/SHA for edits and attachment IDs for retirement; preserve conflict
drafts. Abort account/component work and late navigation. ADMIN recovery needs
explicit restore/delete acceptance. Active content remains text/download only.

Existing extension pages negotiate resource API v1. Resolve `?space=<id>` under
current grants before mounting, pass immutable resource facts, and bind action
headers independently of payload. Abort retired reads/mounts/actions; backend
rechecks access. See [the contract](../docs/STORAGE_SPACES.md#extension-storage-contract).

`ArtifactFileControls` serves passive/native renderers through explicit exports,
collection suggestions, bounded live probes and existing storage/Undo lifetimes.
Legacy probes share the reader. `core/api/abort.ts` observes late work; retired
responses cancel unread bodies before rejection and cannot navigate on late 401.

`core/artifact-views/{associations,source-view}.ts` resolves one-hop relations
only among already-presented same-directory candidates. Never glob, expand or
prefetch history. Limits: eight candidates, two readers, 1 MiB/20 seconds per
view. Reject partial sets, malformed/unreadable data and ambiguous/weak revisions.
Cache only the active source under viewer/thread/path/candidate identity with
zero inactive retention. Ordinary/legacy readers stay available; only unique
valid views take preference. Completeness is view-specific. Preserve explicit
Code selection, canonical editing and copy.

`core/extensions/` reuses authenticated startup `/api/plugins` discovery/module
transports. Page-only browser API v1 remains supported; artifact v1 is additive
in `artifacts`, separate from `surfaces`. Never register artifacts as pages.
`PluginArtifactPresentation` mounts through the existing Shadow DOM lifecycle
or validates a passive return. Shadow DOM isolates styles, not privileges;
operator-installed modules are trusted and share no React runtime.

Select handlers by installed suffix; ambiguity uses ordinary access. Mount by
account/thread/path/module/source SHA-256/locale/theme. Cancellation retires
services, disposes controllers/raster URLs and prevents late toasts.
Bound discovery, module and transcript reads. `preview=namespace/id` keys
include viewer and byte budgets. A busy response offers Retry. On failure,
offer Download for binary files or bounded text for recognized text formats.
Permission/path failures remain terminal. See [plugin capability](../contracts/artifact_view/plugins.md).

`core/artifact-views/` owns passive `*.view.json` v1 and `ArtifactView`: six flat
primitives, authored strings and explicit local exports. Unknown structure
rejects rich rendering without hiding originals. Decode UTF-8 fatally; bounded
stream reads handle ignored Range. Validate metadata/body completeness and keep
source/projection reads separate. Export only server-owned `presented_files`,
not incidental discovery or unanswered `present_files`. Live probes bind
thread/view/revision/exact paths, with four readers/ten-second deadlines and
unused-data disposal. Card actions reuse lifetime-aware save/share/Undo with
explicit paths and inline collection hints; no recursive/implicit export.

Effect-owned `ArtifactImageSession` binds account/thread/path/revision, including
StrictMode replay. Validate complete PNG/JPEG framing before decode; bound bytes,
pixels and concurrency, own cancellation/URL revocation, and keep placeholders
on failure. See [contract/budgets](../contracts/artifact_view/README.md). No
business schema, calculation, interpreter or module registration belongs here.

Installed projections use canonical source SHA-256 revisions. Keep source and
projection queries separate; Code/edit/copy/download use canonical bytes. Bad
projections fall back to bounded source reads. Never save or reconcile drafts
from projection bytes. Full-file selection binds thread/path; completion refreshes
the active query.

Product scope: [frontend isolation guide](../docs/FRONTEND_ISOLATION.md#product-scope).

**Stack**: Next.js 16, React 19, TypeScript 5.8, Tailwind CSS 4, pnpm 10.26.2. Requires Node.js 22+ and pnpm 10.26.2+.

### Core dependencies

- **LangGraph SDK** (`@langchain/langgraph-sdk` ^1.5.3) — Agent orchestration and streaming
- **LangChain Core** (`@langchain/core` ^1.1.15) — Fundamental AI building blocks
- **TanStack Query** (`@tanstack/react-query` ^5.90.17) — Server state management
- **UI**: Shadcn UI, MagicUI, React Bits, and Vercel AI SDK elements (generated from registries — see Code Style)

## Commands

| Command          | Purpose                                       |
| ---------------- | --------------------------------------------- |
| `pnpm dev`       | Start the development server with Webpack     |
| `pnpm build`     | Production build                              |
| `pnpm check`     | Lint + type check (run before committing)     |
| `pnpm lint`      | ESLint only                                   |
| `pnpm lint:fix`  | ESLint with auto-fix                          |
| `pnpm format`    | Prettier check (`pnpm format:write` to apply) |
| `pnpm test`      | Run unit tests with Rstest                    |
| `pnpm test:e2e`  | Run E2E tests with Playwright (Chromium)      |
| `pnpm typecheck` | TypeScript type check (`tsc --noEmit`)        |
| `pnpm start`     | Start production server                       |

The production Docker image resolves the `packageManager`-pinned pnpm release
at build time into the shared `/opt/corepack` cache. Keep that cache readable by
the chart's non-root uid 1000 so `pnpm start` never downloads its toolchain.
Next builds type-check product sources with `tsconfig.build.json`; provider-dependent
test fixtures remain in the default `tsconfig.json` checked by `pnpm check`.
The frontend image must build without sibling backend source files.

Rstest tests in `tests/unit/` mirror `src/`; import product modules through `@/`.

Webpack is the default development bundler. Use `DEER_FLOW_DEV_BUNDLER=turbo` with `pnpm dev` to opt in to Turbopack when diagnosing a local Next.js bundler issue.

Rstest has node and happy-dom projects in `rstest.config.ts`. Pure logic uses
`*.test.ts(x)`; React hooks/components use `*.dom.test.ts(x)`. Keep effect,
cleanup and store-order tests under real React rather than mocking React.
Only DOM tests pay the document environment's higher runtime cost.
Content/state DOM suites may mock decorative `FlipDisplay` with
`tests/unit/helpers/flip-display.tsx` (keeps children, classes and keyed
remounts): Happy DOM 21 rejects a cancelled animation's `finished` promise.

Use task-specific disk-backed `TMPDIR`, `TMP` and `TEMP` for suites. Set
`HARTMESH_TEST_OUTPUT` to a directory there when using Rstest's
`--dev.writeToDisk`; its compiled bundles then stay outside this checkout.
Bound focused runs with `--pool.maxWorkers 2 --pool.minWorkers 1
--maxConcurrency 2`; keep total test workers at four or fewer.

For browser checks beside a running development server, set `NEXT_DIST_DIR` to
an ignored `.next-hartmesh-*` directory and `NEXT_TSCONFIG_PATH` to a task-owned
config under `.hartmesh-test/` extending `../tsconfig.build.json`. Next can update
that config without rewriting the tracked build config. Use the same variables
for build and start, distinct ports, and disk-backed suite temporary directories.

E2E tests live under `tests/e2e/` and use Playwright with Chromium. They mock all backend APIs via `page.route()` network interception and test real page interactions (navigation, chat input, streaming responses). Config: `playwright.config.ts`.

`playwright.real-backend.config.ts` tests the real replay Gateway without
provider credentials. `playwright.gateway-auth.config.ts` uses the same
isolated runner with authentication enabled to verify login, SSR session
forwarding, CSRF enforcement, and logout through the app's same-origin proxy.

`playwright.docker-acceptance.config.ts` exercises production images and real
application services with synthetic inference. Run it from the repository root
through `python3 scripts/docker_acceptance.py`; see [release validation](../RELEASING.md#docker-acceptance).

## Architecture

```
Frontend (Next.js) ──▶ LangGraph SDK ──▶ LangGraph Backend (lead_agent)
                                              ├── Sub-Agents
                                              └── Tools & Skills
```

The frontend is a stateful chat application. Users create **threads** (conversations), send messages, set thread-scoped `/goal` completion conditions, and receive streamed AI responses. The backend orchestrates agents that can produce **artifacts** (files/code), **todos**, and goal state updates.

### Source Layout (`src/`)

- **`app/`** — Next.js App Router. Routes include `/` (a redirect to the workspace, which sends someone signed out to sign-in), `/workspace/chats/[thread_id]` (authenticated chat), `/workspace/agents/[agent_name]/chats/[thread_id]` and `/workspace/agents/new` (custom agents), `/workspace/files` (the person's own files, kept across conversations), `/artifacts/view` (chrome-free window that renders one markdown artifact with the panel's own renderer), the `(auth)/{login,setup,auth/callback}` flow, and `/api/…` route handlers (e.g. `/api/memory`).
- **`components/`** — React components:
  - `ui/` — Shadcn UI primitives (auto-generated, ESLint-ignored)
  - `ai-elements/` — Vercel AI SDK elements (auto-generated, ESLint-ignored)
  - `workspace/` — Chat page components (messages, artifacts, settings)
- **`core/`** — Business logic, the heart of the app. Domains include `threads/` (creation, streaming, state), `api/` (LangGraph client singleton), `agents/` (custom agents), `subagents/` (runtime worker catalog and administrator mutations), `auth/` (authentication), `artifacts/`, `artifact-delivery/` (run-scoped undelivered-file verdicts, from the stream while the page that heard them is open and from `GET .../runs/{run_id}/delivery` afterwards, so the correction survives a reload), `files/` (the person's own files: list, open, remove, and keeping a conversation's file there), `channels/` (IM connections), `integrations/` (managed third-party integration status/install clients such as Lark CLI), `turn-progress/` (the stage a running turn reports, and the activity row's label from it; a client-made upload placeholder never counts as model output), `i18n/` (en-US, zh-CN), `settings/`, `memory/`, `skills/`, `messages/`, `mcp/`, `models/`, `input-polish/` (pre-send draft rewrite API), `voice-input/` (browser speech-recognition helpers), `suggestions/`, `tasks/`, `todos/`, `tools/`, `workspace-changes/` (run-scoped changed-file summaries and diff fetching), `config/`, `notification/`, `product/` (the deployment's product name), plus rendering helpers (`rehype/`, `streamdown/`) and `utils/`.

A presentation — the chips and archive action under an answer — is drawn from
`additional_kwargs.presented_files` on whichever message carries it, or from a
`present_files` tool call. Three producers write that tag: `present_files`, a
tool that presented what its call was asked to make (`bash` with a `present`
argument), and the runtime, which tags the turn's final assistant message when
the turn produced files nobody presented (also stamping
`presented_by: "runtime"`). `core/messages/utils.ts` reads the tag rather than
any tool name, so a new producer needs no client change; the present-files
group renders its first message's prose above the files, which is what keeps a
tagged answer readable rather than replaced by its own chips.

`core/streamdown/plugins.ts` keeps `singleDollarTextMath: false`: one `$` is
literal currency, while `$$` requests mathematics. Never restore single-dollar
math to repair literal LaTeX; it consumes currency amounts and their spaces.
Do not promote spans containing backslashes either: Windows paths and escaped
asterisks between amounts must remain ordinary text.

Double-dollar markers mid-line render inline; markers beginning a line open a
display fence. `normalizeLatexMathDelimiters` in `preprocess.ts` rewrites both
`\(...\)` and `\[...\]`. Join multiline inline spans onto one line so their
markers cannot become display fences and consume the next display block.
Single-dollar formulas intentionally remain literal. Currency regressions must
assert rendered behavior, never `textContent`: extracted DOM text can retain
currency while KaTeX displays a formula. Keep the absence-of-`katex` assertions in
`tests/unit/core/streamdown-currency.test.ts` and the assistant-bubble case of
`tests/e2e/user-message-plain-text.spec.ts`.

The chat mode is one dial, read in one place: `core/threads/run-context.ts`
answers which modes a model can offer (`offersReasoningMode`), which mode a
stored choice resolves to on it (`resolveChatMode`), the effort a picked mode
writes (`reasoningEffortForMode`) and what a run sends (`runFlagsForMode`).
Both pickers and both places that start a run derive from it; nothing else
reads the mode to decide behaviour. Plan mode is Ultra's alone and Reasoning
is offered only where the model honours `reasoning_effort` — the measurements
behind both are in the module comment. The unit test pins the table row by
row. The Pro and Ultra descriptions in both locales say what each mode does;
that is a convention, not a check, so change them with the table.

Historical report parsing, formatting, companion naming, native DOM/CSS and
translations belong to the provider package at
`backend/extensions/sources/hartmesh-legacy-report/`. The installed module mounts without
host React or UI imports, using shared host file controls and bounded raster
loading. Its bytes projector uses the existing extension lifecycle and generic
artifact route; the compatibility query is an installed declaration. Missing or
disabled adapters leave ordinary source access. Provider packaging/configuration
must explicitly install and activate it; the core has no special activation path.

The package preserves the 10rem container-based KPI floor. Keep the monetary
one-line, own-tile and overflow assertions in
`tests/e2e/business-report-card.spec.ts`; box width alone and zero overflow do not
prove that figures remain readable. Domain unit tests use the real independent
mount and actual host controls; production browser tests load its real assets.

`core/extensions/filing.ts` resolves an optional versioned, synchronous installed
file-collection callback. Its input is an immutable path/destination/presented
snapshot and its output is a validated single collection component. Invalid,
throwing or conflicting decisions use no collection. No callback grants file
eligibility or expands directories. Filing mutations wait while plugin discovery
is pending or failed, so uncertainty cannot silently change a destination.

_My files_ (`/workspace/files`, `core/files/`) is what the person kept, from
every conversation: the Gateway keeps it per user and every sandbox of theirs
mounts it at `/mnt/user-data/files`, so a report kept in one chat is on the
next one's disk. Keeping copies exact bytes and never overwrites (a taken name
gets the next `_N`). Shared host card controls keep explicit, eligible, currently available exports;
the artifact panel keeps the open file under uploads or outputs
(`canKeepInMyFiles`). Neither mutates storage in static/mock mode. Both use
`useSaveToMyFiles`, which keeps each path, names its destination once and reports
partial failures without undoing earlier copies. Installed filing contributions
provide collection hints for ordinary file actions; Files/Shared API clients know
only the explicit folder supplied by their caller. Presentation
stays an outputs contract: a file from _My files_ is handed over by copying it
into outputs.

_Shared_ is the second tab of the same page (`?tab=shared` opens it;
`core/shared/`): what anyone at the company published, readable by everyone.
The Gateway keeps one directory for the tenant and every sandbox mounts it
read-only at `/mnt/user-data/shared`, so only publishing puts anything there.
Shared card controls publish their selected exports, the artifact panel publishes
the open file, and a My Files row publishes that file directly
(`canPublishToShared`). Collection policy belongs to installed contributions;
the compatibility package alone recognizes its historical flat personal folder.
Arbitrary personal folders and nested legacy folders do not become company
folders. Collection names are data and never translated. Save and Share toasts
name the chosen folder. All three Share controls use `useShareWithEveryone`.
A new copy returns `201`. Undo is available only when the Gateway advertises
Shared removal and every new copy's receipt supplies `can_remove`, its
publication ID and original revision. The client reuses the same operation ID
for a retry. Ordinary storage shows the successful share without Undo.
A `200` identifies an existing copy with the same bytes. Show the already-shared
notice with a link to Shared, and exclude that copy from Undo. Repeat clicks
always consult the Gateway. Shared rows show the publication time and publisher
resolved to a display name. The native browser offers Remove only after the
Gateway advertises it and a fresh selection confirms the caller's authority
and supplies the required identity. Both tabs sit under the sidebar's _Files_
heading.

Files filters inspect only loaded name/path metadata, with Unicode-normalized
case matching and deterministic Name/Newest ordering (`core/file-areas/selection`).
Each mounted tab owns its filter/order; navigation resets them. Shared's newest
order uses publication time, falling back to modified time. Found and truncated
counts use the loaded array, and no matches is distinct from empty storage.
Mobile name cells also show the folder. Keep original file objects and paths for
downloads, Share, Delete and publication-ID Remove; never mutate query data.

Provider-key status separates loading, confirmed unmanaged and recoverable error.
Validate status metadata before adopting it; failed reads retain known rows and
drafts with Retry, while mutation/probe controls require confirmed status. Status
reads and body parsing have a ten-second deadline and component/account/generation
fences; optional history cannot retire usable status. Preserve account-owned model
invalidation after a successful mutation closes Settings. Tool rows explain that
testing is unsupported; model probes map only fixed timeout/rate-limit hints and
keep unknown reasons generic. No raw provider errors or keys enter probe copy.

Remove and Undo pass the displayed file's `publication_id` to DELETE as
`expected_publication_id`; a 409 requires refreshing and choosing the current
publication. Null explicitly identifies an operator-placed file. Only older
Gateways omit the field; that compatibility path retains server-side locking and
authorization but cannot fence an action against its earlier displayed identity.

The deployment owns two presentation settings, both read from
`GET /api/features` (`core/features`): `ui.starters` is Home's starter grid —
choosing one fills the composer through the prompt-input controller, focuses it
and sends nothing, because the first moment is "pick the thing, drop the file,
say the month" — and `ui.profile` decides who is offered the developer screens.
Under `business`, someone who is not an administrator is offered Skills only
when private management is effectively delegated. Tools, subagents, integrations
and scheduled-task recipe chips remain hidden, and Home
drops the product blurb. Channels and memory stay: the phone someone messages
it from and what the agent remembers about them are theirs, not the
deployment's. Hiding is presentation, not authorization — the routes are
guarded by attributed owner/admin authority, startup delegation and applicable
resource visibility. The API remains the authority for every operation.
Runtime product identity comes from the public `/api/v1/auth/product-identity`
(`core/product/server.ts`). Layouts pass one complete validated snapshot to
`RuntimeProductProvider`, i18n, titles/icons and entry pages. Configured product
and provider slots never use tenant fields. Default mode retains `ui.product_name`
and the legacy name API. Keep strict and legacy name validation separate. Retain admitted identity on
failure; fresh failures show unavailable. See [contract](../docs/RUNTIME_PRODUCT_IDENTITY.md).

Legacy tenant branding (`useBranding`) selects the authenticated features response's
company name, colors, logo availability and optional provider support. It can
replace default product naming in the sidebar/title/About; loading or failed
initial reads show neither name. Colors are metadata, not applied web CSS.
Configured product mode preserves product slots. Sign-in/setup never show tenant
material. Support uses validated HTTPS with no referrer or added context.

Strict tenant presentation (`core/features/tenant-presentation.tsx`) captures one
complete authenticated name/logo snapshot inside the account-owned workspace
subtree. Validate its bounded schema/content/product binding. Retain it across
focus, client navigation and server refreshes; retire/cancel with the account.
Fresh failures show unavailable. `useBranding` ignores live tenant fields in
strict mode while other features keep refreshing. The header shows a separately
named Workspace context with fixed readable colors, including collapsed names.
Static/legacy paths stay compatible. See [the contract](../docs/TENANT_PRESENTATION.md).
Keep session recovery and the index's pure route control outside private admission.
Uncertain SSR auth keeps the provider path; new SSR scope conflicts retire it
until a full reload, including when later SSR responses become uncertain again.
Files tabs derive from `useSearchParams`, including same-page/history navigation.
Keep the original handle for interrupted Home writes and check by GET. A clean
retry keeps the chip and takes a new handle.

One rule governs what happens while the answer is unknown: a control someone
might need stays offered, and copy the deployment authors waits. So the screens
stay (a Gateway reporting no `ui` block reads as `developer`, and an upgrade
never takes one away), while the blurb and the grid render only once the
deployment has answered. Starters default to the profile — `business` opens on
a small built-in set, `developer` on none — so an untouched deployment gains
nothing it did not ask for, and when a grid exists the legacy suggestion row
under the composer steps aside rather than sitting beside it. `InputBox` also
mounts on static demo threads, so that gate lives inside `SuggestionList`,
which a demo thread never reaches.

- **`hooks/`** — Shared React hooks
- **`lib/`** — Utilities (`cn()` from clsx + tailwind-merge)
- **`styles/`** — Global CSS with Tailwind v4 `@import` syntax and CSS variables for theming
- **`typings/`** — Ambient TypeScript declarations
- `src/env.js` owns environment validation

More specific `AGENTS.md` files under `src/` contain the frontend sections split from this file.

## Code Style

- **Imports**: Enforced ordering (builtin → external → internal → parent → sibling), alphabetized, newlines between groups. Use inline type imports: `import { type Foo }`.
- **Unused variables**: Prefix with `_`.
- **Class names**: Use `cn()` from `@/lib/utils` for conditional Tailwind classes.
- **Path alias**: `@/*` maps to `src/*`.
- **Components**: `ui/` and `ai-elements/` are generated from registries (Shadcn, MagicUI, React Bits, Vercel AI SDK) — don't manually edit these.

## Environment

Backend API URLs are optional; an nginx proxy is used by default:

```
NEXT_PUBLIC_BACKEND_BASE_URL=http://localhost:8001
NEXT_PUBLIC_LANGGRAPH_BASE_URL=http://localhost:8001/api
```

Leave these unset for the standard `make dev` / Docker flow, where nginx serves the public `/api/langgraph/*` prefix and rewrites it to Gateway's native `/api/*` routes.

To reach a dev server on anything other than localhost — a LAN address, or a proxied hostname — list the host in `DEER_FLOW_DEV_ALLOWED_ORIGINS` (comma-separated; a full URL is reduced to its host). It feeds Next's `allowedDevOrigins`, which gates `/_next/*`, fonts, and HMR. Without it those requests get a 403 and the page renders server-side but never hydrates, so nothing on it — including the login form — responds. Development only; production builds ignore it.

## Resources

- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [LangChain Core Concepts](https://js.langchain.com/docs/concepts)
- [TanStack Query Documentation](https://tanstack.com/query/latest)
- [Next.js App Router](https://nextjs.org/docs/app)

## Contributing

Follow the existing `src/` structure, type and handle errors, and update guidance
when architecture or commands change. Run `pnpm check` before committing.
Unit tests live in `tests/unit/` (`pnpm test`), E2E in `tests/e2e/` (`pnpm test:e2e`).

`pnpm perf:check` measures unique JS and CSS for production `/login` and
static-demo workspace routes on temporary local ports. It compares totals in
`.next/performance-results.json` with `performance-budgets.json`. Fix route
ownership or split points. Document and review any measured budget increase.

`/workspace/attention` owns durable Work-backed human requests, separate from chat
clarification cards. Poll every30 seconds while mounted; zero inactive retention,
account retirement and failed-current-access handling apply to titles and counts.
Capture routing/request/response revisions with drafts; never silently rebase a
human decision or uncertain retry. Linked Work controls lead to Attention, whose
canonical decision includes every reviewed response ID. Read is independent and
no response invokes run/resume. Generic inputs survive optional plugin absence;
file references use existing Spaces and never imply upload or mounting.

Telegram screens: [the client guide](src/core/telegram-bots/AGENTS.md).

Managed storage UX uses the existing account-and-role-scoped features query:
`useStorage` fails closed for absent/malformed/failed answers and polls personal
preparation and faults every 15 seconds; `useSeats` ignores the actual storage-off zero-seat
envelope. HTTP clients retain structured refusal metadata and presentation maps
it with the current locale. Personal readiness never closes Shared. Files retries
busy reads every five seconds and retries recovery at most once per failure
episode, with manual Retry available. First-message capacity refusal remains
sticky; other send refusals retire after newer valid discovery. Composer recovery
restores only the captured attempt, preserves subsequent edits and excludes
failed attachment chips without blocking text-only Send. Native download frames
capture the account `FileActionLifetime` signal; component retirement fences
notices without canceling same-account navigation's streamed transfer. Unknown download outcomes need a manual link retry. Avoid a second GET or buffered Blob.
Use `playwright.auth.config.ts` for registration. Replay fixtures SSR-redirect Login.
Retry a failed fetch once for conversation metadata GET and history POST. Unknown
identity locks the composer. Reload restarts both reads.
Ordinary sends track admission beyond SDK `submit()` resolution. Private input
and empty-metadata identities track thread-create and run errors without wire
tokens. Stop captures its attempt and blocks new sends. A failed cancellation
remains uncertain. Propagate text/file admission errors to PromptInput. Clear
drafts/references and release captured browser Files only on confirmed admission.

Collapsed provider support keeps its named logo link. Browser-mocked Playwright suites use
`tests/fixtures/product-gateway.mjs` for product identity and explicit legacy selection (other routes 503);
retain real-service outage checks. See the
[runtime contract](../docs/RUNTIME_PRODUCT_IDENTITY.md).

An admitted composer send anchors its visible human row until reader input.
History, replay and busy-run inputs never initiate that anchor.

Workspace and agent Files share `components/workspace/file-browser`. Use one
renderer on every device. Read older Gateways through the authorized file lists. Negotiate each source’s
operations, recheck restored paths and keep history in account memory. A refused
versioned operation has no path-only fallback.

Fold marked snapshot members after authorized discovery and collection reads.
Agent publication requires agent_publish, Manage + Inspect and consent to the previewed company audience.
Undo sends its exact receipt and created IDs. Copied-turn attribution needs the
thread’s verified seed IDs. Continued attachment chips use the returned uploads
path and belong to the signed-in account and private thread.

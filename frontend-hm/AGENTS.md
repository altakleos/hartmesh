# AGENTS.md

This file guides development of the Hartmesh frontend in `frontend-hm/`. The
sibling `CLAUDE.md` imports it via `@AGENTS.md`. This app owns its dependencies,
assets, API clients, and tests. Never import or link application material from
the pinned upstream `../frontend/`; port changes deliberately. The Gateway
HTTP/SSE/WebSocket protocols and shared `../contracts/` fixtures define the
backend boundary. See [the isolation guide](../docs/FRONTEND_ISOLATION.md).

## Project Overview

`ArtifactFileControls` serves passive and native renderers through explicit
exports, collection suggestions, bounded live probes and existing storage/Undo
lifetimes. Legacy probes share that reader. `core/api/abort.ts` observes late
work; retired fetch responses cancel unread bodies before rejection without
allowing late 401 navigation.

`core/artifact-views/associations.ts` and `source-view.ts` resolve one-hop source
relations from already-presented same-directory view candidates only. Never
glob, expand directories or prefetch historical views. Cap candidates at eight,
readers at two and each view at 1 MiB/20 seconds; refuse partial candidate sets,
malformed/unreadable documents and ambiguous or weak matching revisions.
Cache only the active source under its viewer/thread/path/candidate identity,
with zero inactive retention. Ordinary/legacy rendering remains available during
resolution; a unique valid view takes preference without hiding the source entry.
Its own completeness/revision governs display, independently of a truncated
canonical source. Preserve explicit Code selection and canonical editing/copy.

`core/extensions/` captures the authenticated startup plugin snapshot through
the existing `/api/plugins` discovery and module transports. Page-only browser
API v1 stays supported. Artifact capability v1 is additive in `artifacts`,
separate from `surfaces`; the host never registers an artifact in the page array.
`PluginArtifactPresentation` mounts independent DOM through the existing
Shadow DOM lifecycle or validates a passive return with the strict view decoder.
Shadow DOM isolates styles, not plugin privileges. Operator-installed modules
remain trusted code and do not receive a shared React runtime.

Handler selection comes only from installed suffix declarations; an ambiguous
match uses ordinary file access. Mounts belong to account, thread, path, module
entry, source SHA-256, locale and theme. Cancellation retires supported host
services, observes late results, disposes native controllers and raster URLs,
and prevents late action toasts. Module/discovery/transcript reads have byte
bounds and finite deadlines. Generic projections use `preview=namespace/id`,
separate query keys and declared source/preview budgets. Malformed display data
falls back to bounded canonical source; permission/path failures remain terminal.
See [the public capability](../contracts/artifact_view/plugins.md).

`core/artifact-views/` owns the passive `*.view.json` v1 contract and the generic
`ArtifactView` renderer. It accepts only six flat primitives, authored strings
and explicit local exports. Unknown structure rejects rich rendering; original
file access remains. View responses use fatal UTF-8 decoding and a bounded
stream reader even when Range is ignored. Metadata and body completeness are
checked before rendering. Source and projection reads remain separate.

View export eligibility comes from server-owned `presented_files` tags, not
incidental discovery or an unanswered `present_files` request. Live probes are
keyed by thread, view path, observed revision and exact paths, use four readers
with ten-second deadlines, and discard unused data. Card actions reuse the
existing lifetime-aware per-file save/share/Undo services with explicit selected
paths and an inline collection hint. There is no recursive copy or implicit
source/image export.

An effect-owned `ArtifactImageSession` belongs to account/thread/path/revision,
including StrictMode replay. It validates complete PNG/JPEG framing before a
browser decode, limits per-image and aggregate bytes/pixels and concurrent loads,
and owns cancellation and URL revocation. Image failures remain placeholders.
See [the contract and budgets](../contracts/artifact_view/README.md); no business
schema, calculation, interpreter or module registration belongs to this host.

Installed artifact contributions request their declared bounded projection. Its
revision is the canonical source SHA-256. Projection and source queries remain
separate; code view, editing, copying and downloading use canonical bytes.
Malformed or unsupported projections fall back to a bounded source read; never
reconcile an editor draft or save from projection bytes. Full-file selection is
scoped to thread/path and run completion refreshes the active query.

HartMesh is a Next.js 16 interface to the Gateway's LangGraph-compatible runtime,
with authenticated conversations, streaming, artifacts, Files/Shared and settings.
Upstream routes are selected deliberately; current scope is recorded in the
[isolation guide](../docs/FRONTEND_ISOLATION.md#product-scope).

**Stack**: Next.js 16, React 19, TypeScript 5.8, Tailwind CSS 4, pnpm 10.26.2. Requires Node.js 22+ and pnpm 10.26.2+.

### Core dependencies

- **LangGraph SDK** (`@langchain/langgraph-sdk` ^1.5.3) — Agent orchestration and streaming
- **LangChain Core** (`@langchain/core` ^1.1.15) — Fundamental AI building blocks
- **TanStack Query** (`@tanstack/react-query` ^5.90.17) — Server state management
- **UI**: Shadcn UI, MagicUI, React Bits, and Vercel AI SDK elements (generated from registries — see Code Style)

Route layouts use Next.js metadata directly; the product has no Nuxt image module.

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

Unit tests live under `tests/unit/` and mirror the `src/` layout (e.g., `tests/unit/core/api/stream-mode.test.ts` tests `src/core/api/stream-mode.ts`). Powered by Rstest; import source modules via the `@/` path alias.

Webpack is the default development bundler. Use `DEER_FLOW_DEV_BUNDLER=turbo` with `pnpm dev` to opt in to Turbopack when diagnosing a local Next.js bundler issue.

Rstest runs them as two projects (`rstest.config.ts`). `*.test.ts` / `*.test.tsx` run in a plain **node** environment — that is nearly the whole suite, and it is the default for anything that is pure logic. `*.dom.test.ts` / `*.dom.test.tsx` run in **happy-dom**, for tests that need a document: hooks driven through `renderHook` from `@testing-library/react`, and components. Keep the split — a DOM environment costs roughly 3x the runtime of the node suite, so tests that do not render should not opt into it. A hook whose behavior only exists under real React (effect ordering, cleanup on unmount, re-render on store change) belongs in a `.dom.test.*` file rather than a node test that mocks `react` itself.

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

One `$` is money, two are mathematics. `core/streamdown/plugins.ts` sets
`singleDollarTextMath: false`, because with it on, remark-math read the span
between any two currency amounts in a sentence as an equation: a released build
printed "August 2026: $201,487.04 in revenue across 502 jobs, an average of
$401.37 per job." as an italic serif formula with both dollar signs and every
space between them gone. One amount in a sentence was safe and two were not, an
odd count left a stray dollar sign behind, and neither a comma, a decimal point
nor a following space prevented it — which is why it survived a release. Every
figure in a business report is currency, so that is the ordinary sentence, not
an edge case.

Two dollar signs are now the only way to ask for mathematics, and they still
render both inline and display, because what separates the two is where the
delimiters sit rather than how many there are: a marker that begins a line
opens a display fence, one mid-line is inline. `normalizeLatexMathDelimiters`
in `preprocess.ts` therefore rewrites `\(...\)` as well as `\[...\]`, and
dropping that second half would silently turn every `\(...\)` a model emits
into plain text. Because an inline span is only inline when its markers stay
mid-line, a `\(...\)` that crosses a line break is joined onto one line; left
alone it opened a fence, ate its own formula as fence meta, and then paired
with the next genuine display block and destroyed that one too.

The accepted cost is that a model emitting a single-dollar formula prints it
literally. That is the price of keeping currency intact, and a bug report about
literal LaTeX must not be closed by turning the flag back on — doing so
restores the defect for every business report. Promoting a single-dollar span
to a double when it contains a backslash is not a safe recovery either: an
ordinary sentence whose text between two amounts holds a Windows path or an
escaped asterisk would be promoted straight back into an equation.

The regression for this has to assert the render, never `textContent`. The
released capture's extracted DOM text was correct — dollar signs and spaces
intact — while the pixels showed an equation, so a `textContent` assertion over
a KaTeX render is a control that cannot fail.
`tests/unit/core/streamdown-currency.test.ts` and the assistant-bubble case in
`tests/e2e/user-message-plain-text.spec.ts` assert the absence of a `katex`
class instead.

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
folders. Collection names are data and never translated. Both toasts name the folder, because the person did not choose it. All
three go through `useShareWithEveryone`, the same shape as saving: it publishes
each path, says so once, and carries **Undo** in that toast, because handing a
file to the whole company is one click and taking it back must be too. Whether
a file is already shared is the server's word, never the page's memory: the
publish route answers `200` with the entry that already holds the same bytes
instead of copying them (`publishToShared` returns `alreadyShared`), so a
second click from any tab or session gets an _already shared_ toast with a way
to the Shared tab, and **Undo** takes back only what that click put there. The
buttons are never disabled on that account; a page's memory of its own clicks
is exactly what a tab switch loses. The listing carries when each file was
published, who published it — resolved server-side to the person a colleague
would recognise, never the stored id — and `can_remove`, which the server
decides per caller (the publisher, or an admin) so the page never reasons about
roles: _Remove_ is offered exactly where it would succeed. Both tabs live under
one heading, _Files_, so the sidebar entry and the page agree whichever tab is
open.

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
Under `business`, someone who is not an administrator is not offered skills,
tools, subagents, integrations or the scheduled-task recipe chips, and Home
drops the product blurb. Channels and memory stay: the phone someone messages
it from and what the agent remembers about them are theirs, not the
deployment's. Hiding is presentation, not authorization — the routes are
unchanged and `authorization` has no permission covering these APIs;
`system_role` is what limits a person, and the API already checks it.
The product's name is the deployment's too (`ui.product_name`, HartMesh when
unset). Each route layout reads it from the public `GET /api/v1/auth/product`
(`core/product/server.ts`) and hands it to `I18nProvider`, which builds the
dictionary with it, so every string that names the product says the
configured name and `useProductName()` reads it back from that dictionary —
one copy, never a second one to keep in step. The same read titles the tab
(`generateMetadata`) and heads the sign-in and setup pages; a Gateway that
does not answer leaves HartMesh. No copy names the framework under the
product, and nothing links to it (a unit test reads the locale sources).
`branding` (`useBranding`) is the tenant bundle's company name, colours and
whether `/api/branding/logo` has a picture: a named company replaces the
product's name in the sidebar header, the tab title (`useDocumentTitle`) and
About, and turns the composer disclaimer and the Settings blurb neutral.
About is that name and the version, nothing else. "Unknown" (still loading,
or the fetch failed and will retry) is `isLoading`, and every one of those
surfaces shows neither name until it is false. The sign-in page shows the
product's name, never the company's: the brand is delivered after sign-in.
The same authenticated response carries optional provider support from
`provider.json`: `useBranding` keeps it distinct from company/product identity.
The workspace menu opens only a validated HTTPS link with no referrer or
added context. Files tabs derive from `useSearchParams`, including same-page
navigation and history changes.

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

When adding features:

1. Follow the established `src/` structure
2. Add TypeScript types and proper error handling
3. Write unit tests under `tests/unit/` (`pnpm test`) and E2E tests under `tests/e2e/` (`pnpm test:e2e`)
4. Run `pnpm check` before committing
5. Update this `AGENTS.md` when architecture, commands, or conventions change

Route asset budgets are enforced with `pnpm perf:check`. The command measures
`/login` from a normal production build, then builds in static-demo mode for the
fixture-backed workspace routes. It starts the production server on temporary local
ports, measures the unique JavaScript and CSS files referenced by representative
routes, writes the detailed result to `.next/performance-results.json`, and compares
totals with `performance-budgets.json`. Fix route ownership or split points when a
budget fails; do not raise a ceiling without documenting and reviewing the measured
regression.

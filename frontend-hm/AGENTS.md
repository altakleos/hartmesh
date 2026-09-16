# AGENTS.md

This file guides development of the Hartmesh frontend in `frontend-hm/`. The
sibling `CLAUDE.md` imports it via `@AGENTS.md`. This app owns its dependencies,
assets, API clients, and tests. Never import or link application material from
the pinned upstream `../frontend/`; port changes deliberately. The Gateway
HTTP/SSE/WebSocket protocols and shared `../contracts/` fixtures define the
backend boundary. See [the isolation guide](../docs/FRONTEND_ISOLATION.md).

## Project Overview

DeerFlow Frontend is a Next.js 16 web interface for an AI agent system. It communicates with a LangGraph-based backend to provide thread-based AI conversations with streaming responses, artifacts, and a skills/tools system.

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

Unit tests live under `tests/unit/` and mirror the `src/` layout (e.g., `tests/unit/core/api/stream-mode.test.ts` tests `src/core/api/stream-mode.ts`). Powered by Rstest; import source modules via the `@/` path alias.

Webpack is the default development bundler. Use `DEER_FLOW_DEV_BUNDLER=turbo` with `pnpm dev` to opt in to Turbopack when diagnosing a local Next.js bundler issue.

Rstest runs them as two projects (`rstest.config.ts`). `*.test.ts` / `*.test.tsx` run in a plain **node** environment — that is nearly the whole suite, and it is the default for anything that is pure logic. `*.dom.test.ts` / `*.dom.test.tsx` run in **happy-dom**, for tests that need a document: hooks driven through `renderHook` from `@testing-library/react`, and components. Keep the split — a DOM environment costs roughly 3x the runtime of the node suite, so tests that do not render should not opt into it. A hook whose behavior only exists under real React (effect ordering, cleanup on unmount, re-render on store change) belongs in a `.dom.test.*` file rather than a node test that mocks `react` itself.

E2E tests live under `tests/e2e/` and use Playwright with Chromium. They mock all backend APIs via `page.route()` network interception and test real page interactions (navigation, chat input, streaming responses). Config: `playwright.config.ts`.

`playwright.real-backend.config.ts` tests the real replay Gateway without
provider credentials. `playwright.gateway-auth.config.ts` uses the same
isolated runner with authentication enabled to verify login, SSR session
forwarding, CSRF enforcement, and logout through the app's same-origin proxy.

## Architecture

```
Frontend (Next.js) ──▶ LangGraph SDK ──▶ LangGraph Backend (lead_agent)
                                              ├── Sub-Agents
                                              └── Tools & Skills
```

The frontend is a stateful chat application. Users create **threads** (conversations), send messages, set thread-scoped `/goal` completion conditions, and receive streamed AI responses. The backend orchestrates agents that can produce **artifacts** (files/code), **todos**, and goal state updates.

### Source Layout (`src/`)

- **`app/`** — Next.js App Router. Routes include `/` (landing), `/showcase/[thread_id]` (allowlisted public read-only demos), `/workspace/chats/[thread_id]` (authenticated chat), `/workspace/agents/[agent_name]` and `/workspace/agents/new` (custom agents), `/artifacts/view` (chrome-free window that renders one markdown artifact with the panel's own renderer), `/blog/…`, the `(auth)/{login,setup,auth/callback}` flow, `/[lang]/docs/…`, and `/api/…` route handlers (e.g. `/api/memory`).
- **`components/`** — React components:
  - `ui/` — Shadcn UI primitives (auto-generated, ESLint-ignored)
  - `ai-elements/` — Vercel AI SDK elements (auto-generated, ESLint-ignored)
  - `workspace/` — Chat page components (messages, artifacts, settings)
  - `landing/` — Landing page sections
  - `docs/` — Docs / MDX rendering components
- **`core/`** — Business logic, the heart of the app. Domains include `threads/` (creation, streaming, state), `api/` (LangGraph client singleton), `agents/` (custom agents), `subagents/` (runtime worker catalog and administrator mutations), `auth/` (authentication), `artifacts/`, `artifact-delivery/` (run-scoped undelivered-file verdicts), `business-report/` (the `report.json` contract, its formatting and its companion paths), `channels/` (IM connections), `integrations/` (managed third-party integration status/install clients such as Lark CLI), `tool-plane/` (governance status/history client and legacy-mutation ceiling), `i18n/` (en-US, zh-CN), `settings/`, `memory/`, `skills/`, `messages/`, `mcp/`, `models/`, `input-polish/` (pre-send draft rewrite API), `voice-input/` (browser speech-recognition helpers), `suggestions/`, `tasks/`, `todos/`, `tools/`, `workspace-changes/` (run-scoped changed-file summaries and diff fetching), `config/`, `notification/`, `blog/`, plus rendering helpers (`rehype/`, `streamdown/`) and `utils/`.

A `*.report.json` artifact is previewed as a report card rather than as JSON.
`core/business-report/` parses
[the contract](../contracts/business_report/report.schema.json) and decides it:
a file the app cannot draw stays a JSON file, and the panel's existing
code/preview toggle switches between the two. `formatValue` there is a port of
the skill's own `format_value`, down to rounding the decimal spelling of a
number rather than the binary double, so a figure reads the same on the card as
in the PDF, Word and Excel renders; `tests/unit/core/business-report/` checks
that against cases generated from the Python. Download buttons appear for each
render the thread has presented; a render a later rebuild deleted is still
offered until it is made again. Each chart is addressed inside the report's own
directory, and the brand colour is spent on rules and borders only, because the
card renders on whichever ground the viewer's theme paints.

Three things decide whether a card appears at all: the `.report.json` suffix, a
body that parses as `version: 1`, and — for each picture — the contract's
`charts/<id>.png` shape. A body over `ARTIFACT_PREVIEW_MAX_BYTES` (1 MiB)
arrives truncated and stays JSON until _Load full file_. `report.json` embeds
every cleaned row of the period, which the card never draws, so that ceiling is
around four thousand rows in one period (measured: 121,540 bytes for 502
in-period rows). Dropping `rows` before the size check is the real fix and is
not done. The card's own chrome follows the UI locale while the report body
follows `meta.lang`, which the skill only ever writes as `en-US`.

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

One rule governs what happens while the answer is unknown: a control someone
might need stays offered, and copy the deployment authors waits. So the screens
stay (a Gateway reporting no `ui` block reads as `developer`, and an upgrade
never takes one away), while the blurb and the grid render only once the
deployment has answered. Starters default to the profile — `business` opens on
a small built-in set, `developer` on none — so an untouched deployment gains
nothing it did not ask for, and when a grid exists the legacy suggestion row
under the composer steps aside rather than sitting beside it. `InputBox` also
mounts on the public showcase route, so that gate lives inside
`SuggestionList`: asking for `/api/features` from `InputBox` would 401 and
bounce a showcase visitor to the login page.

Skill, MCP, and managed-integration settings are governance-aware. When
`GET /api/tool-plane/status` succeeds, these existing screens render the safe
active/history notice and disable their legacy direct mutation controls; they
must not optimistically call the old write routes. Immutable/exact-two status
uses the same read-only path and explanation. A status fetch failure is shown as
an unavailable-governance warning and remains fail-closed for mutation. The UI
does not render raw candidate archives, scanner payloads, secrets, or another
user's overlay. Only the explicit `tool_plane_unavailable` response identifies
the configured legacy opt-out and restores legacy controls; other status errors
stay fail-closed. See [the governed tool-plane contract](../docs/GOVERNED_TOOL_PLANE.md).

- **`hooks/`** — Shared React hooks
- **`lib/`** — Utilities (`cn()` from clsx + tailwind-merge)
- **`content/`** — MDX content (blog posts, docs) rendered by the app
- **`styles/`** — Global CSS with Tailwind v4 `@import` syntax and CSS variables for theming
- **`typings/`** — Ambient TypeScript declarations
- Root files: `env.js` (env validation), `mdx-components.ts` (MDX component map)

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

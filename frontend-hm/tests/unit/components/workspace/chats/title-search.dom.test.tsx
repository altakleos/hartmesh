import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { PropsWithChildren } from "react";

const context = rs.hoisted(() => ({ id: "alice", demo: false }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: context.id, system_role: "user" } }),
}));
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => context.demo,
}));
rs.mock("@/core/features", () => ({ useDocumentTitle: () => undefined }));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: enUS }) }));
rs.mock("@/core/threads/hooks", () => ({
  useInfiniteThreads: () => ({
    data: {
      pages: [
        [{ thread_id: "recent", values: { title: "Recent conversation" } }],
      ],
    },
    hasNextPage: false,
  }),
}));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: PropsWithChildren) => children,
  WorkspaceBody: ({ children }: PropsWithChildren) => children,
  WorkspaceHeader: () => null,
}));

import ChatsPage from "@/app/workspace/chats/page";
import { fetch } from "@/core/api/fetcher";
import { enUS } from "@/core/i18n/locales/en-US";

const request = rs.mocked(fetch);
let client: QueryClient;
const app = () => (
  <QueryClientProvider client={client}>
    <ChatsPage key={context.id} />
  </QueryClientProvider>
);
const type = (value: string) =>
  fireEvent.change(screen.getByRole("searchbox"), { target: { value } });
const page = (title: string, cursor: string | null = null) =>
  Response.json({
    items: [
      {
        type: "thread",
        thread_id: title,
        title,
        agent_name: "Research",
        channel_provider: "telegram",
        updated_at: "2026-10-01T00:00:00Z",
      },
    ],
    cursor,
  });

beforeEach(() => {
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  request.mockReset();
  context.id = "alice";
  context.demo = false;
});
afterEach(() => {
  cleanup();
  client.clear();
});

it("debounces title-only requests and uses the search cursor for older hits", async () => {
  request
    .mockResolvedValueOnce(page("Older title", "next"))
    .mockResolvedValueOnce(page("Another title"));
  render(app());
  expect(
    screen.getByRole("searchbox", { name: "Search conversation titles" }),
  ).toBeTruthy();
  type("Old");
  type("Older");
  expect(request).not.toHaveBeenCalled();
  const result = await screen.findByRole("link", { name: /Older title/ });
  expect(result.textContent).toContain("Telegram");
  expect(result.getAttribute("href")).toBe(
    "/workspace/agents/Research/chats/Older%20title",
  );
  expect(request).toHaveBeenCalledTimes(1);
  expect(request).toHaveBeenCalledWith(
    "/api/search",
    expect.objectContaining({
      method: "POST",
      body: JSON.stringify({ query: "Older", types: ["thread"], limit: 20 }),
    }),
  );
  fireEvent.click(screen.getByRole("button", { name: "More results" }));
  await screen.findByRole("link", { name: /Another title/ });
  expect(request).toHaveBeenLastCalledWith(
    "/api/search",
    expect.objectContaining({
      body: JSON.stringify({
        query: "Older",
        types: ["thread"],
        limit: 20,
        cursor: "next",
      }),
    }),
  );
  expect(screen.queryByRole("button", { name: "More results" })).toBeNull();
});

it("aborts a retired account's request and discards its late title", async () => {
  let finish!: (response: Response) => void;
  request.mockImplementationOnce(
    () =>
      new Promise<Response>((done) => {
        finish = done;
      }),
  );
  const view = render(app());
  type("Past");
  await waitFor(() => expect(request).toHaveBeenCalledTimes(1));
  const signal = request.mock.calls[0]?.[1]?.signal;
  context.id = "bob";
  view.rerender(app());
  expect(signal?.aborted).toBe(true);
  await act(async () => finish(page("Private old title")));
  expect(screen.queryByText("Private old title")).toBeNull();
  expect(screen.getByRole<HTMLInputElement>("searchbox").value).toBe("");
  expect(request).toHaveBeenCalledTimes(1);
});

it("distinguishes an unavailable search from an empty result and retries", async () => {
  request
    .mockResolvedValueOnce(new Response(null, { status: 503 }))
    .mockResolvedValueOnce(Response.json({ items: [], cursor: null }));
  render(app());
  type("Henderson");
  await screen.findByText("Conversation search is unavailable.");
  expect(screen.queryByText(/No conversation title contains/)).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await screen.findByText("No conversation title contains “Henderson”.");
  expect(screen.queryByText(/message search/i)).toBeNull();
});

it("keeps the static demo searchable without a Gateway", () => {
  context.demo = true;
  render(app());
  type("recent");
  expect(
    screen.getByRole("link", { name: "Recent conversation" }),
  ).toBeTruthy();
  type("absent");
  expect(
    screen.queryByRole("link", { name: "Recent conversation" }),
  ).toBeNull();
  expect(request).not.toHaveBeenCalled();
});

it("keeps loaded matches and retries the same cursor after a temporary next-page failure", async () => {
  request
    .mockResolvedValueOnce(page("Older title", "next"))
    .mockResolvedValueOnce(new Response(null, { status: 503 }))
    .mockResolvedValueOnce(page("Another title"));
  render(app());
  type("Older");
  await screen.findByRole("link", { name: /Older title/ });
  fireEvent.click(screen.getByRole("button", { name: "More results" }));
  await screen.findByRole("button", { name: "Try again" });
  expect(screen.getByRole("link", { name: /Older title/ })).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await screen.findByRole("link", { name: /Another title/ });
  expect(request.mock.calls[2]?.[1]?.body).toBe(
    request.mock.calls[1]?.[1]?.body,
  );
});

it("withdraws previously loaded matches if the next page refuses access", async () => {
  request
    .mockResolvedValueOnce(page("Older title", "next"))
    .mockResolvedValueOnce(new Response(null, { status: 403 }));
  render(app());
  type("Older");
  await screen.findByRole("link", { name: /Older title/ });
  fireEvent.click(screen.getByRole("button", { name: "More results" }));
  await screen.findByText("Conversation search is unavailable.");
  expect(screen.queryByRole("link", { name: /Older title/ })).toBeNull();
});

import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import type { PropsWithChildren } from "react";

const context = rs.hoisted(() => ({
  id: "alice",
  demo: false,
  messages: false,
  failedFeatures: false,
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: context.id, system_role: "user" } }),
}));
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => context.demo,
}));
rs.mock("@/core/features", () => ({
  useDocumentTitle: () => undefined,
  useFeatures: (
    select: (features: { message_search: boolean }) => boolean,
  ) => ({
    data: select({ message_search: context.messages }),
    isError: context.failedFeatures,
  }),
}));
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
    <ChatsPage />
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
  context.messages = false;
  context.failedFeatures = false;
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

function searchBody(init?: RequestInit) {
  if (typeof init?.body !== "string")
    throw new Error("Expected a search request");
  return JSON.parse(init.body) as { types: string[]; cursor?: string };
}

const hit = {
  type: "message",
  thread_id: "record",
  title: "Conversation at work",
  agent_name: "Research",
  message_id: "m/24",
  seq: 24,
  role: "user",
  updated_at: "2026-10-01T00:00:00Z",
  snippet: [{ text: "We discussed " }, { text: "<April>", matched: true }],
};
const messages = (
  items: unknown[],
  indexing = false,
  cursor: string | null = null,
) => Response.json({ items, cursor, indexing, messages_available: true });

it("shows both groups and safely marks snippets, including separate hits in the same conversation", async () => {
  context.messages = true;
  request.mockImplementation(async (_url, init) =>
    searchBody(init).types[0] === "message"
      ? messages([
          hit,
          {
            ...hit,
            message_id: "m2",
            seq: 25,
            role: "assistant",
            snippet: [{ text: "Other April" }],
          },
        ])
      : page("Older title"),
  );
  render(app());
  expect(
    screen.getByRole("searchbox", { name: "Search conversations" }),
  ).toBeTruthy();
  type("April");
  const titles = await screen.findByRole("region", { name: "Conversations" });
  expect(
    await within(titles).findByRole("link", { name: /Older title/ }),
  ).toBeTruthy();
  const section = screen.getByRole("region", { name: "Messages" });
  const row = await within(section).findByRole("button", {
    name: /We discussed <April>/,
  });
  expect(row.querySelector("mark")?.textContent).toBe("<April>");
  expect(row.querySelector("april")).toBeNull();
  expect(row.querySelector("time")?.dateTime).toBe(hit.updated_at);
  expect(
    await within(section).findByRole("button", { name: /Other April/ }),
  ).toBeTruthy();
});

it("keeps independent title results when the provider returns 503 and can retry messages", async () => {
  context.messages = true;
  let tries = 0;
  request.mockImplementation(async (_url, init) => {
    if (searchBody(init).types[0] === "thread") return page("Older title");
    return ++tries === 1
      ? new Response(null, { status: 503 })
      : messages([hit]);
  });
  render(app());
  type("April");
  await screen.findByText("Message results are not available right now.");
  expect(screen.getByRole("link", { name: /Older title/ })).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await screen.findByRole("button", { name: /We discussed/ });
  expect(
    request.mock.calls.filter(
      ([, init]) => searchBody(init).types[0] === "thread",
    ),
  ).toHaveLength(1);
});

it("removes a disabled worker's message view and uses the title label", async () => {
  context.messages = true;
  request.mockImplementation(async (_url, init) =>
    searchBody(init).types[0] === "thread"
      ? page("Title")
      : Response.json({
          items: [],
          cursor: null,
          indexing: false,
          messages_available: false,
        }),
  );
  render(app());
  type("April");
  await screen.findByRole("searchbox", { name: "Search conversation titles" });
  expect(screen.queryByRole("region", { name: "Messages" })).toBeNull();
  expect(await screen.findByRole("link", { name: /Title/ })).toBeTruthy();
});

it("stops polling a partial index at thirty seconds and retires all timers when the query changes", async () => {
  rs.useFakeTimers();
  context.messages = true;
  const ticks: number[] = [];
  request.mockImplementation(async (_url, init) => {
    const body = searchBody(init);
    if (body.types[0] === "thread") return page("Ready title");
    ticks.push(Date.now());
    return messages([], true);
  });
  const tick = async (ms: number) => {
    await act(async () => rs.advanceTimersByTimeAsync(ms));
    await act(async () => rs.advanceTimersByTimeAsync(1));
  };
  try {
    render(app());
    type("April");
    await tick(260);
    expect(
      screen.getByText(
        "Reading your earlier conversations. Results may be incomplete.",
      ),
    ).toBeTruthy();
    for (let i = 0; i < 16; i++) await tick(2000);
    expect(
      screen.getByText("Still reading. Search again in a minute."),
    ).toBeTruthy();
    expect(ticks.length).toBeGreaterThanOrEqual(14);
    expect(ticks.length).toBeLessThanOrEqual(16);
    const settled = ticks.length;
    await tick(30_000);
    expect(ticks).toHaveLength(settled);
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    await tick(10);
    expect(ticks.length).toBe(settled + 1);
    type("");
    await tick(5000);
    expect(ticks.length).toBe(settled + 1);
  } finally {
    cleanup();
    rs.useRealTimers();
  }
});

it("cancels a selected window on account change and ignores its late response", async () => {
  context.messages = true;
  let complete!: (value: Response) => void;
  let signal: AbortSignal | undefined | null;
  request.mockImplementation(async (url, init) => {
    if (typeof url === "string" && url.includes("/messages/around?")) {
      signal = init?.signal;
      return new Promise<Response>((resolve) => {
        complete = resolve;
      });
    }
    return searchBody(init).types[0] === "thread"
      ? page("Title")
      : messages([hit]);
  });
  const view = render(app());
  type("April");
  const row = await screen.findByRole("button", { name: /We discussed/ });
  expect(
    request.mock.calls.every(
      ([url]) =>
        !(typeof url === "string" && url.includes("/messages/around?")),
    ),
  ).toBe(true);
  fireEvent.click(row);
  await waitFor(() => expect(signal).toBeTruthy());
  context.id = "bob";
  view.rerender(app());
  expect(signal?.aborted).toBe(true);
  await act(async () =>
    complete(
      Response.json({
        target_seq: hit.seq,
        messages: [
          {
            ...hit,
            created_at: hit.updated_at,
            text: "Former caller's private message",
          },
        ],
      }),
    ),
  );
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(screen.queryByText("Former caller's private message")).toBeNull();
  expect(screen.getByRole<HTMLInputElement>("searchbox").value).toBe("");
});

it("refuses a withdrawn or mismatched around window and withholds its navigation", async () => {
  context.messages = true;
  request.mockImplementation(async (url, init) => {
    if (typeof url === "string" && url.includes("/messages/around?"))
      return Response.json({
        target_seq: hit.seq,
        messages: [
          {
            ...hit,
            thread_id: "different",
            created_at: hit.updated_at,
            text: "Unrelated secret",
          },
        ],
      });
    return searchBody(init).types[0] === "thread"
      ? page("Title")
      : messages([hit]);
  });
  render(app());
  type("April");
  fireEvent.click(await screen.findByRole("button", { name: /We discussed/ }));
  const dialog = await screen.findByRole("dialog", { name: "Message context" });
  await within(dialog).findByText("This message is no longer available.");
  expect(within(dialog).queryByRole("link")).toBeNull();
  expect(within(dialog).queryByText("Unrelated secret")).toBeNull();
});

it("paginates messages independently and removes earlier words on a denied next page", async () => {
  context.messages = true;
  request.mockImplementation(async (_url, init) => {
    const body = searchBody(init);
    if (body.types[0] === "thread") return page("Title");
    return body.cursor
      ? new Response(null, { status: 403 })
      : messages([hit], false, "messages-only");
  });
  render(app());
  type("April");
  const section = await screen.findByRole("region", { name: "Messages" });
  fireEvent.click(
    await within(section).findByRole("button", { name: "More results" }),
  );
  await screen.findByRole("searchbox", { name: "Search conversation titles" });
  expect(screen.queryByRole("button", { name: /We discussed/ })).toBeNull();
  expect(screen.getByRole("link", { name: /Title/ })).toBeTruthy();
  expect(request.mock.calls.at(-1)?.[1]?.body).toBe(
    JSON.stringify({
      query: "April",
      types: ["message"],
      limit: 20,
      cursor: "messages-only",
    }),
  );
});

it("hides cached discovery and message words when the active feature read fails", async () => {
  context.messages = true;
  request.mockImplementation(async (_url, init) =>
    searchBody(init).types[0] === "thread" ? page("Title") : messages([hit]),
  );
  const view = render(app());
  type("April");
  await screen.findByRole("button", { name: /We discussed/ });
  context.failedFeatures = true;
  view.rerender(app());
  expect(
    screen.getByRole("searchbox", { name: "Search conversation titles" }),
  ).toBeTruthy();
  expect(screen.queryByRole("region", { name: "Messages" })).toBeNull();
  expect(screen.getByRole("link", { name: /Title/ })).toBeTruthy();
});

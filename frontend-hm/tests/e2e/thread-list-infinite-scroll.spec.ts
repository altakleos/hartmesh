import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

// Issue #3482: the sidebar's "Recent chats" and the /workspace/chats list
// page used to stop at the first 50 threads with no way to load more.
// `useInfiniteThreads()` + a sentinel near the bottom of each list now
// pages through the backend.

const TOTAL_THREADS = 120;
const PAGE_SIZE = 50;

const THREADS = Array.from({ length: TOTAL_THREADS }, (_, i) => {
  // Pad index so titles sort deterministically as strings. Keep updated_at
  // monotonically descending to match the backend's updated_at-desc search
  // order, so paging boundaries are stable across runs.
  const index = String(i + 1).padStart(3, "0");
  return {
    thread_id: `00000000-0000-0000-0000-0000000${index.padStart(5, "0")}`,
    title: `Conversation ${index}`,
    updated_at: new Date(
      Date.UTC(2025, 5, 30, 12, 0, 0) - i * 60_000,
    ).toISOString(),
  };
});

const FIRST_PAGE_LAST = `Conversation ${String(PAGE_SIZE).padStart(3, "0")}`;
const SECOND_PAGE_FIRST = `Conversation ${String(PAGE_SIZE + 1).padStart(3, "0")}`;
// From 60 rows the list is virtual (thread-list-virtualizer.tsx): only rows
// near the scroll position exist, and the 51st need not be one of them. Any
// row past the first page proves the next page arrived.
const PAST_FIRST_PAGE = /^Conversation (?!0[0-4]\d|050)\d{3}$/;

test.describe("Thread list infinite scroll (issue #3482)", () => {
  test("chats list page loads more threads when scrolling to the bottom", async ({
    page,
  }) => {
    mockLangGraphAPI(page, { threads: THREADS });

    await page.goto("/workspace/chats");

    const main = page.locator("main");

    // First page renders.
    await expect(main.getByText(FIRST_PAGE_LAST)).toBeVisible({
      timeout: 15_000,
    });
    // Items past the first page have not been fetched yet.
    await expect(main.getByText(SECOND_PAGE_FIRST)).toHaveCount(0);

    // Scrolling the sentinel into view triggers the next page.
    const sentinel = page.getByTestId("chats-page-sentinel");
    await sentinel.scrollIntoViewIfNeeded();

    await expect(main.getByText(PAST_FIRST_PAGE).first()).toBeVisible({
      timeout: 15_000,
    });
  });

  test("sidebar recent chats loads more threads when scrolling to the bottom", async ({
    page,
  }) => {
    mockLangGraphAPI(page, { threads: THREADS });

    await page.goto("/workspace/chats/new");

    // The 50th thread (end of first page) appears in the sidebar.
    await expect(page.getByText(FIRST_PAGE_LAST).first()).toBeVisible({
      timeout: 15_000,
    });
    // The 51st has not been fetched yet.
    await expect(page.getByText(SECOND_PAGE_FIRST)).toHaveCount(0);

    // Scroll the sidebar sentinel into view to trigger the next page.
    const sentinel = page.getByTestId("recent-chat-list-sentinel");
    await sentinel.scrollIntoViewIfNeeded();

    await expect(page.getByText(PAST_FIRST_PAGE).first()).toBeVisible({
      timeout: 15_000,
    });
  });

  for (const availability of [true, false, "missing", "unavailable"] as const) {
    test(`search describes the title results when message search availability is ${availability}`, async ({
      page,
    }) => {
      mockLangGraphAPI(page, { threads: THREADS });
      await page.route("**/api/features", (route) =>
        route.fulfill({
          status: availability === "unavailable" ? 503 : 200,
          json:
            typeof availability === "boolean"
              ? { message_search: availability }
              : {},
        }),
      );
      const discovered = page.waitForResponse(
        (response) => new URL(response.url()).pathname === "/api/features",
      );
      await page.goto("/workspace/chats");
      await discovered;
      await expect(
        page.getByRole("searchbox", { name: "Search conversation titles" }),
      ).toBeVisible();
      await expect(
        page.locator("main").getByText(/message search/i),
      ).toHaveCount(0);
    });
  }

  test("title search finds older conversations and pages through matches without loading the chat list", async ({
    page,
  }) => {
    mockLangGraphAPI(page, { threads: THREADS });
    let listRequests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/api/langgraph/threads/search"))
        listRequests++;
    });
    const bodies: unknown[] = [];
    await page.route("**/api/search", async (route) => {
      const body = route.request().postDataJSON() as { cursor?: string };
      bodies.push(body);
      const hit = THREADS[body.cursor ? 110 : 105]!;
      await route.fulfill({
        json: {
          items: [
            {
              type: "thread",
              ...hit,
              agent_name: null,
              channel_provider: body.cursor ? null : "telegram",
            },
          ],
          cursor: body.cursor ? null : "more-matches",
          messages_available: false,
          indexing: false,
        },
      });
    });
    await page.goto("/workspace/chats");
    const main = page.locator("main");
    await expect(main.getByText(FIRST_PAGE_LAST)).toBeVisible();
    const before = listRequests;
    await page
      .getByRole("searchbox", { name: "Search conversation titles" })
      .fill("Conversation");
    await expect(page.getByTestId("chats-page-sentinel")).toHaveCount(0);
    await expect(
      main.getByRole("link", { name: /Conversation 106/ }),
    ).toHaveAttribute("href", `/workspace/chats/${THREADS[105]!.thread_id}`);
    await expect(
      main.getByRole("link", { name: /Conversation 106/ }),
    ).toContainText("Telegram");
    await test.info().attach("Search conversation titles", {
      contentType: "image/png",
      body: await page.screenshot(),
    });
    await main.getByRole("button", { name: "More results" }).click();
    await expect(
      main.getByRole("link", { name: /Conversation 111/ }),
    ).toBeVisible();
    expect(bodies).toEqual([
      { query: "Conversation", types: ["thread"], limit: 20 },
      {
        query: "Conversation",
        types: ["thread"],
        limit: 20,
        cursor: "more-matches",
      },
    ]);
    expect(listRequests).toBe(before);
    await page.getByRole("searchbox").fill("");
    await expect(main.getByText(FIRST_PAGE_LAST)).toBeVisible();
  });
});

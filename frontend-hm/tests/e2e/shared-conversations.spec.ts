import { expect, test, type Page } from "@playwright/test";

import { refreshAccount } from "../e2e-real-backend/utils/compose-agents";

import {
  mockLangGraphAPI,
  MOCK_THREAD_ID,
  MOCK_THREAD_ID_2,
} from "./utils/mock-api";

const publication = "7".repeat(32);
const member = {
  path: "report.pdf",
  shared_path: "Conversations/Release/files/report.pdf",
  publication_id: "9".repeat(32),
  size: 17,
  sha256: "e".repeat(64),
  available: true,
  also_named: ["other/report.pdf"],
};
const summary = {
  publication_id: publication,
  title: "Inventory handover",
  publisher: "Sam",
  published_at: "2026-10-01T04:00:00Z",
};
const snapshot = {
  ...summary,
  with: { kind: "employee", id: "employee", name: "Maya" },
  answerer: { kind: "assistant" },
  messages: [
    { type: "human", content: "Please check the inventory." },
    { type: "ai", content: "The count is complete." },
  ],
  files: [
    member,
    {
      ...member,
      path: "missing.pdf",
      available: false,
      shared_path: null,
      publication_id: null,
      also_named: [],
    },
  ],
  transcript: {
    ...member,
    path: "transcript.md",
    shared_path: "Conversations/Release/transcript.md",
  },
  can_remove: true,
};
const collected = (suffix: string, marked: boolean) => ({
  name: suffix,
  path: suffix,
  kind: "file",
  modified: 1712500000,
  size: 10,
  url: "/api/shared/" + suffix,
  can_remove: false,
  published_by: "Sam",
  published_at: summary.published_at,
  ...(marked ? { snapshot_publication_id: publication } : {}),
});
async function setup(page: Page, supports = true) {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: summary.title,
        messages: [{ id: "src-one", type: "human", content: "Hello" }],
      },
      {
        thread_id: MOCK_THREAD_ID_2,
        title: summary.title,
        messages: [
          {
            type: "human",
            id: "copied",
            content: "Copied beginning",
            additional_kwargs: {
              shared_history: { publication_id: publication },
            },
          },
          {
            type: "ai",
            id: "end",
            content: "Copied ending",
            additional_kwargs: {
              shared_history: { publication_id: publication },
            },
          },
        ],
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        ...(supports
          ? {
              shared_conversations: {
                version: 1,
                operations: ["publish", "read", "continue"],
              },
            }
          : {}),
        agents_api: { enabled: true },
        agent_instances: { enabled: false },
        file_browser: {
          versions: [1],
          storage_mode: "ordinary",
          operations: {
            personal: ["select", "folder"],
            shared: ["select", "folder"],
            agent: [],
          },
        },
        storage_spaces: {
          enabled: false,
          state: "off",
          reason: null,
          own: "ready",
          user_spaces: false,
        },
      },
    }),
  );
  const rows = [collected("report.pdf", true), collected("ordinary.md", false)];
  await page.route("**/api/shared", (route) =>
    route.fulfill({
      json: { files: rows, truncated: false, count: rows.length },
    }),
  );
  await page.route("**/api/file-browser/v1/shared/folder?**", (route) =>
    route.fulfill({ json: { files: rows, truncated: false } }),
  );
  await page.route("**/api/shared-conversations", (route) =>
    route.fulfill({
      json: {
        conversations: [summary],
        truncated: true,
        next_before: publication,
      },
    }),
  );
  await page.route("**/api/shared-conversations?before=*", (route) =>
    route.fulfill({
      json: {
        conversations: [
          { ...summary, publication_id: "older", title: "Previous snapshot" },
        ],
        truncated: false,
        next_before: null,
      },
    }),
  );
  await page.route("**/api/shared-conversations/" + publication, (route) =>
    route.fulfill({ json: snapshot }),
  );
  await page.route(
    "**/api/langgraph/threads/" + MOCK_THREAD_ID,
    async (route) => {
      // This test supplies the actual Gateway owner and instance fields used by the header.
      await route.fulfill({
        json: {
          thread_id: MOCK_THREAD_ID,
          agent_instance: null,
          owner_user_id: "default",
          metadata: {},
          values: { title: summary.title },
          status: "idle",
          created_at: summary.published_at,
          updated_at: summary.published_at,
        },
      });
    },
  );
}
for (const view of ["browser", "classic"]) {
  test(`Shared conversations and unmarked files in ${view}`, async ({
    page,
  }) => {
    await setup(page);
    await page.goto("/workspace/files?tab=shared&view=" + view);
    const group = page.getByRole("region", { name: "Conversations" });
    await expect(
      group.getByRole("link", { name: summary.title }),
    ).toBeVisible();
    await expect(
      page.getByText("ordinary.md", { exact: true }).first(),
    ).toBeVisible();
    await expect(page.getByText("report.pdf", { exact: true })).toHaveCount(0);
    await group
      .getByRole("button", { name: "Load more conversations" })
      .click();
    await expect(
      group.getByRole("link", { name: "Previous snapshot" }),
    ).toBeVisible();
    const search = page
      .getByRole("searchbox")
      .or(page.getByRole("textbox", { name: /search/i }));
    await search.fill("inventory handover");
    await expect(
      group.getByRole("link", { name: summary.title }),
    ).toBeVisible();
    await expect(
      group.getByRole("link", { name: "Previous snapshot" }),
    ).toHaveCount(0);
    await group.getByRole("link", { name: summary.title }).click();
    await expect(
      page.getByRole("heading", { name: summary.title }),
    ).toBeVisible();
    await expect(page.getByText("The count is complete.")).toBeVisible();
    await expect(
      page.getByText("No longer shared", { exact: true }),
    ).toBeVisible();
    await expect(page.getByText(/will answer, not Maya/)).toBeVisible();
    await expect(
      page
        .getByRole("listitem")
        .filter({ has: page.getByText("report.pdf", { exact: true }) })
        .first()
        .getByRole("link", { name: "Open" }),
    ).toHaveAttribute(
      "href",
      /api\/shared\/Conversations\/Release\/files\/report.pdf/,
    );
  });
}
test("earlier Gateway leaves raw files available", async ({ page }) => {
  await setup(page, false);
  let snapshotReads = 0;
  page.on("request", (request) => {
    if (request.url().includes("/api/shared-conversations")) snapshotReads++;
  });
  await page.goto("/workspace/files?tab=shared&view=classic");
  await page.getByRole("searchbox").fill("report.pdf");
  await expect(page.getByText("report.pdf", { exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "Conversations" })).toHaveCount(
    0,
  );
  expect(snapshotReads).toBe(0);
});
test("retired manifest discloses only its byline", async ({ page }) => {
  await setup(page);
  await page.route("**/api/shared-conversations/" + publication, (r) =>
    r.fulfill({
      status: 410,
      json: {
        detail: {
          code: "snapshot_removed",
          publisher: "Sam",
          published_at: summary.published_at,
        },
      },
    }),
  );
  await page.goto("/workspace/files/conversations/" + publication);
  await expect(page.getByText(/Sam.*since removed from Shared/)).toBeVisible();
  await expect(
    page.getByRole("main").getByText(summary.title, { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: /Continue with/ })).toHaveCount(
    0,
  );
});

for (const hasUndo of [true, false]) {
  test(`owner reviews frozen copy and respects Undo receipt ${hasUndo}`, async ({
    page,
  }) => {
    await setup(page);
    const writes: unknown[] = [];
    await page.route("**/api/shared-conversations/draft?**", (route) =>
      route.fulfill({
        json: {
          title: summary.title,
          snapshot: "a".repeat(64),
          message_count: 2,
          files: [member],
          already_shared: false,
          publication_id: null,
        },
      }),
    );
    await page.route("**/api/shared-conversations", (route) => {
      writes.push(route.request().postDataJSON());
      return route.fulfill({
        status: 201,
        json: {
          publication_id: publication,
          already_shared: false,
          created_publication_ids: ["new-file", publication],
          undo_receipt: hasUndo ? "b".repeat(64) : null,
        },
      });
    });
    await page.route(
      "**/api/shared-conversations/" + publication + "/undo",
      (route) => {
        writes.push(route.request().postDataJSON());
        return route.fulfill({
          json: {
            removed_publication_ids: ["new-file", publication],
            retained: [],
          },
        });
      },
    );
    await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
    await page
      .getByRole("button", { name: "Share conversation with everyone" })
      .click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText("2 messages");
    await expect(dialog).toContainText("17 byte");
    await expect(dialog).toContainText("Also named other/report.pdf");
    await expect(dialog).toContainText("their assistants");
    await dialog.getByRole("button", { name: "Share", exact: true }).click();
    await expect(page.getByText(/Shared with everyone at/)).toBeVisible();
    expect(writes).toEqual([
      { thread_id: MOCK_THREAD_ID, snapshot: "a".repeat(64) },
    ]);
    if (hasUndo) {
      await page.getByRole("button", { name: "Undo", exact: true }).click();
      await expect
        .poll(() => writes.at(-1))
        .toEqual({
          publication_ids: ["new-file", publication],
          undo_receipt: "b".repeat(64),
        });
    } else {
      await expect(
        page.getByRole("button", { name: "Undo", exact: true }),
      ).toHaveCount(0);
    }
  });
}
test("Continue retries the same operation, preserves returned leaf and shows a single verified history boundary", async ({
  page,
}) => {
  await setup(page);
  const requests: { request_id: string; answerer: { kind: string } }[] = [];
  await page.route(
    "**/api/shared-conversations/" + publication + "/continue",
    (route) => {
      requests.push(route.request().postDataJSON());
      if (requests.length === 1)
        return route.fulfill({ status: 503, json: { detail: "busy" } });
      return route.fulfill({
        json: {
          thread_id: MOCK_THREAD_ID_2,
          answerer: { kind: "assistant" },
          files: [
            {
              path: "report.pdf",
              status: "copied",
              attachment: {
                filename: "report (2).pdf",
                size: 17,
                virtual_path: "/mnt/user-data/uploads/report (2).pdf",
                sha256: "e".repeat(64),
              },
            },
            {
              path: "missing.pdf",
              status: "not_copied",
              reason: "no_longer_shared",
            },
          ],
        },
      });
    },
  );
  await page.route(
    "**/api/threads/" + MOCK_THREAD_ID_2 + "/shared-history",
    (r) =>
      r.fulfill({
        json: { publication_id: publication, message_ids: ["copied", "end"] },
      }),
  );
  await page.goto("/workspace/files/conversations/" + publication);
  const button = page.getByRole("button", { name: /Continue with/ });
  await button.click();
  await expect(
    page.getByRole("alert").filter({ hasText: "busy" }),
  ).toBeVisible();
  await button.click();
  await expect(page).toHaveURL(new RegExp(MOCK_THREAD_ID_2 + "$"));
  expect(requests).toHaveLength(2);
  expect(requests[0]).toEqual({
    request_id: expect.stringMatching(/^[a-f0-9-]{36}$/),
    answerer: { kind: "assistant" },
  });
  expect(requests[1]).toEqual(requests[0]);
  await expect(page.getByText("report (2).pdf", { exact: true })).toBeVisible();
  await expect(page.getByText(/missing.pdf.*no longer shared/i)).toBeVisible();
  await expect(
    page.getByText(/Copied from.*Inventory handover.*Maya.*Sam/),
  ).toHaveCount(1);
  await page.getByRole("button", { name: "Retry copying files" }).click();
  await expect.poll(() => requests.length).toBe(3);
  expect(requests[2]).toEqual(requests[0]);
  const firstRun = page.waitForRequest(
    (r) =>
      r.method() === "POST" &&
      new URL(r.url()).pathname.endsWith("/runs/stream"),
  );
  const composer = page.getByPlaceholder(/how can i assist you/i);
  await composer.fill("Please check the saved report.");
  await composer.press("Enter");
  const body = (await firstRun).postDataJSON();
  const sent = body.input.messages.at(-1);
  expect(sent.additional_kwargs.files).toEqual([
    expect.objectContaining({
      filename: "report (2).pdf",
      path: "/mnt/user-data/uploads/report (2).pdf",
      size: 17,
    }),
  ]);
});

test("a different chat owner and a read-only member cannot request publication or Continue", async ({
  page,
}) => {
  await setup(page);
  await page.route("**/api/v1/auth/me", (route) =>
    route.fulfill({
      json: {
        id: "reader",
        email: "reader@example.com",
        system_role: "user",
        needs_setup: false,
        permissions: ["threads:read", "runs:read"],
      },
    }),
  );
  await page.route("**/api/shared-conversations/" + publication, (route) =>
    route.fulfill({ json: { ...snapshot, can_remove: false } }),
  );
  const writes: string[] = [];
  page.on("request", (request) => {
    if (
      request.url().includes("/api/shared-conversations") &&
      request.method() !== "GET"
    )
      writes.push(request.url());
  });
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  await refreshAccount(page);
  await expect(page.getByText("Hello", { exact: true }).last()).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Share conversation with everyone" }),
  ).toHaveCount(0);
  await page.goto("/workspace/files/conversations/" + publication);
  await refreshAccount(page);
  await expect(
    page.getByRole("heading", { name: summary.title }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: /Continue with|Remove/ }),
  ).toHaveCount(0);
  expect(writes).toEqual([]);
});

test("Remove names retained members and refreshes the Shared group", async ({
  page,
}) => {
  await setup(page);
  let removed = false;
  await page.route("**/api/shared-conversations/" + publication, (route) => {
    if (route.request().method() === "DELETE") {
      removed = true;
      return route.fulfill({
        json: {
          removed_publication_ids: [publication],
          retained: [{ path: "sibling.txt", publisher: "Jess" }],
        },
      });
    }
    return route.fulfill({ json: snapshot });
  });
  await page.route("**/api/shared-conversations", (route) =>
    route.fulfill({
      json: {
        conversations: removed ? [] : [summary],
        truncated: false,
        next_before: null,
      },
    }),
  );
  await page.goto("/workspace/files/conversations/" + publication);
  await page.getByRole("button", { name: "Remove", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("keep their conversations");
  await dialog.getByRole("button", { name: "Remove", exact: true }).click();
  await expect(page).toHaveURL(/tab=shared/);
  await expect(page.getByText(/Files kept.*sibling.txt.*Jess/)).toBeVisible();
  await expect(page.getByRole("region", { name: "Conversations" })).toHaveCount(
    0,
  );
});

test("a thread owner without history-read permission cannot preview or publish a snapshot", async ({
  page,
}) => {
  await setup(page);
  await page.route("**/api/v1/auth/me", (r) =>
    r.fulfill({
      json: {
        id: "default",
        email: "default@test.local",
        system_role: "user",
        permissions: ["threads:read", "threads:write"],
      },
    }),
  );
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  await refreshAccount(page);
  await expect(page.getByText("Hello", { exact: true }).last()).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Share conversation with everyone" }),
  ).toHaveCount(0);
});

import { expect, test, type Page } from "@playwright/test";

import { refreshAccount } from "../e2e-real-backend/utils/compose-agents";
import { workInstance } from "../fixtures/agent-work";

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
    publication_id: "8".repeat(32),
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
    for (const [name, id] of [
      ["Transcript", snapshot.transcript.publication_id],
      ["report.pdf", member.publication_id],
    ] as const) {
      const row = page.getByRole("listitem").filter({
        has: page.getByText(name, { exact: true }),
      });
      const href = `/api/shared-conversations/${publication}/members/${id}`;
      await expect(row.getByRole("link", { name: "Open" })).toHaveAttribute(
        "href",
        href,
      );
      await expect(row.getByRole("link", { name: "Download" })).toHaveAttribute(
        "href",
        href + "?download=true",
      );
    }
  });
}
test("an open reader follows its selected member links after replacement", async ({
  page,
  context,
}) => {
  await setup(page);
  await page.goto("/workspace/files/conversations/" + publication);
  const row = page.getByRole("listitem").filter({
    has: page.getByText("report.pdf", { exact: true }),
  });
  await expect(row.getByRole("link", { name: "Open" })).toBeVisible();
  const href = `/api/shared-conversations/${publication}/members/${member.publication_id}`;
  await context.route(
    (url) => url.pathname === href,
    (route) =>
      route.fulfill({
        status: 410,
        json: {
          detail:
            "This file is no longer available in this shared conversation.",
        },
      }),
  );
  for (const name of ["Open", "Download"]) {
    const pending = context.waitForEvent("page");
    await row.getByRole("link", { name }).click();
    const opened = await pending;
    await expect(opened.locator("body")).toContainText("no longer available");
    const url = new URL(opened.url());
    expect(url.pathname).toBe(href);
    expect(url.searchParams.get("download")).toBe(
      name === "Open" ? null : "true",
    );
    await opened.close();
  }
});

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

async function agentSource(
  page: Page,
  permissions: number,
  agentPublishing = true,
) {
  const agent = { ...workInstance(), permissions };
  mockLangGraphAPI(page, {
    features: { agentInstancesEnabled: true },
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: summary.title,
        messages: [{ id: "h", type: "human", content: "Source words" }],
        agent_instance: {
          id: agent.id,
          name: agent.name,
          custody: agent.custody,
          status: agent.status,
          requester_id: "default",
        },
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        agent_instances: { enabled: true, execution: "ready" },
        shared_conversations: {
          version: 1,
          operations: [
            "read",
            "continue",
            "publish",
            ...(agentPublishing ? ["agent_publish"] : []),
          ],
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === "/api/agent-instances/" + agent.id,
    (route) => route.fulfill({ json: agent }),
  );
  return agent;
}
for (const state of [
  { permissions: 4, supports: true, description: /Can see all work/ },
  {
    permissions: 3,
    supports: true,
    description: /Only someone who can manage/,
  },
  { permissions: 7, supports: false, description: null },
]) {
  test(`agent share discovery and read-only access ${state.permissions}/${state.supports}`, async ({
    page,
  }) => {
    await agentSource(page, state.permissions, state.supports);
    const requests: string[] = [];
    page.on("request", (r) => {
      if (
        r.url().includes("/api/shared-conversations/draft") ||
        (r.method() === "POST" && r.url().endsWith("/api/shared-conversations"))
      )
        requests.push(r.url());
    });
    await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
    await expect(
      page.getByText("Source words", { exact: true }).last(),
    ).toBeVisible();
    const share = page.getByRole("button", {
      name: "Share conversation with everyone",
    });
    if (state.description) {
      await expect(share).toBeDisabled();
      await share.focus();
      await expect(share).toHaveAccessibleDescription(state.description);
      await page.keyboard.press("Enter");
      await expect(page.getByRole("dialog")).toHaveCount(0);
    } else await expect(share).toHaveCount(0);
    expect(requests).toEqual([]);
  });
}
test("agent manager reviews current audiences and explicitly accepts each changed snapshot", async ({
  page,
}) => {
  const agent = await agentSource(page, 7);
  const writes: unknown[] = [];
  const digests = ["a".repeat(64), "b".repeat(64)];
  await page.route("**/api/shared-conversations/draft?**", (route) =>
    route.fulfill({
      json: {
        title: summary.title,
        snapshot: digests[writes.length ? 1 : 0],
        message_count: 2,
        files: [member],
        already_shared: false,
        publication_id: null,
        with: { kind: "employee", id: agent.id, name: agent.name },
        audience: {
          starter: "Jo",
          starter_can_read: writes.length === 0,
          authors: ["Jo", "Kim"],
          inspect_count: 3,
          chat_count: 7,
        },
      },
    }),
  );
  await page.route("**/api/shared-conversations", (route) => {
    writes.push(route.request().postDataJSON());
    return writes.length === 1
      ? route.fulfill({
          status: 409,
          json: { detail: { code: "snapshot-changed" } },
        })
      : route.fulfill({
          status: 201,
          json: {
            publication_id: publication,
            created_publication_ids: [publication],
            already_shared: false,
            undo_receipt: null,
          },
        });
  });
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  await page
    .getByRole("button", { name: "Share conversation with everyone" })
    .click();
  const d = page.getByRole("dialog");
  await expect(d).toContainText(/conversation starter \(Jo\) can read/i);
  await expect(d).toContainText("Messages written by Jo, Kim");
  await expect(d).toContainText("work (3)");
  await expect(d).toContainText("Researcher (7)");
  await expect(d).toContainText("other/report.pdf");
  const submit = d.getByRole("button", { name: "Share", exact: true }),
    ack = d.getByRole("checkbox");
  for (const digest of digests) {
    if (digest === digests[1]) {
      await expect(d).not.toContainText(/starter \(Jo\) can read/i);
      await expect(d).toContainText("Messages written by Jo, Kim");
      await expect(d).toContainText("can ask about the files’ contents");
    }
    await expect(ack).not.toBeChecked();
    await expect(submit).toBeDisabled();
    await ack.check();
    await submit.click();
    await expect
      .poll(() => writes.at(-1))
      .toEqual({
        thread_id: MOCK_THREAD_ID,
        snapshot: digest,
        acknowledge: { audience: "company", snapshot: digest },
      });
  }
  await expect(d).toHaveCount(0);
  expect(writes).toHaveLength(2);
});

test("a changed answerer requires another click and a new request for the offered assistant", async ({
  page,
}) => {
  await setup(page);
  const agent = { kind: "agent", id: "employee", name: "Maya" };
  const fallback = { kind: "assistant", reason: "paused" };
  const writes: { request_id: string; answerer: unknown }[] = [];
  await page.route("**/api/shared-conversations/" + publication, (r) =>
    r.fulfill({
      json: { ...snapshot, answerer: writes.length ? fallback : agent },
    }),
  );
  await page.route(
    "**/api/shared-conversations/" + publication + "/continue",
    (r) => {
      writes.push(r.request().postDataJSON());
      return writes.length === 1
        ? r.fulfill({
            status: 409,
            json: { detail: { code: "answerer_changed", answerer: fallback } },
          })
        : r.fulfill({
            json: {
              thread_id: MOCK_THREAD_ID_2,
              answerer: fallback,
              files: [],
            },
          });
    },
  );
  await page.goto("/workspace/files/conversations/" + publication);
  await expect(
    page.getByText(/available files \(1\).*Maya’s files/),
  ).toBeVisible();
  await page.getByRole("button", { name: "Continue with Maya" }).click();
  await expect(page.getByText(/Who can answer has changed/)).toBeVisible();
  await expect(page.getByText(/Maya is paused/)).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Continue with Maya" }),
  ).toHaveCount(0);
  expect(writes).toHaveLength(1);
  await page.getByRole("button", { name: "Continue with HartMesh" }).click();
  await expect(page).toHaveURL(new RegExp(MOCK_THREAD_ID_2 + "$"));
  expect(writes[0]?.answerer).toEqual({ kind: "agent", id: "employee" });
  expect(writes[1]?.answerer).toEqual({ kind: "assistant" });
  expect(writes[1]?.request_id).not.toBe(writes[0]?.request_id);
});

test("same-agent Continue replays one request and sends the returned Home filename in its first message", async ({
  page,
}) => {
  await setup(page);
  const agent = workInstance(),
    answerer = { kind: "agent", id: agent.id, name: agent.name };
  const binding = {
    id: agent.id,
    name: agent.name,
    custody: agent.custody,
    status: agent.status,
    requester_id: "default",
  };
  await page.route("**/api/features", (r) =>
    r.fulfill({
      json: {
        agents_api: { enabled: true },
        agent_instances: { enabled: true, execution: "ready" },
        shared_conversations: {
          version: 1,
          operations: ["read", "publish", "continue", "agent_publish"],
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === "/api/agent-instances/" + agent.id,
    (r) => r.fulfill({ json: agent }),
  );
  await page.route("**/api/langgraph/threads/" + MOCK_THREAD_ID_2, (r) =>
    r.fulfill({
      json: {
        thread_id: MOCK_THREAD_ID_2,
        agent_instance: binding,
        owner_user_id: "default",
        metadata: {},
        values: { title: summary.title },
        status: "idle",
        created_at: summary.published_at,
        updated_at: summary.published_at,
      },
    }),
  );
  await page.route(
    "**/api/threads/" + MOCK_THREAD_ID_2 + "/shared-history",
    (r) =>
      r.fulfill({
        json: { publication_id: publication, message_ids: ["copied", "end"] },
      }),
  );
  await page.route("**/api/shared-conversations/" + publication, (r) =>
    r.fulfill({
      json: {
        ...snapshot,
        with: { kind: "employee", id: agent.id, name: "Maya" },
        answerer,
      },
    }),
  );
  const calls: { request_id: string; answerer: unknown }[] = [];
  await page.route(
    "**/api/shared-conversations/" + publication + "/continue",
    (r) => {
      calls.push(r.request().postDataJSON());
      if (calls.length === 1)
        return r.fulfill({ status: 503, json: { detail: "busy" } });
      if (calls.length === 2)
        return r.fulfill({
          status: 409,
          json: { detail: { code: "continuation_interrupted" } },
        });
      return r.fulfill({
        json: {
          thread_id: MOCK_THREAD_ID_2,
          answerer,
          files: [
            {
              status: "copied",
              path: member.path,
              attachment: {
                source: {
                  kind: "shared_publication",
                  publication_id: member.publication_id,
                  sha256: member.sha256,
                },
                filename: "report (2).pdf",
                virtual_path: "/mnt/user-data/uploads/report (2).pdf",
                sha256: member.sha256,
                size: member.size,
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
  await page.goto("/workspace/files/conversations/" + publication);
  const button = page.getByRole("button", { name: "Continue with Researcher" });
  await button.click();
  await expect(
    page.getByRole("alert").filter({ hasText: "busy" }),
  ).toBeVisible();
  await button.click();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: /may already have created the conversation/ }),
  ).toBeVisible();
  await button.click();
  await expect(page).toHaveURL(new RegExp(MOCK_THREAD_ID_2 + "$"));
  await expect(page.getByText("report (2).pdf", { exact: true })).toBeVisible();
  await expect(page.getByText(/Researcher answers from here/)).toBeVisible();
  await page.getByRole("button", { name: "Retry copying files" }).click();
  await expect.poll(() => calls.length).toBe(4);
  expect(calls[0]?.answerer).toEqual({ kind: "agent", id: agent.id });
  expect(calls[1]).toEqual(calls[0]);
  expect(calls[2]).toEqual(calls[0]);
  expect(calls[3]).toEqual(calls[0]);
  const operations =
    "/api/agent-instances/" +
    agent.id +
    "/conversations/" +
    MOCK_THREAD_ID_2 +
    "/files/operations";
  let uploads = 0;
  await page.route(
    (url) => url.pathname === operations,
    (r) => {
      uploads++;
      return r.fulfill({
        status: 201,
        json: {
          path: "uploads/note (2).txt",
          sandbox_path: "/mnt/user-data/uploads/note (2).txt",
          size: 4,
          sha256: "c".repeat(64),
        },
      });
    },
  );
  await page.getByLabel("Upload files").setInputFiles({
    name: "note.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("note"),
  });
  await expect.poll(() => uploads).toBe(1);
  const request = page.waitForRequest(
    (r) => r.method() === "POST" && r.url().endsWith("/runs/stream"),
  );
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("Read the copied report.");
  await input.press("Enter");
  const body = (await request).postDataJSON();
  expect(body.input.messages.at(-1).additional_kwargs.files).toEqual([
    expect.objectContaining({
      filename: "note (2).txt",
      path: "/mnt/user-data/uploads/note (2).txt",
      size: 4,
    }),
    expect.objectContaining({
      filename: "report (2).pdf",
      path: "/mnt/user-data/uploads/report (2).pdf",
      size: 17,
    }),
  ]);
  expect(uploads).toBe(1);
});

test("Chinese shows the Home audience and an interrupted Continue keeps its original request", async ({
  page,
  baseURL,
}) => {
  await page
    .context()
    .addCookies([{ name: "locale", value: "zh-CN", url: baseURL! }]);
  await setup(page);
  await page.route("**/api/shared-conversations/" + publication, (r) =>
    r.fulfill({
      json: {
        ...snapshot,
        answerer: { kind: "agent", id: "employee", name: "Maya" },
      },
    }),
  );
  const requests: unknown[] = [];
  await page.route(
    "**/api/shared-conversations/" + publication + "/continue",
    (r) => {
      requests.push(r.request().postDataJSON());
      return r.fulfill({
        status: 409,
        json: { detail: { code: "continuation_interrupted" } },
      });
    },
  );
  await page.goto("/workspace/files/conversations/" + publication);
  const button = page.getByRole("button", { name: "与Maya继续对话" });
  await expect(page.getByText(/从公司共享区移除对话后/)).toBeVisible();
  await button.click();
  await expect(
    page.getByText(/之前的尝试可能已创建对话或复制了文件/),
  ).toBeVisible();
  await button.click();
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]).toEqual(requests[0]);
});

test("Chinese names the existing and company audiences before agent publication", async ({
  page,
  baseURL,
}) => {
  await page
    .context()
    .addCookies([{ name: "locale", value: "zh-CN", url: baseURL! }]);
  const agent = await agentSource(page, 7);
  await page.route("**/api/shared-conversations/draft?**", (route) =>
    route.fulfill({
      json: {
        title: "交接",
        snapshot: "a".repeat(64),
        message_count: 2,
        files: [member],
        already_shared: false,
        publication_id: null,
        with: { kind: "employee", id: agent.id, name: agent.name },
        audience: {
          starter: "小林",
          starter_can_read: false,
          authors: ["小林"],
          inspect_count: 1,
          chat_count: 4,
        },
      },
    }),
  );
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  await page.getByRole("button", { name: "与所有人分享此对话" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("消息作者：小林");
  await expect(dialog).not.toContainText("发起人（小林）也能阅读");
  await expect(dialog).toContainText("可以向其询问文件内容");
  await expect(
    dialog.getByRole("checkbox", { name: /可读取公司共享区的所有人/ }),
  ).not.toBeChecked();
  await expect(
    dialog.getByRole("button", { name: "分享", exact: true }),
  ).toBeDisabled();
});

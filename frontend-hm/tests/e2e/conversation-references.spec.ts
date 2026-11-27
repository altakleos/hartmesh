import { expect, test, type Page } from "@playwright/test";

import { refreshAccount } from "../e2e-real-backend/utils/compose-agents";

import {
  mockLangGraphAPI,
  MOCK_THREAD_ID,
  MOCK_THREAD_ID_2,
} from "./utils/mock-api";

async function open(page: Page, enabled: boolean, allow: boolean) {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        messages: [{ type: "human", content: "Start here", id: "first" }],
      },
    ],
  });
  await page.route("**/api/features", async (route) => {
    return route.fulfill({
      json: {
        agents_api: { enabled: true },
        conversation_references: { enabled, max_references: 3 },
      },
    });
  });
  let requests = 0;
  await page.route("**/api/threads/" + MOCK_THREAD_ID_2, (route) => {
    requests++;
    return allow
      ? route.fulfill({
          json: {
            thread_id: MOCK_THREAD_ID_2,
            owner_user_id: "default",
            values: { title: "Budget review" },
            agent_instance: null,
          },
        })
      : route.fulfill({ status: 404, json: { detail: "Thread not found" } });
  });
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  // The hydrated thread confirms the account refresh listener is ready.
  await expect(page.getByText("Start here", { exact: true })).toBeVisible();
  await refreshAccount(page);
  const box = page.locator('textarea[name="message"]');
  await expect(box).toBeEnabled();
  const address = new URL("/workspace/chats/" + MOCK_THREAD_ID_2, page.url())
    .href;
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.evaluate((text) => navigator.clipboard.writeText(text), address);
  return { box, address, requests: () => requests };
}
test("a confirmed chat link becomes a removable one-turn grant in SDK context", async ({
  page,
}) => {
  const { box } = await open(page, true, true);
  await box.fill("Compare the budget ");
  await box.press("End");
  await box.press("Control+v");
  const remove = page.getByRole("button", {
    name: "Remove conversation reference: Budget review",
  });
  await expect(remove).toBeVisible();
  await expect(box).toHaveValue("Compare the budget ");
  await remove.click();
  await expect(remove).toHaveCount(0);
  await box.press("End");
  await box.press("Control+v");
  await expect(remove).toBeVisible();
  const admitted = page.waitForRequest(
    (request) =>
      request.method() === "POST" &&
      new URL(request.url()).pathname.endsWith("/runs/stream"),
  );
  await box.press("Enter");
  const body = (await admitted).postDataJSON() as {
    context: Record<string, unknown>;
    input: { messages: unknown[] };
  };
  expect(body.context.conversation_references).toEqual([MOCK_THREAD_ID_2]);
  expect(JSON.stringify(body.input.messages)).not.toContain(MOCK_THREAD_ID_2);
  await expect(remove).toHaveCount(0);
});
for (const state of ["disabled", "denied"]) {
  test(
    state + " references leave the pasted conversation link as editable text",
    async ({ page }) => {
      const { box, address, requests } = await open(
        page,
        state !== "disabled",
        false,
      );
      await box.press("Control+v");
      await expect(box).toHaveValue(address);
      if (state === "denied") await expect.poll(requests).toBe(1);
      expect(requests()).toBe(state === "denied" ? 1 : 0);
      await expect(
        page.getByRole("button", { name: /Remove conversation reference/ }),
      ).toHaveCount(0);
    },
  );
}

test("restored tool history shows the saved filename, source and size from metadata only", async ({
  page,
}) => {
  const receipt = {
    kind: "conversation",
    filename: "plan (2).xlsx",
    source_title: "Budget review",
    size: 12288,
    sha256: "a".repeat(64),
    virtual_path: "/mnt/user-data/uploads/plan (2).xlsx",
  };
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        messages: [
          { type: "human", id: "ask", content: "Bring over the plan" },
          {
            type: "ai",
            id: "ai-tool",
            content: "",
            tool_calls: [
              {
                id: "call",
                type: "tool_call",
                name: "import_conversation_file",
                args: {},
              },
            ],
          },
          {
            type: "tool",
            id: "copy",
            name: "import_conversation_file",
            tool_call_id: "call",
            content: "Other text without the file name",
            additional_kwargs: { imported_files: [receipt] },
          },
          { type: "ai", id: "done", content: "The file is ready." },
        ],
      },
    ],
  });
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  await expect(page.getByTestId("conversation-file-import-receipt")).toHaveText(
    "Imported plan (2).xlsx from Budget review (12 KiB)",
  );
  await page.reload();
  await expect(
    page.getByTestId("conversation-file-import-receipt"),
  ).toHaveCount(1);
});

test("submitting a slash command retires the pending consent lookup before it can change the draft", async ({
  page,
}) => {
  const { box, address } = await open(page, true, true);
  function gate() {
    let release!: () => void;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { promise, resolve: release };
  }
  const lookup = gate();
  const goal = gate();
  await page.route("**/api/threads/" + MOCK_THREAD_ID_2, async (route) => {
    await lookup.promise;
    await route.fallback();
  });
  await page.route(
    "**/api/threads/" + MOCK_THREAD_ID + "/goal",
    async (route) => {
      if (route.request().method() !== "PUT") return route.fallback();
      await goal.promise;
      return route.fulfill({ status: 409, json: { detail: "Goal changed" } });
    },
  );
  const source = page.waitForRequest(
    (req) => new URL(req.url()).pathname === "/api/threads/" + MOCK_THREAD_ID_2,
  );
  await box.fill("/goal Review ");
  await box.press("End");
  await box.press("Control+v");
  await source;
  await expect(box).toHaveValue("/goal Review " + address);
  const submitted = page.waitForRequest(
    (req) =>
      req.method() === "PUT" && new URL(req.url()).pathname.endsWith("/goal"),
  );
  const ended = page
    .waitForEvent("requestfailed", {
      predicate: (req) =>
        new URL(req.url()).pathname === "/api/threads/" + MOCK_THREAD_ID_2,
      timeout: 4000,
    })
    .then(
      () => true,
      () => false,
    );
  await box.press("Enter");
  await submitted;
  lookup.resolve();
  try {
    expect(await ended).toBe(true);
    await expect(
      page.getByRole("button", {
        name: "Remove conversation reference: Budget review",
      }),
    ).toHaveCount(0);
    await expect(box).toHaveValue("/goal Review " + address);
  } finally {
    goal.resolve();
  }
});

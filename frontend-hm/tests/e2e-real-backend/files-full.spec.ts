/** Real product UI with deterministic HTTP refusals; native backing is a separate CI gate. */
import { expect, test, type Page } from "@playwright/test";

import { enUS } from "../../src/core/i18n/locales/en-US";
import { handleRunStream, mockLangGraphAPI } from "../e2e/utils/mock-api";

const threadId = "00000000-0000-0000-0000-000000003900";
const path = "/mnt/user-data/outputs/result.md";
const ready = {
  enabled: true,
  state: "ready",
  reason: null,
  own: "ready",
  user_spaces: true,
};
async function discovery(page: Page, storage: object) {
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: false },
        storage_spaces: storage,
        branding: {
          provider: {
            display_name: "Acme",
            support_url: "https://support.example.com",
          },
        },
      },
    }),
  );
}
async function userReader(page: Page) {
  await page.route("**/api/v1/auth/me", (route) =>
    route.fulfill({
      json: {
        id: "default",
        email: "default@test.local",
        system_role: "user",
        needs_setup: false,
      },
    }),
  );
  await page.evaluate(() =>
    document.dispatchEvent(new Event("visibilitychange")),
  );
}
function noGeometry(text: string) {
  expect(text).not.toMatch(/\b(?:Space|slot|pool|GiB|capacity)\b/i);
}

test("first-message thread creation refusal restores the draft without a new sidebar conversation", async ({
  page,
}) => {
  mockLangGraphAPI(page, { threads: [] });
  await discovery(page, ready);
  await page.route("**/api/langgraph/threads", (route) =>
    route.request().method() === "POST"
      ? route.fulfill({
          status: 507,
          json: {
            detail: { code: "files-full", message: "INTERNAL_CAPACITY" },
          },
        })
      : route.fallback(),
  );
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("Do not lose this first draft");
  await input.press("Enter");
  await expect(page.getByText(enUS.chat.filesFull("Acme"))).toBeVisible();
  await expect(input).toHaveValue("Do not lose this first draft");
  await expect(input).toBeDisabled();
  await expect(page).toHaveURL(/\/chats\/new$/);
  await expect(
    page
      .locator("[data-sidebar='sidebar']")
      .getByRole("link", { name: "New Chat", exact: true }),
  ).toHaveCount(0);
  await expect(page.getByText("INTERNAL_CAPACITY")).toHaveCount(0);
});

test("a person whose files are full reads the full sentences and no size", async ({
  page,
}) => {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: threadId,
        title: "Earlier conversation",
        artifacts: [path],
        messages: [
          { type: "human", id: "human-1", content: "Produce result" },
          {
            type: "ai",
            id: "ai-1",
            content: "Result",
            tool_calls: [
              {
                id: "present-1",
                name: "present_files",
                args: { filepaths: [path] },
              },
            ],
          },
        ],
      },
    ],
  });
  await discovery(page, ready);
  await page.route("**/runs/stream*", (route) =>
    route.fulfill({
      status: 507,
      json: { detail: { code: "files-full", message: "INTERNAL_CAPACITY" } },
    }),
  );
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("Please keep this draft");
  await input.press("Enter");
  await expect(page.getByText(enUS.chat.filesFull("Acme"))).toBeVisible({
    timeout: 15_000,
  });
  await expect(input).toHaveValue("Please keep this draft");
  await expect(input).toBeDisabled();
  await expect(page).toHaveURL(/\/chats\/new$/);
  noGeometry(await page.getByRole("alert").first().innerText());

  await page.unroute("**/runs/stream*");
  let uploadRequests = 0;
  await page.route("**/api/threads/**/uploads", (route) => {
    uploadRequests++;
    return route.fulfill({
      status: 507,
      json: { detail: { code: "files-full", message: "INTERNAL_CAPACITY" } },
    });
  });
  await page.reload();
  await input.fill("Continue without this attachment");
  await page.getByLabel("Upload files").setInputFiles({
    name: "statement.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("fixture"),
  });
  await input.press("Enter");
  await expect(
    page.getByText(enUS.uploads.filesFull("statement.txt", "Acme")),
  ).toBeVisible();
  await expect(input).toHaveValue("Continue without this attachment");
  await expect(input).toBeEnabled();
  await expect(
    page.getByRole("link", { name: /Acme/ }).first(),
  ).toHaveAttribute("href", "https://support.example.com/");
  await input.press("Enter");
  await expect(
    page.getByText("Hello from DeerFlow!", { exact: true }),
  ).toBeVisible();
  expect(uploadRequests).toBe(1);

  await page.route("**/api/threads/**/files", (route) =>
    route.fulfill({ status: 507, json: { detail: { code: "files-full" } } }),
  );
  await page.route("**/api/shared/publish", (route) =>
    route.fulfill({ status: 507, json: { detail: { code: "files-full" } } }),
  );
  await page.goto(`/workspace/chats/${threadId}`);
  await page.getByText("result.md", { exact: true }).first().click();
  await page
    .getByRole("button", { name: "Save to My files", exact: true })
    .click();
  await expect(page.getByText(enUS.files.saveFull("Acme"))).toBeVisible();
  await page
    .getByRole("button", { name: "Share with everyone", exact: true })
    .click();
  await expect(page.getByText(enUS.shared.full("Acme"))).toBeVisible();
  await expect(page.getByText("INTERNAL_CAPACITY")).toHaveCount(0);
  noGeometry(await page.locator("[data-sonner-toaster]").innerText());
});

test("files that are being prepared, an account without a seat and files that did not pass the check each show their sentence", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  for (const [own, sentence] of [
    ["not-assigned", enUS.chat.filesPreparing("Acme")],
    ["no-seat", enUS.chat.filesNoSeat(true, "Acme")],
    ["unverified", enUS.chat.myFilesUnchecked],
  ] as const) {
    await discovery(page, { ...ready, state: "incomplete", own });
    await page.goto("/workspace/chats/new");
    await expect(page.getByText(sentence)).toBeVisible();
    await expect(page.getByTestId("add-attachments-button")).toBeDisabled();
    await expect(page.getByPlaceholder(/how can i assist you/i)).toBeEnabled();
    await page.goto("/workspace/files");
    await expect(page.getByText(sentence)).toBeVisible();
    await page.getByTestId("files-tab-shared").click();
    await expect(page.getByTestId("shared-files-empty")).toBeVisible();
    noGeometry(await page.locator("main").last().innerText());
  }
});

test("a deployment whose storage is too small reads differently for a user and an administrator", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await discovery(page, {
    ...ready,
    state: "unavailable",
    reason: "disk-too-small",
  });
  await page.goto("/workspace/chats/new");
  await expect(
    page.getByText(enUS.agentInstances.unavailable.diskTooSmall("Acme")),
  ).toBeVisible();
  await expect(
    page.getByText(enUS.agentInstances.unavailable.reference("disk-too-small")),
  ).toBeVisible();
  await userReader(page);
  await expect(page.getByText(enUS.chat.filesUnavailable)).toBeVisible();
  await expect(
    page.getByText(enUS.agentInstances.unavailable.askAdmin),
  ).toBeVisible();
  await expect(
    page.getByText(enUS.agentInstances.unavailable.reference("disk-too-small")),
  ).toHaveCount(0);
  await page.getByRole("link", { name: "My files", exact: true }).click();
  await expect(page.getByText(enUS.chat.filesUnavailable)).toBeVisible();
  await page.getByTestId("files-tab-shared").click();
  await expect(page.getByText(enUS.chat.filesUnavailable)).toBeVisible();
  noGeometry(await page.locator("main").last().innerText());
});

test("an actual preparing list refusal recovers after newer storage discovery", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  let prepared = false;
  let reads = 0;
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: false },
        storage_spaces: {
          ...ready,
          state: prepared ? "ready" : "incomplete",
          own: prepared ? "ready" : "not-assigned",
        },
      },
    }),
  );
  await page.route("**/api/files", (route) => {
    reads++;
    return prepared
      ? route.fulfill({ json: { files: [], count: 0, truncated: false } })
      : route.fulfill({
          status: 503,
          json: {
            detail: {
              code: "storage-unavailable",
              reason: "space-not-assigned",
            },
          },
        });
  });
  await page.goto("/workspace/files");
  await expect(page.getByTestId("my-files-storage-notice")).toBeVisible();
  await expect.poll(() => reads, { timeout: 15_000 }).toBeGreaterThanOrEqual(4);
  prepared = true;
  await page.evaluate(() =>
    document.dispatchEvent(new Event("visibilitychange")),
  );
  await expect(page.getByTestId("my-files-empty")).toBeVisible({
    timeout: 20_000,
  });
  expect(reads).toBeGreaterThan(1);
});

test("a delayed native download survives switching Files tabs", async ({
  page,
}) => {
  const file = {
    path: "statement.txt",
    name: "statement.txt",
    size: 7,
    modified: 1,
    virtual_path: "/mnt/user-data/files/statement.txt",
    url: "/api/files/statement.txt",
  };
  mockLangGraphAPI(page, { files: [file] });
  await discovery(page, ready);
  let release!: () => void;
  let admitted!: () => void;
  const started = new Promise<void>((resolve) => {
    admitted = resolve;
  });
  const headers = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route(
    "**/api/files/statement.txt?download=true",
    async (route) => {
      admitted();
      await headers;
      await route.fulfill({
        contentType: "application/octet-stream",
        headers: {
          "Content-Disposition": 'attachment; filename="statement.txt"',
        },
        body: "fixture",
      });
    },
  );
  await page.goto("/workspace/files");
  const pending = page.waitForEvent("download");
  await page.getByRole("link", { name: "Download statement.txt" }).click();
  await started;
  await page.getByTestId("files-tab-shared").click();
  await expect(page.getByTestId("shared-files-empty")).toBeVisible();
  release();
  const download = await pending;
  expect(await download.failure()).toBeNull();
  expect(download.suggestedFilename()).toBe("statement.txt");
});

test("a preparing send refusal retires when the personal files become ready", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  let own = "ready";
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: false },
        storage_spaces: {
          ...ready,
          own,
          state: own === "ready" ? "ready" : "incomplete",
        },
        branding: { provider: { display_name: "Acme" } },
      },
    }),
  );
  await page.route("**/runs/stream*", (route) => {
    own = "not-assigned";
    return route.fulfill({
      status: 503,
      json: {
        detail: { code: "storage-unavailable", reason: "space-not-assigned" },
      },
    });
  });
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("Keep this text while preparing files");
  await input.press("Enter");
  await expect(page.getByText(enUS.chat.filesPreparing("Acme"))).toBeVisible();
  await expect(input).toHaveValue("Keep this text while preparing files");
  await expect(page.getByTestId("add-attachments-button")).toBeDisabled();
  own = "ready";
  await expect(page.getByTestId("add-attachments-button")).toBeEnabled({
    timeout: 25_000,
  });
  await expect(page.getByText(enUS.chat.filesPreparing("Acme"))).toHaveCount(0);
  await expect(input).toHaveValue("Keep this text while preparing files");
});

test("held admission preserves a later draft and attachment through a blocked submit", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await discovery(page, ready);
  let release!: () => void;
  let started!: () => void;
  const admitted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const headers = new Promise<void>((resolve) => {
    release = resolve;
  });
  let sends = 0;
  let uploads = 0;
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      new URL(request.url()).pathname.endsWith("/uploads")
    )
      uploads++;
  });
  await page.route("**/runs/stream*", async (route) => {
    sends++;
    if (sends === 1) {
      started();
      await headers;
    }
    await handleRunStream(route);
  });
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("First message awaiting admission");
  await input.press("Enter");
  await admitted;
  await input.fill("Keep this later unsent draft");
  await page.getByLabel("Upload files").setInputFiles({
    name: "later.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("fixture"),
  });
  await page
    .locator("form")
    .filter({ has: input })
    .evaluate((form) =>
      form.dispatchEvent(
        new Event("submit", { bubbles: true, cancelable: true }),
      ),
    );
  await expect(input).toHaveValue("Keep this later unsent draft");
  await expect(page.getByText("later.txt", { exact: true })).toBeVisible();
  expect(sends).toBe(1);
  expect(uploads).toBe(0);
  release();
  await expect(
    page.getByText("Hello from DeerFlow!", { exact: true }),
  ).toBeVisible();
  await expect(input).toHaveValue("Keep this later unsent draft");
  await expect(page.getByText("later.txt", { exact: true })).toBeVisible();
  expect(sends).toBe(1);
});

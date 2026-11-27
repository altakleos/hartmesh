import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const STARTERS = [
  {
    id: "business-review",
    title: "Monthly business review",
    prompt: "Build a monthly business review from the spreadsheet I attach.",
  },
  {
    id: "summarize-document",
    title: "Summarize a document",
    prompt: "Read the document I attach and summarize it.",
  },
  {
    id: "spreadsheet-questions",
    title: "Ask about a spreadsheet",
    prompt: "Help me interpret the spreadsheet I attach.",
  },
];

async function mockWorkspace(
  page: Page,
  {
    profile,
    starters = STARTERS,
  }: {
    profile: "business" | "developer";
    starters?: typeof STARTERS;
  },
) {
  mockLangGraphAPI(page);
  // Routed after the shared mock so these win.
  await page.route("**/api/features", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        agents_api: { enabled: true },
        browser_control: { enabled: true },
        mcp_tasks: { enabled: true },
        ui: { profile, starters },
      }),
    }),
  );
}

test.describe("workspace home", () => {
  test("opens business chat directly on the composer", async ({ page }) => {
    await mockWorkspace(page, { profile: "business" });
    await Promise.all([
      page.waitForResponse(
        (response) =>
          new URL(response.url()).pathname === "/api/features" && response.ok(),
      ),
      page.goto("/workspace/chats/new"),
    ]);

    const composer = page.getByPlaceholder(/how can i assist you/i);
    await expect(composer).toBeVisible({ timeout: 15_000 });
    for (const starter of STARTERS) {
      await expect(
        page.getByRole("button", { name: starter.title }),
      ).toHaveCount(0);
    }
    await expect(
      page.getByText("Pick one to fill the message box", { exact: false }),
    ).toHaveCount(0);
    await expect(
      page.getByText("Welcome to HartMesh.", { exact: false }),
    ).toHaveCount(0);
    await expect(page.getByText("Hello, again!", { exact: true })).toHaveCount(
      0,
    );
    await expect(page.getByText("👋", { exact: true })).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "Surprise", exact: true }),
    ).toHaveCount(0);
    await expect(page.getByText(/is AI and can make mistakes/)).toHaveCount(0);
    await composer.fill("Help me plan the month.");
    await expect(composer).toHaveValue("Help me plan the month.");
  });

  test("keeps the developer blurb without a row of suggested prompts", async ({
    page,
  }) => {
    await mockWorkspace(page, { profile: "developer", starters: [] });
    await page.goto("/workspace/chats/new");

    await expect(
      page.getByText("Welcome to HartMesh.", { exact: false }),
    ).toBeVisible({ timeout: 15_000 });
    await expect(
      page.getByRole("button", { name: "Surprise", exact: true }),
    ).toHaveCount(0);
    await expect(page.getByText(/is AI and can make mistakes/)).toHaveCount(0);
  });
});

// The narrowed settings of someone who is not an administrator are
// deliberately not tested here: this
// harness runs with authentication disabled, and that mode's server-side
// session is an administrator whatever `/api/v1/auth/me` returns, so any such
// case would pass with the feature deleted. The wire from `ui.profile` to the
// settings dialog is pinned by
// `tests/unit/components/workspace/settings/settings-dialog.dom.test.tsx`.

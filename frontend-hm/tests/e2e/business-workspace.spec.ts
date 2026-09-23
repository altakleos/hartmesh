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

test.describe("a business workspace", () => {
  test("opens on what the person can do, and puts a starter in the box", async ({
    page,
  }) => {
    await mockWorkspace(page, { profile: "business" });
    await page.goto("/workspace/chats/new");

    const starters = page.getByTestId("welcome-starters");
    await expect(starters).toBeVisible({ timeout: 15_000 });
    await expect(
      starters.getByRole("button", { name: "Monthly business review" }),
    ).toBeVisible();
    // The product blurb names agents, skills and artifacts; this workspace
    // opens on the work instead.
    await expect(
      page.getByText("Welcome to HartMesh.", { exact: false }),
    ).toHaveCount(0);
    // And the row this one replaced steps aside rather than sitting under it.
    await expect(page.getByRole("button", { name: "Surprise" })).toHaveCount(0);

    await starters
      .getByRole("button", { name: "Monthly business review" })
      .click();

    // Filled, not sent: the person adds their file and their own words.
    await expect(page.getByRole("textbox")).toHaveValue(STARTERS[0]!.prompt);
    await expect(page.getByTestId("message-list-item")).toHaveCount(0);
  });

  test("leaves a deployment that configured nothing exactly as it was", async ({
    page,
  }) => {
    // The upgrade-safety rule in both directions: no grid appears, the blurb
    // stays, and the suggestion row this feature can replace is untouched.
    await mockWorkspace(page, { profile: "developer", starters: [] });
    await page.goto("/workspace/chats/new");

    await expect(page.getByRole("button", { name: "Surprise" })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByTestId("welcome-starters")).toHaveCount(0);
    await expect(
      page.getByText("Welcome to HartMesh.", { exact: false }),
    ).toBeVisible();
  });
});

// The narrowed settings of someone who is not an administrator are
// deliberately not tested here: this
// harness runs with authentication disabled, and that mode's server-side
// session is an administrator whatever `/api/v1/auth/me` returns, so any such
// case would pass with the feature deleted. The wire from `ui.profile` to the
// settings dialog is pinned by
// `tests/unit/components/workspace/settings/settings-dialog.dom.test.tsx`.

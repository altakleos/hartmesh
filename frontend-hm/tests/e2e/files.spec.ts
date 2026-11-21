import { expect, test } from "@playwright/test";

import { installLegacyReportPlugin } from "./utils/legacy-report-plugin";
import { mockLangGraphAPI } from "./utils/mock-api";

const AUGUST = {
  path: "Reports/2026-08-business-review.pdf",
  name: "2026-08-business-review.pdf",
  size: 48_213,
  modified: Date.now() / 1000 - 3600,
  virtual_path: "/mnt/user-data/files/Reports/2026-08-business-review.pdf",
  url: "/api/files/Reports/2026-08-business-review.pdf",
};

test.describe("My files", () => {
  test("is reachable from the sidebar and lists what was kept", async ({
    page,
  }) => {
    mockLangGraphAPI(page, { threads: [], files: [AUGUST] });
    await installLegacyReportPlugin(page);

    await page.goto("/workspace/chats/new");
    await page
      .locator("[data-sidebar='sidebar']")
      .locator("a[href='/workspace/files']")
      .click();
    await page.waitForURL("**/workspace/files");

    const row = page.getByTestId(`my-file-${AUGUST.path}`);
    await expect(row).toBeVisible({ timeout: 15_000 });
    await expect(row).toContainText("Reports");
    await expect(row).toContainText("47.1 KiB");
    await expect(
      row.getByRole("link", { name: `Download ${AUGUST.name}` }),
    ).toHaveAttribute("href", /\/api\/files\/Reports\/.*\?download=true$/);
  });

  test("older Gateways keep the file available and offer no path-only deletion", async ({
    page,
  }) => {
    mockLangGraphAPI(page, { threads: [], files: [AUGUST] });
    await installLegacyReportPlugin(page);
    await page.goto("/workspace/files");
    const row = page.getByTestId(`my-file-${AUGUST.path}`);
    await expect(row).toBeVisible();
    await expect(
      row.getByRole("button", { name: `Delete ${AUGUST.name}` }),
    ).toHaveCount(0);
  });
});

test("same-page Shared navigation and browser history follow the URL", async ({
  page,
}) => {
  mockLangGraphAPI(page, { threads: [], files: [AUGUST] });
  await installLegacyReportPlugin(page);
  await page.route("**/api/shared", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ files: [], count: 0, truncated: false }),
    }),
  );
  await page.goto("/workspace/files");
  await expect(page.getByTestId("my-files-list")).toBeVisible();
  await page.route("**/api/shared/publish", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ...AUGUST,
        virtual_path:
          "/mnt/user-data/shared/Reports/2026-08-business-review.pdf",
        can_remove: true,
      }),
    }),
  );
  await page
    .getByRole("button", { name: `Share with everyone ${AUGUST.name}` })
    .click();
  await page
    .getByRole("button", { name: "Open Company shared", exact: true })
    .click();
  await expect(page.getByTestId("shared-files-empty")).toBeVisible();
  await page.goBack();
  await expect(page.getByTestId("my-files-list")).toBeVisible();
  await page.goForward();
  await expect(page.getByTestId("shared-files-empty")).toBeVisible();
  await page.getByTestId("files-tab-mine").click();
  await expect(page).toHaveURL(/\/workspace\/files$/);
  await expect(page.getByTestId("my-files-list")).toBeVisible();
  await page.getByTestId("files-tab-shared").click();
  await expect(page).toHaveURL(/\?tab=shared$/);
  await page.reload();
  await expect(page.getByTestId("shared-files-empty")).toBeVisible();
  await page.evaluate(() => window.history.pushState(null, "", "?tab=unknown"));
  await expect(page.getByTestId("my-files-list")).toBeVisible();
});

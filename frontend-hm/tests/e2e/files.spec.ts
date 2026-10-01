import { expect, test } from "@playwright/test";

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

  test("asks before removing a file, then removes it", async ({ page }) => {
    mockLangGraphAPI(page, { threads: [], files: [AUGUST] });

    await page.goto("/workspace/files");
    const row = page.getByTestId(`my-file-${AUGUST.path}`);
    await expect(row).toBeVisible({ timeout: 15_000 });

    await row.getByRole("button", { name: `Delete ${AUGUST.name}` }).click();
    await expect(
      page.getByText(`Delete ${AUGUST.name}? This cannot be undone.`),
    ).toBeVisible();
    await page.getByRole("button", { name: "Delete", exact: true }).click();

    await expect(row).toHaveCount(0);
    await expect(page.getByTestId("my-files-empty")).toBeVisible();
  });
});

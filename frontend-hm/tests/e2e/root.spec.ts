import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

test.describe("Root", () => {
  test("goes straight to the workspace", async ({ page }) => {
    mockLangGraphAPI(page);

    await page.goto("/");

    await page.waitForURL("**/workspace/**");
    await expect(page).toHaveURL(/\/workspace\//);
  });

  for (const path of ["/blog/posts", "/en/docs"]) {
    test(`${path} is not served`, async ({ page }) => {
      const response = await page.goto(path);

      expect(response?.status()).toBe(404);
    });
  }
});

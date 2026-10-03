import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

for (const width of [1280, 390]) {
  test(`support is reachable at ${width}px without sending workspace context`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 800 });
    mockLangGraphAPI(page, { threads: [] });
    await page.route("**/api/features", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          agents_api: { enabled: true },
          branding: {
            company_name: "Customer Co.",
            provider: {
              display_name: "Example Hosting",
              support_url: "https://help.example.test/contact",
            },
          },
        }),
      }),
    );
    let requestHeaders: Record<string, string> = {};
    await page.context().route("https://help.example.test/**", (route) => {
      requestHeaders = route.request().headers();
      return route.fulfill({ status: 200, body: "Support" });
    });
    await page.goto("/workspace/chats/new");
    if (width < 768)
      await page.locator("[data-sidebar='trigger']:visible").first().click();
    await page.getByRole("button", { name: "Settings and more" }).click();
    const support = page.getByRole("menuitem", {
      name: "Contact Example Hosting support",
    });
    await expect(support).toBeVisible();
    await expect(
      page.getByRole("menuitem", { name: "About Customer Co." }),
    ).toBeVisible();
    await page.screenshot({
      path: test.info().outputPath(`support-${width}.png`),
    });
    const popupPromise = page.waitForEvent("popup");
    await support.click();
    const popup = await popupPromise;
    await popup.waitForLoadState();
    expect(popup.url()).toBe("https://help.example.test/contact");
    expect(requestHeaders.referer).toBeUndefined();
    expect(requestHeaders.authorization).toBeUndefined();
    expect(requestHeaders.cookie).toBeUndefined();
    await popup.close();
  });
}

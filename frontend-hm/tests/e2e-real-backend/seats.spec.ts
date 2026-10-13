import { expect, test } from "@playwright/test";

import { enUS } from "../../src/core/i18n/locales/en-US";
import { mockLangGraphAPI } from "../e2e/utils/mock-api";

// Registration is covered in e2e-auth/no-seats.spec.ts: this workspace fixture
// intentionally disables authentication and SSR redirects away from Login.
test("the seats line and the disabled Add person", async ({ page }) => {
  mockLangGraphAPI(page);
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: false },
        storage_spaces: { enabled: true, state: "ready", own: "ready" },
        accounts: { seats: { in_use: 3, allowed: 3 } },
        branding: {
          provider: {
            display_name: "Acme",
            support_url: "https://support.example.com",
          },
        },
      },
    }),
  );
  await page.goto("/workspace/chats/new");
  await page
    .locator("[data-sidebar='sidebar']")
    .getByRole("button", { name: /Settings and more/ })
    .click();
  await page.getByRole("menuitem", { name: "Settings", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Settings" });
  await dialog.getByRole("button", { name: "Account", exact: true }).click();
  await expect(
    dialog.getByText(enUS.settings.account.seats(3, 3)),
  ).toBeVisible();
  const add = dialog.getByRole("button", {
    name: enUS.settings.account.addPersonSubmit,
  });
  await expect(add).toBeDisabled();
  await expect(add).toHaveAttribute("aria-describedby", /.+/);
  await expect(
    dialog.getByText(enUS.settings.account.noSeats(3, "Acme")),
  ).toBeVisible();
  await expect(dialog.getByRole("link", { name: /Acme/ })).toHaveAttribute(
    "href",
    "https://support.example.com/",
  );
});

import { expect, test } from "@playwright/test";

import { enUS } from "../../src/core/i18n/locales/en-US";

test("a refused registration shows the seat remedy instead of server details", async ({
  page,
}) => {
  await page.route("**/api/v1/auth/setup-status", (route) =>
    route.fulfill({
      json: {
        needs_setup: false,
        registration_enabled: true,
        sign_on_only: false,
      },
    }),
  );
  await page.route("**/api/v1/auth/providers", (route) =>
    route.fulfill({ json: { providers: [] } }),
  );
  await page.route("**/api/v1/auth/register", (route) =>
    route.fulfill({
      status: 403,
      json: { detail: { code: "no_seats_left", message: "INTERNAL_SEATS" } },
    }),
  );
  await page.goto("/login");
  await page.getByRole("button", { name: enUS.login.noAccountSignUp }).click();
  await page
    .getByLabel(enUS.login.email, { exact: true })
    .fill("new@test.local");
  await page
    .getByLabel(enUS.login.password, { exact: true })
    .fill("fixture-password");
  await page.getByRole("button", { name: enUS.login.createAccount }).click();
  await expect(page.getByText(enUS.auth.noSeats)).toBeVisible();
  await expect(page.getByText("INTERNAL_SEATS")).toHaveCount(0);
});

for (const signOnOnly of [false, true]) {
  test(`a sign-on seat refusal uses the same sentence in sign-on-only=${signOnOnly}`, async ({
    page,
  }) => {
    await page.route("**/api/v1/auth/setup-status", (route) =>
      route.fulfill({
        json: {
          needs_setup: false,
          registration_enabled: true,
          sign_on_only: signOnOnly,
        },
      }),
    );
    await page.route("**/api/v1/auth/providers", (route) =>
      route.fulfill({
        json: {
          providers: signOnOnly
            ? [{ id: "sso", type: "oidc", display_name: "SSO" }]
            : [],
        },
      }),
    );
    await page.goto("/login?error=no_seats_left");
    await expect(page.getByText(enUS.auth.noSeats)).toBeVisible();
  });
}

import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const source = {
  source_id: "integrations:provider/office/helper",
  name: "helper",
  category: "integrations",
  enabled: true,
};

test("delegated ordinary owner copies a provided skill and sees private provenance", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await page.route("**/api/v1/auth/me", (route) =>
    route.fulfill({
      json: {
        id: "owner-a",
        email: "owner@example.test",
        system_role: "user",
        needs_setup: false,
      },
    }),
  );
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: false },
        ui: { profile: "business", starters: [] },
        customer_administration: { local_skill_management: true },
      },
    }),
  );
  let copied = false;
  await page.route("**/api/skills", (route) =>
    route.fulfill({
      json: {
        skills: [
          {
            name: "helper",
            description: "Provided workflow",
            category: "integrations",
            enabled: true,
          },
          ...(copied
            ? [
                {
                  name: "helper-private",
                  description: "Owner workflow",
                  category: "custom",
                  enabled: true,
                  editable: true,
                  origin: {
                    ...source,
                    source_name: "helper",
                    source_category: "integrations",
                    revision: "a".repeat(64),
                  },
                },
              ]
            : []),
        ],
      },
    }),
  );
  await page.route("**/api/skills/clone-sources", (route) =>
    route.fulfill({ json: { sources: [source] } }),
  );
  await page.route("**/api/skills/clone-preview", (route) =>
    route.fulfill({
      json: {
        ...source,
        revision: "a".repeat(64),
        can_export: true,
        file_count: 2,
        total_bytes: 40,
      },
    }),
  );
  let body: unknown;
  await page.route("**/api/skills/clone", async (route) => {
    body = route.request().postDataJSON();
    copied = true;
    await route.fulfill({
      json: {
        success: true,
        skill_name: "helper-private",
        message: "Private copy created.",
      },
    });
  });
  await page.goto("/workspace/chats/new?settings=skills");
  const settings = page.getByRole("dialog", { name: "Settings" });
  await expect(
    settings.getByRole("button", { name: "Tools", exact: true }),
  ).toHaveCount(0);
  await settings.getByRole("button", { name: "Copy provided skill" }).click();
  const clone = page.getByRole("dialog", {
    name: "Create a private skill copy",
  });
  await clone
    .getByRole("combobox", { name: "Provided skill", exact: true })
    .selectOption(source.source_id);
  await expect(clone.getByLabel("Private name")).toHaveValue("helper-private");
  await expect(
    clone.getByRole("button", { name: "Create private copy" }),
  ).toBeEnabled();
  await clone.getByRole("button", { name: "Create private copy" }).click();
  await expect(clone).toBeHidden();
  expect(body).toEqual({
    source_id: source.source_id,
    name: "helper-private",
    expected_revision: "a".repeat(64),
    allow_baseline_override: false,
  });
  await settings.getByRole("tab", { name: "Private", exact: true }).click();
  await expect(settings.getByText(/Private copy of helper/)).toBeVisible();
});

test("same-name copy requires consent and preserves a conflict for the owner", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        ui: { profile: "developer", starters: [] },
        customer_administration: { local_skill_management: true },
      },
    }),
  );
  await page.route("**/api/skills/clone-sources", (route) =>
    route.fulfill({ json: { sources: [source] } }),
  );
  await page.route("**/api/skills/clone-preview", (route) =>
    route.fulfill({
      json: {
        ...source,
        revision: "b".repeat(64),
        can_export: true,
        file_count: 2,
        total_bytes: 40,
      },
    }),
  );
  let body: unknown;
  await page.route("**/api/skills/clone", async (route) => {
    body = route.request().postDataJSON();
    await route.fulfill({
      status: 409,
      json: { detail: "Private skill already exists." },
    });
  });
  await page.goto("/workspace/chats/new?settings=skills");
  await page.getByRole("button", { name: "Copy provided skill" }).click();
  const clone = page.getByRole("dialog", {
    name: "Create a private skill copy",
  });
  await clone
    .getByRole("combobox", { name: "Provided skill", exact: true })
    .selectOption(source.source_id);
  await clone.getByLabel("Private name").fill("helper");
  await expect(
    clone.getByRole("button", { name: "Create private copy" }),
  ).toBeDisabled();
  await clone
    .getByLabel("Override the provided name in my workspace", { exact: true })
    .check();
  await clone.getByRole("button", { name: "Create private copy" }).click();
  await expect(clone.getByRole("alert")).toHaveText(
    "Private skill already exists.",
  );
  expect(body).toEqual({
    source_id: source.source_id,
    name: "helper",
    expected_revision: "b".repeat(64),
    allow_baseline_override: true,
  });
});

import { expect, test } from "@playwright/test";

import {
  addBrief,
  agent,
  COLLEAGUE,
  setupAgents,
  refreshAccount,
} from "./utils/compose-agents";

test("role, brief and supervisor lead to a first conversation with the company notice", async ({
  page,
}) => {
  const state = await setupAgents(page, {
    empty: true,
    instance: agent({ custody: "company", owner_id: null }),
  });
  const dialog = await addBrief(page, "company");
  await dialog.getByRole("button", { name: "Next", exact: true }).click();
  await dialog.getByRole("combobox", { name: "Supervisor" }).fill("alex@");
  await dialog.getByRole("option", { name: /Alex Rivera/ }).click();
  await dialog
    .getByRole("button", { name: "Add AI employee", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Researcher is ready" }),
  ).toBeVisible();
  await expect(
    page.getByText(/Conversations with an AI employee can be read by everyone/),
  ).toBeVisible();
  expect(state.creations[0]?.supervisor_id).toBe(COLLEAGUE.id);
  expect(state.runs).toBe(0);
  expect(state.executionStarts).toEqual([]);
});

test("a person who is not an administrator reads Administrators add AI employees and has no button", async ({
  page,
}) => {
  await setupAgents(page, {
    role: "user",
    instance: agent({ custody: "company", owner_id: null }),
  });
  await page.goto("/workspace/employees");
  await expect(
    page.getByRole("button", { name: "Add AI employee", exact: true }),
  ).toBeEnabled();
  await refreshAccount(page);
  await expect(
    page
      .locator("p")
      .filter({ hasText: /^Administrators add AI employees\.$/ }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Add AI employee", exact: true }),
  ).toHaveCount(0);
});

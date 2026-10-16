import { expect, test, type Page } from "@playwright/test";

import { agent, detail, setupAgents } from "./utils/compose-agents";
async function confirm(page: Page, action: string) {
  await page.getByRole("button", { name: action, exact: true }).click();
  const dialog = page.getByRole("dialog");
  if (action === "Delete") await dialog.getByRole("textbox").fill("Researcher");
  await dialog.getByRole("button", { name: action, exact: true }).click();
}

test("Pause, Resume, Retire, Bring back and Delete show their texts for both kinds", async ({
  page,
}) => {
  const state = await setupAgents(page);
  for (const custody of ["personal", "company"] as const) {
    state.instance = agent({
      custody,
      owner_id: custody === "personal" ? "default" : null,
    });
    state.visible = true;
    await detail(page, "settings");
    await confirm(page, "Pause");
    await expect(
      page.getByRole("button", { name: "Resume", exact: true }),
    ).toBeEnabled();
    await page.getByRole("button", { name: "Resume", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "Pause", exact: true }),
    ).toBeEnabled();
    await page.getByRole("button", { name: "Retire", exact: true }).click();
    await expect(
      page
        .getByRole("dialog")
        .getByText(
          custody === "personal"
            ? /counts as one of your AI assistants/
            : /counts as one of the AI employees/,
        ),
    ).toBeVisible();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Retire", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Bring back", exact: true }),
    ).toBeEnabled();
    await page.getByRole("button", { name: "Bring back", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "Pause", exact: true }),
    ).toBeEnabled();
    await confirm(page, "Delete");
    await expect(
      page.getByText("Researcher was deleted.", { exact: true }),
    ).toBeVisible();
  }
});

test("the list of the kind moves the agent between its groups", async ({
  page,
}) => {
  const state = await setupAgents(page);
  for (const custody of ["personal", "company"] as const) {
    state.instance = agent({
      custody,
      owner_id: custody === "personal" ? "default" : null,
    });
    const list =
      custody === "personal" ? "/workspace/assistants" : "/workspace/employees";
    for (const [action, group] of [
      ["Pause", "Paused"],
      ["Resume", "Active"],
      ["Retire", "Retired"],
    ]) {
      await detail(page, "settings");
      if (action === "Resume")
        await page.getByRole("button", { name: action, exact: true }).click();
      else await confirm(page, action!);
      await page.goto(list);
      const section = page.locator("section").filter({
        has: page.getByRole("heading", { name: group, exact: true }),
      });
      await expect(
        section.getByRole("link", { name: /Researcher/ }),
      ).toBeVisible();
    }
  }
});

test("Add is offered after Delete", async ({ page }) => {
  const state = await setupAgents(page);
  state.feature.agent_instances!.ai_assistants!.per_user = 1;
  await page.goto("/workspace/assistants");
  await expect(
    page.getByRole("button", { name: "Add AI assistant", exact: true }),
  ).toBeDisabled();
  await detail(page, "settings");
  await confirm(page, "Delete");
  await expect(
    page.getByText("You can add 1 AI assistant.", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Add AI assistant", exact: true }),
  ).toBeEnabled();
});

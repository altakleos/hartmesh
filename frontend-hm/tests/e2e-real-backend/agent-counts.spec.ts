import { expect, test } from "@playwright/test";

import { AGENT_ID, agent, setupAgents } from "./utils/compose-agents";

test("the count line of each page shows its forms and no screen shows a size", async ({
  page,
}) => {
  const state = await setupAgents(page);
  await page.goto("/workspace/assistants");
  await expect(
    page.getByText("You have 1 of the 3 AI assistants each person can have."),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Add AI assistant", exact: true }),
  ).toBeEnabled();
  state.feature.agent_instances!.ai_assistants!.per_user = 1;
  await page.reload();
  await expect(
    page.getByText("Researcher is your AI assistant. Each person can have 1."),
  ).toBeVisible();
  state.instance = agent({ custody: "company", owner_id: null });
  await page.goto("/workspace/employees");
  await expect(
    page.getByText("0 of 4 AI employees", { exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Add AI employee", exact: true })
    .click();
  await expect(page.locator("body")).not.toContainText(
    /\b(?:GiB|MiB|GB|MB|bytes)\b/,
  );
});

test("Add shows the limit of each kind and the not-offered sentence at a count of 0", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.feature.agent_instances!.ai_assistants!.per_user = 1;
  await page.goto("/workspace/assistants");
  await expect(
    page.getByRole("button", { name: "Add AI assistant", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByText(/You already have an AI assistant, Researcher/),
  ).toBeVisible();
  state.feature.agent_instances!.ai_employees!.in_use = 4;
  await page.goto("/workspace/employees");
  await expect(
    page.getByText(/All 4 AI employees allowed here are taken/),
  ).toBeVisible();
  state.feature.agent_instances!.ai_employees!.allowed = 0;
  state.instance = agent({ custody: "company", owner_id: null });
  await page.reload();
  await expect(
    page
      .locator("p")
      .filter({ hasText: /^AI employees are not offered here\.$/ }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: /Researcher/ }).first(),
  ).toBeVisible();
  state.feature.agent_instances!.ai_assistants!.per_user = 0;
  state.instance = agent();
  await page.goto("/workspace/assistants");
  await expect(
    page
      .locator("p")
      .filter({ hasText: /^AI assistants are not offered here\.$/ }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: /Researcher/ }).first(),
  ).toBeVisible();
});

test("a reload during a creation shows Finish setting up, which completes it", async ({
  page,
}) => {
  const state = await setupAgents(page, { empty: true });
  state.unfinished = [
    {
      id: AGENT_ID,
      name: "Researcher",
      custody: "personal",
      created_at: "2026-10-16T08:00:00Z",
    },
  ];
  const remembered = {
    body: {
      creation_id: "f".repeat(32),
      name: "Researcher",
      custody: "personal",
      definition_name: "research",
    },
    definition_name: "research",
    copy_of: null,
  };
  await page.addInitScript(
    (value) =>
      localStorage.setItem(
        "deerflow.agent-instances.unfinished.default",
        JSON.stringify(value),
      ),
    remembered,
  );
  await page.goto("/workspace/assistants");
  await page.reload();
  await page
    .getByRole("button", { name: "Finish setting up", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Researcher is ready" }),
  ).toBeVisible();
  expect(state.creations).toEqual([remembered.body]);
});

import { expect, test } from "@playwright/test";

import { inputRequest } from "../fixtures/attention";

import {
  agent,
  assignment,
  detail,
  MOCK_THREAD_ID,
  setupAgents,
  WORK_ID,
} from "./utils/compose-agents";

test("badge, names and the header line of each kind appear in Recent chats, the header, Work and Attention", async ({
  page,
}) => {
  const state = await setupAgents(page);
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(
    page.getByText("Your AI assistant", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("AI", { exact: true }).first()).toBeVisible();
  state.instance.owner_id = "colleague";
  await page.reload();
  await expect(
    page.getByText("Alex Rivera's AI assistant", { exact: true }),
  ).toBeVisible();
  state.instance = agent({
    custody: "company",
    owner_id: null,
    supervisor: { kind: "human", subject_id: "colleague" },
  });
  // Canonical thread identity has its own wire field, independent of metadata.
  await page.route(`**/api/langgraph/threads/${MOCK_THREAD_ID}`, (route) =>
    route.fulfill({
      json: {
        thread_id: MOCK_THREAD_ID,
        metadata: {},
        values: {},
        status: "idle",
        agent_instance: {
          id: state.instance.id,
          name: "Researcher",
          custody: "company",
          status: "active",
          requester_id: "default",
        },
      },
    }),
  );
  await page.reload();
  await expect(
    page.getByText("AI employee · supervised by Alex Rivera", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", {
      name: "AI employee Researcher Guide discussion",
      exact: true,
    }),
  ).toBeVisible();
  state.work = [assignment({ creator_id: "colleague" })];
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(page.getByText(/Delegated by: Alex Rivera/)).toBeVisible();
  await expect(page.getByText("AI", { exact: true }).first()).toBeVisible();
  state.humanInputs = [
    inputRequest({
      id: "e".repeat(32),
      instance_id: state.instance.id,
      work_id: WORK_ID,
      recipient_id: "colleague",
    }),
  ];
  await page.goto(`/workspace/attention?request=${"e".repeat(32)}`);
  await expect(
    page.getByRole("heading", { name: "Attention", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Alex Rivera", { exact: true }).first(),
  ).toBeVisible();
  await expect(page.getByText("AI", { exact: true }).first()).toBeVisible();
  await expect(page.locator("body")).not.toContainText("recipient_id");
});

test("shows Also working in another conversation", async ({ page }) => {
  const state = await setupAgents(page);
  state.instance.activity.running_runs = 1;
  state.instance.activity.running = [
    {
      run_id: "other-run",
      thread_id: "other-thread",
      requester_id: "colleague",
      work_id: null,
      objective: null,
    },
  ];
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(
    page.getByText("Also working in another conversation", { exact: true }),
  ).toBeVisible();
});

for (const [custody, name, href] of [
  ["personal", "My assistants", "/workspace/assistants"],
  ["company", "AI employees", "/workspace/employees"],
] as const) {
  test(`the ${custody} detail breadcrumb returns to its own list`, async ({
    page,
  }) => {
    await setupAgents(page, {
      instance: agent({
        custody,
        owner_id: custody === "personal" ? "default" : null,
      }),
    });
    await detail(page, "conversations");
    const breadcrumb = page.getByRole("navigation", { name: "breadcrumb" });
    await expect(
      breadcrumb.getByRole("link", { name, exact: true }),
    ).toHaveAttribute("href", href);
    await expect(breadcrumb).not.toContainText("Agent instances");
  });
}

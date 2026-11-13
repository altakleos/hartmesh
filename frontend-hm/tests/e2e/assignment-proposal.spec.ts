import { expect, test } from "@playwright/test";

import { mockLangGraphAPI, MOCK_THREAD_ID } from "./utils/mock-api";
const id = "a".repeat(32);
const detail = {
  id,
  name: "Analyst",
  principal: { kind: "nonhuman", subject_id: "agent:" + id },
  custody: "company",
  owner_id: null,
  creator_id: "default",
  supervisor: { kind: "human", subject_id: "default" },
  definition_revision: "d".repeat(64),
  home_id: "e".repeat(32),
  status: "active",
  generation: 1,
  permissions: 7,
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 1,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
test("historical proposal displays passive details in its assistant conversation", async ({
  page,
}, testInfo) => {
  mockLangGraphAPI(page, {
    features: { agentInstancesEnabled: true },
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Installation guide review",
        agent_instance: {
          id,
          name: "Analyst",
          custody: "company",
          status: "active",
          requester_id: "default",
        },
        messages: [
          {
            id: "human",
            type: "human",
            content:
              "Please review the installation guide and report any verified gaps by Friday.",
          },
          {
            id: "issuer",
            type: "ai",
            content: "",
            tool_calls: [
              {
                id: "call",
                name: "propose_assignment",
                args: {
                  proposal: {
                    objective: "Review the installation guide",
                    success_criteria: "Explain verified gaps with references",
                  },
                },
              },
            ],
          },
          {
            id: "proposal",
            type: "tool",
            name: "propose_assignment",
            tool_call_id: "call",
            content: "MODEL ONLY",
            artifact: {
              assignment_card: {
                version: 1,
                kind: "assignment_card",
                tool_call_id: "call",
                draft: {
                  objective: "Review the installation guide",
                  success_criteria: "Explain verified gaps with references",
                  due_text: "Friday afternoon",
                  due_at: null,
                },
              },
            },
          },
        ],
      },
    ],
  });
  await page.route("**/api/agent-instances**", (route) =>
    route.fulfill({ json: detail }),
  );
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  const card = page.getByTestId("assignment-card");
  await expect(card).toBeVisible({ timeout: 30000 });
  await expect(
    card.getByRole("link", { name: "Work", exact: true }),
  ).toHaveAttribute("href", `/workspace/instances?instance=${id}&tab=work`);
  await expect(card.getByRole("button")).toHaveCount(0);
  await expect(card).toContainText("This message saved and started nothing");
  await expect(page.getByText("MODEL ONLY", { exact: true })).toHaveCount(0);
  const image = testInfo.outputPath("assignment-proposal.png");
  await page.screenshot({ path: image, fullPage: true });
  await testInfo.attach("Assignment proposal", {
    path: image,
    contentType: "image/png",
  });
});

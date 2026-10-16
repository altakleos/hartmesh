import { expect, test } from "@playwright/test";

import {
  assignment,
  detail,
  MOCK_THREAD_ID,
  NOW,
  setupAgents,
  WORK_ID,
} from "./utils/compose-agents";
const attempt = {
  id: "f".repeat(32),
  status: "running" as const,
  thread_id: MOCK_THREAD_ID,
  run_id: "run",
  requester_id: "default",
  workspace: "ready" as const,
};

test("shows starting workspace, Working, Stop and Stopped", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.work = [
    assignment({
      attempt: { ...attempt, status: "starting", workspace: "starting" },
    }),
  ];
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(
    page
      .getByText(
        /Starting Researcher's workspace|Researcher's workspace is taking/,
      )
      .first(),
  ).toBeVisible();
  state.work[0]!.attempt = attempt;
  await page.getByRole("button", { name: "Refresh Work", exact: true }).click();
  await expect(
    page.getByRole("heading", {
      name: "Researcher\'s work: Working",
      exact: true,
    }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(
    page.getByRole("dialog").getByText(/Stop Researcher working on this/),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop now", exact: true })
    .click();
  await expect(
    page.getByRole("heading", {
      name: "Researcher\'s work: Stopped",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeEnabled();
});

test("shows Interrupted with its reason and Resume", async ({ page }) => {
  const state = await setupAgents(page);
  state.work = [
    assignment({
      attempt: {
        ...attempt,
        status: "cancelled",
        contained: { reason: "container-absent", at: NOW },
      },
    }),
  ];
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(
    page.getByRole("heading", {
      name: "Researcher\'s work: Interrupted",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByText(/Researcher's workspace stopped unexpectedly/),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeEnabled();
});

test("a second assignment shows the limit and what is running", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.work = [assignment()];
  state.capacity = {
    max: 1,
    in_use: 1,
    running: [
      {
        instance_id: "1".repeat(32),
        instance_name: "Archivist",
        work_id: "2".repeat(32),
        objective: "Organize references",
        requester_id: "colleague",
        started_at: NOW,
      },
    ],
  };
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(
    page.getByRole("button", { name: "Work on this", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByText(/Another assignment is running, and only one can run/),
  ).toBeVisible();
  await expect(
    page.getByText(
      /Archivist is working on “Organize references”.*Alex Rivera/,
    ),
  ).toBeVisible();
  expect(state.runs).toBe(0);
});

test("the conversation of an assignment whose command met a blocked site shows the denied line and never the card", async ({
  page,
}) => {
  await setupAgents(page, {
    messages: [
      {
        type: "ai",
        id: "call",
        content: "",
        tool_calls: [
          { id: "call-site", name: "bash", args: { command: "read site" } },
        ],
      },
      {
        type: "tool",
        id: "result",
        tool_call_id: "call-site",
        name: "bash",
        content: "Denied",
        artifact: {
          sandbox_network_denied: { host: "example.test", port: 443 },
        },
      },
      { type: "ai", id: "final", content: "A source could not be reached." },
    ],
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(
    page
      .getByTestId("network-denials")
      .getByText(/Researcher could not reach example.test/),
  ).toBeVisible();
  await expect(
    page.getByText("A site is blocked", { exact: true }),
  ).toHaveCount(0);
});

test("after a brief was applied, Resume is disabled with its sentence until a manager confirms the assignment", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.work = [
    assignment({
      needs_mandate_reconciliation: true,
      attempt: { ...attempt, status: "cancelled" },
    }),
  ];
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByText(
      /Researcher's brief changed after this assignment was saved/,
    ),
  ).toBeVisible();
  await page
    .getByLabel("Note or response", { exact: true })
    .fill("The assignment still fits the updated brief.");
  await page
    .getByRole("button", { name: "Confirm for the updated brief", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeEnabled();
  expect(state.runs).toBe(0);
});

test("an assignment whose agent asked a question reads Blocked with the question, and the question is in Attention", async ({
  page,
}) => {
  const state = await setupAgents(page);
  const requestId = "e".repeat(32),
    question = "Which reference should I use?";
  state.work = [
    assignment({
      status: "blocked",
      human_input_request_id: requestId,
      blocker: { id: requestId, revision: 1, kind: "information", question },
      attempt: { ...attempt, status: "succeeded" },
    }),
  ];
  state.humanInputs = [
    {
      id: requestId,
      instance_id: state.instance.id,
      instance_name: "Researcher",
      work_id: WORK_ID,
      work_objective: "Review the guide",
      assignment_revision: 1,
      basis_id: requestId,
      basis_revision: 1,
      revision: 1,
      request_revision: 1,
      purpose: "information",
      question,
      reason: "A source is needed",
      expected_response: "The reference name",
      choices: [],
      sources: [],
      state: "pending",
      closed_reason: null,
      recipient_id: "default",
      creator_id: "agent:" + state.instance.id,
      creator_kind: "nonhuman",
      needs_routing: false,
      read_revision: 0,
      can_respond: true,
      can_manage: true,
      can_recover_response: true,
      can_recover_management: true,
      updated_at: NOW,
    },
  ];
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await expect(
    page.getByRole("button", {
      name: "Review the guide · Blocked · Normal",
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.getByText(question, { exact: true }).first()).toBeVisible();
  await expect(
    page.getByRole("heading", {
      name: "Researcher\'s work: Ended without a result",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByText(/stopped without reporting a result or asking a question/),
  ).toHaveCount(0);
  await page.goto("/workspace/attention");
  await expect(
    page.getByRole("button", { name: /Researcher.*Which reference/ }),
  ).toBeVisible();
});

/** Real product rendering with deterministic APIs; SQL/native gates own authority. */
import { expect, test } from "@playwright/test";

import {
  AGENT_ID,
  assignment,
  detail,
  MOCK_THREAD_ID,
  setupAgents,
} from "./utils/compose-agents";

test("pending containment keeps its exact retry and blocks new conversations", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.pending = true;
  await detail(page, "settings");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Pause", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "New conversation", exact: true }),
  ).toBeDisabled();
  await expect(
    page
      .getByText(
        /change.*progress|still.*confirmed|not.*confirmed|being.*changed/i,
      )
      .first(),
  ).toBeVisible();
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => state.lifecycle.length).toBe(2);
  expect(state.lifecycle[1]).toEqual(state.lifecycle[0]);
});

test("bound chat uses canonical identity and audited attachments while hiding browser controls", async ({
  page,
}) => {
  await setupAgents(page);
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(
    page.getByRole("link", { name: "Researcher", exact: true }).first(),
  ).toHaveAttribute("href", `/workspace/instances?instance=${AGENT_ID}`);
  await expect(
    page.getByText("Your AI assistant", { exact: true }),
  ).toBeVisible();
  await expect(page.getByTestId("add-attachments-button")).toBeVisible();
  await expect(page.getByTestId("browser-trigger")).toHaveCount(0);
  await expect(page.getByTestId("sidecar-header-trigger")).toHaveCount(0);
});

for (const outcome of ["bound", "failed", "unbound"] as const) {
  test(`legacy capabilities wait for the canonical thread row: ${outcome}`, async ({
    page,
  }) => {
    const state = await setupAgents(page);
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    await page.route(
      `**/api/langgraph/threads/${MOCK_THREAD_ID}`,
      async (route) => {
        await ready;
        return route.fulfill(
          outcome === "failed"
            ? { status: 503, json: { detail: "Lookup unavailable" } }
            : {
                json: {
                  thread_id: MOCK_THREAD_ID,
                  status: "idle",
                  metadata: { agent_instance_id: "forged" },
                  values: {},
                  agent_instance:
                    outcome === "bound"
                      ? {
                          id: AGENT_ID,
                          name: "Researcher",
                          custody: "personal",
                          status: "active",
                          requester_id: "default",
                        }
                      : null,
                },
              },
        );
      },
    );
    await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
    await expect(page.getByRole("textbox").first()).toBeVisible();
    await expect(page.getByTestId("browser-trigger")).toHaveCount(0);
    await expect(page.getByTestId("add-attachments-button")).toHaveCount(0);
    release();
    if (outcome === "unbound")
      await expect(page.getByTestId("browser-trigger")).toBeVisible();
    else if (outcome === "bound")
      await expect(
        page.getByText("Your AI assistant", { exact: true }),
      ).toBeVisible();
    else await expect(page.getByRole("textbox").first()).toBeDisabled();
    if (outcome !== "failed")
      await expect(page.getByTestId("add-attachments-button")).toBeVisible();
    expect(state.runs).toBe(0);
  });
}

test("Work delegation requires separate activation, retries exactly and retains named history", async ({
  page,
}) => {
  const state = await setupAgents(page);
  const bodies: unknown[] = [];
  await page.route(`**/api/agent-instances/${AGENT_ID}/work`, async (route) => {
    if (route.request().method() !== "POST") return route.fallback();
    bodies.push(route.request().postDataJSON());
    state.work = [assignment({ objective: "Review the deployment guide" })];
    if (bodies.length === 1)
      state.history.push({
        id: "delegated",
        actor_id: "default",
        actor_kind: "human",
        action: "delegate",
        revision: 1,
        assignment_revision: 1,
        created_at: "2026-10-16T08:00:00Z",
        record: { ...state.work[0] },
        note: null,
      });
    if (bodies.length === 1) return route.abort("failed");
    return route.fulfill({ status: 201, json: state.work[0] });
  });
  await detail(page, "work");
  await page
    .getByLabel("Objective", { exact: true })
    .fill("Review the deployment guide");
  await page
    .getByLabel("Success criteria", { exact: true })
    .fill("Explain verified gaps");
  await page
    .getByRole("button", { name: "Delegate Work", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Retry same request", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Work on this", exact: true }),
  ).toBeEnabled();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toEqual(bodies[0]);
  expect(state.runs).toBe(0);
  await page
    .getByRole("button", { name: "Withdraw assignment", exact: true })
    .click();
  await expect(
    page.getByRole("button", {
      name: "Review the deployment guide · Cancelled · Normal",
      exact: true,
    }),
  ).toBeVisible();
  expect(state.commands[0]).toMatchObject({
    action: "cancel",
    expected_revision: 1,
    expected_assignment_revision: 1,
  });
  await page.getByText("History", { exact: true }).click();
  await expect(page.getByText(/You.*Assignment saved/)).toBeVisible();
  await expect(page.getByText(/You.*Assignment withdrawn/)).toBeVisible();
  expect(state.executionStarts).toEqual([]);
  await expect(page.locator("body")).not.toContainText("Human: default");
});

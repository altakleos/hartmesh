import { expect, test } from "@playwright/test";

import { workInstance } from "../fixtures/agent-work";

import { mockLangGraphAPI, MOCK_THREAD_ID } from "./utils/mock-api";

test("a conversation shows a busy job, then its saved deadline and manager stop", async ({
  page,
}) => {
  const agent = workInstance();
  mockLangGraphAPI(page, {
    features: { agentInstancesEnabled: true },
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Research requests",
        messages: [
          { id: "request", type: "human", content: "Check both entries." },
        ],
        agent_instance: {
          id: agent.id,
          name: agent.name,
          custody: agent.custody,
          status: agent.status,
          requester_id: "default",
        },
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agent_instances: { enabled: true, execution: "ready" },
        subagent_batches: {
          enabled: true,
          worker_running: true,
          repository_available: true,
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === "/api/agent-instances/" + agent.id,
    (route) => route.fulfill({ json: agent }),
  );
  let stopped = false;
  await page.route(
    `**/api/threads/${MOCK_THREAD_ID}/subagent-batches*`,
    (route) =>
      route.fulfill({
        json: [
          {
            id: "comparison",
            title: "Check both entries",
            subagent_type: "general-purpose",
            status: stopped ? "completed" : "running",
            total_items: 2,
            max_live_items: 2,
            max_running_items: 1,
            counts: {
              pending: 0,
              queued: 0,
              leased: 0,
              running: stopped ? 0 : 1,
              succeeded: 1,
              failed: 0,
              cancelled: stopped ? 1 : 0,
            },
            follow_up: stopped
              ? {
                  state: "ended",
                  reason: "stopped_by_manager",
                  finished: 1,
                  total: 2,
                  timed_out: 1,
                }
              : { state: "working", finished: 1, total: 2 },
            controls: stopped ? [] : ["cancel"],
          },
        ],
      }),
  );
  await page.route(
    `**/api/threads/${MOCK_THREAD_ID}/subagent-batches/comparison/items*`,
    (route) =>
      route.fulfill({
        json: [
          {
            id: "second",
            item_key: "Second entry",
            status: "cancelled",
            stop_reason: "time_limit",
            error: "Not run: time limit.",
          },
        ],
      }),
  );

  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  const busy = page.getByText(
    "This conversation is busy until the large job ends.",
  );
  await expect(busy).toBeVisible();
  await page.getByRole("button", { name: "Batches" }).click();
  const drawer = page.getByRole("dialog", { name: "Subagent batches" });
  await expect(drawer).toContainText("Working in this conversation.");
  stopped = true;
  await expect(drawer).toContainText("Stopped by a manager.");
  await expect(drawer).toContainText(
    "1 of 2 checks finished. Time limit reached.",
  );
  await expect(busy).toHaveCount(0);
  await drawer.getByRole("button", { name: "View items" }).click();
  await expect(drawer).toContainText(
    "This check did not finish before the time limit.",
  );
});

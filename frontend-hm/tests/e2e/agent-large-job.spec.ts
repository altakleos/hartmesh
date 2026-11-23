import { expect, test } from "@playwright/test";

import { MOCK_THREAD_ID, mockLangGraphAPI } from "./utils/mock-api";

test("a startup cancellation opens retained checks above the composer and stays dismissed after reload", async ({
  page,
}) => {
  const thread = MOCK_THREAD_ID;
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: thread,
        title: "Check reports",
        agent_instance: {
          id: "employee-a",
          name: "River",
          custody: "company",
          status: "active",
          requester_id: "default",
        },
        messages: [
          { id: "message-one", type: "human", content: "Check the reports" },
        ],
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agent_instances: { enabled: true },
        subagent_batches: {
          enabled: false,
          worker_running: false,
          repository_available: true,
        },
      },
    }),
  );
  await page.route(`**/api/threads/${thread}/subagent-batches?*`, (route) =>
    route.fulfill({
      json: [
        {
          id: "job-one",
          title: "Reports",
          status: "cancelled",
          subagent_type: "general-purpose",
          read_only: true,
          controls: [],
          continuation_status: "cancelled",
          total_items: 3,
          counts: {
            pending: 0,
            queued: 0,
            running: 0,
            leased: 0,
            succeeded: 1,
            failed: 0,
            cancelled: 2,
          },
          follow_up: {
            state: "ended",
            reason: "unavailable",
            finished: 1,
            total: 3,
          },
        },
      ],
    }),
  );
  await page.goto(`/workspace/chats/${thread}`);
  const notice = page
    .getByRole("status")
    .filter({ hasText: "This large job was cancelled before it finished" });
  await expect(notice).toContainText("1 of 3 checks finished");
  await expect(notice).toContainText("Large jobs are switched off");
  await notice.getByRole("button", { name: "Open Batches" }).click();
  await expect(
    page.getByRole("dialog").getByText("Reports", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("dialog").getByRole("link", { name: "Export JSONL" }),
  ).toHaveAttribute("href", /job-one\/results\.jsonl$/);
  await page.keyboard.press("Escape");
  await notice.getByRole("button", { name: "Dismiss" }).click();
  await expect(notice).toHaveCount(0);
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Batches", exact: true }),
  ).toBeVisible();
  await expect(notice).toHaveCount(0);
});

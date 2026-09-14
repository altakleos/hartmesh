import { expect, test } from "@playwright/test";

import {
  mockLangGraphAPI,
  MOCK_RUN_ID,
  MOCK_THREAD_ID,
} from "./utils/mock-api";

test("inspects a policy stop and downloads its bounded evidence bundle", async ({
  page,
}) => {
  mockLangGraphAPI(page, {
    threads: [{ thread_id: MOCK_THREAD_ID, title: "Policy evidence" }],
  });
  await page.route(`**/api/threads/${MOCK_THREAD_ID}/runs`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify([
        { run_id: MOCK_RUN_ID, created_at: "2026-09-04T12:00:00Z" },
      ]),
    }),
  );
  await page.route(
    `**/api/threads/${MOCK_THREAD_ID}/runs/${MOCK_RUN_ID}/evidence`,
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema: "hartmesh.run-evidence-summary",
          schema_version: 1,
          overview: {
            run_ref: "run-safe-reference",
            thread_ref: "thread-safe-reference",
            status: "success",
            accepted_at: "2026-09-04T12:00:00Z",
            updated_at: "2026-09-04T12:01:00Z",
            terminal_reason: "repeated_tool_loop",
            policy: { profile: "interactive-v1", digest: "a".repeat(64) },
            completeness: "complete",
          },
          timeline: [
            {
              seq: 4,
              at: "2026-09-04T12:00:30Z",
              kind: "policy_decision",
              decision: "stop",
              reason_code: "repeated_tool_loop",
              current: 5,
              limit: 5,
              state_digest: "b".repeat(64),
            },
          ],
          sections: {
            policy: {
              state: "available",
              data: { decision_count: 1, counters: { turns: 3 } },
            },
            artifacts: {
              state: "not_applicable",
              data: { file_count: 0, bundle_state: "available" },
            },
            // An unknown future section: the UI must not dump its payload
            // into the DOM.
            internal_diagnostics: {
              state: "available",
              data: { payload: "raw-secret-must-not-render" },
            },
          },
          qualification: { state: "unverified" },
        }),
      }),
  );
  await page.route(
    `**/api/threads/${MOCK_THREAD_ID}/runs/${MOCK_RUN_ID}/artifacts/evidence-bundle`,
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/zip",
        headers: {
          "Content-Disposition": 'attachment; filename="run-evidence.zip"',
        },
        body: "bounded bundle",
      }),
  );

  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await page.getByTestId("evidence-trigger").click();
  await expect(
    page.getByRole("heading", { name: "Run evidence" }),
  ).toBeVisible();
  await expect(
    page.getByText("Repeated equivalent tool loop", { exact: true }),
  ).toBeVisible();
  await expect(page.getByLabel("Qualification: Unverified")).toBeVisible();
  await page.getByText("Execution policy").click();
  await expect(page.getByText("decision count")).toBeVisible();
  await page.getByRole("button", { name: "Download evidence bundle" }).click();
  await expect(page.getByText("raw-secret-must-not-render")).toHaveCount(0);
});

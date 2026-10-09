/** Real product UI, deterministic request APIs; SQL and native storage have separate gates. */
import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "../e2e/utils/mock-api";

const id = "a".repeat(32),
  instance = "b".repeat(32),
  work = "c".repeat(32);
test("Attention discovers addressed input without opening an AI employee and replies without a run", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        storage_spaces: { enabled: true },
      },
    }),
  );
  let row = {
    id,
    instance_id: instance,
    work_id: work,
    instance_name: "Documentation AI employee",
    work_objective: "Review guide",
    assignment_revision: 1,
    basis_id: "d".repeat(32),
    basis_revision: 1,
    revision: 1,
    request_revision: 1,
    purpose: "information",
    question: "Which guide should be checked?",
    reason: "A source is needed",
    expected_response: "A reference or explanation",
    choices: [],
    sources: [],
    state: "pending",
    closed_reason: null,
    recipient_id: "default",
    creator_id: "manager",
    needs_routing: false,
    read_revision: 0,
    can_respond: true,
    can_manage: true,
    can_recover_response: true,
    can_recover_management: true,
    updated_at: "2026-10-09T00:00:00Z",
  };
  const replies: object[] = [];
  let runs = 0;
  let overviews = 0;
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (request.method() === "POST" && /\/runs(?:\/|$)/.test(path)) runs++;
    if (path === "/workspace/instances") overviews++;
  });
  await page.route("**/api/agent-instances/**/work/**", (route) =>
    route.fulfill({
      json: {
        id: work,
        instance_id: instance,
        revision: row.revision,
        assignment_revision: 1,
        outcome: null,
      },
    }),
  );
  await page.route("**/api/spaces**", (route) =>
    route.fulfill({ json: { spaces: [] } }),
  );
  await page.route("**/api/human-input**", async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname.endsWith("/responses")) {
      if (method === "POST") {
        const body = route.request().postDataJSON() as {
          text: string;
          operation_id: string;
        };
        expect(body).not.toHaveProperty("action");
        replies.push({
          id: "e".repeat(32),
          actor_id: "default",
          request_revision: 1,
          text: body.text,
          disposition: "supplied",
          sources: [],
          created_at: row.updated_at,
        });
        row = { ...row, state: "answered", revision: row.revision + 1 };
        return route.fulfill({
          json: {
            ...row,
            receipt: {
              operation_id: body.operation_id,
              response_id: "e".repeat(32),
              actor_id: "default",
            },
          },
        });
      }
      return route.fulfill({
        json: {
          responses: url.searchParams.get("offset") === "100" ? [] : replies,
          has_more: false,
        },
      });
    }
    if (url.pathname.endsWith("/" + id)) return route.fulfill({ json: row });
    const view = url.searchParams.get("view") ?? "pending";
    return route.fulfill({
      json: {
        requests: view === row.state || view === "all" ? [row] : [],
        counts: {
          pending: row.state === "pending" ? 1 : 0,
          routing: 0,
          answered: row.state === "answered" ? 1 : 0,
        },
        has_more: false,
      },
    });
  });
  await page.goto("/workspace/chats");
  await page.getByRole("link", { name: /Attention/ }).click();
  await page
    .getByRole("button", { name: /Documentation AI employee.*Which guide/ })
    .click();
  await expect(page.getByRole("heading", { name: row.question })).toBeVisible();
  await page
    .getByLabel("Response", { exact: true })
    .fill("Use the revised installation guide.");
  await page.getByRole("button", { name: "Supply response" }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Saved response receipt" }),
  ).toBeVisible();
  await expect(
    page.getByText("Use the revised installation guide.", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: /Awaiting check \(1\)/ }),
  ).toBeVisible();
  expect(runs).toBe(0);
  expect(overviews).toBe(0);
  expect(replies).toHaveLength(1);
});

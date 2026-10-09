/** Browser interaction contract with deterministic HTTP fixtures.
 * Real SQL, worker settlement and native containment have separate backend gates.
 */
import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

test("a human reply starts no run; Open Work selects the exact assignment for explicit Resume", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  const instance = "a".repeat(32),
    work = "b".repeat(32),
    requestId = "c".repeat(32);
  const resident = {
    id: instance,
    name: "Documentation AI employee",
    principal: { kind: "nonhuman", subject_id: "agent:" + instance },
    custody: "company",
    owner_id: null,
    creator_id: "default",
    supervisor: { kind: "human", subject_id: "default" },
    definition_revision: "d".repeat(64),
    home_id: "e".repeat(32),
    status: "active",
    generation: 1,
    permissions: 7,
  };
  let record = {
    id: work,
    instance_id: instance,
    creator_id: "default",
    definition_revision: resident.definition_revision,
    objective: "Review the installation guide",
    success_criteria: "Explain the verified gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 1,
    revision: 2,
    status: "blocked",
    progress: "Source clarification needed",
    next_action: "Await human input",
    sources: [],
    review_state: "none",
    outcome: null as null | {
      id: string;
      statement: string;
      evidence_revision: number;
      sources: never[];
    },
    review: null,
    blocker: {
      id: requestId,
      revision: 1,
      kind: "information",
      question: "Which guide should I review?",
    } as object | null,
    attempt: {
      id: "f".repeat(32),
      status: "succeeded",
      thread_id: "work-chat",
    },
    execution_available: true,
    availability: "explicit_activation",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: "2026-10-09T00:00:00Z",
    updated_at: "2026-10-09T00:00:00Z",
    human_input_request_id: requestId,
  };
  let input = {
    id: requestId,
    instance_id: instance,
    work_id: work,
    instance_name: resident.name,
    work_objective: record.objective,
    assignment_revision: 1,
    basis_id: requestId,
    basis_revision: 1,
    revision: 1,
    request_revision: 1,
    purpose: "information",
    question: "Which guide should I review?",
    reason: "A source is needed",
    expected_response: "The guide name",
    choices: [],
    sources: [],
    state: "pending",
    closed_reason: null,
    recipient_id: "default",
    creator_id: resident.principal.subject_id,
    creator_kind: "nonhuman",
    needs_routing: false,
    read_revision: 0,
    can_respond: true,
    can_manage: true,
    can_recover_response: true,
    can_recover_management: true,
    updated_at: record.updated_at,
  };
  const responses: object[] = [];
  let activations = 0;
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        storage_spaces: { enabled: true },
      },
    }),
  );
  await page.route("**/api/spaces**", (route) =>
    route.fulfill({ json: { spaces: [] } }),
  );
  await page.route("**/api/agent-instances**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("/activate")) {
      const body = route.request().postDataJSON() as {
        thread_id: string;
        expected_revision: number;
      };
      expect(path).toContain(`/work/${work}/activate`);
      expect(body.thread_id).toBe("work-chat");
      expect(body.expected_revision).toBe(2);
      activations++;
      record = {
        ...record,
        revision: 3,
        status: "submitted",
        blocker: null,
        progress: "Guide reviewed",
        next_action: "Human review required",
        review_state: "pending",
        outcome: {
          id: "1".repeat(32),
          statement: "The guide gaps are documented",
          evidence_revision: 1,
          sources: [],
        },
      };
      return route.fulfill({ json: record });
    }
    if (path.endsWith("/work/" + work)) return route.fulfill({ json: record });
    if (path.endsWith("/work"))
      return route.fulfill({ json: { work: [record] } });
    if (path.endsWith("/definition"))
      return route.fulfill({
        json: {
          revision: resident.definition_revision,
          config: {
            name: "documentation",
            work_policy: { enabled: true, review_required: true },
          },
          soul: "Maintain documentation.",
        },
      });
    if (path.endsWith("/lifecycle"))
      return route.fulfill({ json: { operations: [] } });
    if (path.endsWith("/grants"))
      return route.fulfill({ json: { grants: [] } });
    if (path.endsWith("/memory"))
      return route.fulfill({
        status: 501,
        json: { detail: "Memory disabled in this fixture" },
      });
    if (path.endsWith("/" + instance)) return route.fulfill({ json: resident });
    return route.fulfill({ json: { instances: [resident] } });
  });
  await page.route("**/api/human-input**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("/responses")) {
      if (route.request().method() === "POST") {
        const body = route.request().postDataJSON() as {
          text: string;
          operation_id: string;
        };
        responses.push({
          id: "2".repeat(32),
          actor_id: "default",
          actor_kind: "human",
          text: body.text,
          disposition: "supplied",
          sources: [],
          request_revision: 1,
          created_at: record.updated_at,
        });
        input = { ...input, state: "answered", revision: 2 };
        return route.fulfill({
          json: {
            ...input,
            receipt: {
              operation_id: body.operation_id,
              response_id: "2".repeat(32),
              request_id: requestId,
            },
          },
        });
      }
      const offset = Number(
        new URL(route.request().url()).searchParams.get("offset") ?? "0",
      );
      return route.fulfill({
        json: {
          responses: responses.slice(offset, offset + 100),
          has_more: false,
        },
      });
    }
    if (path.endsWith("/history"))
      return route.fulfill({ json: { events: [], has_more: false } });
    if (path.endsWith("/" + requestId)) return route.fulfill({ json: input });
    return route.fulfill({
      json: {
        requests: [input],
        counts: {
          pending: input.state === "pending" ? 1 : 0,
          routing: 0,
          answered: responses.length,
          all: 1,
        },
        has_more: false,
      },
    });
  });
  await page.goto(`/workspace/attention?request=${requestId}`);
  await expect(
    page.getByRole("heading", { name: "Which guide should I review?" }),
  ).toBeVisible();
  await page
    .getByLabel("Response", { exact: true })
    .fill("Use the installation guide.");
  await page
    .getByRole("button", { name: "Supply response", exact: true })
    .click();
  await expect(
    page.getByText("Use the installation guide.", { exact: true }),
  ).toBeVisible();
  expect(activations).toBe(0);
  await page.getByRole("link", { name: "Open Work", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`instance=${instance}&work=${work}`));
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeVisible();
  expect(activations).toBe(0);
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  await expect(
    page.getByText("The guide gaps are documented", { exact: true }),
  ).toBeVisible();
  expect(activations).toBe(1);
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("link", { name: "Open execution conversation" }),
  ).toHaveAttribute("href", "/workspace/chats/work-chat");
});

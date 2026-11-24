import { expect, test, type Page } from "@playwright/test";

import { workInstance } from "../fixtures/agent-work";

import { mockLangGraphAPI } from "./utils/mock-api";

async function reviewProposal(
  page: Page,
  outcome: "receipt" | "lost" | "revoked",
) {
  const instance = { ...workInstance(), custody: "company", permissions: 7 };
  const base = "/api/agent-instances/" + instance.id;
  const id = "a".repeat(32);
  const candidate = "c".repeat(64);
  const current = instance.definition_revision;
  const generation = instance.generation;
  const writes: unknown[] = [];
  let adopted = false;
  let permissions = 7;
  const detailPath = base + "/proposals/" + id;
  let detailReads = 0;
  let retiredRead = false;
  page.on("requestfailed", (request) => {
    if (new URL(request.url()).pathname === detailPath)
      retiredRead = request.failure()?.errorText === "net::ERR_ABORTED";
  });
  let releaseDetail!: () => void;
  const paused = new Promise<void>((resolve) => {
    releaseDetail = resolve;
  });
  const proposal = () => ({
    id,
    instance_id: instance.id,
    employee_name: instance.name,
    field: "work_policy.enabled",
    base_value: false,
    proposed_value: true,
    current_value: adopted,
    base_generation: generation,
    current_generation: generation + Number(adopted),
    base_revision: current,
    current_revision: adopted ? candidate : current,
    candidate_digest: candidate,
    status: adopted ? "adopted" : "open",
    stale: false,
    adopt_blocked_reason: null,
    created_at: "2026-05-03T12:00:00Z",
    decided_at: adopted ? "2026-05-03T12:30:00Z" : null,
    decided_by: adopted ? "default" : null,
    resolved_by: null,
    effective_work_policy: {
      enabled: true,
      default_priority: "normal",
      review_required: true,
      allow_derived: true,
      max_derived_per_activation: 2,
      responsibilities: [{ key: "orders", label: "Customer orders" }],
    },
  });
  mockLangGraphAPI(page, { features: { agentInstancesEnabled: true } });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        agent_instances: {
          enabled: true,
          configuration_proposals: true,
          execution: "ready",
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === base,
    (route) =>
      route.fulfill({
        json: {
          ...instance,
          permissions,
          generation: generation + Number(adopted),
          definition_revision: adopted ? candidate : current,
        },
      }),
  );
  await page.route("**" + base + "/lifecycle", (route) =>
    route.fulfill({ json: { operations: [] } }),
  );
  await page.route("**" + base + "/definition", (route) =>
    route.fulfill({
      json: {
        revision: current,
        config: { name: "researcher" },
        soul: "Process orders",
      },
    }),
  );
  await page.route(
    (url) => url.pathname === base + "/proposals",
    (route) =>
      route.fulfill({ json: { proposals: [proposal()], next_cursor: null } }),
  );
  await page.route("**" + detailPath, async (route) => {
    detailReads++;
    if (outcome === "revoked") {
      await paused;
      return route.fulfill({ json: { ...proposal(), status: "adopted" } });
    }
    await route.fulfill({ json: proposal() });
  });
  await page.route("**" + base + "/proposals/" + id + "/adopt", (route) => {
    writes.push(route.request().postDataJSON());
    adopted = outcome !== "revoked";
    return outcome !== "receipt"
      ? route.abort("aborted")
      : route.fulfill({ json: proposal() });
  });
  await page.goto(
    "/workspace/instances?instance=" + instance.id + "&tab=settings",
  );
  const panel = page.getByRole("region", { name: "Configuration proposals" });
  await expect(panel).toContainText("At proposal time · Off");
  await expect(panel).toContainText("Proposed setting · On");
  await expect(panel).toContainText("up to 2 per activation");
  await expect(panel).toContainText("Customer orders");
  expect(writes).toHaveLength(0);
  await panel.getByRole("button", { name: "Adopt change" }).click();
  if (outcome !== "receipt") await expect.poll(() => detailReads).toBe(1);
  if (outcome === "revoked") {
    // Instance details poll current grants even while the recovery GET is in flight.
    permissions = 1;
    await expect(panel).toHaveCount(0, { timeout: 20_000 });
    releaseDetail();
    await expect.poll(() => retiredRead).toBe(true);
    expect(writes).toHaveLength(1);
    expect(detailReads).toBe(1);
    return;
  }
  await expect(panel.getByText("Adopted", { exact: true })).toBeVisible();
  await page.reload();
  await expect(panel.getByText("Adopted", { exact: true })).toBeVisible();
  await expect(panel).toContainText("At proposal time · Off");
  await expect(panel).toContainText("Proposed setting · On");
  await expect(panel).toContainText("Current setting · On");
  await expect(panel.getByRole("button", { name: "Adopt change" })).toHaveCount(
    0,
  );
  expect(writes).toEqual([
    {
      base_generation: generation,
      base_revision: current,
      candidate_digest: candidate,
    },
  ]);
}
test("manager can inspect, adopt, and reload an employee proposal", async ({
  page,
}) => {
  await reviewProposal(page, "receipt");
});

test("manager can recover a committed proposal after the write response is lost", async ({
  page,
}) => {
  await reviewProposal(page, "lost");
});

test("losing the employee grant retires an in-flight proposal status read", async ({
  page,
}) => {
  await reviewProposal(page, "revoked");
});

/** Real product rendering with deterministic instance API fixtures.
 * SQL authority and native containment are qualified by backend/native gates.
 */
import { expect, test } from "@playwright/test";

import { MOCK_THREAD_ID, mockLangGraphAPI } from "../e2e/utils/mock-api";

const id = "a".repeat(32),
  home = "b".repeat(32);
const resident = {
  id,
  name: "Resident analyst",
  custody: "company",
  owner_id: null,
  creator_id: "default",
  supervisor: { kind: "human", subject_id: "default" },
  principal: { kind: "nonhuman", subject_id: "agent:" + id },
  definition_revision: "d".repeat(64),
  generation: 1,
  status: "active",
  home_id: home,
  permissions: 7,
};

test("pending containment keeps its exact retry and blocks new conversations", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        storage_spaces: { enabled: true },
        browser_control: { enabled: true },
      },
    }),
  );
  let pending = false;
  let captured: unknown;
  await page.route("**/api/agent-instances**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("/lifecycle") && route.request().method() === "POST") {
      const body: unknown = route.request().postDataJSON();
      if (captured) expect(body).toEqual(captured);
      else captured = body;
      pending = true;
      return route.fulfill({
        status: 202,
        json: {
          instance: { ...resident, status: "suspended", generation: 2 },
          complete: false,
          operation_id: "fixture",
        },
      });
    }
    if (path.endsWith("/lifecycle"))
      return route.fulfill({
        json: {
          operations: pending
            ? [{ ...(captured as object), complete: false, retry: captured }]
            : [],
        },
      });
    if (path.endsWith("/definition"))
      return route.fulfill({
        json: {
          revision: resident.definition_revision,
          config: { name: "analyst" },
          soul: "Adopted company instructions",
        },
      });
    if (path.endsWith("/grants"))
      return route.fulfill({ json: { grants: [] } });
    if (path.endsWith("/memory"))
      return route.fulfill({
        status: 501,
        json: { detail: "Fixture backend has no memory scope" },
      });
    if (path.endsWith("/" + id))
      return route.fulfill({
        json: {
          ...resident,
          ...(pending ? { status: "suspended", generation: 2 } : {}),
        },
      });
    return route.fulfill({ json: { instances: [resident] } });
  });
  await page.goto(`/workspace/instances?instance=${id}`);
  await expect(
    page.getByRole("heading", { name: "Resident analyst" }),
  ).toBeVisible();
  await page.getByLabel("Confirm action", { exact: true }).check();
  await page.getByRole("button", { name: "Suspend", exact: true }).click();
  await expect(
    page.getByText(
      "Containment pending. The agent cannot run; stopping its environment is not yet confirmed.",
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Start conversation", exact: true }),
  ).toHaveCount(0);
  await page
    .getByRole("button", { name: "Retry captured operation", exact: true })
    .click();
  await expect(
    page.getByRole("link", { name: "Open Home", exact: true }),
  ).toHaveAttribute("href", `/workspace/spaces?space=${home}`);
});

test("bound chat shows canonical identity and Home and hides legacy uploads/browser", async ({
  page,
}) => {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Bound work",
        metadata: { agent_instance_id: "forged-client-value" },
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        storage_spaces: { enabled: true },
        browser_control: { enabled: true },
      },
    }),
  );
  await page.route(
    `**/api/agent-instances/conversations/${MOCK_THREAD_ID}/instance`,
    (route) => route.fulfill({ json: { instance: resident } }),
  );
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(
    page.getByRole("link", { name: "Resident analyst", exact: true }),
  ).toHaveAttribute("href", `/workspace/instances?instance=${id}`);
  await expect(
    page.getByRole("link", { name: "Open Home", exact: true }),
  ).toHaveAttribute("href", `/workspace/spaces?space=${home}`);
  await expect(page.getByTestId("add-attachments-button")).toHaveCount(0);
  await expect(page.getByTestId("browser-trigger")).toHaveCount(0);
  await expect(page.getByRole("textbox").first()).toBeVisible();
});

for (const outcome of ["bound", "failed", "unbound"] as const) {
  test(`legacy capabilities wait for a positive unbound result: ${outcome}`, async ({
    page,
  }) => {
    mockLangGraphAPI(page, {
      threads: [{ thread_id: MOCK_THREAD_ID, title: "Binding check" }],
    });
    await page.route("**/api/features", (route) =>
      route.fulfill({
        json: {
          agents_api: { enabled: true },
          storage_spaces: { enabled: true },
          browser_control: { enabled: true },
        },
      }),
    );
    let release!: () => void;
    let entered!: () => void;
    const requested = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    await page.route(
      `**/api/agent-instances/conversations/${MOCK_THREAD_ID}/instance`,
      async (route) => {
        entered();
        await ready;
        await route.fulfill(
          outcome === "failed"
            ? { status: 503, json: { detail: "Lookup unavailable" } }
            : { json: { instance: outcome === "bound" ? resident : null } },
        );
      },
    );
    let sidecarLookups = 0;
    page.on("request", (request) => {
      if (request.postData()?.includes("sidecar")) sidecarLookups++;
    });
    await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
    await requested;
    await expect(page.getByRole("textbox").first()).toBeVisible();
    await expect(page.getByTestId("add-attachments-button")).toHaveCount(0);
    await expect(page.getByTestId("browser-trigger")).toHaveCount(0);
    await expect(page.getByTestId("sidecar-header-trigger")).toHaveCount(0);
    expect(sidecarLookups).toBe(0);
    const draft = "Keep this draft through binding discovery";
    await page.getByRole("textbox").first().fill(draft);
    release();
    if (outcome === "unbound")
      await expect(page.getByTestId("add-attachments-button")).toBeVisible();
    else {
      if (outcome === "bound")
        await expect(
          page.getByRole("link", { name: "Resident analyst", exact: true }),
        ).toBeVisible();
      else
        await page.waitForResponse(
          (response) =>
            response.url().includes("/instance") && response.status() === 503,
        );
      await expect(page.getByTestId("add-attachments-button")).toHaveCount(0);
      await expect(page.getByTestId("browser-trigger")).toHaveCount(0);
      expect(sidecarLookups).toBe(0);
    }
    await expect(page.getByRole("textbox").first()).toHaveValue(draft);
    await expect(page.getByRole("textbox").first()).toBeFocused();
  });
}

import { expect, test } from "@playwright/test";

import {
  handleRunStream,
  mockLangGraphAPI,
  MOCK_THREAD_ID,
  MOCK_SIDECAR_THREAD_ID,
} from "./utils/mock-api";

const first = {
  id: "first",
  name: "first",
  model: "first",
  display_name: "First available",
  supports_thinking: false,
};
const preferred = {
  id: "preferred",
  name: "preferred",
  model: "preferred",
  display_name: "Agent preferred",
  supports_thinking: true,
};

for (const scenario of [
  {
    saved: "first",
    agentDefault: "preferred",
    expected: "first",
    thinking: false,
  },
  {
    saved: "removed",
    agentDefault: "preferred",
    expected: "preferred",
    thinking: true,
  },
  {
    saved: "removed",
    agentDefault: "also-removed",
    expected: "first",
    thinking: false,
  },
]) {
  test(`submits ${scenario.expected} for saved ${scenario.saved} and agent default ${scenario.agentDefault}`, async ({
    page,
  }) => {
    let submitted: Record<string, unknown> | undefined;
    let releaseAgent!: () => void;
    const agentReady = new Promise<void>((resolve) => {
      releaseAgent = resolve;
    });
    mockLangGraphAPI(page, {
      agents: [{ name: "catalog-agent", model: scenario.agentDefault }],
      runStreamHandler: async (route) => {
        submitted = (
          route.request().postDataJSON() as { context: Record<string, unknown> }
        ).context;
        return handleRunStream(route);
      },
    });
    await page.addInitScript(
      ({ saved }) =>
        localStorage.setItem(
          "deerflow.local-settings",
          JSON.stringify({ context: { model_name: saved, mode: "pro" } }),
        ),
      { saved: scenario.saved },
    );
    await page.route("**/api/models", (route) =>
      route.fulfill({
        json: { models: [first, preferred], token_usage: { enabled: false } },
      }),
    );
    await page.route("**/api/agents/catalog-agent", async (route) => {
      await agentReady;
      await route.fulfill({
        json: { name: "catalog-agent", model: scenario.agentDefault },
      });
    });
    await page.goto("/workspace/agents/catalog-agent/chats/new");
    const input = page.getByPlaceholder(/how can i assist you/i);
    await expect(input).toBeVisible();
    await input.fill("Use the currently available model");
    if (scenario.saved === "removed") {
      await expect(page.getByTestId("model-availability")).toContainText(
        "Loading models",
      );
      await input.press("Enter");
      await expect(input).toHaveValue("Use the currently available model");
      expect(submitted).toBeUndefined();
    }
    releaseAgent();
    await expect(page.getByTestId("model-availability")).toBeHidden();
    await input.press("Enter");
    await expect.poll(() => submitted?.model_name).toBe(scenario.expected);
    expect(submitted?.mode).toBe(scenario.thinking ? "pro" : "flash");
    expect(submitted?.thinking_enabled).toBe(scenario.thinking);
  });
}

test("preserves an empty-catalog draft and sends it after a key save refreshes models", async ({
  page,
}) => {
  let enabled = false;
  let tested = 0;
  const runs: Record<string, unknown>[] = [];
  mockLangGraphAPI(page, {
    runStreamHandler: async (route) => {
      runs.push(route.request().postDataJSON() as Record<string, unknown>);
      return handleRunStream(route);
    },
  });
  await page.route("**/api/models", (route) =>
    route.fulfill({
      json: { models: enabled ? [first] : [], token_usage: { enabled: false } },
    }),
  );
  const status = () => ({
    available: true,
    refusal: null,
    providers: [
      {
        provider: "openai",
        variable: "OPENAI_API_KEY",
        kind: "models",
        source: enabled ? "product" : "none",
        product_key: enabled ? "set" : "absent",
        changed_at: null,
        changed_by: null,
      },
    ],
  });
  await page.route("**/api/provider-keys", (route) =>
    route.fulfill({ json: status() }),
  );
  await page.route("**/api/provider-keys/events?*", (route) =>
    route.fulfill({ json: { events: [] } }),
  );
  await page.route("**/api/provider-keys/openai/test", (route) => {
    tested++;
    return route.fulfill({
      json: { result: "accepted", reason: null, model: "first" },
    });
  });
  await page.route("**/api/provider-keys/openai", (route) => {
    enabled = true;
    return route.fulfill({ json: { provider: status().providers[0] } });
  });
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await expect(page.getByTestId("model-availability")).toContainText(
    "No models are available",
  );
  await input.fill("Keep this draft until a model is available");
  await input.press("Enter");
  await expect(input).toHaveValue("Keep this draft until a model is available");
  expect(runs).toHaveLength(0);
  // Settings and the restored draft share the mounted model observer.
  await page.goto("/workspace/chats/new?settings=account");
  const dialog = page.getByRole("dialog", { name: "Settings" });
  await dialog.getByRole("button", { name: "Add key", exact: true }).click();
  await dialog.getByLabel("OpenAI key").fill("SYNTHETIC-TEST-KEY");
  await dialog.getByRole("button", { name: "Test key", exact: true }).click();
  await expect(
    dialog.getByText("The provider accepted this key. It has not been saved."),
  ).toBeVisible();
  expect(enabled).toBe(false);
  await dialog.getByRole("button", { name: "Save key", exact: true }).click();
  await expect(
    dialog.getByText("Saved. Your next message uses your OpenAI key."),
  ).toBeVisible();
  await dialog.getByRole("button", { name: "Close", exact: true }).click();
  await expect(page.getByTestId("model-availability")).toBeHidden();
  await expect(input).toHaveValue("Keep this draft until a model is available");
  await input.press("Enter");
  await expect.poll(() => runs.length).toBe(1);
  expect((runs[0]?.context as Record<string, unknown>).model_name).toBe(
    "first",
  );
  expect(tested).toBe(1);
});

test("distinguishes a failed catalog from empty and keeps the draft through retry", async ({
  page,
}) => {
  let fail = true;
  mockLangGraphAPI(page);
  await page.route("**/api/models", (route) =>
    route.fulfill(
      fail
        ? { status: 503, json: { detail: "Unavailable" } }
        : { json: { models: [first], token_usage: { enabled: false } } },
    ),
  );
  await page.goto("/workspace/chats/new");
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("Retain on failed model load");
  await expect(page.getByTestId("model-availability")).toContainText(
    "Models could not be loaded",
  );
  await expect(page.getByTestId("model-availability")).not.toContainText(
    "No models are available",
  );
  await input.press("Enter");
  await expect(input).toHaveValue("Retain on failed model load");
  fail = false;
  await page
    .getByRole("button", { name: "Reload models", exact: true })
    .click();
  await expect(page.getByTestId("model-availability")).toBeHidden();
  await expect(input).toHaveValue("Retain on failed model load");
});

test("allows goal status and clear with no models but preserves a new goal draft", async ({
  page,
}) => {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Existing conversation",
        messages: [],
      },
    ],
  });
  let writes = 0;
  let deletes = 0;
  await page.route("**/api/models", (route) =>
    route.fulfill({ json: { models: [], token_usage: { enabled: false } } }),
  );
  await page.route("**/api/threads/*/goal", (route) => {
    if (route.request().method() === "PUT") writes++;
    if (route.request().method() === "DELETE") deletes++;
    return route.fulfill({ json: { goal: null } });
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(page.getByTestId("model-availability")).toContainText(
    "No models are available",
  );
  const input = page.getByPlaceholder(/how can i assist you/i);
  await input.fill("/goal");
  await input.press("Escape");
  await input.press("Enter");
  await expect(input).toHaveValue("");
  await input.fill("/goal clear");
  await input.press("Enter");
  await expect.poll(() => deletes).toBe(1);
  await expect(input).toHaveValue("");
  await input.fill("/goal Build the report");
  await input.press("Enter");
  await expect(input).toHaveValue("/goal Build the report");
  expect(writes).toBe(0);
});

test("preserves a side-chat draft with no models and submits its resolved fallback after retry", async ({
  page,
}) => {
  let fail = true;
  let submitted: Record<string, unknown> | undefined;
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Main conversation",
        messages: [
          {
            type: "ai",
            id: "parent-ai",
            content: "A synthetic parent answer.",
          },
        ],
      },
      {
        thread_id: MOCK_SIDECAR_THREAD_ID,
        title: "Side conversation",
        metadata: { deerflow_sidecar: true, parent_thread_id: MOCK_THREAD_ID },
        messages: [
          { type: "ai", id: "side-ai", content: "A synthetic side answer." },
        ],
      },
    ],
    runStreamHandler: async (route) => {
      submitted = (
        route.request().postDataJSON() as { context: Record<string, unknown> }
      ).context;
      return handleRunStream(route);
    },
  });
  await page.route("**/api/models", (route) =>
    route.fulfill(
      fail
        ? { status: 503, json: { detail: "Unavailable" } }
        : { json: { models: [first], token_usage: { enabled: false } } },
    ),
  );
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await page.getByTestId("sidecar-header-trigger").click();
  const panel = page.getByTestId("sidecar-panel");
  const input = page.getByPlaceholder(/deeper follow-up/i);
  await expect(panel.getByTestId("sidecar-model-availability")).toContainText(
    "Models could not be loaded",
  );
  await input.fill("Keep the side draft");
  await input.press("Enter");
  await expect(input).toHaveValue("Keep the side draft");
  expect(submitted).toBeUndefined();
  fail = false;
  await panel
    .getByRole("button", { name: "Reload models", exact: true })
    .click();
  await expect(panel.getByTestId("sidecar-model-availability")).toBeHidden();
  await expect(input).toHaveValue("Keep the side draft");
  await input.press("Enter");
  await expect.poll(() => submitted?.model_name).toBe("first");
  expect(submitted?.mode).toBe("flash");
  expect(submitted?.thread_id).toBe(MOCK_SIDECAR_THREAD_ID);
});

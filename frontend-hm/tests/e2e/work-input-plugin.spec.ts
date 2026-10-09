/** Real optional package UI over deterministic HTTP; SQL/facade tests are separate. */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const source = readFileSync(
  resolve(
    "../examples/deerflow-extension-work-input/deerflow_extension_work_input/static/index.mjs",
  ),
  "utf-8",
);
const requestId = "a".repeat(32);
async function install(
  page: Page,
  options: { enabled?: boolean; broken?: boolean } = {},
) {
  mockLangGraphAPI(page);
  await page.route("**/api/plugins", (route) =>
    route.fulfill({
      json: [
        {
          viewer_id: "default",
          namespace: "example.work-input",
          module: "work-input.v1",
          entry: `/api/plugins/modules/work-input.v1/${"c".repeat(64)}.mjs`,
          transport: "inline-v1",
          title: "Quote response example",
          description: "",
          settings: { enabled: options.enabled !== false },
          backend_actions: ["get", "respond"],
        },
      ],
    }),
  );
  await page.route("**/api/plugins/modules/work-input.v1/*.mjs", (route) =>
    route.fulfill({
      contentType: "text/javascript",
      body: options.broken
        ? 'throw new Error("Example renderer unavailable")'
        : source,
    }),
  );
  await page.route("**/api/plugins/example.work-input/actions/get", (route) =>
    route.fulfill({
      json: {
        id: route.request().postDataJSON().request_id as string,
        question: "Supply the missing quote details",
        reason: "Needed for the comparison",
        purpose: "information",
        can_respond: true,
        state: "pending",
        request_revision: 1,
        assignment_revision: 1,
      },
    }),
  );
  await page.route("**/api/human-input**", (route) =>
    route.fulfill({
      json: {
        requests: [],
        counts: { pending: 0, routing: 0, answered: 0 },
        has_more: false,
      },
    }),
  );
}

async function load(page: Page) {
  await page.goto("/workspace/extensions/example.work-input/quote");
  await page.getByLabel("Request ID", { exact: true }).fill(requestId);
  await page.getByRole("button", { name: "Load request", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Supply response", exact: true }),
  ).toBeEnabled();
}

test("invalid quote remains editable and changing request identity requires reload", async ({
  page,
}) => {
  await install(page);
  await page.route(
    "**/api/plugins/example.work-input/actions/respond",
    (route) =>
      route.fulfill({
        json: { status: "invalid", message: "Invalid currency" },
      }),
  );
  await load(page);
  await page
    .getByRole("button", { name: "Supply response", exact: true })
    .click();
  await expect(
    page.getByText(/Nothing was submitted; correct the fields/),
  ).toBeVisible();
  await expect(page.getByLabel("Currency", { exact: true })).toBeEnabled();
  await page.getByLabel("Request ID", { exact: true }).fill("b".repeat(32));
  await expect(
    page.getByRole("button", { name: "Supply response", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByText(
      "Supply the missing quote details — Needed for the comparison",
    ),
  ).toHaveCount(0);
});

test("lost response retries the exact captured request and submission starts no run", async ({
  page,
}) => {
  await install(page);
  const bodies: unknown[] = [];
  let runs = 0;
  await page.route(/\/api\/.*(?:runs|activate)/, (route) => {
    runs++;
    return route.fulfill({ status: 500 });
  });
  await page.route(
    "**/api/plugins/example.work-input/actions/respond",
    (route) => {
      bodies.push(route.request().postDataJSON());
      return bodies.length === 1
        ? route.abort("failed")
        : route.fulfill({
            json: {
              status: "supplied",
              validation: "Field format checked only.",
            },
          });
    },
  );
  await load(page);
  await page.getByLabel("Supplier", { exact: true }).fill("Northwind");
  await page
    .getByLabel("Delivery date (YYYY-MM-DD)", { exact: true })
    .fill("2026-11-10");
  await page.getByLabel("Currency", { exact: true }).fill("EUR");
  await page.getByLabel("Quoted total", { exact: true }).fill("125.50");
  await page
    .getByRole("button", { name: "Supply response", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Retry exact response", exact: true }),
  ).toBeEnabled();
  await expect(page.getByLabel("Supplier", { exact: true })).toBeDisabled();
  await expect(
    page.getByRole("link", { name: "Open generic Attention controls" }),
  ).toHaveAttribute("href", `/workspace/attention?request=${requestId}`);
  await page
    .getByRole("button", { name: "Retry exact response", exact: true })
    .click();
  await expect(
    page.getByText(/Response supplied — awaiting check/),
  ).toBeVisible();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toEqual(bodies[0]);
  expect(runs).toBe(0);
});

for (const mode of ["disabled", "failing"] as const) {
  test(`generic Attention remains available with ${mode} optional UI`, async ({
    page,
  }) => {
    await install(page, {
      enabled: mode !== "disabled",
      broken: mode === "failing",
    });
    await page.goto("/workspace/extensions/example.work-input/quote");
    await expect(
      page.getByRole("button", { name: "Supply response", exact: true }),
    ).toHaveCount(0);
    await page.goto("/workspace/attention");
    await expect(
      page.getByRole("heading", { name: "Attention", exact: true }),
    ).toBeVisible();
  });
}

test("switching from a factual request to a decision keeps fallback bound to the displayed request", async ({
  page,
}) => {
  await install(page);
  await load(page);
  const next = "b".repeat(32);
  await page.route("**/api/plugins/example.work-input/actions/get", (route) =>
    route.fulfill({
      json: {
        id: next,
        question: "Choose scope",
        reason: "Manager decision required",
        purpose: "decision",
        can_respond: true,
        state: "pending",
      },
    }),
  );
  await page.getByLabel("Request ID", { exact: true }).fill(next);
  await expect(
    page.getByRole("link", { name: "Open generic Attention controls" }),
  ).toHaveAttribute("href", "/workspace/attention");
  await page.getByRole("button", { name: "Load request", exact: true }).click();
  await expect(
    page.getByText("Choose scope — Manager decision required"),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Supply response", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByRole("link", { name: "Open generic Attention controls" }),
  ).toHaveAttribute("href", `/workspace/attention?request=${next}`);
});

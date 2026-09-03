import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

function mockMcpConfig(page: Parameters<typeof mockLangGraphAPI>[0]) {
  return page.route("**/api/mcp/config", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        mcp_servers: {
          github: {
            enabled: true,
            type: "http",
            url: "https://example.test/mcp",
            description: "GitHub tools",
          },
        },
      }),
    }),
  );
}

async function mockGovernedStatus(
  page: Parameters<typeof mockLangGraphAPI>[0],
) {
  await page.route("**/api/tool-plane/status?**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        version: 1,
        scope: { version: 1, kind: "deployment_base" },
        governance_state: "governed",
        active_revision_id: "revision-1",
        active_revision_digest: "a".repeat(64),
        generation: 3,
        projection_digest: "a".repeat(64),
        drift: false,
        immutable: false,
        durable: true,
        validation_policy_digest: "b".repeat(64),
      }),
    }),
  );
  await page.route("**/api/tool-plane/revisions?**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        version: 1,
        scope: { version: 1, kind: "deployment_base" },
        revisions: [],
      }),
    }),
  );
}

test.describe("governed tool-plane settings", () => {
  test("shows safe actor/time evidence and disables legacy MCP controls", async ({
    page,
  }) => {
    mockLangGraphAPI(page);
    await mockMcpConfig(page);
    await mockGovernedStatus(page);
    await page.route("**/api/tool-plane/revisions?**", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          version: 1,
          scope: { version: 1, kind: "deployment_base" },
          revisions: [
            {
              version: 1,
              revision_id: "revision-1",
              revision_digest: "a".repeat(64),
              scope: { version: 1, kind: "deployment_base" },
              content_digest: "c".repeat(64),
              state: "promoted",
              staging_actor_digest: "d".repeat(64),
              staged_at: "2026-09-03T12:00:00Z",
              promotion_actor_digest: "e".repeat(64),
              promoted_at: "2026-09-03T12:01:00Z",
            },
          ],
        }),
      }),
    );

    await page.goto("/workspace/chats/new?settings=tools");

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog.getByText("Governed revisions")).toBeVisible();
    await expect(dialog.getByText("dddddddddddd…")).toBeVisible();
    await expect(dialog.getByText("eeeeeeeeeeee…")).toBeVisible();
    await expect(dialog.getByRole("switch")).toBeDisabled();
    await expect(
      dialog.getByRole("button", { name: "Add server" }),
    ).toBeDisabled();
  });

  test("keeps controls disabled when governance requests fail unexpectedly", async ({
    page,
  }) => {
    mockLangGraphAPI(page);
    await mockMcpConfig(page);
    await page.route("**/api/tool-plane/**", (route) =>
      route.fulfill({
        status: 500,
        contentType: "application/json",
        body: JSON.stringify({
          detail: {
            code: "internal_error",
            message: "Governance lookup failed",
          },
        }),
      }),
    );

    await page.goto("/workspace/chats/new?settings=tools");

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog.getByText("Revision governance status is unavailable")).toBeVisible();
    await expect(dialog.getByRole("switch")).toBeDisabled();
  });

  test("allows legacy controls only for the explicit unavailable response", async ({
    page,
  }) => {
    mockLangGraphAPI(page);
    await mockMcpConfig(page);

    await page.goto("/workspace/chats/new?settings=tools");

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog.getByText("Governed revisions")).toHaveCount(0);
    await expect(dialog.getByRole("switch")).toBeEnabled();
    await expect(
      dialog.getByRole("button", { name: "Add server" }),
    ).toBeEnabled();
  });

  test("disables public and custom skill mutations under governance", async ({
    page,
  }) => {
    mockLangGraphAPI(page, {
      skills: [
        {
          name: "public-helper",
          description: "A public skill",
          category: "public",
          enabled: true,
        },
        {
          name: "custom-helper",
          description: "A custom skill",
          category: "custom",
          enabled: true,
        },
      ],
    });
    await mockGovernedStatus(page);

    await page.goto("/workspace/chats/new?settings=skills");

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog.getByText("Governed revisions")).toBeVisible();
    await expect(
      dialog.getByRole("button", { name: "Install .skill" }),
    ).toBeDisabled();
    await expect(
      dialog.getByRole("button", { name: "Create skill" }),
    ).toBeDisabled();
    await expect(dialog.getByRole("switch")).toBeDisabled();

    await dialog.getByRole("tab", { name: "Custom" }).click();
    await expect(dialog.getByText("custom-helper")).toBeVisible();
    await expect(dialog.getByRole("switch")).toBeDisabled();
  });

  test("disables managed integration installation under governance", async ({
    page,
  }) => {
    mockLangGraphAPI(page);
    await mockGovernedStatus(page);

    await page.goto("/workspace/chats/new?settings=integrations");

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog.getByText("Governed revisions")).toBeVisible();
    await expect(
      dialog.getByRole("button", { name: "Install", exact: true }),
    ).toBeDisabled();
  });
});

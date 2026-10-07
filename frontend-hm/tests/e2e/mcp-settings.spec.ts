import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

test.describe("MCP server settings", () => {
  test("edits one server without dropping advanced fields or siblings", async ({
    page,
  }) => {
    mockLangGraphAPI(page);

    let servers = {
      local: {
        enabled: true,
        description: "Local tools",
        command: "uvx",
        args: ["local-tools"],
      },
      remote: {
        enabled: false,
        description: "Remote tools",
        type: "http",
        url: "https://example.test/mcp",
        headers: { "X-API-Key": "***" },
        routing: { mode: "prefer" },
      },
    };
    let submittedUpdate:
      | { server_name: string; server: (typeof servers)["remote"] }
      | undefined;

    await page.route("**/api/mcp/config", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ mcp_servers: servers }),
      });
    });
    await page.route("**/api/mcp/config/server", async (route) => {
      if (route.request().method() !== "PUT") {
        await route.fallback();
        return;
      }
      submittedUpdate = route
        .request()
        .postDataJSON() as typeof submittedUpdate;
      servers = {
        ...servers,
        [submittedUpdate!.server_name]: submittedUpdate!.server,
      };
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ mcp_servers: servers }),
      });
    });

    await page.goto("/workspace/chats/new?settings=tools");

    const settingsDialog = page.getByRole("dialog", { name: "Settings" });
    await expect(settingsDialog).toBeVisible();
    await settingsDialog.getByRole("button", { name: "Edit remote" }).click();

    const editor = page.getByRole("dialog", { name: "Edit MCP server" });
    const definitionBox = editor.getByRole("textbox");
    const definition = JSON.parse(await definitionBox.inputValue()) as {
      mcpServers: typeof servers;
    };
    definition.mcpServers.remote.description = "Updated remote tools";
    await definitionBox.fill(JSON.stringify(definition));
    await editor.getByRole("button", { name: "Save" }).click();

    await expect(editor).toBeHidden();
    await expect(
      settingsDialog.getByText("Updated remote tools"),
    ).toBeVisible();
    expect(submittedUpdate).toEqual({
      server_name: "remote",
      server: {
        enabled: false,
        description: "Updated remote tools",
        type: "http",
        url: "https://example.test/mcp",
        headers: { "X-API-Key": "***" },
        routing: { mode: "prefer" },
      },
    });
  });
});

for (const delegated of [false, true]) {
  test(`local controls follow effective delegation: ${delegated}`, async ({
    page,
  }) => {
    mockLangGraphAPI(page);
    await page.route("**/api/features", async (route) =>
      route.fulfill({
        json: {
          agents_api: { enabled: false },
          ui: { profile: "developer", starters: [] },
          customer_administration: { local_mcp_management: delegated },
        },
      }),
    );
    await page.route("**/api/mcp/config", async (route) =>
      route.fulfill({
        json: {
          mcp_servers: {
            local: { enabled: true, command: "uvx", args: ["fixture"] },
            remote: {
              enabled: true,
              type: "http",
              url: "https://example.test/mcp",
            },
          },
        },
      }),
    );
    await page.goto("/workspace/chats/new?settings=tools");
    const settings = page.getByRole("dialog", { name: "Settings" });
    await expect(settings).toBeVisible();
    const localEdit = settings.getByRole("button", { name: "Edit local" });
    if (delegated) await expect(localEdit).toBeEnabled();
    else {
      await expect(localEdit).toBeDisabled();
      await expect(
        settings.getByText("Customization requires provider enablement."),
      ).toBeVisible();
    }
    await expect(
      settings.getByRole("button", { name: "Edit remote" }),
    ).toBeEnabled();
  });
}

test("closes local management after effective discovery becomes unavailable", async ({
  page,
}) => {
  mockLangGraphAPI(page);
  let unavailable = false;
  let failedReads = 0;
  await page.route("**/api/features", async (route) => {
    if (unavailable) {
      failedReads += 1;
      await route.fulfill({ status: 503, json: { detail: "Unavailable" } });
    } else
      await route.fulfill({
        json: {
          agents_api: { enabled: false },
          ui: { profile: "developer", starters: [] },
          customer_administration: { local_mcp_management: true },
        },
      });
  });
  await page.route("**/api/mcp/config", async (route) =>
    route.fulfill({
      json: {
        mcp_servers: {
          local: { enabled: true, command: "uvx", args: ["fixture"] },
          remote: {
            enabled: true,
            type: "http",
            url: "https://example.test/mcp",
          },
        },
      },
    }),
  );
  await page.goto("/workspace/chats/new?settings=tools");
  const settings = page.getByRole("dialog", { name: "Settings" });
  const localEdit = settings.getByRole("button", { name: "Edit local" });
  await expect(localEdit).toBeEnabled();
  unavailable = true;
  await page.evaluate(() =>
    window.dispatchEvent(new Event("visibilitychange")),
  );
  await expect.poll(() => failedReads).toBeGreaterThan(0);
  await expect(localEdit).toBeDisabled({ timeout: 10000 });
  await expect(
    settings.getByRole("button", { name: "Edit remote" }),
  ).toBeEnabled();
});

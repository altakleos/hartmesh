import { expect, test } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

for (const entry of [
  {
    locale: "en-US",
    settings: "Settings",
    personal:
      "Personal chats, custom agents and schedules use their configured connections.",
    grants:
      "Personal AI assistants and AI employees use the company connections granted on their Access tab.",
    final: "the final reply for an unattended large job",
  },
  {
    locale: "zh-CN",
    settings: "设置",
    personal: "个人对话、自定义智能体和定时任务使用各自配置的连接。",
    grants:
      "个人 AI 助理和 AI 员工使用在各自的“访问权限”选项卡中授予的公司连接。",
    final: "无人值守的大型任务的最终回复",
  },
] as const) {
  test(`explains connection grants in Settings in ${entry.locale}`, async ({
    page,
    baseURL,
  }) => {
    await page
      .context()
      .addCookies([{ name: "locale", value: entry.locale, url: baseURL! }]);
    mockLangGraphAPI(page, { features: { agentInstancesEnabled: true } });
    await page.route("**/api/features", (route) =>
      route.fulfill({
        json: {
          agent_instances: {
            enabled: true,
            execution: "ready",
            connections: true,
          },
        },
      }),
    );
    await page.route("**/api/mcp/config", (route) =>
      route.fulfill({ json: { mcp_servers: {} } }),
    );
    await page.goto("/workspace/chats/new?settings=tools");
    const settings = page.getByRole("dialog", { name: entry.settings });
    await expect(settings).toContainText(entry.personal);
    await expect(settings).toContainText(entry.grants);
    await expect(settings).toContainText(entry.final);
  });
}

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

test("saves and checks a remote connection, then edits its description without replacing the stored token", async ({
  page,
}, info) => {
  mockLangGraphAPI(page);
  const stored: Record<string, Record<string, unknown>> = {};
  const sequence: string[] = [];
  await page.route("**/api/mcp/config", (route) =>
    route.fulfill({ json: { mcp_servers: stored } }),
  );
  await page.route("**/api/mcp/config/servers", (route) => {
    const payload = route.request().postDataJSON() as {
      mcp_servers: Record<string, Record<string, unknown>>;
    };
    expect(payload.mcp_servers.notes?.headers).toEqual({
      Authorization: "Bearer example",
    });
    stored.notes = {
      ...payload.mcp_servers.notes,
      headers: { Authorization: "***" },
      session_init_timeout: null,
      extra_setting: "preserved",
    };
    sequence.push("create");
    return route.fulfill({ json: { mcp_servers: stored } });
  });
  await page.route("**/api/mcp/config/server", (route) => {
    const payload = route.request().postDataJSON() as {
      server_name: string;
      server: Record<string, unknown>;
    };
    expect(payload).toEqual({
      server_name: "notes",
      server: { ...stored.notes, description: "Company notes" },
    });
    stored.notes = payload.server;
    sequence.push("edit");
    return route.fulfill({ json: { mcp_servers: stored } });
  });
  await page.route("**/api/mcp/config/servers/notes/check", (route) => {
    expect(route.request().postData()).toBeNull();
    sequence.push("check");
    return route.fulfill({
      json:
        sequence.length === 2
          ? { status: "failed", reason: "refused" }
          : { status: "connected", tool_count: 2 },
    });
  });
  await page.goto("/workspace/chats/new?settings=tools");
  const settings = page.getByRole("dialog", { name: "Settings" });
  await settings.getByRole("button", { name: "Add connection" }).click();
  const form = page.getByRole("dialog", { name: "Add connection" });
  await form.getByLabel("Name", { exact: true }).fill("notes");
  await form
    .getByLabel("Address", { exact: true })
    .fill("https://mcp.example.com/notes");
  await form.getByLabel("Token (optional)", { exact: true }).fill("example");
  await info.attach("connection-form", {
    body: await form.screenshot(),
    contentType: "image/png",
  });
  await form.getByRole("button", { name: "Save and check" }).click();
  await expect(settings.getByRole("status")).toContainText(
    "Saved. The connection was refused.",
  );
  await settings.getByRole("button", { name: "Edit notes" }).click();
  const edit = page.getByRole("dialog", { name: "Edit connection" });
  await expect(edit.getByLabel("Name", { exact: true })).toBeDisabled();
  await edit
    .getByLabel("Description (optional)", { exact: true })
    .fill("Company notes");
  await edit.getByRole("button", { name: "Save and check" }).click();
  await expect(settings.getByRole("status")).toContainText(
    "Saved. Connected. 2 tools.",
  );
  expect(sequence).toEqual(["create", "check", "edit", "check"]);
});

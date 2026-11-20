import { expect, test } from "@playwright/test";

import { workInstance } from "../fixtures/agent-work";

import { mockLangGraphAPI, MOCK_THREAD_ID } from "./utils/mock-api";

test("a manager can review the audience, give a connection, and remove it", async ({
  page,
}, testInfo) => {
  const agent = { ...workInstance(), permissions: 4 };
  const base = "/api/agent-instances/" + agent.id;
  mockLangGraphAPI(page, { features: { agentInstancesEnabled: true } });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        agent_instances: {
          enabled: true,
          execution: "ready",
          connections: true,
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === base,
    (route) => route.fulfill({ json: agent }),
  );
  await page.route("**" + base + "/lifecycle", (route) =>
    route.fulfill({ json: { operations: [] } }),
  );
  let granted = false;
  const writes: string[] = [];
  await page.route("**" + base + "/connections", (route) =>
    route.fulfill({
      json: {
        chat_member_count: 3,
        connections: [
          {
            id: "notes",
            name: "Notes",
            host: "notes.example.test",
            description: "Weekly schedules",
            given: granted,
            given_at: granted ? "2026-01-15T10:00:00Z" : null,
            given_by: null,
            reason: null,
            can_give: true,
          },
        ],
      },
    }),
  );
  await page.route("**" + base + "/connections/notes", (route) => {
    const method = route.request().method();
    writes.push(method);
    granted = method === "PUT";
    return route.fulfill({ status: 204 });
  });
  await page.goto("/workspace/instances?instance=" + agent.id + "&tab=access");
  const control = page.getByRole("switch", { name: "Notes", exact: true });
  await expect(control).toBeVisible();
  await control.click();
  const dialog = page.getByRole("dialog", {
    name: "Give Notes to Researcher?",
  });
  await expect(dialog).toContainText("3 people can chat");
  await expect(dialog).toContainText("Shared folder");
  expect(writes).toEqual([]);
  const photo = testInfo.outputPath("give-connection.png");
  await page.screenshot({ path: photo });
  await testInfo.attach("Review connection access", {
    path: photo,
    contentType: "image/png",
  });
  await dialog
    .getByRole("button", { name: "Give connection", exact: true })
    .click();
  await expect(control).toBeChecked();
  const disclosure = page.getByRole("button", {
    name: /Researcher can use: Notes/,
  });
  await disclosure.click();
  await expect(page.getByRole("dialog", { name: "Connections" })).toContainText(
    "without asking",
  );
  await page.keyboard.press("Escape");
  await control.click();
  await expect(control).not.toBeChecked();
  await expect(disclosure).toHaveCount(0);
  expect(writes).toEqual(["PUT", "DELETE"]);
});

test("a chat member sees active connections on the employee and conversation, including a manager change while reading", async ({
  page,
}, testInfo) => {
  const agent = { ...workInstance(), permissions: 1 };
  const base = "/api/agent-instances/" + agent.id;
  let enabled = true,
    names = ["Weekly notes"],
    reads = 0;
  mockLangGraphAPI(page, {
    features: { agentInstancesEnabled: true },
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Weekly schedule",
        messages: [
          { id: "note-1", type: "human", content: "Show the weekly notes." },
        ],
        agent_instance: {
          id: agent.id,
          name: agent.name,
          custody: agent.custody,
          status: agent.status,
          requester_id: "default",
        },
      },
    ],
  });
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        agents_api: { enabled: true },
        agent_instances: {
          enabled: true,
          execution: "ready",
          connections: enabled,
        },
      },
    }),
  );
  await page.route(
    (url) => url.pathname === base,
    (route) => route.fulfill({ json: agent }),
  );
  await page.route("**" + base + "/connections", (route) => {
    reads++;
    return route.fulfill({
      json: {
        connections: names.map((name) => ({
          name,
          host: "notes.example.test",
        })),
      },
    });
  });
  await page.goto("/workspace/instances?instance=" + agent.id);
  await expect(
    page.getByRole("button", { name: /Researcher can use: Weekly notes/ }),
  ).toBeVisible();
  await expect(
    page.getByRole("tab", { name: "Access", exact: true }),
  ).toHaveCount(0);
  await page.clock.install();
  await page.goto("/workspace/chats/" + MOCK_THREAD_ID);
  const active = page.getByRole("button", { name: /Researcher can use:/ });
  await expect(active).toContainText("Weekly notes");
  const photo = testInfo.outputPath("chat-connections.png");
  await page.screenshot({ path: photo });
  await testInfo.attach("Chat member sees current connections", {
    path: photo,
    contentType: "image/png",
  });
  // Keep this tab open while another person changes the grant.
  names = ["Team calendar"];
  await page.clock.fastForward(31_000);
  await expect(active).toContainText("Team calendar");
  names = [];
  await page.clock.fastForward(31_000);
  await expect(active).toHaveCount(0);
  enabled = false;
  names = ["Hidden connection"];
  const before = reads;
  await page.reload();
  await expect(
    page.getByText("Show the weekly notes.", { exact: true }),
  ).toBeVisible();
  await expect(active).toHaveCount(0);
  expect(reads).toBe(before);
});

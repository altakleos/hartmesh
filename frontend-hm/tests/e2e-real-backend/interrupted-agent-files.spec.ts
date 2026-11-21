import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";

import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Route,
} from "@playwright/test";

import { agent, MOCK_THREAD_ID, setupAgents } from "./utils/compose-agents";

let child: ChildProcess;
let directory: string;
let gateway: { origin: string; instance: string; home: string };
const cookie = { cookie: "access_token=fixture" };
const op = () => randomUUID().replaceAll("-", "");

async function startGateway() {
  const backend = path.resolve("../backend");
  child = spawn(
    path.join(backend, ".venv/bin/python"),
    ["tests/agent_file_browser_gateway.py", MOCK_THREAD_ID, directory],
    {
      cwd: backend,
      env: {
        ...process.env,
        PYTHONPATH: backend,
        DEER_FLOW_HOME: directory,
        AUTH_JWT_SECRET: "",
      },
      stdio: ["ignore", "pipe", "inherit"],
    },
  );
  const lines = createInterface({ input: child.stdout! });
  let started = false;
  try {
    for await (const line of lines) {
      if (line.startsWith("{")) {
        gateway = JSON.parse(line) as typeof gateway;
        started = true;
        break;
      }
    }
    expect(started).toBeTruthy();
    expect(gateway.origin).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
  } finally {
    lines.close();
  }
}
async function stopGateway() {
  if (child?.exitCode == null && child?.signalCode == null) {
    const stopped = once(child, "exit");
    child.kill("SIGTERM");
    await stopped;
  }
}
test.beforeAll(async () => {
  directory = await mkdtemp(path.join(tmpdir(), "agent-file-browser-"));
  await startGateway();
});
test.afterAll(async () => {
  await stopGateway();
  if (directory) await rm(directory, { recursive: true, force: true });
});
test.afterEach(async ({ page }) => {
  // A completed file action invalidates the listing in the background. Once
  // the test page retires, Playwright may already have cancelled that read.
  await page.unrouteAll({ behavior: "ignoreErrors" });
});

function homeURL() {
  return `${gateway.origin}/api/agent-instances/${gateway.instance}/home`;
}
async function seed(
  request: APIRequestContext,
  name: string,
  data: string,
  expected?: string,
) {
  const response = await request.put(`${homeURL()}/content`, {
    headers: cookie,
    data,
    params: {
      path: name,
      operation_id: op(),
      ...(expected ? { expected_sha256: expected } : {}),
    },
  });
  expect(response.ok(), await response.text()).toBeTruthy();
  return ((await response.json()) as { sha256: string }).sha256;
}
async function stored(request: APIRequestContext, name: string) {
  const response = await request.get(
    `${gateway.origin}/api/spaces/${gateway.home}/text`,
    { headers: cookie, params: { path: name } },
  );
  expect(response.ok(), await response.text()).toBeTruthy();
  return response.json() as Promise<{ text: string; sha256: string }>;
}
async function wire(
  page: Page,
  intercept: (route: Route, url: string) => Promise<void>,
) {
  // Only page chrome and chat history use the shared fixture. Every tested
  // file read, write and status response uses the gateway's real SQL and Home.
  await setupAgents(page, {
    instance: agent({ id: gateway.instance, home_id: gateway.home }),
  });
  page.setDefaultNavigationTimeout(30_000);
  await page.route("**/api/spaces/**", async (route) => {
    const url = new URL(route.request().url());
    await route.fulfill({
      response: await route.fetch({
        url: gateway.origin + url.pathname + url.search,
        headers: { ...route.request().headers(), ...cookie },
      }),
    });
  });
  await page.route("**/api/agent-instances/**", async (route) => {
    const url = new URL(route.request().url());
    if (
      !/\/(home\/operations(?:\/[^/]+)?|conversations\/[^/]+\/files\/operations(?:\/[^/]+)?)$/.test(
        url.pathname,
      )
    )
      return route.fallback();
    await intercept(route, gateway.origin + url.pathname + url.search);
  });
}
async function files(page: Page) {
  await page.goto(
    `/workspace/instances?instance=${gateway.instance}&tab=files`,
    { waitUntil: "domcontentloaded" },
  );
  await expect(page.getByRole("button", { name: "New folder" })).toBeVisible({
    timeout: 30_000,
  });
}

test("checking a lost rename or delete leaves the current file in place", async ({
  page,
  request,
}) => {
  let revision = await seed(request, "current.txt", "original");
  let interrupted = "",
    writes = 0;
  const reads: string[] = [];
  await wire(page, async (route, url) => {
    const sent = route.request();
    if (sent.method() === "POST") {
      const body = sent.postDataJSON() as {
        action: string;
        operation_id: string;
      };
      interrupted = body.operation_id;
      writes++;
      revision = await seed(
        request,
        "current.txt",
        `replacement-${writes}`,
        revision,
      );
      if (body.action === "rename") await route.abort("connectionreset");
      else await route.fulfill({ status: 502, body: "unavailable" });
    } else {
      expect(sent.method()).toBe("GET");
      expect(sent.postData()).toBeNull();
      expect(url).toContain(`/home/operations/${interrupted}`);
      reads.push(url);
      await route.fulfill({
        response: await route.fetch({
          url,
          headers: { ...sent.headers(), ...cookie },
        }),
      });
    }
  });
  await files(page);
  for (const [index, action] of ["Rename", "Delete"].entries()) {
    const row = page.getByRole("row").filter({ hasText: "current.txt" });
    await row.getByRole("button", { name: action, exact: true }).click();
    const dialog = page.getByRole("dialog");
    if (action === "Rename")
      await dialog.getByRole("textbox").fill("moved.txt");
    await dialog.getByRole("button", { name: action, exact: true }).click();
    await expect(page.getByText("No result is recorded yet.")).toBeVisible();
    const before = reads.length;
    await page.getByRole("button", { name: "Check again" }).click();
    await expect.poll(() => reads.length).toBeGreaterThan(before);
    expect(writes).toBe(index + 1);
    expect((await stored(request, "current.txt")).text).toBe(
      `replacement-${writes}`,
    );
    await page
      .getByRole("article")
      .filter({ hasText: "current.txt" })
      .getByRole("button", { name: "Close" })
      .click();
  }
});

test("checks completed chat and Files uploads, folder creation and edits through their original receipts", async ({
  page,
  request,
}) => {
  const changes: string[] = [],
    checks: string[] = [];
  await wire(page, async (route, url) => {
    const sent = route.request();
    const response = await route.fetch({
      url,
      headers: { ...sent.headers(), ...cookie },
    });
    expect(response.ok(), await response.text()).toBeTruthy();
    if (sent.method() === "GET") {
      expect(sent.postData()).toBeNull();
      checks.push(new URL(url).pathname.split("/").at(-1)!);
      await route.fulfill({ response });
    } else {
      changes.push(
        new URL(url).searchParams.get("operation_id") ??
          (sent.postDataJSON() as { operation_id: string }).operation_id,
      );
      await route.fulfill({ status: 502, body: "unavailable" });
    }
  });
  const upload = {
    name: "browser.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("observed"),
  };
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await page.locator('input[type="file"]').first().setInputFiles(upload);
  await expect(
    page.getByText("Saved in Researcher's files").first(),
  ).toBeVisible();
  await files(page);
  await page.locator('input[type="file"]').first().setInputFiles(upload);
  await expect.poll(() => checks.length).toBe(2);
  expect((await stored(request, "browser.txt")).text).toBe("observed");
  await page.getByRole("button", { name: "New folder" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("textbox").fill("browser-folder");
  await dialog.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("The change was completed.")).toBeVisible();
  await page.getByRole("button", { name: "browser.txt", exact: true }).click();
  await dialog.getByRole("textbox").fill("edited");
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(dialog).toBeHidden();
  expect((await stored(request, "browser.txt")).text).toBe("edited");
  expect(changes).toHaveLength(4);
  expect(checks).toEqual(changes);
});

test("a check can see no record while the first upload is committing, then recover the same result", async ({
  page,
  request,
}) => {
  let mutation: ReturnType<APIRequestContext["put"]> | undefined;
  let writes = 0;
  const checks: string[] = [];
  await wire(page, async (route, url) => {
    const sent = route.request();
    if (sent.method() === "PUT") {
      writes++;
      mutation = request.put(url, {
        headers: cookie,
        data: sent.postDataBuffer()!,
      });
      const waiting = await request.get(gateway.origin + "/test/intent", {
        headers: cookie,
      });
      expect((await waiting.json()) as object).toEqual({ entered: true });
      await route.abort("connectionreset");
    } else {
      expect(sent.method()).toBe("GET");
      expect(sent.postData()).toBeNull();
      const result = await route.fetch({
        url,
        headers: { ...sent.headers(), ...cookie },
      });
      checks.push(((await result.json()) as { status: string }).status);
      await route.fulfill({ response: result });
    }
  });
  await files(page);
  await page
    .locator('input[type="file"]')
    .first()
    .setInputFiles({
      name: "deferred.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("committed once"),
    });
  await expect(page.getByText("No result is recorded yet.")).toBeVisible();
  expect(checks).toEqual(["unknown"]);
  await request.post(gateway.origin + "/test/commit", { headers: cookie });
  expect((await mutation!)?.ok()).toBeTruthy();
  await page.getByRole("button", { name: "Check again" }).click();
  await expect(page.getByText("Saved in Researcher's files")).toBeVisible();
  expect(writes).toBe(1);
  expect(checks.at(-1)).toBe("complete");
  expect((await stored(request, "deferred.txt")).text).toBe("committed once");
});

test("an in-flight first rename or delete cannot be repeated by checking its missing receipt", async ({
  page,
  request,
}) => {
  await seed(request, "concurrent.txt", "previous");
  const observe = async () =>
    (await (
      await request.get(gateway.origin + "/test/observed", { headers: cookie })
    ).json()) as { receipts: number; current: string; destination: boolean };
  const initial = await observe();
  let mutation: ReturnType<APIRequestContext["post"]> | undefined;
  let writes = 0;
  let reads = 0;
  let expectedHandle = "";
  await wire(page, async (route, url) => {
    const sent = route.request();
    if (sent.method() === "POST") {
      const body = sent.postDataJSON() as {
        action: string;
        operation_id: string;
      };
      expectedHandle = body.operation_id;
      writes++;
      mutation = request.post(url, { headers: cookie, data: body });
      const held = await request.get(gateway.origin + "/test/intent", {
        headers: cookie,
        params: { action: body.action },
      });
      expect(await held.json()).toEqual({ entered: true });
      if (body.action === "rename") await route.abort("connectionreset");
      else await route.fulfill({ status: 502, body: "unavailable" });
    } else {
      expect(sent.method()).toBe("GET");
      expect(sent.postData()).toBeNull();
      expect(url).toContain("/home/operations/" + expectedHandle);
      const result = await route.fetch({
        url,
        headers: { ...sent.headers(), ...cookie },
      });
      expect(await result.json()).toEqual({ status: "unknown" });
      reads++;
      await route.fulfill({ response: result });
    }
  });
  await files(page);
  for (const [index, action] of ["rename", "remove"].entries()) {
    const button = action === "rename" ? "Rename" : "Delete";
    await page
      .getByRole("row")
      .filter({ hasText: "concurrent.txt" })
      .getByRole("button", { name: button, exact: true })
      .click();
    const dialog = page.getByRole("dialog");
    if (action === "rename")
      await dialog.getByRole("textbox").fill("moved-concurrent.txt");
    await dialog.getByRole("button", { name: button, exact: true }).click();
    await expect(page.getByText("No result is recorded yet.")).toBeVisible();
    const before = reads;
    await page.getByRole("button", { name: "Check again" }).click();
    await expect.poll(() => reads).toBeGreaterThan(before);
    const expected = {
      receipts: initial.receipts,
      current: "replacement-" + action,
      destination: false,
    };
    expect(await observe()).toEqual(expected);
    await request.post(gateway.origin + "/test/commit", {
      headers: cookie,
      params: { action },
    });
    expect((await mutation!)?.status()).toBe(503);
    expect(await observe()).toEqual(expected);
    expect(writes).toBe(index + 1);
    await page
      .getByRole("article")
      .filter({ hasText: "concurrent.txt" })
      .getByRole("button", { name: "Close" })
      .click();
  }
});

test("a Home upload survives leaving Files and a Gateway process restart", async ({
  page,
  request,
}) => {
  const before = gateway;
  let writes = 0;
  let down = false;
  await wire(page, async (route, url) => {
    const sent = route.request();
    if (sent.method() === "PUT") {
      const committed = await route.fetch({
        url,
        headers: { ...sent.headers(), ...cookie },
      });
      expect(committed.ok()).toBeTruthy();
      writes++;
      await stopGateway();
      down = true;
      await route.fulfill({ status: 502, body: "Gateway stopped" });
    } else {
      expect(sent.method()).toBe("GET");
      expect(sent.postData()).toBeNull();
      if (down) return route.fulfill({ status: 502, body: "Gateway stopped" });
      await route.fulfill({
        response: await route.fetch({
          url,
          headers: { ...sent.headers(), ...cookie },
        }),
      });
    }
  });
  await files(page);
  await page
    .locator('input[type="file"]')
    .first()
    .setInputFiles({
      name: "restart.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("persisted"),
    });
  await expect(
    page.getByText("The result is currently unavailable."),
  ).toBeVisible();
  // Use the actual workspace link so the route unmounts while the account remains signed in.
  await page.locator('a[href="/workspace/files"]').first().click();
  await expect(page).toHaveURL(/\/workspace\/files/);
  await startGateway();
  down = false;
  expect(gateway.home).toBe(before.home);
  expect(gateway.instance).toBe(before.instance);
  await page.goBack();
  await expect(page).toHaveURL(/tab=files/);
  await page.getByRole("button", { name: "Check again" }).click();
  await expect(page.getByText("Saved in Researcher's files")).toBeVisible();
  expect(writes).toBe(1);
  expect((await stored(request, "restart.txt")).text).toBe("persisted");
});

test("a clean chat refusal supports a deliberate fresh upload; an unconfirmed numbered attachment only offers removal", async ({
  page,
  request,
}) => {
  const contract = JSON.parse(
    readFileSync("../contracts/agent-file-outcome-v1.json", "utf8"),
  ) as { field: string; clean: string };
  await seed(request, "uploads/collision.txt", "first");
  const writes: string[] = [];
  const reads: string[] = [];
  await wire(page, async (route, url) => {
    const sent = route.request();
    const response = await route.fetch({
      url,
      headers: { ...sent.headers(), ...cookie },
    });
    if (sent.method() === "POST") {
      writes.push(new URL(url).searchParams.get("operation_id")!);
      if (writes.length === 1)
        expect(
          ((await response.json()) as Record<string, unknown>)[contract.field],
        ).toBe(contract.clean);
      if (writes.length === 3)
        expect(((await response.json()) as { reason: string }).reason).toBe(
          "storage-busy",
        );
    } else {
      expect(sent.method()).toBe("GET");
      expect(sent.postData()).toBeNull();
      reads.push(((await response.json()) as { status: string }).status);
    }
    await route.fulfill({ response });
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  const choose = (name: string) =>
    page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name,
        mimeType: "text/plain",
        buffer: Buffer.from("next"),
      });
  await choose("clean-chat.txt");
  const clean = page.getByRole("article").filter({ hasText: "clean-chat.txt" });
  await expect(
    clean.getByText("clean-chat.txt could not be saved. Try again."),
  ).toBeVisible();
  await clean.getByRole("button", { name: "Try again" }).click();
  await expect(clean.getByText("Saved in Researcher's files")).toBeVisible();
  expect(new Set(writes).size).toBe(2);
  await clean.getByRole("button", { name: "Remove" }).click();
  await expect(clean).toHaveCount(0);
  await choose("collision.txt");
  await expect(
    page.getByText("This change is still being checked."),
  ).toBeVisible();
  expect(
    await (
      await request.post(gateway.origin + "/test/settle", { headers: cookie })
    ).json(),
  ).toEqual({ settled: 1 });
  await page.getByRole("button", { name: "Check again" }).click();
  const chip = page.getByRole("article").filter({ hasText: "collision.txt" });
  await expect(
    chip.getByText(/could not confirm whether collision.txt was saved/),
  ).toBeVisible();
  await expect(chip.getByRole("button")).toHaveCount(1);
  await expect(chip.getByRole("button", { name: "Remove" })).toBeVisible();
  expect((await stored(request, "uploads/collision (2).txt")).text).toBe(
    "next",
  );
  expect(writes).toHaveLength(3);
  expect(reads).toContain("pending");
  expect(reads.at(-1)).toBe("unconfirmed");
});

test("a chat losing its file permission stops checking and reveals no saved reference", async ({
  page,
  request,
}) => {
  let writes = 0;
  let reads = 0;
  await wire(page, async (route, url) => {
    const sent = route.request();
    const response = await route.fetch({
      url,
      headers: { ...sent.headers(), ...cookie },
    });
    if (sent.method() === "POST") {
      expect(response.ok()).toBeTruthy();
      writes++;
      expect(
        (
          await request.post(gateway.origin + "/test/revoke-use", {
            headers: cookie,
          })
        ).ok(),
      ).toBeTruthy();
      await route.fulfill({ status: 502, body: "unavailable" });
      return;
    }
    expect(sent.method()).toBe("GET");
    expect(sent.postData()).toBeNull();
    expect(response.status()).toBe(404);
    expect(await response.text()).not.toContain("uploads/");
    reads++;
    await route.fulfill({ response });
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`, {
    waitUntil: "domcontentloaded",
  });
  await page
    .locator('input[type="file"]')
    .first()
    .setInputFiles({
      name: "denied.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("private"),
    });
  await expect(
    page.getByText("You no longer have access to check this change."),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Check again" })).toHaveCount(
    0,
  );
  await expect(page.getByText("Saved in Researcher's files")).toHaveCount(0);
  expect(reads).toBe(1);
  expect(writes).toBe(1);
});

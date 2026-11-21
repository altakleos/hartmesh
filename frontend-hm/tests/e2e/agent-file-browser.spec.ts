import { expect, test } from "@playwright/test";

import {
  agent,
  AGENT_ID,
  HOME_ID,
  refreshAccount,
  setupAgents,
} from "../e2e-real-backend/utils/compose-agents";

test("discovery pages past inventory-only entries and the selected agent uses versioned reads", async ({
  page,
}) => {
  const network: string[] = [];
  const state = await setupAgents(page);
  state.feature.file_browser = {
    versions: [1],
    storage_mode: "spaces",
    operations: {
      personal: ["select"],
      shared: ["select"],
      agent: ["select", "folder", "content", "text"],
    },
  };
  const entries = Array.from({ length: 100 }, (_, i) => ({
    id: String(i).padStart(32, "0"),
    name: "Inventory " + i,
    supervisor_name: null,
    status: "active",
  }));
  await page.route("**/api/agent-instances?**", (route) => {
    const url = new URL(route.request().url());
    return route.fulfill({
      json: {
        instances:
          url.searchParams.get("custody") === "company"
            ? []
            : url.searchParams.get("offset") === "100"
              ? [agent()]
              : entries,
        unfinished: [],
      },
    });
  });
  await page.route("**/api/files", (route) =>
    route.fulfill({ json: { files: [], count: 0, truncated: false } }),
  );
  await page.route("**/api/file-browser/v1/agents/**", (route) => {
    const u = new URL(route.request().url());
    network.push(u.pathname + "?" + u.searchParams.get("path"));
    if (u.pathname.endsWith("/folder")) {
      const folder = u.searchParams.get("path");
      const row =
        folder === "outputs"
          ? { name: "nested", path: "outputs/nested", kind: "directory" }
          : folder === "outputs/nested"
            ? {
                name: "answer.txt",
                path: "outputs/nested/answer.txt",
                kind: "file",
              }
            : { name: "outputs", path: "outputs", kind: "directory" };
      return route.fulfill({
        json: {
          files: [{ ...row, size: 4, modified: 1730000000, accessible: true }],
          truncated: false,
        },
      });
    }
    if (u.pathname.endsWith("/text"))
      return route.fulfill({
        json: {
          text: "reader-safe",
          sha256: "a".repeat(64),
          concurrency: "exclusive-host-window",
        },
      });
    if (u.pathname.endsWith("/content"))
      return route.fulfill({ contentType: "text/plain", body: "reader-safe" });
    return route.fulfill({ status: 404 });
  });
  await page.goto("/workspace/files?view=browser");
  await page.getByRole("button", { name: "Locations" }).click();
  await expect(page.getByRole("link", { name: "Inventory 0" })).toHaveCount(0);
  await page.getByRole("button", { name: /Load more/ }).click();
  await page.getByRole("link", { name: "Researcher" }).click();
  await expect(page).toHaveURL(new RegExp("instance=" + AGENT_ID));
  await page.getByRole("button", { name: "Outputs", exact: true }).click();
  await page.getByRole("button", { name: "nested", exact: true }).click();
  await expect(page).toHaveURL(/folder=outputs%2Fnested/);
  await page.getByRole("button", { name: "answer.txt", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "File contents" }),
  ).toHaveValue("reader-safe");
  expect(network).toContain(
    "/api/file-browser/v1/agents/" +
      AGENT_ID +
      "/text?outputs/nested/answer.txt",
  );
  await page.keyboard.press("Escape");
  await page
    .getByRole("button", { name: "More actions for answer.txt" })
    .click();
  const request = page.waitForRequest((r) =>
    r
      .url()
      .includes("/content?path=outputs%2Fnested%2Fanswer.txt&download=true"),
  );
  const saving = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download", exact: true }).click();
  await request;
  expect((await saving).suggestedFilename()).toBe("answer.txt");
  await expect(page.getByRole("button", { name: /Rename|Delete/ })).toHaveCount(
    0,
  );
});

test("a worker refusing a versioned request never triggers a path-only retry", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.feature.file_browser = {
    versions: [1],
    storage_mode: "spaces",
    operations: { agent: ["folder", "content", "text"] },
  };
  const legacy: string[] = [];
  await page.route("**/api/file-browser/v1/agents/**", (route) =>
    route.fulfill({ status: 404, json: { detail: "Unavailable" } }),
  );
  await page.route("**/api/spaces/*/files?**", (route) => {
    legacy.push(route.request().url());
    return route.fulfill({ json: { files: [], truncated: false } });
  });
  await page.goto(
    "/workspace/instances?instance=" +
      AGENT_ID +
      "&tab=files&folder=private%2Fnested&view=browser",
  );
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "This folder is no longer available." }),
  ).toBeVisible();
  expect(
    legacy.filter((url) => url.includes("/" + HOME_ID + "/files")),
  ).toEqual([]);
});

for (const scenario of ["denied", "offline", "retired account"] as const) {
  test(`agent download leaves no late file on ${scenario}`, async ({
    page,
  }) => {
    const state = await setupAgents(page);
    state.feature.file_browser = {
      versions: [1],
      storage_mode: "spaces",
      operations: { agent: ["folder", "text", "content"] },
    };
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const downloads: string[] = [];
    page.on("download", (d) => downloads.push(d.suggestedFilename()));
    const reads: string[] = [];
    await page.route("**/api/file-browser/v1/agents/**", async (route) => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith("/folder"))
        return route.fulfill({
          json: {
            files: [
              {
                name: "report.txt",
                path: "report.txt",
                kind: "file",
                size: 4,
                modified: 1730000000,
                accessible: true,
              },
            ],
            truncated: false,
          },
        });
      if (url.pathname.endsWith("/content")) {
        reads.push(url.searchParams.get("path")!);
        if (scenario === "retired account") {
          await held;
          return route
            .fulfill({
              contentType: "text/plain",
              body: "former account bytes",
            })
            .catch(() => undefined);
        }
        return route.fulfill({
          status: scenario === "denied" ? 403 : 503,
          json: { detail: "private diagnostic" },
        });
      }
      return route.fulfill({ status: 404 });
    });
    await page.goto(
      "/workspace/instances?instance=" + AGENT_ID + "&tab=files&view=browser",
    );
    await page
      .getByRole("button", { name: "More actions for report.txt" })
      .click();
    const menu = page.getByRole("dialog", { name: "report.txt" });
    await menu.getByRole("button", { name: "Download", exact: true }).click();
    await expect.poll(() => reads).toEqual(["report.txt"]);
    if (scenario === "retired account") {
      state.user.id = "another-account";
      state.instance.permissions = 0;
      await refreshAccount(page);
      await expect(menu).toHaveCount(0);
      release();
      await expect(
        page.getByRole("button", { name: "More actions for report.txt" }),
      ).toHaveCount(0);
    } else {
      await expect(menu.getByRole("alert")).toHaveText(
        scenario === "denied"
          ? "You no longer have access to report.txt."
          : "Could not download report.txt. Try again.",
      );
    }
    expect(downloads).toEqual([]);
  });
}

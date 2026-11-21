import { expect, test, type Page } from "@playwright/test";

import { refreshAccount } from "../e2e-real-backend/utils/compose-agents";

import { installLegacyReportPlugin } from "./utils/legacy-report-plugin";
import { mockLangGraphAPI } from "./utils/mock-api";

const long = "文".repeat(240),
  title = "Quarterly summary.pdf";
const makeFile = (path: string) => ({
  path,
  name: path.split("/").at(-1)!,
  size: 4831,
  modified: 1730000000,
  virtual_path: "/mnt/user-data/files/" + path,
  url: "/api/files/" + path,
});
async function setup(page: Page, paths: string[], partial = false) {
  const files = paths.map(makeFile);
  mockLangGraphAPI(page, { threads: [], files });
  await installLegacyReportPlugin(page);
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        file_browser: {
          versions: [1],
          storage_mode: "ordinary",
          operations: { personal: ["select"], shared: ["select"], agent: [] },
        },
        agents_api: { enabled: true },
        agent_instances: { enabled: false },
        storage_spaces: {
          enabled: false,
          state: "off",
          reason: null,
          own: "ready",
          user_spaces: false,
        },
      },
    }),
  );
  await page.route("**/api/files", (route) =>
    route.fulfill({ json: { files, count: files.length, truncated: partial } }),
  );
}
for (const width of [320, 360, 390, 430, 768, 1024, 1280, 1440]) {
  test(`visible menu, deep path and nested navigation at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({
      width,
      height: width === 320 ? 568 : width === 768 ? 1024 : 800,
    });
    await setup(page, [long + "/" + long + "/" + title]);
    await page.goto(
      "/workspace/files?view=browser&folder=" +
        encodeURIComponent(long + "/" + long),
    );
    if (width === 320)
      await page.addStyleTag({ content: "html{font-size:200%}" });
    const menu = page.getByRole("button", {
      name: "More actions for " + title,
    });
    await expect(menu).toBeVisible({ timeout: 90_000 });
    const box = await menu.boundingBox();
    expect(box!.width).toBeGreaterThanOrEqual(44);
    expect(box!.height).toBeGreaterThanOrEqual(44);
    const fullPath = page.getByRole("button", {
      name: "Full path",
      exact: true,
    });
    if (width === 320) {
      const box = await fullPath.boundingBox();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    }
    await fullPath.click();
    await expect(page.getByRole("dialog")).toContainText(long);
    await page.keyboard.press("Escape");
    await menu.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("dialog")).toContainText(title);
    await page.keyboard.press("Escape");
    await expect(menu).toBeFocused();
    await expect(
      page.getByRole("link", { name: title, exact: true }),
    ).toHaveAttribute("href", /api\/files\//);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth + 1,
      ),
    ).toBe(true);
  });
}
test("bounded search and Back preserve a later result's row control", async ({
  page,
}) => {
  const paths = Array.from(
    { length: 105 },
    (_, i) => "reports/report-" + String(i).padStart(3, "0") + ".txt",
  );
  await setup(page, paths, true);
  await page.goto("/workspace/files");
  await page
    .getByRole("searchbox", { name: "Search loaded files" })
    .fill("report-");
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  const parent = page
    .getByRole("listitem")
    .filter({ hasText: "report-103.txt" })
    .getByRole("button", { name: "Open containing folder: reports" });
  await parent.click();
  await expect(page).toHaveURL(/folder=reports/);
  await page.goBack();
  await expect(page.getByRole("searchbox")).toHaveValue("report-");
  await expect(parent).toBeFocused();
});
test("classic view remains selectable, and unavailable destructive actions send no request", async ({
  page,
}) => {
  await setup(page, ["report.pdf"]);
  const writes: string[] = [];
  page.on("request", (r) => {
    if (r.method() === "DELETE") writes.push(r.url());
  });
  await page.goto("/workspace/files");
  await page
    .getByRole("button", { name: "More actions for report.pdf" })
    .click();
  await expect(page.getByRole("button", { name: /Delete|Remove/ })).toHaveCount(
    0,
  );
  await expect(page.getByRole("dialog")).toContainText(
    "Everyone at the company",
  );
  await page.keyboard.press("Escape");
  await page.getByRole("link", { name: "Use classic file view" }).click();
  await expect(page.getByTestId("my-file-report.pdf")).toBeVisible();
  expect(writes).toEqual([]);
});

test.describe("touch deployment fallback", () => {
  test.use({
    hasTouch: true,
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile Safari",
    viewport: { width: 390, height: 844 },
  });
  test("classic is the default while the opt-in follows folder navigation", async ({
    page,
  }) => {
    await setup(page, ["papers/note.txt"]);
    await page.goto("/workspace/files");
    await expect(page.getByTestId("my-file-papers/note.txt")).toBeVisible();
    await page.getByRole("link", { name: "Use file browser" }).click();
    await page.getByRole("button", { name: "papers", exact: true }).click();
    await expect(page).toHaveURL(/view=browser/);
    await expect(
      page.getByRole("button", { name: "More actions for note.txt" }),
    ).toBeVisible();
  });
});

async function managedShared(page: Page) {
  await setup(page, ["report.txt"]);
  const entry = {
    ...makeFile("report.txt"),
    published_by: "default",
    published_at: "2026-10-01T00:00:00Z",
    from_thread_id: null,
    can_remove: true,
    publication_id: "b".repeat(32),
  };
  await page.route("**/api/features", (route) =>
    route.fulfill({
      json: {
        file_browser: {
          versions: [1],
          storage_mode: "spaces",
          operations: {
            personal: ["select"],
            shared: ["select", "remove"],
            agent: [],
          },
        },
        storage_spaces: {
          enabled: true,
          state: "ready",
          reason: null,
          own: "ready",
          user_spaces: true,
        },
      },
    }),
  );
  await page.route("**/api/shared", (route) =>
    route.fulfill({ json: { files: [entry], count: 1, truncated: false } }),
  );
  return entry;
}

test("Shared confirmation sends the observed revision and disables removal after a worker refuses the guarded route", async ({
  page,
}) => {
  const entry = await managedShared(page);
  const posts: unknown[] = [],
    deletes: string[] = [];
  const revision = "1".repeat(64);
  await page.route("**/api/file-browser/v1/shared/selection?**", (route) =>
    route.fulfill({ json: { ...entry, revision } }),
  );
  await page.route("**/api/file-browser/v1/shared/remove", (route) => {
    posts.push(route.request().postDataJSON());
    return route.fulfill({ status: 404, json: { detail: "Not Found" } });
  });
  page.on("request", (r) => {
    if (r.method() === "DELETE") deletes.push(r.url());
  });
  await page.goto("/workspace/files?tab=shared&view=browser");
  await page
    .getByRole("button", { name: "More actions for report.txt" })
    .click();
  await page.getByRole("button", { name: "Remove", exact: true }).click();
  const confirmation = page.getByRole("dialog", {
    name: "Remove from Shared?",
  });
  await expect(confirmation).toContainText("report.txt");
  await confirmation
    .getByRole("button", { name: "Remove", exact: true })
    .click();
  await expect(confirmation).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Remove", exact: true }),
  ).toHaveCount(0);
  expect(posts).toEqual([
    {
      path: "report.txt",
      expected_revision: revision,
      expected_publication_id: entry.publication_id,
      operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
    },
  ]);
  expect(deletes).toEqual([]);
});

test("Undo uses the original confirmed created copy and its actual destination", async ({
  page,
}) => {
  const entry = await managedShared(page);
  const revision = "2".repeat(64),
    posts: unknown[] = [];
  await page.route("**/api/shared/publish", (route) =>
    route.fulfill({
      status: 201,
      json: { ...entry, path: "Filed/report.txt", revision },
    }),
  );
  await page.route("**/api/file-browser/v1/shared/remove", (route) => {
    posts.push(route.request().postDataJSON());
    return route.fulfill({ status: 204 });
  });
  await page.goto("/workspace/files?view=browser");
  await page
    .getByRole("button", { name: "More actions for report.txt" })
    .click();
  await page.getByRole("button", { name: "Share with everyone" }).click();
  await expect(
    page.getByText("Shared report.txt with everyone at the company, in Filed"),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Undo", exact: true }).click();
  await expect.poll(() => posts.length).toBe(1);
  expect(posts[0]).toEqual({
    path: "Filed/report.txt",
    expected_revision: revision,
    expected_publication_id: entry.publication_id,
    operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
  });
});

test("Chinese details preserve keyboard focus and full names in forced colors", async ({
  page,
  context,
  baseURL,
}) => {
  await context.addCookies([{ name: "locale", value: "zh-CN", url: baseURL! }]);
  await page.emulateMedia({
    colorScheme: "dark",
    forcedColors: "active",
    reducedMotion: "reduce",
  });
  await setup(page, ["报告.txt"]);
  await page.goto("/workspace/files?view=browser");
  const more = page.getByRole("button", { name: "报告.txt 的更多操作" });
  await more.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: "报告.txt" })).toContainText(
    "仅你可见",
  );
  await page.keyboard.press("Escape");
  await expect(more).toBeFocused();
});

for (const access of [
  "reader",
  "different publisher",
  "operator file",
  "omitted identity",
] as const) {
  test(`Shared removal respects ${access}`, async ({ page }) => {
    const entry = await managedShared(page);
    const operator = access === "operator file";
    await page.route("**/api/v1/auth/me", (route) =>
      route.fulfill({
        json: {
          id: "default",
          email: "default@test.local",
          system_role: operator ? "admin" : "user",
          needs_setup: false,
          permissions:
            access === "reader"
              ? ["threads:read"]
              : ["threads:read", "threads:delete", "threads:write"],
        },
      }),
    );
    const selected = {
      ...entry,
      publication_id: operator
        ? null
        : access === "omitted identity"
          ? undefined
          : entry.publication_id,
      can_remove: access !== "different publisher",
    };
    await page.route("**/api/shared", (route) =>
      route.fulfill({
        json: { files: [selected], count: 1, truncated: false },
      }),
    );
    await page.route("**/api/file-browser/v1/shared/selection?**", (route) =>
      route.fulfill({ json: { ...selected, revision: "3".repeat(64) } }),
    );
    const posts: unknown[] = [];
    await page.route("**/api/file-browser/v1/shared/remove", (route) => {
      posts.push(route.request().postDataJSON());
      return route.fulfill({ status: 204 });
    });
    await page.goto("/workspace/files?tab=shared&view=browser");
    await expect(
      page.getByRole("button", { name: "More actions for report.txt" }),
    ).toBeVisible();
    await refreshAccount(page);
    await page
      .getByRole("button", { name: "More actions for report.txt" })
      .click();
    const remove = page.getByRole("button", { name: "Remove", exact: true });
    if (operator) {
      await remove.click();
      await page
        .getByRole("dialog", { name: "Remove from Shared?" })
        .getByRole("button", { name: "Remove", exact: true })
        .click();
      await expect.poll(() => posts.length).toBe(1);
      expect(posts[0]).toMatchObject({
        expected_publication_id: null,
        expected_revision: "3".repeat(64),
        path: "report.txt",
      });
    } else {
      await expect(remove).toHaveCount(0);
      expect(posts).toEqual([]);
    }
  });
}

test("missing a Shared row keeps other eligible publications removable", async ({
  page,
}) => {
  const entry = await managedShared(page),
    other = {
      ...entry,
      path: "other.txt",
      name: "other.txt",
      publication_id: "f".repeat(32),
    };
  await page.route("**/api/shared", (route) =>
    route.fulfill({
      json: { files: [entry, other], count: 2, truncated: false },
    }),
  );
  await page.route("**/api/file-browser/v1/shared/selection?**", (route) =>
    new URL(route.request().url()).searchParams.get("path") === "report.txt"
      ? route.fulfill({
          status: 404,
          json: { detail: "Resource or file is unavailable" },
        })
      : route.fulfill({ json: { ...other, revision: "a".repeat(64) } }),
  );
  await page.goto("/workspace/files?tab=shared&view=browser");
  await page
    .getByRole("button", { name: "More actions for report.txt" })
    .click();
  const refused = page.waitForResponse(
    (r) => r.status() === 404 && r.url().includes("/shared/selection"),
  );
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Remove", exact: true })
    .click();
  await refused;
  await page.keyboard.press("Escape");
  await page
    .getByRole("button", { name: "More actions for other.txt" })
    .click();
  await page.getByRole("button", { name: "Remove", exact: true }).click();
  await expect(
    page.getByRole("dialog", { name: "Remove from Shared?" }),
  ).toContainText("other.txt");
});

test("positive pointer changes update the default while an explicit choice persists", async ({
  page,
}) => {
  await page.addInitScript(() => {
    const original = window.matchMedia.bind(window);
    const fine = original("(any-pointer: fine)");
    let positive = false;
    Object.defineProperty(fine, "matches", { get: () => positive });
    window.matchMedia = (query) =>
      query === "(any-pointer: fine)" ? fine : original(query);
    window.addEventListener("fixture-fine", () => {
      positive = !positive;
      fine.dispatchEvent(new Event("change"));
    });
  });
  await setup(page, ["note.txt"]);
  await page.goto("/workspace/files");
  await expect(page.getByTestId("my-file-note.txt")).toBeVisible();
  await page.evaluate(() => window.dispatchEvent(new Event("fixture-fine")));
  await expect(
    page.getByRole("button", { name: "More actions for note.txt" }),
  ).toBeVisible();
  await page.getByRole("link", { name: "Use classic file view" }).click();
  await page.evaluate(() => {
    window.dispatchEvent(new Event("fixture-fine"));
    window.dispatchEvent(new Event("fixture-fine"));
  });
  await expect(page.getByTestId("my-file-note.txt")).toBeVisible();
});

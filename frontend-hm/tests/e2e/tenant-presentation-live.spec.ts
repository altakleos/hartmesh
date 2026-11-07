/** Real services only. See docs/TENANT_PRESENTATION.md for the isolated recipe. */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";

const enabled = process.env.HARTMESH_LIVE_TENANT_PRESENTATION === "1";
test.use({ actionTimeout: 15_000 });
const directory = process.env.HARTMESH_TENANT_TEST_DIR ?? "";
const python = resolve("../backend/.venv/bin/python");
const helper = resolve("../backend/tests/support/tenant_presentation_live.py");
function activate(profile: string, location = directory) {
  console.log(`Activating synthetic presentation ${profile}`);
  return JSON.parse(
    execFileSync(
      python,
      [helper, "activate", profile, "--directory", location],
      {
        timeout: 90_000,
        encoding: "utf8",
      },
    ),
  ) as unknown;
}
function stop(location = directory) {
  execFileSync(python, [helper, "stop", "--directory", location], {
    timeout: 30_000,
  });
}
async function signIn(page: Page) {
  const setup = await (
    await page.request.get("/api/v1/auth/setup-status")
  ).json();
  if (setup.needs_setup) {
    expect(
      (
        await page.request.post("/api/v1/auth/initialize", {
          data: {
            email: "operator@example.com",
            password: "SyntheticTenant!8642",
            remember_me: false,
          },
        })
      ).status(),
    ).toBe(201);
  }
  await page.context().clearCookies();
  await page.goto("/login");
  await page.getByLabel("Email", { exact: true }).fill("operator@example.com");
  await page
    .getByLabel("Password", { exact: true })
    .fill("SyntheticTenant!8642");
  await page.getByRole("button", { name: "Sign In", exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
}
async function admitted(page: Page, expected: string) {
  const result = await page.request.get("/api/branding/presentation");
  expect(result.status()).toBe(200);
  expect(result.headers()["cache-control"]).toBe("private, no-store");
  expect(result.headers().vary).toMatch(/Cookie/);
  const body = await result.json();
  expect(body.tenant.name).toBe(expected);
  const group = page.getByRole("group", {
    name: `Workspace: ${expected}`,
    exact: true,
  });
  await expect(group).toBeVisible();
  await expect(group).toHaveAttribute("data-snapshot-id", body.snapshot_id);
  await expect(group.locator("img")).toHaveAttribute("src", body.tenant.logo);
  await expect
    .poll(() =>
      group
        .locator("img")
        .evaluate(
          (image: HTMLImageElement) => image.complete && image.naturalWidth > 0,
        ),
    )
    .toBe(true);
  expect(
    await group.evaluate(
      (element) => element.scrollWidth <= element.clientWidth + 1,
    ),
  ).toBe(true);
  const colors = await group.locator("span").evaluate((element) => ({
    color: getComputedStyle(element).color,
    background: getComputedStyle(element.parentElement!).backgroundColor,
  }));
  // The affected text uses the existing fixed black/white palette: 21:1.
  expect(colors).toEqual({
    color: "rgb(0, 0, 0)",
    background: "rgb(255, 255, 255)",
  });
  return body;
}

test("one build admits complete tenant snapshots through authenticated activation and restoration", async ({
  page,
  browser,
  request,
}) => {
  test.setTimeout(600_000);
  test.skip(
    !enabled,
    "requires isolated real Gateway, Docker read-only inputs, and production Next",
  );
  const revised = "Example Tenant One Revised " + "X".repeat(53);
  const expectedSettings = () =>
    readFileSync(resolve(directory, "active/report-profile.json"), "utf8");
  try {
    const selectedA = activate("A");
    for (const path of [
      "/api/branding/presentation",
      "/api/branding/logo",
      "/api/features",
    ])
      expect((await request.get(path)).status()).toBe(401);
    for (const path of [
      "/login",
      "/setup",
      "/api/v1/auth/product",
      "/api/v1/auth/product-identity",
      "/favicon.ico",
    ]) {
      const response = await request.get(path);
      const text = await response.text();
      expect(text).not.toContain("Example Tenant");
      expect(text).not.toContain("snapshot_id");
    }
    await signIn(page);
    const first = await admitted(page, "Example Tenant One");
    expect(first).toEqual(selectedA);
    const product = await (
      await page.request.get("/api/v1/auth/product-identity")
    ).json();
    await expect(
      page.getByText("Example Product", { exact: true }).first(),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Example Provider support" }).first(),
    ).toHaveAttribute("href", product.provider.support_url);
    expect(
      await page.locator('link[rel="icon"]').last().getAttribute("href"),
    ).toBe(product.product.browser_icon);
    await page.getByRole("button", { name: /settings and more/i }).click();
    await page
      .getByRole("menuitem", { name: "About Example Product", exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: "Example Product", exact: true }),
    ).toBeVisible();
    await expect(page.getByText(/HartMesh component version/)).toBeVisible();
    await page.getByRole("button", { name: "Close", exact: true }).click();

    // Preserve a real resource and user-visible draft across operator activation.
    await page
      .getByPlaceholder(/how can i assist you/i)
      .fill("Keep this unsent draft");
    const csrf = (await page.context().cookies()).find(
      (cookie) => cookie.name === "csrf_token",
    )!.value;
    const threadId = crypto.randomUUID();
    expect(
      (
        await page.request.post("/api/langgraph/threads", {
          headers: { "X-CSRF-Token": csrf },
          data: { thread_id: threadId, metadata: {} },
        })
      ).ok(),
    ).toBe(true);
    const keptBytes = Buffer.from(
      "Retain this synthetic source file unchanged.\n",
      "utf8",
    );
    const upload = await page.request.post(`/api/threads/${threadId}/uploads`, {
      headers: { "X-CSRF-Token": csrf },
      multipart: {
        files: {
          name: "preserved.txt",
          mimeType: "text/plain",
          buffer: keptBytes,
        },
      },
    });
    expect(upload.ok(), await upload.text()).toBe(true);
    const fileURL = `/api/threads/${threadId}/artifacts/mnt/user-data/uploads/preserved.txt`;
    expect(await (await page.request.get(fileURL)).body()).toEqual(keptBytes);
    stop();
    const outageRefresh = page.waitForResponse(
      (response) =>
        response.request().headers().rsc === "1" &&
        response.url().includes("/workspace/chats/"),
    );
    await page.evaluate(() =>
      (
        window as unknown as { next: { router: { refresh: () => void } } }
      ).next.router.refresh(),
    );
    await outageRefresh;
    await expect(
      page.getByText(
        /Service identity reader unavailable\. Retaining the verified identity/,
      ),
    ).toBeVisible();
    await expect(page.getByTestId("tenant-context")).toHaveAttribute(
      "data-snapshot-id",
      first.snapshot_id,
    );
    await expect(page.getByTestId("tenant-logo")).toHaveAttribute(
      "src",
      first.tenant.logo,
    );
    activate("B");
    await page.getByRole("link", { name: "My files", exact: true }).click();
    await expect(page).toHaveURL(/\/workspace\/files/);
    await expect(
      page.getByRole("heading", { name: "Files", exact: true }),
    ).toBeVisible();
    await expect(page.getByTestId("tenant-context")).toHaveAttribute(
      "data-snapshot-id",
      first.snapshot_id,
    );
    await page.getByRole("link", { name: "New chat", exact: true }).click();
    await expect(page).toHaveURL(/\/workspace\/chats\/new/);
    await expect(page.getByPlaceholder(/how can i assist you/i)).toBeVisible();
    await page.evaluate(() =>
      document.dispatchEvent(new Event("visibilitychange")),
    );
    await expect(
      page.getByText("Revised starter", { exact: true }),
    ).toBeVisible();
    await expect(page.getByTestId("tenant-context")).toHaveAttribute(
      "data-snapshot-id",
      first.snapshot_id,
    );

    // The actual Next App Router requests a new server tree without a full load.
    const refreshed = page.waitForResponse(
      (response) =>
        response.request().headers().rsc === "1" &&
        response.url().includes("/workspace/chats/"),
    );
    await page.evaluate(() =>
      (
        window as unknown as { next: { router: { refresh: () => void } } }
      ).next.router.refresh(),
    );
    await refreshed;
    await expect(page.getByTestId("tenant-context")).toHaveAttribute(
      "data-snapshot-id",
      first.snapshot_id,
    );
    await expect(page.getByTestId("tenant-logo")).toHaveAttribute(
      "src",
      first.tenant.logo,
    );
    await expect(page.getByPlaceholder(/how can i assist you/i)).toHaveValue(
      "Keep this unsent draft",
    );
    await page.reload();
    const second = await admitted(page, revised);
    expect(second.snapshot_id).not.toBe(first.snapshot_id);
    expect(second.tenant.logo).not.toBe(first.tenant.logo);
    expect(
      (await page.request.get(`/api/langgraph/threads/${threadId}`)).ok(),
    ).toBe(true);
    expect(await (await page.request.get(fileURL)).body()).toEqual(keptBytes);
    for (const scheme of ["light", "dark"] as const) {
      await page.emulateMedia({ colorScheme: scheme });
      await admitted(page, revised);
    }
    await page.locator('[data-sidebar="rail"]').click();
    await expect(
      page.getByRole("group", { name: `Workspace: ${revised}`, exact: true }),
    ).toBeVisible();
    expect(
      await page
        .getByTestId("tenant-logo")
        .evaluate((element) => element.getBoundingClientRect().width),
    ).toBeLessThanOrEqual(32);
    await page.locator('[data-sidebar="rail"]').click();

    // An admitted document survives a strict reader outage; fresh documents refuse.
    for (const failure of [
      "Missing",
      "Unreadable",
      "Invalid",
      "Mismatch",
      "ProductMismatch",
      "Unsupported",
    ]) {
      activate(failure);
      await expect(page.getByTestId("tenant-context")).toHaveAttribute(
        "data-snapshot-id",
        second.snapshot_id,
      );
      const fresh = await page.context().newPage();
      await fresh.goto("/workspace/chats/new");
      await expect(fresh.getByRole("status")).toHaveText(
        "Workspace identity unavailable",
      );
      expect(await fresh.getByTestId("tenant-context").count()).toBe(0);
      const refused = await fresh.request.get("/api/branding/presentation");
      expect(refused.status()).toBe(503);
      expect(refused.headers()["cache-control"]).toBe("private, no-store");
      await fresh.close();
    }
    activate("A");
    await page.reload();
    const restored = await admitted(page, "Example Tenant One");
    expect(restored).toEqual(first);
    expect(expectedSettings()).toBe('{"independent":"revised"}');
    await expect(
      page.getByText("Revised starter", { exact: true }),
    ).toBeVisible();
    expect(
      (await page.request.get(`/api/langgraph/threads/${threadId}`)).ok(),
    ).toBe(true);
    expect(await (await page.request.get(fileURL)).body()).toEqual(keptBytes);

    // Delay an actual authenticated A response, then start a new B document.
    const delayed = await page.context().newPage();
    let release: () => void = () => undefined;
    let captured = false;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let delayNext = true;
    await delayed.route("**/api/branding/presentation", async (route) => {
      if (!delayNext) return route.continue();
      delayNext = false;
      const actual = await route.fetch();
      expect(actual.status()).toBe(200);
      captured = true;
      await gate;
      await route.fulfill({ response: actual }).catch(() => undefined);
    });
    try {
      await delayed.goto("/workspace/chats/new");
      await expect.poll(() => captured, { timeout: 15_000 }).toBe(true);
      activate("B");
      await delayed.reload();
      await admitted(delayed, revised);
      release();
      await admitted(delayed, revised);
    } finally {
      release();
      await delayed.close();
    }

    activate("Only");
    await page.reload();
    await expect(
      page.getByText("Example Product", { exact: true }).first(),
    ).toBeVisible();
    expect(await page.getByTestId("tenant-context").count()).toBe(0);
    expect(
      (await (await page.request.get("/api/branding/presentation")).json())
        .mode,
    ).toBe("product-only");
    await expect(
      page.getByText("Revised starter", { exact: true }),
    ).toBeVisible();

    // Separate fixture has its own inputs, account database, home and JWT secret.
    stop();
    const isolated = directory + "-two";
    execFileSync(python, [helper, "prepare", "--directory", isolated], {
      timeout: 60_000,
    });
    try {
      activate("Two", isolated);
      expect(
        (await page.request.get("/api/branding/presentation")).status(),
      ).toBe(401);
      expect(
        await page.getByText("Example Tenant Two", { exact: true }).count(),
      ).toBe(0);
      const otherContext = await browser.newContext({
        baseURL: new URL(page.url()).origin,
      });
      const other = await otherContext.newPage();
      await signIn(other);
      const two = await admitted(other, "Example Tenant Two");
      expect(two.snapshot_id).not.toBe(first.snapshot_id);
      expect(
        await other.getByText("Example Tenant One", { exact: true }).count(),
      ).toBe(0);
      await otherContext.close();
    } finally {
      stop(isolated);
    }
    activate("Legacy");
    await page.reload();
    await expect(
      page.getByText("Example Legacy Tenant", { exact: true }).first(),
    ).toBeVisible();
    expect(
      (await (await page.request.get("/api/branding/presentation")).json())
        .mode,
    ).toBe("legacy");
    activate("Default");
    await page.reload();
    await expect(
      page.getByText("HartMesh", { exact: true }).first(),
    ).toBeVisible();

    // Session loss retires protected state before another account can see it.
    activate("A");
    await page.reload();
    await admitted(page, "Example Tenant One");
    const oldAccount = await (await page.request.get("/api/v1/auth/me")).json();
    const csrfCurrent = (await page.context().cookies()).find(
      (cookie) => cookie.name === "csrf_token",
    )!.value;
    await page.evaluate(() => {
      (
        window as unknown as { tenantProofDocument?: boolean }
      ).tenantProofDocument = true;
    });
    const registered = await page.request.post("/api/v1/auth/register", {
      headers: { "X-CSRF-Token": csrfCurrent },
      data: {
        email: "second@example.com",
        password: "SyntheticTenant!8642",
        remember_me: false,
      },
    });
    expect(registered.status()).toBe(201);
    const newAccount = await registered.json();
    expect(newAccount.id).not.toBe(oldAccount.id);
    expect(newAccount.system_role).toBe("user");
    await page.evaluate(() =>
      (
        window as unknown as { next: { router: { refresh: () => void } } }
      ).next.router.refresh(),
    );
    await expect
      .poll(
        async () => {
          try {
            return await page.evaluate(
              () =>
                (window as unknown as { tenantProofDocument?: boolean })
                  .tenantProofDocument === true,
            );
          } catch {
            return true;
          }
        },
        { timeout: 15_000 },
      )
      .toBe(false);
    await admitted(page, "Example Tenant One");
    expect((await (await page.request.get("/api/v1/auth/me")).json()).id).toBe(
      newAccount.id,
    );
    expect([403, 404]).toContain(
      (await page.request.get(`/api/langgraph/threads/${threadId}`)).status(),
    );
    // A verified client refresh also changes account scope. Unchanged initial
    // SSR props must not force that newer admission back into a reload loop.
    const third = await page.request.post("/api/v1/auth/register", {
      headers: {
        "X-CSRF-Token": (await page.context().cookies()).find(
          (cookie) => cookie.name === "csrf_token",
        )!.value,
      },
      data: {
        email: "third@example.com",
        password: "SyntheticTenant!8642",
        remember_me: false,
      },
    });
    expect(third.status()).toBe(201);
    const thirdAccount = await third.json();
    expect(thirdAccount.id).not.toBe(newAccount.id);
    await page.evaluate(() => {
      (
        window as unknown as { tenantClientRefresh?: boolean }
      ).tenantClientRefresh = true;
    });
    const freshAccount = page.waitForResponse((response) =>
      response.url().endsWith("/api/v1/auth/me"),
    );
    const freshPresentation = page.waitForResponse((response) =>
      response.url().endsWith("/api/branding/presentation"),
    );
    await page.evaluate(() =>
      document.dispatchEvent(new Event("visibilitychange")),
    );
    expect((await (await freshAccount).json()).id).toBe(thirdAccount.id);
    expect((await freshPresentation).status()).toBe(200);
    await admitted(page, "Example Tenant One");
    expect(
      await page.evaluate(
        () =>
          (window as unknown as { tenantClientRefresh?: boolean })
            .tenantClientRefresh,
      ),
    ).toBe(true);
    await page.getByRole("button", { name: /settings and more/i }).click();
    await page.getByRole("menuitem", { name: "Settings", exact: true }).click();
    await page.getByRole("button", { name: "Account", exact: true }).click();
    await page.getByRole("button", { name: "Sign Out", exact: true }).click();
    await expect(page).toHaveURL(/\/login/);
    expect(await page.getByTestId("tenant-context").count()).toBe(0);
    expect((await request.get("/api/branding/presentation")).status()).toBe(
      401,
    );
  } finally {
    stop();
  }
});

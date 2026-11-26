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
async function presentation(page: Page) {
  const result = await page.request.get("/api/branding/presentation");
  expect(result.status()).toBe(200);
  expect(result.headers()["cache-control"]).toBe("private, no-store");
  expect(result.headers().vary).toMatch(/Cookie/);
  return result.json();
}
async function productSidebar(
  page: Page,
  productName: string,
  {
    collapsedLogo,
    tenantLogo,
  }: { collapsedLogo?: string; tenantLogo?: string } = {},
) {
  const sidebar = page.locator('[data-sidebar="sidebar"]');
  await expect(sidebar).toBeVisible();
  if (collapsedLogo) {
    await expect(sidebar.locator("img").first()).toHaveAttribute(
      "src",
      collapsedLogo,
    );
  } else {
    await expect(sidebar.getByText(productName, { exact: true })).toBeVisible();
  }
  const customers = /Example (?:Legacy )?Tenant/i;
  const initials = /^(?:ET|EL)$/i;
  await expect(sidebar).not.toContainText(customers);
  await expect(sidebar.getByText(initials)).toHaveCount(0);
  const markup = await sidebar.evaluate((element) => element.outerHTML);
  expect(markup).not.toMatch(customers);
  expect(markup).not.toMatch(
    /tenant-context|data-snapshot-id|\/api\/branding\/logo/,
  );
  expect(markup).not.toMatch(/(?:alt|title|aria-label)="(?:ET|EL)"/i);
  if (tenantLogo) expect(markup).not.toContain(tenantLogo);
  const accessible = await sidebar.ariaSnapshot();
  expect(accessible).not.toMatch(customers);
  expect(accessible).not.toMatch(/\b(?:ET|EL)\b/i);
}
async function admitted(page: Page, expected: string) {
  const body = await presentation(page);
  expect(body.mode).toBe("tenant");
  expect(body.tenant.name).toBe(expected);
  await productSidebar(page, "Example Product", {
    tenantLogo: body.tenant.logo,
  });
  return body;
}

const shareAudience = (company: string) =>
  `Everyone at ${company} and their assistants can read this copy and its files.`;

async function openSharePreview(page: Page, company: string) {
  await page
    .getByRole("button", {
      name: "Share conversation with everyone",
      exact: true,
    })
    .click();
  const dialog = page.getByRole("dialog", {
    name: "Share with everyone",
    exact: true,
  });
  await expect(
    dialog.getByText(shareAudience(company), { exact: true }),
  ).toBeVisible();
  return dialog;
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
      page.locator('[data-sidebar="sidebar"] img').first(),
    ).toHaveAttribute("src", product.product.logo);
    await expect(
      page.getByRole("link", { name: "Example Provider support" }),
    ).toHaveCount(0);
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
    await expect(
      page.getByRole("link", { name: "Example Provider support" }),
    ).toHaveAttribute("href", product.provider.support_url);
    await page.getByRole("button", { name: "Close", exact: true }).click();

    // Preserve a real resource and user-visible draft across operator activation.
    await page
      .getByPlaceholder(/how can i assist you/i)
      .fill("Keep this unsent draft");
    const csrf = (await page.context().cookies()).find(
      (cookie) => cookie.name === "csrf_token",
    )!.value;
    const threadId = crypto.randomUUID();
    const threadPage = `/workspace/chats/${threadId}`;
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
    await productSidebar(page, "Example Product", {
      tenantLogo: first.tenant.logo,
    });
    activate("B");
    await page.getByRole("link", { name: "Files", exact: true }).click();
    await expect(page).toHaveURL(/\/workspace\/files/);
    await expect(
      page.getByRole("heading", { name: "My files", level: 1, exact: true }),
    ).toBeVisible();
    await productSidebar(page, "Example Product", {
      tenantLogo: first.tenant.logo,
    });
    await page.getByRole("link", { name: "New chat", exact: true }).click();
    await expect(page).toHaveURL(/\/workspace\/chats\/new/);
    await expect(page.getByPlaceholder(/how can i assist you/i)).toBeVisible();
    await page.evaluate(() =>
      document.dispatchEvent(new Event("visibilitychange")),
    );
    await expect(
      page.getByText("Revised starter", { exact: true }),
    ).toBeVisible();
    await productSidebar(page, "Example Product");

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
    await productSidebar(page, "Example Product");
    await expect(page.getByPlaceholder(/how can i assist you/i)).toHaveValue(
      "Keep this unsent draft",
    );

    // The read-only Share preview names the audience from this document's selection.
    await page.evaluate(
      (path) =>
        (
          window as unknown as {
            next: { router: { push: (path: string) => void } };
          }
        ).next.router.push(path),
      threadPage,
    );
    await expect(page).toHaveURL(new URL(threadPage, page.url()).href);
    const oldShare = await openSharePreview(page, first.tenant.name);
    await oldShare.getByRole("button", { name: "Cancel", exact: true }).click();
    await page.reload();
    const second = await admitted(page, revised);
    expect(second.snapshot_id).not.toBe(first.snapshot_id);
    expect(second.tenant.logo).not.toBe(first.tenant.logo);
    const newShare = await openSharePreview(page, revised);
    await newShare.getByRole("button", { name: "Cancel", exact: true }).click();
    await page.getByRole("link", { name: "New chat", exact: true }).click();
    await expect(page).toHaveURL(/\/workspace\/chats\/new/);
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
      page.locator('[data-slot="sidebar"][data-state]'),
    ).toHaveAttribute("data-state", "collapsed");
    await productSidebar(page, "Example Product", {
      collapsedLogo: product.product.logo,
      tenantLogo: second.tenant.logo,
    });
    await page.locator('[data-sidebar="rail"]').click();
    const retainedDraft = "Keep editing while strict admission is unavailable";
    await page.getByPlaceholder(/how can i assist you/i).fill(retainedDraft);

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
      await productSidebar(page, "Example Product", {
        tenantLogo: second.tenant.logo,
      });
      await expect(page.getByPlaceholder(/how can i assist you/i)).toHaveValue(
        retainedDraft,
      );
      const fresh = await page.context().newPage();
      await fresh.goto("/workspace/chats/new");
      await expect(fresh.getByRole("status")).toHaveText(
        "Workspace identity unavailable",
      );
      await expect(fresh.locator('[data-sidebar="sidebar"]')).toHaveCount(0);
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
    let settled = false;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let delayNext = true;
    await delayed.route("**/api/branding/presentation", async (route) => {
      if (!delayNext) return route.continue();
      delayNext = false;
      const actual = await route.fetch();
      expect(actual.status()).toBe(200);
      expect(await actual.json()).toEqual(first);
      captured = true;
      await gate;
      await route.fulfill({ response: actual }).catch(() => undefined);
      settled = true;
    });
    try {
      await delayed.goto(threadPage);
      await expect.poll(() => captured, { timeout: 15_000 }).toBe(true);
      activate("B");
      await delayed.reload();
      await admitted(delayed, revised);
      const share = await openSharePreview(delayed, revised);
      release();
      await expect.poll(() => settled).toBe(true);
      await expect(
        share.getByText(shareAudience(revised), { exact: true }),
      ).toBeVisible();
      await share.getByRole("button", { name: "Cancel", exact: true }).click();
      await admitted(delayed, revised);
    } finally {
      release();
      await delayed.close();
    }

    const productOnly = activate("Only");
    await page.reload();
    await productSidebar(page, "Example Product");
    const selected = await presentation(page);
    expect(selected).toEqual(productOnly);
    expect(selected.mode).toBe("product-only");
    expect(selected.tenant).toBeNull();
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
    expect((await presentation(page)).mode).toBe("legacy");
    const legacyFeatures = await page.request.get("/api/features");
    expect(legacyFeatures.status()).toBe(200);
    expect((await legacyFeatures.json()).branding.company_name).toBe(
      "Example Legacy Tenant",
    );
    await productSidebar(page, "HartMesh");
    activate("Default");
    await page.reload();
    expect((await presentation(page)).mode).toBe("legacy");
    await productSidebar(page, "HartMesh");

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
    await expect(page.locator('[data-sidebar="sidebar"]')).toHaveCount(0);
    expect((await request.get("/api/branding/presentation")).status()).toBe(
      401,
    );
  } finally {
    stop();
  }
});

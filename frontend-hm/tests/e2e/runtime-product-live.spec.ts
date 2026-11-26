/**
 * Real Gateway + production Next, no API mocks. Opt in with
 * HARTMESH_LIVE_PRODUCT_IDENTITY=1, PLAYWRIGHT_SKIP_WEB_SERVER=1,
 * PLAYWRIGHT_BASE_URL and HARTMESH_TEST_PRODUCT_NAME. The Gateway must use
 * disposable SQLite/home/config, normal local authentication and no integrations.
 * Run default, Example Product One, Example Product Two against the same build.
 */
import { expect, test } from "@playwright/test";

import { contrastRatio } from "../fixtures/contrast-ratio";

const enabled = process.env.HARTMESH_LIVE_PRODUCT_IDENTITY === "1";
const product = process.env.HARTMESH_TEST_PRODUCT_NAME ?? "HartMesh";
const provider = process.env.HARTMESH_TEST_PROVIDER_NAME;

test("runtime identity across public entry and authenticated product information", async ({
  page,
  request,
}) => {
  test.setTimeout(90_000);
  test.skip(
    !enabled,
    "requires isolated real services with ordinary authentication",
  );
  const identity = await request.get("/api/v1/auth/product-identity");
  expect(identity.status()).toBe(200);
  const body = await identity.json();
  expect(body.name).toBe(product);
  expect((await request.get("/api/features")).status()).toBe(401);
  expect((await request.get("/api/branding/logo")).status()).toBe(401);

  await page.goto("/login");
  await expect(
    page.getByRole("heading", { name: product, exact: true }),
  ).toBeVisible();
  await expect(page).toHaveTitle(product);
  if (provider) {
    await expect(
      page.getByRole("link", { name: `${provider} support` }),
    ).toHaveAttribute("href", body.provider.support_url);
    await expect(
      page.getByRole("img", { name: `${product} logo` }),
    ).toBeVisible();
    expect(
      await page.locator('link[rel="icon"]').last().getAttribute("href"),
    ).toBe(body.product.browser_icon);
    const contrast = await page
      .getByRole("link", { name: `${provider} support` })
      .evaluate((element) => {
        const css = getComputedStyle(element);
        return { color: css.color, background: css.backgroundColor };
      });
    expect(contrast).toEqual({
      color: "rgb(0, 0, 0)",
      background: "rgb(255, 255, 255)",
    });
  }
  if (provider) {
    for (const scheme of ["light", "dark"] as const) {
      await page.emulateMedia({ colorScheme: scheme });
      await page.setViewportSize({ width: 390, height: 844 });
      await expect
        .poll(() =>
          contrastRatio(
            page.getByRole("heading", { name: product, exact: true }),
          ),
        )
        .toBeGreaterThanOrEqual(3);
      expect(
        await contrastRatio(
          page.getByRole("link", { name: `${provider} support` }),
        ),
      ).toBeGreaterThanOrEqual(4.5);
      const output = process.env.HARTMESH_PRODUCT_SCREENSHOT_DIR;
      if (output)
        await page.screenshot({
          path: `${output}/${product.replaceAll(" ", "-")}-${scheme}.png`,
        });
    }
    await page.setViewportSize({ width: 1280, height: 900 });
  }
  await page.goto("/setup");
  await expect(
    page.getByRole("heading", { name: product, exact: true }),
  ).toBeVisible();
  const setup = await (
    await page.request.get("/api/v1/auth/setup-status")
  ).json();
  if (setup.needs_setup) {
    const initialized = await page.request.post("/api/v1/auth/initialize", {
      data: {
        email: "operator@example.com",
        password: "SyntheticProduct!8642",
        remember_me: false,
      },
    });
    expect(initialized.status(), await initialized.text()).toBe(201);
  } else {
    const login = await page.request.post("/api/v1/auth/login/local", {
      form: {
        username: "operator@example.com",
        password: "SyntheticProduct!8642",
        remember_me: "false",
      },
    });
    expect(login.status()).toBe(200);
  }
  await page.goto("/workspace");
  await expect(page.getByRole("link", { name: "New chat" })).toBeVisible();
  if (provider) {
    expect(
      await contrastRatio(page.getByText(product, { exact: true })),
    ).toBeGreaterThanOrEqual(4.5);
    const sidebar = page.locator('[data-sidebar="sidebar"]');
    const logo = sidebar.locator("img").first();
    await expect(logo).toHaveAttribute("src", body.product.logo);
    await expect(sidebar.getByRole("link", { name: /support/i })).toHaveCount(
      0,
    );
    const rail = page.locator('[data-sidebar="rail"]');
    await rail.click();
    await expect(logo).toHaveAttribute("src", body.product.logo);
    await expect(sidebar.getByRole("link", { name: /support/i })).toHaveCount(
      0,
    );
    await rail.click();
  }
  await page.getByRole("button", { name: /settings and more/i }).click();
  await page.getByRole("menuitem", { name: /About/ }).click();
  if (provider) {
    await expect(
      page.getByRole("heading", { name: product, exact: true }),
    ).toBeVisible();
    await expect(page.getByText(/HartMesh component version/)).toBeVisible();
    await expect(
      page.getByRole("link", { name: `${provider} support` }),
    ).toHaveAttribute("href", body.provider.support_url);
    expect(
      await contrastRatio(page.getByText(/HartMesh component version/)),
    ).toBeGreaterThanOrEqual(4.5);
  } else await expect(page.getByText(/Workspace version/)).toBeVisible();
  const output = process.env.HARTMESH_PRODUCT_SCREENSHOT_DIR;
  if (output)
    await page.screenshot({
      path: `${output}/${product.replaceAll(" ", "-")}-about.png`,
    });
});

// Explicit opt-in stops ONLY the caller-supplied isolated test Gateway process.
test("reader outage preserves an established page and makes fresh identity unavailable", async ({
  page,
  context,
}) => {
  const pid = Number(process.env.HARTMESH_PRODUCT_OUTAGE_GATEWAY_PID);
  test.skip(
    !enabled || !Number.isInteger(pid) || pid <= 1,
    "requires a caller-owned disposable Gateway PID",
  );
  await page.goto("/login");
  await expect(
    page.getByRole("heading", { name: product, exact: true }),
  ).toBeVisible();
  const title = await page.title();
  const icon = await page
    .locator('link[rel="icon"]')
    .last()
    .getAttribute("href");
  process.kill(pid, "SIGTERM");
  await expect
    .poll(
      async () => {
        try {
          return (
            await page.request.get("/api/v1/auth/product-identity")
          ).status();
        } catch {
          return 503;
        }
      },
      { timeout: 15000 },
    )
    .toBeGreaterThanOrEqual(500);
  await expect(
    page.getByRole("heading", { name: product, exact: true }),
  ).toBeVisible();
  expect(await page.title()).toBe(title);
  expect(
    await page.locator('link[rel="icon"]').last().getAttribute("href"),
  ).toBe(icon);
  const fresh = await context.newPage();
  await fresh.goto("/login");
  await expect(
    fresh.getByRole("heading", {
      name: "Service identity unavailable",
      exact: true,
    }),
  ).toBeVisible();
  await expect(fresh).toHaveTitle("Service identity unavailable");
  await expect(fresh.getByRole("link", { name: /support/ })).toHaveCount(0);
  expect(
    await fresh.locator('link[rel="icon"]').last().getAttribute("href"),
  ).toBe("data:,");
});

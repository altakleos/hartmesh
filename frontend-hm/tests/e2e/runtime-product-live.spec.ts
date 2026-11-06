/**
 * Real Gateway + production Next, no API mocks. Opt in with
 * HARTMESH_LIVE_PRODUCT_IDENTITY=1, PLAYWRIGHT_SKIP_WEB_SERVER=1,
 * PLAYWRIGHT_BASE_URL and HARTMESH_TEST_PRODUCT_NAME. The Gateway must use
 * disposable SQLite/home/config, normal local authentication and no integrations.
 * Run default, Example Product One, Example Product Two against the same build.
 */
import { expect, test, type Locator } from "@playwright/test";

async function contrastRatio(locator: Locator) {
  return locator.evaluate((element) => {
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = 1;
    const ctx = canvas.getContext("2d")!;
    const rgba = (color: string) => {
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillStyle = color;
      ctx.fillRect(0, 0, 1, 1);
      return Array.from(ctx.getImageData(0, 0, 1, 1).data);
    };
    const luminance = (values: number[]) =>
      values
        .slice(0, 3)
        .map((v) => {
          const c = v / 255;
          return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
        })
        .reduce((n, c, i) => n + c * [0.2126, 0.7152, 0.0722][i]!, 0);
    const fg = luminance(rgba(getComputedStyle(element).color));
    let parent: Element | null = element;
    let bg = [255, 255, 255, 255];
    while (parent) {
      const value = rgba(getComputedStyle(parent).backgroundColor);
      if (value[3] === 255) {
        bg = value;
        break;
      }
      parent = parent.parentElement;
    }
    const b = luminance(bg);
    return (Math.max(fg, b) + 0.05) / (Math.min(fg, b) + 0.05);
  });
}

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
    await expect(
      page.getByRole("link", { name: `${provider} support` }).first(),
    ).toBeVisible();
  }
  await page.getByRole("button", { name: /settings and more/i }).click();
  await page.getByRole("menuitem", { name: /About/ }).click();
  if (provider) {
    await expect(
      page.getByRole("heading", { name: product, exact: true }),
    ).toBeVisible();
    await expect(page.getByText(/HartMesh component version/)).toBeVisible();
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

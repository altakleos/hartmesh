import { expect, test, type Page } from "@playwright/test";

const company = "Example Tenant One";
const provider = "Example Provider";
const support = "https://support.example.test/help";
const product = process.env.HARTMESH_TEST_PRODUCT_NAME ?? "HartMesh";
const configuredProvider = process.env.HARTMESH_TEST_PROVIDER_NAME;

async function signIn(page: Page) {
  const credentials = {
    email: "operator@example.com",
    password: "SyntheticTenant!8642",
    remember_me: false,
  };
  const setup = await page.request.get("/api/v1/auth/setup-status");
  expect(setup.ok()).toBe(true);
  if ((await setup.json()).needs_setup) {
    const created = await page.request.post("/api/v1/auth/initialize", {
      data: credentials,
    });
    expect(created.status()).toBe(201);
    await page.context().clearCookies();
  }
  await page.goto("/login");
  await expect(page.locator("body")).not.toContainText(company);
  await page.getByLabel("Email", { exact: true }).fill(credentials.email);
  await page.getByLabel("Password", { exact: true }).fill(credentials.password);
  await page.getByRole("button", { name: "Sign In", exact: true }).click();
  await expect(page).toHaveURL(/\/workspace\/chats\/new/);
}

test("one tolerant bundle supplies private company content while page titles and navigation use the public product", async ({
  page,
  request,
}) => {
  test.skip(
    process.env.HARTMESH_LIVE_TENANT_BUNDLE !== "1",
    "requires a real authenticated Gateway and production Next server",
  );
  test.setTimeout(120_000);
  const removedPath = "/api/branding/presentation";
  const obsoleteRequests: string[] = [];
  page.on("request", (event) => {
    if (new URL(event.url()).pathname === removedPath)
      obsoleteRequests.push(event.url());
  });
  for (const path of ["/api/features", "/api/branding/logo"]) {
    expect((await request.get(path)).status()).toBe(401);
  }
  const publicIdentity = await request.get("/api/v1/auth/product-identity");
  expect(publicIdentity.ok()).toBe(true);
  const identity = await publicIdentity.json();
  expect(identity.mode).toBe(configuredProvider ? "configured" : "default");
  expect(identity.name).toBe(product);
  expect(JSON.stringify(identity)).not.toContain(company);
  await signIn(page);

  const features = await page.request.get("/api/features");
  expect(features.ok()).toBe(true);
  expect((await features.json()).branding).toMatchObject({
    company_name: company,
    colors: { primary: "#123456", secondary: "#abcdef" },
    has_logo: true,
    provider: { display_name: provider, support_url: support },
  });
  const logo = await page.request.get("/api/branding/logo");
  expect(logo.status()).toBe(200);
  expect(logo.headers()["cache-control"]).toBe("private, max-age=300");
  expect(logo.headers()["content-type"]).toBe("image/png");
  expect((await logo.body()).subarray(0, 8)).toEqual(
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
  );
  expect((await page.request.get(removedPath)).status()).toBe(404);
  await expect(page).toHaveTitle(`New chat - ${product}`);
  await expect(page.getByText(/is AI and can make mistakes/)).toHaveCount(0);

  const sidebar = page.locator('[data-sidebar="sidebar"]');
  const header = sidebar.locator('[data-sidebar="header"]');
  await expect(header.getByText(product, { exact: true })).toBeVisible();
  expect(await sidebar.ariaSnapshot()).not.toContain(company);
  expect(await sidebar.innerHTML()).not.toContain(company);
  await expect(sidebar.locator('img[src*="/api/branding/logo"]')).toHaveCount(
    0,
  );
  if (configuredProvider) {
    await expect(header.locator("img")).toHaveAttribute(
      "src",
      identity.product.logo,
    );
  } else {
    await expect(header.locator("img")).toHaveCount(0);
  }
  await page.locator('[data-sidebar="rail"]').click();
  await expect(
    page.locator('[data-slot="sidebar"][data-state]'),
  ).toHaveAttribute("data-state", "collapsed");
  const mark = header.getByTitle(product, { exact: true });
  if (configuredProvider) {
    await expect(mark.locator("img")).toHaveAttribute(
      "src",
      identity.product.logo,
    );
  } else {
    await expect(mark).toHaveText("H");
  }
  await expect(header.getByText("ET", { exact: true })).toHaveCount(0);
  await page.locator('[data-sidebar="rail"]').click();

  await page.getByRole("button", { name: /settings and more/i }).click();
  await page
    .getByRole("menuitem", { name: `About ${product}`, exact: true })
    .click();
  const about = page.getByRole("dialog");
  await expect(
    about.getByRole("heading", {
      name: configuredProvider ? product : company,
      exact: true,
    }),
  ).toBeVisible();
  const supportLink = about.getByRole("link", {
    name: configuredProvider
      ? `${configuredProvider} support`
      : `Contact ${provider} support`,
    exact: true,
  });
  await expect(supportLink).toHaveAttribute("href", support);
  await expect(supportLink).toHaveAttribute("referrerpolicy", "no-referrer");
  await expect(
    about.getByText(
      configuredProvider ? /HartMesh component version/ : /Workspace version/,
    ),
  ).toBeVisible();
  await about.getByRole("button", { name: "Close", exact: true }).click();

  await page.getByRole("link", { name: "Files", exact: true }).click();
  await expect(page).toHaveTitle(`My files - ${product}`);
  const threadId = crypto.randomUUID();
  const csrf = (await page.context().cookies()).find(
    (cookie) => cookie.name === "csrf_token",
  )!.value;
  const thread = await page.request.post("/api/langgraph/threads", {
    headers: { "X-CSRF-Token": csrf },
    data: { thread_id: threadId, metadata: {} },
  });
  expect(thread.ok()).toBe(true);
  await page.goto(`/workspace/chats/${threadId}`);
  await page
    .getByRole("button", {
      name: "Share conversation with everyone",
      exact: true,
    })
    .click();
  const share = page.getByRole("dialog", {
    name: "Share with everyone",
    exact: true,
  });
  await expect(
    share.getByText(
      `Everyone at ${company} and their assistants can read this copy and its files.`,
      { exact: true },
    ),
  ).toBeVisible();
  await share.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(obsoleteRequests).toEqual([]);
});

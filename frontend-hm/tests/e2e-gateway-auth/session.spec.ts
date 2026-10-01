import { expect, test } from "@playwright/test";

test("login, SSR session, CSRF, and logout work through the Hartmesh proxy", async ({
  page,
  context,
}) => {
  const email = `hm-${Date.now()}@example.com`;
  const password = "local-test-password-123456";
  // The replay database is empty. Initializing an admin is distinct from
  // registering a regular user; otherwise SSR correctly redirects to /setup.
  const initialized = await context.request.post("/api/v1/auth/initialize", {
    data: { email: `admin-${email}`, password },
  });
  // A retry shares the same isolated Gateway and may find its existing admin.
  expect([201, 409]).toContain(initialized.status());
  await context.clearCookies();
  const registered = await context.request.post("/api/v1/auth/register", {
    data: { email, password },
  });
  expect(registered.status()).toBe(201);
  await context.clearCookies();

  expect((await context.request.get("/api/v1/auth/me")).status()).toBe(401);
  await page.goto("/workspace/chats/new");
  await expect(page).toHaveURL(/\/login(?:\?|$)/);
  await page.getByLabel("Email", { exact: true }).fill(email);
  await page.getByLabel("Password", { exact: true }).fill(password);
  await page.getByRole("button", { name: "Sign In", exact: true }).click();
  await expect(page.getByPlaceholder(/how can i assist you/i)).toBeVisible();

  const cookies = await context.cookies();
  expect(
    cookies.find((cookie) => cookie.name === "access_token")?.httpOnly,
  ).toBe(true);
  const csrf = cookies.find((cookie) => cookie.name === "csrf_token")?.value;
  expect(csrf).toBeTruthy();
  const withoutCsrf = await context.request.post("/api/langgraph/threads", {
    data: { thread_id: crypto.randomUUID(), metadata: {} },
  });
  expect(withoutCsrf.status()).toBe(403);
  const created = await context.request.post("/api/langgraph/threads", {
    headers: { "X-CSRF-Token": csrf! },
    data: { thread_id: crypto.randomUUID(), metadata: {} },
  });
  expect(created.ok()).toBe(true);

  await page.reload();
  await expect(page.getByPlaceholder(/how can i assist you/i)).toBeVisible();
  const logout = await context.request.post("/api/v1/auth/logout", {
    headers: { "X-CSRF-Token": csrf! },
  });
  expect(logout.ok()).toBe(true);
  await page.reload();
  await expect(page).toHaveURL(/\/login(?:\?|$)/);
});

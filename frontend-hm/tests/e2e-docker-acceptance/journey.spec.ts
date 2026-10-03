import { expect, test } from "@playwright/test";

const artifact =
  "Synthetic acceptance artifact.\nUploaded text: UPLOAD-ACCEPTANCE-726\n";

test("production Docker login, upload, streamed tools, files and persisted history", async ({
  page,
  context,
}) => {
  const email = "acceptance@example.com";
  const password = "synthetic-acceptance-password-123";
  const initialized = await context.request.post("/api/v1/auth/initialize", {
    data: { email: "admin@example.com", password },
  });
  expect(initialized.status(), await initialized.text()).toBe(201);
  await context.clearCookies();
  const registered = await context.request.post("/api/v1/auth/register", {
    data: { email, password },
  });
  expect(registered.status(), await registered.text()).toBe(201);
  await context.clearCookies();

  await page.goto("/workspace/chats/new");
  await expect(page).toHaveURL(/\/login(?:\?|$)/);
  await page.getByLabel("Email", { exact: true }).fill(email);
  await page.getByLabel("Password", { exact: true }).fill(password);
  await page.getByRole("button", { name: "Sign In", exact: true }).click();
  const input = page.getByPlaceholder(/how can i assist you/i);
  await expect(input).toBeVisible();
  const cookies = await context.cookies();
  expect(
    cookies.find((cookie) => cookie.name === "access_token")?.httpOnly,
  ).toBe(true);
  const csrf = cookies.find((cookie) => cookie.name === "csrf_token")!.value;
  expect(
    (
      await context.request.post("/api/langgraph/threads", {
        data: { metadata: {} },
      })
    ).status(),
  ).toBe(403);

  await page.getByLabel("Upload files").setInputFiles({
    name: "input.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("UPLOAD-ACCEPTANCE-726\n"),
  });
  const streamed = page.waitForResponse(
    (response) =>
      response.url().includes("/runs/stream") &&
      response.request().method() === "POST",
  );
  await input.fill(
    "acceptance:file — read my upload and create the synthetic artifact.",
  );
  await input.press("Enter");
  const stream = await streamed;
  expect(stream.status()).toBe(200);
  expect(stream.headers()["content-type"]).toContain("text/event-stream");
  await expect(
    page.getByText("Acceptance complete. Your synthetic artifact is ready.", {
      exact: true,
    }),
  ).toBeVisible({ timeout: 60_000 });
  expect(await stream.finished()).toBeNull();
  await expect(page).toHaveURL(/\/workspace\/chats\/[a-f0-9-]+$/);
  const chatUrl = page.url();
  const thread = new URL(chatUrl).pathname.split("/").at(-1)!;
  const artifactUrl = `/api/threads/${thread}/artifacts/mnt/user-data/outputs/acceptance.txt`;
  const downloaded = await context.request.get(artifactUrl);
  expect(downloaded.ok(), await downloaded.text()).toBe(true);
  expect(await downloaded.text()).toBe(artifact);
  const range = await context.request.get(artifactUrl, {
    headers: { Range: "bytes=0-8" },
  });
  expect(range.status()).toBe(206);
  expect(await range.text()).toBe("Synthetic");

  // The actions use real authenticated endpoints; their resulting lists render in the UI.
  const headers = { "X-CSRF-Token": csrf };
  const kept = await context.request.post(`/api/threads/${thread}/files`, {
    headers,
    data: { path: "/mnt/user-data/outputs/acceptance.txt" },
  });
  expect(kept.ok(), await kept.text()).toBe(true);
  const shared = await context.request.post("/api/shared/publish", {
    headers,
    data: { thread_id: thread, path: "/mnt/user-data/outputs/acceptance.txt" },
  });
  expect(shared.status(), await shared.text()).toBe(201);
  await page.goto("/workspace/files");
  await expect(
    page.getByRole("link", { name: "acceptance.txt", exact: true }),
  ).toBeVisible();
  await page.getByRole("tab", { name: "Shared", exact: true }).click();
  await expect(
    page.getByRole("link", { name: "acceptance.txt", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: `${test.info().outputDir}/files.png`,
    fullPage: true,
  });

  await page.goto(chatUrl);
  await expect(
    page.getByText("Acceptance complete. Your synthetic artifact is ready.", {
      exact: true,
    }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByText("Acceptance complete. Your synthetic artifact is ready.", {
      exact: true,
    }),
  ).toBeVisible();
  await page.screenshot({
    path: `${test.info().outputDir}/chat.png`,
    fullPage: true,
  });
  const logout = await context.request.post("/api/v1/auth/logout", { headers });
  expect(logout.ok()).toBe(true);
  await page.reload();
  await expect(page).toHaveURL(/\/login(?:\?|$)/);
  expect((await context.request.get("/api/files")).status()).toBe(401);
});

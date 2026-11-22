import { expect, test } from "@playwright/test";

test("searches through the installed conversation interface", async ({
  page,
  context,
}) => {
  const phase = process.env.HARTMESH_SEARCH_BROWSER_PHASE;
  expect(["active", "off", "replay"]).toContain(phase);
  const active = phase === "active";
  const login = await context.request.post("/api/v1/auth/login/local", {
    form: {
      username: "search-0@example.com",
      password: "synthetic-acceptance-password-123",
    },
  });
  expect(login.status()).toBe(200);
  const features = await context.request.get("/api/features");
  expect(await features.json()).toMatchObject({ message_search: active });
  await page.goto("/workspace/chats");
  const search = page.getByRole("searchbox", {
    name: active ? "Search conversations" : "Search conversation titles",
  });
  if (active) {
    await search.fill("saffron ledger witness");
    const message = page
      .getByRole("region", { name: "Messages" })
      .getByRole("button", { name: /saffron ledger witness/i });
    await message.click();
    const context = page.getByRole("dialog", { name: "Message context" });
    await expect(context).toContainText("saffron ledger witness");
    await context.getByRole("link", { name: "Open conversation" }).click();
  } else {
    await search.fill("Quarterly archive");
    await page.getByRole("link", { name: /Quarterly archive/ }).click();
  }
  await expect(page).toHaveURL(/\/workspace\/chats\/sample-0000$/);
});

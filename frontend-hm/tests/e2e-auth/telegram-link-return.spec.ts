import { expect, test } from "@playwright/test";

const ADDRESS = "/workspace?settings=channels&telegram=chat";

test("a link opened signed-out keeps its page and query through sign-in", async ({
  page,
}) => {
  await page.goto(ADDRESS);

  await expect(page).toHaveURL(/\/login\?next=/, { timeout: 15_000 });
  const next = new URL(page.url()).searchParams.get("next");
  expect(next).toBe(ADDRESS);
});

test("the workspace page keeps the query of a link after sign-in", async ({
  context,
  page,
  baseURL,
}) => {
  // A session the gateway cannot check reaches the workspace, not sign-in.
  await context.addCookies([
    { name: "access_token", value: "fixture", url: baseURL ?? "" },
  ]);
  await page.goto(ADDRESS);

  await expect(page).toHaveURL(/\/workspace\/chats\/new\?settings=channels/, {
    timeout: 15_000,
  });
});

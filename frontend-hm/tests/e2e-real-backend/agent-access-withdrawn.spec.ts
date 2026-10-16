import { expect, test } from "@playwright/test";

import { inputRequest } from "../fixtures/attention";

import { MOCK_THREAD_ID, setupAgents } from "./utils/compose-agents";

test("the chat is read-only and the agent is gone from the list of its kind, Recent chats and Attention counts", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.humanInputs = [
    inputRequest({
      id: "e".repeat(32),
      instance_id: state.instance.id,
      recipient_id: "default",
    }),
  ];
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(page.getByRole("textbox").first()).toBeEnabled();
  await expect(
    page.getByRole("link", { name: /Attention/ }).locator("[title]"),
  ).toHaveAttribute("title", "1 waiting for your reply");
  await expect(
    page.getByRole("link", { name: /Guide discussion/ }),
  ).toBeVisible();
  state.withdrawn = true;
  await page.reload();
  await expect(
    page.getByText("You no longer have access to Researcher.", { exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("textbox").first()).toBeDisabled();
  await page.goto("/workspace/assistants");
  await expect(page.getByRole("link", { name: /Researcher/ })).toHaveCount(0);
  await expect(
    page.getByRole("link", { name: /Guide discussion/ }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("link", { name: /Attention/ }).locator("[title]"),
  ).toHaveAttribute("title", "0 waiting for your reply");
});

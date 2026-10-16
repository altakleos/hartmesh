import { expect, test } from "@playwright/test";

import { addBrief, MOCK_THREAD_ID, setupAgents } from "./utils/compose-agents";

test("role and brief in one step lead to a first conversation with the welcome", async ({
  page,
}) => {
  const state = await setupAgents(page, { empty: true });
  const dialog = await addBrief(page, "personal");
  await expect(dialog.getByText(/Step /)).toHaveCount(0);
  await dialog
    .getByRole("button", { name: "Add AI assistant", exact: true })
    .click();
  await expect(page).toHaveURL(
    new RegExp(`/workspace/chats/${MOCK_THREAD_ID}`),
  );
  await expect(
    page.getByRole("heading", { name: "Researcher is ready" }),
  ).toBeVisible();
  await expect(
    page.getByText("Researcher does not see My files.", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Researcher can read Shared.", { exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("textbox").first()).toHaveValue(
    /Hello Researcher/,
  );
  expect(state.definitions[0]?.soul).toBe(
    "Research carefully. Ask before changing shared files.",
  );
  expect(state.creations[0]?.custody).toBe("personal");
  expect(state.runs).toBe(0);
  expect(state.executionStarts).toEqual([]);
});

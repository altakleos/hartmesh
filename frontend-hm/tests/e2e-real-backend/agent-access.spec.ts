import { expect, test } from "@playwright/test";

import {
  COLLEAGUE,
  detail,
  MOCK_THREAD_ID,
  setupAgents,
  refreshAccount,
} from "./utils/compose-agents";

for (const inspect of [true, false]) {
  test(
    inspect
      ? "a colleague saved with Can chat and Can see all work opens a conversation and the Files tab"
      : "a second colleague with Can chat alone sees a produced file by name and no list on the Files tab",
    async ({ page }) => {
      const state = await setupAgents(page, {
        messages: [
          {
            type: "ai",
            id: "output",
            content: "Here is the report.",
            additional_kwargs: {
              presented_files: ["/mnt/user-data/outputs/report.txt"],
            },
          },
        ],
      });
      state.files.push({
        name: "report.txt",
        path: "outputs/report.txt",
        kind: "file",
        size: 5,
        modified: 1,
        accessible: true,
      });
      await detail(page, "access");
      await page
        .getByRole("button", { name: "Add a colleague", exact: true })
        .click();
      await page
        .getByRole("combobox", { name: "Add a colleague" })
        .fill(COLLEAGUE.email);
      await page.getByRole("option", { name: /Alex Rivera/ }).click();
      const row = page.getByRole("group", { name: "Alex Rivera", exact: true });
      await expect(row.getByLabel("Can chat", { exact: true })).toBeChecked();
      await expect(
        row.getByLabel("Can see all work", { exact: true }),
      ).toBeChecked();
      if (!inspect) {
        await row.getByLabel("Can see all work", { exact: true }).uncheck();
        await expect(
          row.getByText(/cannot open the files Researcher produces/),
        ).toBeVisible();
      }
      await row.getByRole("button", { name: "Save", exact: true }).click();
      await expect
        .poll(
          () =>
            state.grants.find((g) => g.user_id === COLLEAGUE.id)?.permissions,
        )
        .toBe(inspect ? 3 : 1);
      state.user.id = COLLEAGUE.id;
      state.user.email = COLLEAGUE.email;
      state.user.system_role = "user";
      state.instance.permissions = inspect ? 3 : 1;
      await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
      await expect(page.getByRole("textbox").first()).toBeEnabled();
      await refreshAccount(page);
      if (!inspect) {
        await expect(
          page.getByText("report.txt", { exact: true }).first(),
        ).toBeVisible();
        await expect(
          page.getByText(/To open Researcher's files you need/).first(),
        ).toBeVisible();
        await expect(
          page.getByRole("link", { name: "report.txt", exact: true }),
        ).toHaveCount(0);
      }
      await detail(page, "files");
      await refreshAccount(page);
      if (inspect) {
        await page
          .getByRole("button", { name: "outputs", exact: true })
          .click();
        await expect(
          page.getByText("report.txt", { exact: true }).first(),
        ).toBeVisible();
      } else {
        await expect(
          page.getByText(/To see Researcher's files you need/),
        ).toBeVisible();
        await expect(page.getByRole("table")).toHaveCount(0);
      }
    },
  );
}

test("with work in flight the dialog names the work and Apply when idle applies it later", async ({
  page,
}) => {
  const state = await setupAgents(page);
  state.idleConflict = true;
  state.instance.activity = {
    ...state.instance.activity,
    running_runs: 1,
    running: [
      {
        run_id: "run",
        thread_id: MOCK_THREAD_ID,
        requester_id: COLLEAGUE.id,
        work_id: "c".repeat(32),
        objective: "Review the guide",
      },
    ],
  };
  state.grants.push({ user_id: COLLEAGUE.id, permissions: 3 });
  await detail(page, "access");
  const row = page.getByRole("group", { name: "Alex Rivera", exact: true });
  await row.getByLabel("Can manage", { exact: true }).check();
  await row.getByRole("button", { name: "Save", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByText(/the assignment “Review the guide”/),
  ).toBeVisible();
  await dialog
    .getByRole("button", { name: "Apply when Researcher is idle", exact: true })
    .click();
  state.instance.activity = {
    ...state.instance.activity,
    running_runs: 0,
    running: [],
  };
  await expect
    .poll(
      () => state.grants.find((g) => g.user_id === COLLEAGUE.id)?.permissions,
      { timeout: 15000 },
    )
    .toBe(7);
  expect(state.lifecycle).toHaveLength(2);
  expect(state.lifecycle[1]).toEqual(state.lifecycle[0]);
});

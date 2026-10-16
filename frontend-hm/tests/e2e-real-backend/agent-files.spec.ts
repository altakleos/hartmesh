import { expect, test } from "@playwright/test";

import { detail, MOCK_THREAD_ID, setupAgents } from "./utils/compose-agents";
const file = {
  name: "note.txt",
  mimeType: "text/plain",
  buffer: Buffer.from("notes"),
};

test("a file from the composer and from the Files tab is saved at once with no waiting state", async ({
  page,
}) => {
  const state = await setupAgents(page);
  for (const location of ["chat", "files"]) {
    let release!: () => void;
    state.uploadGate = new Promise<void>((resolve) => {
      release = resolve;
    });
    if (location === "chat")
      await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
    else await detail(page, "files");
    await page.locator('input[type="file"]').first().setInputFiles(file);
    await expect(page.getByText(/Uploading…/).first()).toBeVisible();
    release();
    await expect(
      page.getByText("Saved in Researcher's files", { exact: true }).first(),
    ).toBeVisible();
    await expect(page.locator("body")).not.toContainText(
      /waiting for.*workspace/i,
    );
  }
  expect(state.writes).toHaveLength(2);
  expect(state.writes.every((w) => !!w.operation)).toBe(true);
  expect(state.runs).toBe(0);
  expect(state.executionStarts).toEqual([]);
  await detail(page, "files");
  await page.getByRole("button", { name: "uploads", exact: true }).click();
  await expect(
    page.getByText("note.txt", { exact: true }).first(),
  ).toBeVisible();
});

test("an output shows Produced by", async ({ page }) => {
  const state = await setupAgents(page);
  state.files.push({
    name: "report.txt",
    path: "outputs/report.txt",
    kind: "file",
    size: 5,
    modified: 1,
    accessible: true,
  });
  await detail(page, "files");
  await page.getByRole("button", { name: "outputs", exact: true }).click();
  await expect(
    page.getByText(/Produced by Researcher on/).first(),
  ).toBeVisible();
});

test("files that are full and a file that is too large show their sentences", async ({
  page,
}) => {
  const state = await setupAgents(page);
  for (const refusal of ["files-full", "file-too-large"] as const) {
    state.uploadRefusal = refusal;
    for (const location of ["chat", "files"]) {
      if (location === "chat")
        await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
      else await detail(page, "files");
      await page.locator('input[type="file"]').first().setInputFiles(file);
      await expect(
        page
          .getByText(
            refusal === "files-full"
              ? /Researcher's files are full/
              : /note.txt is larger than/,
          )
          .first(),
      ).toBeVisible();
      await expect(page.locator("body")).not.toContainText(
        "PRIVATE DIAGNOSTIC",
      );
      await expect(page.locator("body")).not.toContainText(
        /available bytes|quota|capacity/i,
      );
      if (refusal === "files-full")
        await expect(page.locator("body")).not.toContainText(
          /\b(?:GiB|MiB|GB|MB)\b/,
        );
    }
  }
});

test("the no-start observer detects chat runs, Work activation and workspace prewarm", async ({
  page,
}) => {
  const state = await setupAgents(page);
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(page.getByRole("textbox").first()).toBeEnabled();
  const paths = [
    `/api/langgraph/threads/${MOCK_THREAD_ID}/runs/stream`,
    `/api/agent-instances/${state.instance.id}/work/${"c".repeat(32)}/activate`,
    `/api/threads/${MOCK_THREAD_ID}/workspace/prewarm`,
  ];
  for (const path of paths) {
    // Exercise the actual request observer without starting backend work.
    await page.route(`**${path}`, (route) =>
      route.fulfill({ json: { fixture: true } }),
    );
    await page.evaluate(async (target) => {
      await fetch(target, { method: "POST" });
    }, path);
  }
  expect(state.executionStarts.map((request) => request.path)).toEqual(paths);
});

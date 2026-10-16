import { expect, test } from "@playwright/test";

import {
  EXECUTION_REASONS,
  STORAGE_REASONS,
} from "../../src/core/features/api";

import {
  assignment,
  detail,
  MOCK_THREAD_ID,
  refreshAccount,
  setupAgents,
} from "./utils/compose-agents";

test("each reason disables the controls with its sentence and shows no server text", async ({
  page,
}) => {
  test.setTimeout(120000);
  const state = await setupAgents(page);
  Object.assign(state.feature.agent_instances!, {
    detail: "PRIVATE DIAGNOSTIC",
  });
  const executionSentences: Record<(typeof EXECUTION_REASONS)[number], string> =
    {
      "storage-unavailable":
        "AI assistants and AI employees are not available right now.",
      "agents-none": "AI assistants are not offered here.",
      "operator-hold":
        "AI assistants and AI employees are paused by Example Provider.",
      "host-replaced":
        "Researcher cannot work yet: the service moved to a new server, and Example Provider has to confirm the move. Researcher's files are kept.",
      "space-unverified":
        "Researcher's files did not pass the safety check, so Researcher cannot work. The files are kept. Nothing else is affected.",
      "space-not-assigned":
        "AI assistants and AI employees are not available right now.",
    };
  const storageSentences: Record<(typeof STORAGE_REASONS)[number], string> = {
    "disk-too-small":
      "Files are not available right now: the storage of this deployment is smaller than what is set for it. Chats work without files until Example Provider has fixed this.",
    "storage-pool-missing":
      "Files are not available right now: the storage has not been set up. Chats work without files until Example Provider has fixed this.",
    "inventory-unreadable":
      "Files are not available right now: the storage could not be read. Everything in it is kept. Chats work without files until Example Provider has fixed this.",
    "host-unqualified":
      "The server is not yet set up to run AI assistants and AI employees safely.",
    "admission-failed":
      "AI assistants and AI employees cannot start right now: a safety check on the server failed. Their files are kept.",
  };
  const rows = [
    ...EXECUTION_REASONS.map((reason) => ({ reason, storage: null })),
    ...STORAGE_REASONS.map((storage) => ({
      reason: "storage-unavailable" as const,
      storage,
    })),
  ];
  state.work = [
    assignment(),
    assignment({
      id: "9".repeat(32),
      objective: "Continue the checklist",
      attempt: {
        id: "f".repeat(32),
        status: "cancelled",
        thread_id: MOCK_THREAD_ID,
        requester_id: "default",
      },
    }),
  ];
  for (const { reason, storage } of rows) {
    state.feature.agent_instances!.execution = "unavailable";
    state.feature.agent_instances!.reason = reason;
    state.feature.storage_spaces!.reason = storage;
    state.feature.storage_spaces!.state = storage ? "unavailable" : "ready";
    state.instance.execution = { execution: "unavailable", reason };
    await detail(page, "work");
    await page.getByRole("button", { name: /Review the guide ·/ }).click();
    await expect(
      page.getByRole("button", { name: "New conversation", exact: true }),
    ).toBeDisabled();
    await expect(
      page.getByRole("button", { name: "Work on this", exact: true }),
    ).toBeDisabled();
    const sentence = storage
      ? storageSentences[storage]
      : executionSentences[reason];
    await expect(
      page.getByText(sentence, { exact: true }).first(),
    ).toBeVisible();
    await expect(page.locator("body")).not.toContainText("PRIVATE DIAGNOSTIC");
    await expect(page.locator("body")).not.toContainText(reason);
    if (storage) {
      // The approved support reference can equal a wire code; prose cannot.
      const prose = await page
        .locator("p")
        .filter({ hasNotText: /^Reference for support:/ })
        .allTextContents();
      expect(prose.join(" ")).not.toContain(storage);
    }
    for (const name of ["My assistants", "AI employees"]) {
      const entry = page
        .locator('[data-sidebar="sidebar"]')
        .getByRole(reason === "agents-none" ? "button" : "link", {
          name,
          exact: true,
        });
      await expect(entry).toHaveAttribute("aria-describedby", /.+/);
      if (reason === "agents-none")
        await expect(entry).toHaveAttribute("aria-disabled", "true");
      else await expect(entry.locator("svg.text-amber-600")).toBeVisible();
    }
    if (reason === "operator-hold")
      await expect(
        page
          .getByText(
            /AI assistants and AI employees are paused by Example Provider/,
          )
          .first(),
      ).toBeVisible();
    if (
      storage ||
      reason === "host-replaced" ||
      reason === "space-unverified"
    ) {
      await expect(
        page.getByRole("link", { name: /Example Provider/ }).first(),
      ).toHaveAttribute("href", "https://support.example.test/help");
      await expect(
        page.getByText(/Reference for support:/).first(),
      ).toBeVisible();
    }
    await page
      .getByRole("button", { name: /Continue the checklist ·/ })
      .click();
    await expect(
      page.getByRole("button", { name: "Resume", exact: true }),
    ).toBeDisabled();
    await expect(
      page.getByText(sentence, { exact: true }).first(),
    ).toBeVisible();
    await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
    await expect(page.getByRole("textbox").first()).toBeDisabled();
    await expect(
      page.getByText(sentence, { exact: true }).first(),
    ).toBeVisible();
  }
  state.user.system_role = "user";
  state.instance.execution = {
    execution: "unavailable",
    reason: "space-unverified",
  };
  state.feature.agent_instances!.reason = "space-unverified";
  await detail(page, "work");
  await page.getByRole("button", { name: /Review the guide ·/ }).click();
  await refreshAccount(page);
  await expect(
    page.getByText("Ask an administrator.", { exact: true }).first(),
  ).toBeVisible();
});

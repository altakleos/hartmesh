import { expect, test } from "@playwright/test";

import { workInstance } from "../fixtures/agent-work";

import { mockLangGraphAPI } from "./utils/mock-api";

test("a manager accepts one unfinished Home change and sees refreshed readiness", async ({
  page,
}, testInfo) => {
  mockLangGraphAPI(page);
  const resident = workInstance();
  const home = "b".repeat(32);
  const operation = "c".repeat(32);
  const generation = 3;
  const homePath = `/api/spaces/${home}`;
  const detailPath = `/api/agent-instances/${resident.id}`;
  const row = {
    operation_id: operation,
    generation,
    phase: "pending",
    created_at: new Date(Date.now() - 6 * 60_000).toISOString(),
    request: { action: "put", path: "uploads/example.txt" },
    result: null,
  };
  const bodies: unknown[] = [];
  const refreshed = new Set<string>();
  let releaseHomeRead!: () => void;
  const homeRead = new Promise<void>((resolve) => {
    releaseHomeRead = resolve;
  });
  await page.route(`**${detailPath}/lifecycle`, (route) =>
    route.fulfill({ json: { operations: [] } }),
  );
  await page.route(`**${detailPath}/definition`, (route) =>
    route.fulfill({
      json: {
        revision: resident.definition_revision,
        config: { name: "researcher" },
        soul: "Review research notes.",
      },
    }),
  );
  await page.route(
    (url) => url.pathname === detailPath,
    (route) => {
      if (bodies.length) refreshed.add("instance");
      return route.fulfill({
        json: {
          ...resident,
          supervisor: { kind: "human", subject_id: "default" },
          home_id: home,
          home_generation: generation,
          home: bodies.length
            ? { state: "available", workspace: "absent" }
            : { state: "available", workspace: "running" },
        },
      });
    },
  );
  await page.route(`**${homePath}`, async (route) => {
    if (bodies.length) {
      refreshed.add("home");
      await homeRead;
    }
    return route.fulfill({
      json: {
        id: home,
        permissions: 31,
        generation,
        status: "active",
        storage_state: bodies.length ? "available" : "recovery-pending",
      },
    });
  });
  await page.route(`**${homePath}/files?*`, (route) =>
    route.fulfill({
      json: {
        files: [
          {
            name: "uploads",
            path: "uploads",
            kind: "directory",
            accessible: true,
            size: 0,
            modified: 1720000000,
          },
        ],
        truncated: false,
      },
    }),
  );
  await page.route(`**${homePath}/recovery`, (route) => {
    if (route.request().method() === "POST") {
      bodies.push(route.request().postDataJSON());
      return route.fulfill({ json: {} });
    }
    return route.fulfill({
      json: {
        operations: bodies.length ? [] : [row],
        attachments: bodies.length
          ? []
          : [{ id: "e".repeat(32), phase: "active" }],
        backups: [],
        can_fence: true,
      },
    });
  });
  await page.route(
    (url) =>
      url.pathname === `${homePath}/recovery/${operation}` &&
      url.searchParams.get("generation") === String(generation),
    (route) => {
      if (bodies.length) refreshed.add("operation");
      return route.fulfill({
        json: {
          ...row,
          phase: "failed",
          result: {
            resolution: "owner-accepted-current-state",
            actor_kind: "human",
            actor_id: "default",
          },
        },
      });
    },
  );

  await page.goto(`/workspace/instances?instance=${resident.id}&tab=files`);
  const files = page.getByRole("tabpanel", { name: "Files" });
  await expect(
    files.getByRole("button", { name: "uploads", exact: true }),
  ).toBeVisible();
  await expect(files.getByText(/saving uploads\/example\.txt/i)).toBeVisible();
  const accept = { name: "Accept the files as they are", exact: true };
  await files.getByRole("button", accept).click();
  const dialog = page.getByRole("dialog");
  const confirm = dialog.getByRole("button", accept);
  await expect(confirm).toBeDisabled();
  await expect(dialog.getByText(/numbered|number in its name/i)).toBeVisible();
  const confirmation = testInfo.outputPath("review-home-change.png");
  await page.screenshot({ path: confirmation });
  await testInfo.attach("Manager reviews the unfinished change", {
    path: confirmation,
    contentType: "image/png",
  });
  expect(bodies).toEqual([]);
  await dialog.getByRole("checkbox").check();
  await expect(confirm).toBeEnabled();
  const ready = files.getByText(/Researcher can work again/i);
  try {
    await confirm.click();
    await expect.poll(() => refreshed.has("home")).toBe(true);
    await expect(ready).toHaveCount(0);
  } finally {
    releaseHomeRead();
  }
  await expect(files.getByText(/current files were accepted/i)).toBeVisible();
  await expect(ready).toBeVisible();
  expect(bodies).toEqual([
    {
      generation,
      operation_id: operation,
      file_change_only: true,
      acknowledge_uncertain_outcome: true,
    },
  ]);
  expect(refreshed).toEqual(new Set(["operation", "home", "instance"]));
  const outcome = testInfo.outputPath("confirmed-home-recovery.png");
  await page.screenshot({ path: outcome });
  await testInfo.attach("Confirmed change and refreshed readiness", {
    path: outcome,
    contentType: "image/png",
  });
});

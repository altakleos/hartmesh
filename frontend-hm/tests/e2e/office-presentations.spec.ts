import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

import {
  agent,
  MOCK_THREAD_ID,
  setupAgents,
} from "../e2e-real-backend/utils/compose-agents";

import { mockLangGraphAPI } from "./utils/mock-api";

const filepath = "/mnt/user-data/outputs/sample.xlsx";
const source = readFileSync("tests/fixtures/office/sample.xlsx");
const etag = createHash("sha256").update(source).digest("hex");
const modulePath =
  "../backend/extensions/sources/hartmesh-office-preview/hartmesh_office_preview/static/index.mjs";
const entry = `/api/plugins/hartmesh.office-preview/assets/${"c".repeat(64)}/static/index.mjs`;
const messages = [
  { type: "human", id: "request", content: "Create a sample workbook." },
  {
    type: "ai",
    id: "answer",
    content: "Here is the workbook.",
    tool_calls: [
      { id: "present", name: "present_files", args: { filepaths: [filepath] } },
    ],
  },
  {
    type: "tool",
    id: "result",
    name: "present_files",
    tool_call_id: "present",
    content: "Files presented.",
    additional_kwargs: { presented_files: [filepath] },
  },
];

async function installPackage(page: Page) {
  await page.route("**/api/plugins", (route) =>
    route.fulfill({
      json: [
        {
          viewer_id: "default",
          namespace: "hartmesh.office-preview",
          module: "office-preview.v1",
          entry,
          transport: "assets-v1",
          title: "Office file previews",
          description: "",
          settings: { enabled: true },
          backend_actions: [],
          artifact_presentations: [
            ["workbook", ".xlsx"],
            ["document", ".docx"],
            ["deck", ".pptx"],
          ].map(([id, suffix]) => ({
            id,
            suffixes: [suffix],
            source_max_bytes: 16 * 1024 * 1024,
            preview_max_bytes: 1024 * 1024,
            projection_marker: `office-${id}-v1`,
          })),
        },
      ],
    }),
  );
  await page.route(`**${entry}`, (route) =>
    route.fulfill({
      contentType: "text/javascript",
      body: readFileSync(modulePath),
    }),
  );
}

test("an ordinary chat previews the workbook and downloads its original bytes", async ({
  page,
  context,
}) => {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Workbook sample",
        messages,
        artifacts: [filepath],
      },
    ],
  });
  await installPackage(page);
  const url = `**/api/threads/${MOCK_THREAD_ID}/artifacts${filepath}`;
  await page.route(`${url}?preview=*`, (route) =>
    route.fulfill({
      contentType: "application/json",
      headers: {
        ETag: `"${etag}"`,
        "X-Artifact-Projection": "office-workbook-v1",
        "X-Artifact-Source-Bytes": String(source.length),
      },
      body: readFileSync("tests/fixtures/office/workbook.json"),
    }),
  );
  await context.route(`${url}?download=true`, (route) =>
    route.fulfill({
      contentType:
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      headers: { "Content-Disposition": 'attachment; filename="sample.xlsx"' },
      body: source,
    }),
  );
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await page.getByText("sample.xlsx", { exact: true }).first().click();
  const panel = page.locator("#artifacts");
  await expect(
    panel.getByRole("table", { name: "Sheet: Samples" }),
  ).toBeVisible();
  await expect(panel.getByRole("cell", { name: "Apples" })).toBeVisible();
  const downloaded = page.waitForEvent("download");
  await panel.getByRole("button", { name: "Download", exact: true }).click();
  const file = await downloaded;
  expect(file.suggestedFilename()).toBe("sample.xlsx");
  expect(readFileSync(await file.path())).toEqual(source);
});

test("an AI employee's workbook keeps the ordinary file link", async ({
  page,
}) => {
  await setupAgents(page, {
    instance: agent({ custody: "company" }),
    messages,
  });
  await installPackage(page);
  const projections: string[] = [];
  page.on("request", (request) => {
    if (new URL(request.url()).searchParams.has("preview"))
      projections.push(request.url());
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(page.getByRole("link", { name: "sample.xlsx" })).toHaveAttribute(
    "href",
    new RegExp(`/artifacts${filepath.replace(".", "\\.")}$`),
  );
  expect(projections).toEqual([]);
});

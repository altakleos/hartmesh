import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

/**
 * DF16, through the real app: a revised report must not offer dead downloads.
 *
 * A tenant built a report with PDF, Word and Excel, revised it with a `prose`
 * change that rendered nothing, and reopened on a fresh login. The card still
 * advertised all three; all three returned 404. The cumulative presented-files
 * list is what the card was drawing from, and that list is history — it does
 * not shrink when a rebuild deletes the previous draft's files.
 *
 * These routes are mocks. They exercise the card's decision and the URLs it
 * asks for; they do not exercise real authentication, the released
 * filesystem, or the Gateway's own path resolution.
 */

const THREAD_ID = "00000000-0000-0000-0000-000000003141";
const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const NAME = "2026-08-business-review";
const REPORT_PATH = `${DIRECTORY}/${NAME}.report.json`;
const PDF_PATH = `${DIRECTORY}/${NAME}.pdf`;
const DOCX_PATH = `${DIRECTORY}/${NAME}.docx`;
const XLSX_PATH = `${DIRECTORY}/${NAME}.xlsx`;

const REPORT_JSON = readFileSync(
  join(
    dirname(fileURLToPath(import.meta.url)),
    "..",
    "fixtures",
    "business-report",
    "2026-08-business-review.report.json",
  ),
  "utf-8",
);

const PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64",
);

/** The turn that presented all three renders — history, and it stays. */
function presentedEverything() {
  return [
    {
      type: "human",
      id: "msg-human-report",
      content: [{ type: "text", text: "August report" }],
    },
    {
      type: "ai",
      id: "msg-ai-report",
      content: "Here is your August review.",
      tool_calls: [
        {
          id: "present-report",
          name: "present_files",
          args: { filepaths: [REPORT_PATH, PDF_PATH, DOCX_PATH, XLSX_PATH] },
        },
      ],
    },
  ];
}

async function openTheReport(
  page: Page,
  { live }: { live: readonly string[] },
) {
  const probed: string[] = [];
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: THREAD_ID,
        title: "August review",
        messages: presentedEverything(),
        // The cumulative list after the revision: every render the thread
        // ever presented is still named here, including the deleted ones.
        artifacts: [REPORT_PATH, PDF_PATH, DOCX_PATH, XLSX_PATH],
      },
    ],
  });
  await page.route(
    `**/api/threads/${THREAD_ID}/artifacts${REPORT_PATH}`,
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: REPORT_JSON,
      }),
  );
  await page.route(
    `**/api/threads/${THREAD_ID}/artifacts${DIRECTORY}/charts/*.png`,
    (route) =>
      route.fulfill({ status: 200, contentType: "image/png", body: PNG }),
  );
  for (const path of [PDF_PATH, DOCX_PATH, XLSX_PATH]) {
    await page.route(
      `**/api/threads/${THREAD_ID}/artifacts${path}*`,
      (route) => {
        probed.push(`${path}|${route.request().headers().range ?? ""}`);
        if (!live.includes(path)) {
          // What the tenant's browser got for a deleted render.
          return route.fulfill({ status: 404, body: "Not Found" });
        }
        return route.fulfill({
          status: 206,
          headers: {
            "Content-Range": "bytes 0-0/1024",
            "Accept-Ranges": "bytes",
          },
          body: "x",
        });
      },
    );
  }

  await page.goto(`/workspace/chats/${THREAD_ID}`);
  await expect(page.getByText(`${NAME}.report.json`).first()).toBeVisible({
    timeout: 15_000,
  });
  await page.getByText(`${NAME}.report.json`).first().click();
  return { card: page.getByTestId("business-report-card"), probed };
}

test.describe("report card current renders", () => {
  test("offers nothing when the revision deleted every render", async ({
    page,
  }) => {
    const { card } = await openTheReport(page, { live: [] });

    await expect(card).toBeVisible({ timeout: 15_000 });
    // The card is drawn from the current report either way.
    await expect(
      card.getByRole("heading", { name: "August 2026 Business Review" }),
    ).toBeVisible();
    // The defect: three links, all dead. None of them may appear.
    for (const label of [
      "Download the PDF",
      "Download the Word",
      "Download the Excel",
    ]) {
      await expect(card.getByRole("link", { name: label })).toHaveCount(0);
    }
    // ...and the person is told plainly, once the answer is known.
    await expect(card.getByText("No file to download yet")).toBeVisible();
  });

  test("offers only the render that survived the revision", async ({
    page,
  }) => {
    const { card, probed } = await openTheReport(page, { live: [PDF_PATH] });

    await expect(card).toBeVisible({ timeout: 15_000 });
    await expect(
      card.getByRole("link", { name: "Download the PDF" }),
    ).toBeVisible();
    await expect(
      card.getByRole("link", { name: "Download the Word" }),
    ).toHaveCount(0);
    await expect(
      card.getByRole("link", { name: "Download the Excel" }),
    ).toHaveCount(0);
    await expect(card.getByText("No file to download yet")).toHaveCount(0);

    // The probe is bounded and goes to the authenticated artifact route.
    expect(probed.length).toBeGreaterThan(0);
    for (const entry of probed) {
      expect(entry).toContain("bytes=0-0");
    }

    // The visible link is the ordinary download URL, unchanged.
    await expect(
      card.getByRole("link", { name: "Download the PDF" }),
    ).toHaveAttribute("href", new RegExp(`${NAME}\\.pdf\\?download=true$`));
  });

  test("reaches the same verdict after a fresh reload", async ({ page }) => {
    const first = await openTheReport(page, { live: [XLSX_PATH] });
    await expect(first.card).toBeVisible({ timeout: 15_000 });
    await expect(
      first.card.getByRole("link", { name: "Download the Excel" }),
    ).toBeVisible();

    // A fresh browser rehydrates the same cumulative list through /history.
    await page.reload();
    // Wait for the rehydrated list before opening it, exactly as the first
    // load does: clicking into a panel that is still hydrating makes this a
    // test of timing rather than of the verdict.
    await expect(page.getByText(`${NAME}.report.json`).first()).toBeVisible({
      timeout: 15_000,
    });
    await page.getByText(`${NAME}.report.json`).first().click();
    const card = page.getByTestId("business-report-card");
    await expect(card).toBeVisible({ timeout: 15_000 });
    await expect(
      card.getByRole("link", { name: "Download the Excel" }),
    ).toBeVisible({ timeout: 15_000 });
    await expect(
      card.getByRole("link", { name: "Download the PDF" }),
    ).toHaveCount(0);
  });
});

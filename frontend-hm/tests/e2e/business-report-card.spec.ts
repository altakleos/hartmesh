import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const THREAD_ID = "00000000-0000-0000-0000-000000003140";
const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const REPORT_PATH = `${DIRECTORY}/2026-08-business-review.report.json`;
const PDF_PATH = `${DIRECTORY}/2026-08-business-review.pdf`;
const XLSX_PATH = `${DIRECTORY}/2026-08-business-review.xlsx`;

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

// A one-pixel PNG, so the chart request resolves without a binary fixture.
const PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64",
);

function presentReportMessages() {
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
          args: { filepaths: [REPORT_PATH, PDF_PATH, XLSX_PATH] },
        },
      ],
    },
  ];
}

async function openTheReport(page: Page) {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: THREAD_ID,
        title: "August review",
        messages: presentReportMessages(),
        // What `present_files` presented: the report and two of its three
        // renders, so the card must offer PDF and Excel and not Word.
        artifacts: [REPORT_PATH, PDF_PATH, XLSX_PATH],
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

  await page.goto(`/workspace/chats/${THREAD_ID}`);
  await expect(
    page.getByText("2026-08-business-review.report.json").first(),
  ).toBeVisible({ timeout: 15_000 });
  await page.getByText("2026-08-business-review.report.json").first().click();
  // The panel is a side panel on a wide screen and a dialog on a phone, so
  // the card is addressed by its own handle rather than through either.
  return page.getByTestId("business-report-card");
}

test.describe("business report card", () => {
  test("shows a built report as the card its downloads were rendered from", async ({
    page,
  }) => {
    const card = await openTheReport(page);

    await expect(card).toBeVisible({ timeout: 15_000 });
    await expect(
      card.getByRole("heading", { name: "August 2026 Business Review" }),
    ).toBeVisible();
    await expect(
      card.getByText("Example Services Co. · August 2026 · Draft 1"),
    ).toBeVisible();
    await expect(card.getByTestId("business-report-checks-line")).toContainText(
      "Totals match your file: $74,702.61 across 164 jobs.",
    );
    await expect(card.getByRole("link", { name: "PDF" })).toBeVisible();
    await expect(card.getByRole("link", { name: "Excel" })).toBeVisible();
    await expect(card.getByRole("link", { name: "Word" })).toHaveCount(0);

    // The charts are the report's own pictures, addressed inside its directory.
    const chart = card.getByAltText("Revenue by week");
    await expect(chart).toBeVisible();
    await expect(chart).toHaveJSProperty("naturalWidth", 1);

    // The JSON itself is still one toggle away.
    const artifactsPanel = page.locator("#artifacts");
    await artifactsPanel.getByRole("radio").first().click();
    await expect(card).toHaveCount(0);
    await expect(artifactsPanel.locator(".cm-editor")).toBeVisible();
    await expect(
      artifactsPanel.getByText('"August 2026 Business Review"'),
    ).toBeVisible();
  });

  test("stays readable on a phone", async ({ page }) => {
    await page.setViewportSize({ width: 360, height: 780 });
    const card = await openTheReport(page);

    await expect(card).toBeVisible({ timeout: 15_000 });
    // The downloads are the point of the card on a phone, so they must be
    // reachable without scrolling sideways; only the tables scroll.
    await expect(card.getByRole("link", { name: "PDF" })).toBeInViewport();

    const overflow = await page.evaluate(() => {
      const root = document.documentElement;
      return root.scrollWidth - root.clientWidth;
    });
    expect(overflow).toBeLessThanOrEqual(0);
  });
});

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
        // What the thread has presented: the report and two of its three
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
  // The card proves a render is still there before it offers it, so the two
  // presented renders have to answer the bounded probe as live files.
  for (const path of [PDF_PATH, XLSX_PATH]) {
    await page.route(`**/api/threads/${THREAD_ID}/artifacts${path}*`, (route) =>
      route.fulfill({
        status: 206,
        headers: {
          "Content-Range": "bytes 0-0/1024",
          "Accept-Ranges": "bytes",
        },
        body: "x",
      }),
    );
  }

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
    await expect(
      card.getByRole("link", { name: "Download the PDF" }),
    ).toBeVisible();
    await expect(
      card.getByRole("link", { name: "Download the Excel" }),
    ).toBeVisible();
    await expect(
      card.getByRole("link", { name: "Download the Word" }),
    ).toHaveCount(0);

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
    await expect(
      card.getByRole("link", { name: "Download the PDF" }),
    ).toBeInViewport();

    const overflow = await page.evaluate(() => {
      const root = document.documentElement;
      return root.scrollWidth - root.clientWidth;
    });
    expect(overflow).toBeLessThanOrEqual(0);
  });

  test("keeps every figure on one line inside its own tile in the side panel", async ({
    page,
  }) => {
    // The defect this pins: on a 1440px screen the artifact panel is about
    // 480px wide, a viewport breakpoint put five KPI tiles in it, and
    // `$74,702.61` printed 53px of itself across the number in the tile beside
    // it. Nobody reading the panel could tell which figure went with which
    // label.
    //
    // Layout is the only thing that can catch this, and it has to be measured
    // twice over. The value's box is clamped to its grid track whether or not
    // the text fits, so the box proves nothing; and `break-words` alone drives
    // overflow to zero at any track width, so overflow proves nothing either.
    // What pins the sizing is the line count: a track too narrow for the
    // figure wraps it, and a wrapped figure is a track the fix did not widen.
    await page.setViewportSize({ width: 1440, height: 900 });
    const card = await openTheReport(page);
    await expect(card).toBeVisible({ timeout: 15_000 });

    const boxes = await card.getByTestId("business-report-kpis").evaluate(
      (grid) =>
        Array.from(grid.children).map((tile) => {
          const value = tile.querySelector<HTMLElement>(
            "[data-testid='business-report-kpi-value']",
          )!;
          const range = document.createRange();
          range.selectNodeContents(value);
          const t = tile.getBoundingClientRect();
          return {
            label: value.innerText,
            // One client rect per line box the text occupies.
            lines: range.getClientRects().length,
            overflow: value.scrollWidth - value.clientWidth,
            tile: {
              left: t.left,
              right: t.right,
              top: t.top,
              bottom: t.bottom,
            },
          };
        }),
      { timeout: 15_000 },
    );

    expect(boxes.length).toBeGreaterThan(1);
    for (const { label, lines, overflow } of boxes) {
      expect(
        `${label}: ${lines} line(s), ${Math.max(0, overflow)}px past its tile`,
      ).toBe(`${label}: 1 line(s), 0px past its tile`);
    }
    for (const a of boxes) {
      for (const b of boxes) {
        if (a === b) continue;
        const overlaps =
          a.tile.left < b.tile.right - 0.5 &&
          b.tile.left < a.tile.right - 0.5 &&
          a.tile.top < b.tile.bottom - 0.5 &&
          b.tile.top < a.tile.bottom - 0.5;
        expect(overlaps).toBe(false);
      }
    }
  });
});

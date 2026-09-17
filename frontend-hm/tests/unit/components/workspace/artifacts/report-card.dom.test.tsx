import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";

// The card's *Save to my files* goes through the shared hook, which needs a
// query client and a router the card itself does not; what the card owns is
// which paths it hands over.
const myFiles = rs.hoisted(() => ({
  save: rs.fn<(paths: readonly string[]) => Promise<unknown[]>>(),
  isPending: false,
}));
rs.mock("@/core/files", () => ({
  useSaveToMyFiles: () => ({
    save: myFiles.save,
    isPending: myFiles.isPending,
  }),
}));

import { ReportCard } from "@/components/workspace/artifacts/report-card";
import { parseBusinessReport } from "@/core/business-report";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import fixture from "../../../../fixtures/business-report/2026-08-business-review.report.json";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const REPORT = `${DIRECTORY}/2026-08-business-review.report.json`;
const THREAD = "thread-1";

const report = parseBusinessReport(JSON.stringify(fixture))!;

function renderCard(
  artifacts: string[] = [REPORT],
  override: Partial<typeof report> = {},
  { isMock = false } = {},
) {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <ReportCard
        artifacts={artifacts}
        filepath={REPORT}
        isMock={isMock}
        report={{ ...report, ...override }}
        threadId={THREAD}
      />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  myFiles.save.mockReset();
});

describe("ReportCard", () => {
  it("names the report, the company, the period and the draft", () => {
    renderCard();

    expect(
      screen.getByRole("heading", { name: "August 2026 Business Review" }),
    ).toBeTruthy();
    expect(
      screen.getByText("Example Services Co. · August 2026 · Draft 1"),
    ).toBeTruthy();
  });

  it("shows the figures the way the downloads show them", () => {
    renderCard();

    // The same spellings the PDF, Word and Excel renders produce.
    const kpis = screen.getByTestId("business-report-kpis");
    expect(within(kpis).getByText("$74,702.61")).toBeTruthy();
    expect(within(kpis).getByText("164")).toBeTruthy();
    expect(within(kpis).getByText("$455.50")).toBeTruthy();
    expect(within(kpis).getByText("28.6%")).toBeTruthy();
    expect(kpis.textContent).toContain("▲ 38.7% vs July 2026");
  });

  it("reads a comparison row in its own units", () => {
    const { container } = renderCard();

    // `row_formats` makes the revenue row currency and the jobs row a count,
    // in a table whose column format is a plain number: without it both would
    // read as 53,864.48 and 136 with no currency and no thousands agreement.
    const comparison = within(container.querySelector("#report-comparison")!);
    const revenueRow = comparison.getByText("Revenue").closest("tr")!;
    expect(within(revenueRow).getByText("$53,864.48")).toBeTruthy();
    const jobsRow = comparison.getByText("Jobs").closest("tr")!;
    expect(within(jobsRow).getByText("136")).toBeTruthy();
  });

  it("carries the one line the assistant repeats", () => {
    renderCard();

    const line = screen.getByTestId("business-report-checks-line");
    expect(line.textContent).toContain(
      "Totals match your file: $74,702.61 across 164 jobs.",
    );
    expect(line.textContent).toContain(
      "2 jobs had no technician and are listed as Unassigned.",
    );
  });

  it("lists every check and what was left out", () => {
    renderCard();

    expect(screen.getByText("No job ID appears more than once.")).toBeTruthy();
    expect(screen.getByText("Not included")).toBeTruthy();
    expect(
      screen.getByText(
        "Comparison with August 2025: not included, the files have no rows for that period.",
      ),
    ).toBeTruthy();
  });

  it("addresses each picture inside the report's own directory", () => {
    renderCard();

    const chart = screen.getByAltText("Revenue by week");
    expect(chart.getAttribute("src")).toBe(
      `/api/threads/${THREAD}/artifacts${DIRECTORY}/charts/revenue_by_period.png`,
    );
  });

  it("offers the renders the thread has presented, and only those", () => {
    renderCard([
      REPORT,
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);

    const pdf = screen.getByRole("link", { name: "Download the PDF" });
    expect(pdf.getAttribute("href")).toBe(
      `/api/threads/${THREAD}/artifacts${DIRECTORY}/2026-08-business-review.pdf?download=true`,
    );
    expect(
      screen.getByRole("link", { name: "Download the Excel" }),
    ).toBeTruthy();
    expect(
      screen.queryByRole("link", { name: "Download the Word" }),
    ).toBeNull();
  });

  it("offers no download when nothing was rendered", () => {
    renderCard();

    expect(screen.queryByRole("link", { name: "Download the PDF" })).toBeNull();
    // Nothing to keep either: the button belongs to the downloads.
    expect(
      screen.queryByRole("button", { name: "Save to my files" }),
    ).toBeNull();
  });

  it("keeps the documents the thread has rendered, and only those", () => {
    myFiles.save.mockResolvedValue([]);
    renderCard([
      REPORT,
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);

    fireEvent.click(screen.getByRole("button", { name: "Save to my files" }));

    // The documents, not the JSON the card is drawn from.
    expect(myFiles.save).toHaveBeenCalledWith([
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);
  });

  it("has nowhere to keep a showcase report", () => {
    renderCard(
      [REPORT, `${DIRECTORY}/2026-08-business-review.pdf`],
      {},
      { isMock: true },
    );

    expect(screen.getByRole("link", { name: "Download the PDF" })).toBeTruthy();
    expect(
      screen.queryByRole("button", { name: "Save to my files" }),
    ).toBeNull();
  });

  it("says which file the figures came from", () => {
    renderCard();

    expect(
      screen.getByText(
        "Built from example_services_export_small.csv (uploaded 2026-09-15). Checked by the report script.",
      ),
    ).toBeTruthy();
  });
  it("keeps the sign on a fall too small to show", () => {
    // `_pct_change` returns -0.0 for a drop under 0.05%, and the PDF and the
    // Word file both print -0.0%; `Math.abs` would have lost it.
    renderCard([REPORT], {
      kpis: [
        {
          id: "revenue",
          label: "Revenue",
          value: 74702.61,
          format: "currency",
          delta: { vs: "July 2026", pct: -0 },
        },
      ],
    });

    expect(screen.getByTestId("business-report-kpis").textContent).toContain(
      "-0.0% vs July 2026",
    );
  });

  it("does not award a tick no check earned", () => {
    // The documents print each check's own status and compute no summary, so
    // the glyph is the card's own claim about the file.
    renderCard([REPORT], {
      checks: [
        {
          id: "totals_reconcile",
          status: "not_checked",
          text: "Totals were not checked against your file.",
        },
      ],
    });

    const line = screen.getByTestId("business-report-checks-line");
    expect(line.textContent).not.toContain("✓");
    expect(line.textContent).toContain("not checked");
  });

  it("says why there is nothing to download", () => {
    renderCard();

    expect(
      screen.getByText(
        "No file to download yet — ask for the PDF, Word or Excel version.",
      ),
    ).toBeTruthy();
  });

  it("leaves out a checks section it has nothing to put in", () => {
    renderCard([REPORT], { checks: [], notes: [] });

    expect(screen.queryByText("Checks")).toBeNull();
    expect(screen.queryByTestId("business-report-checks-line")).toBeNull();
  });
});

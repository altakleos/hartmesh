import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, render, screen, within } from "@testing-library/react";

import { ReportCard } from "@/components/workspace/artifacts/report-card";
import { parseBusinessReport } from "@/core/business-report";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import fixture from "../../../../fixtures/business-report/2026-08-business-review.report.json";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const REPORT = `${DIRECTORY}/2026-08-business-review.report.json`;
const THREAD = "thread-1";

const report = parseBusinessReport(JSON.stringify(fixture))!;

function renderCard(artifacts: string[] = [REPORT]) {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <ReportCard
        artifacts={artifacts}
        filepath={REPORT}
        report={report}
        threadId={THREAD}
      />
    </I18nContext.Provider>,
  );
}

afterEach(cleanup);

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
    const comparison = within(container.querySelector("#comparison")!);
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

  it("offers the renders this turn handed over, and only those", () => {
    renderCard([
      REPORT,
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);

    const pdf = screen.getByRole("link", { name: "PDF" });
    expect(pdf.getAttribute("href")).toBe(
      `/api/threads/${THREAD}/artifacts${DIRECTORY}/2026-08-business-review.pdf?download=true`,
    );
    expect(screen.getByRole("link", { name: "Excel" })).toBeTruthy();
    expect(screen.queryByRole("link", { name: "Word" })).toBeNull();
  });

  it("offers no download when nothing was rendered", () => {
    renderCard();

    expect(screen.queryByRole("link", { name: "PDF" })).toBeNull();
  });

  it("says which file the figures came from", () => {
    renderCard();

    expect(
      screen.getByText(
        "Built from example_services_export_small.csv (uploaded 2026-09-15). Checked by the report script.",
      ),
    ).toBeTruthy();
  });
});

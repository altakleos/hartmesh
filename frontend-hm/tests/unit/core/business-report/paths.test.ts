import { describe, expect, it } from "@rstest/core";

import {
  availableReportRenders,
  filingFolderFor,
  isReportRender,
  reportOfRender,
  reportRenderPath,
  reportSiblingPath,
} from "@/core/business-report";

const report =
  "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.report.json";

describe("report companion paths", () => {
  it("addresses a picture inside the report's own directory", () => {
    expect(reportSiblingPath(report, "charts/revenue_by_period.png")).toBe(
      "/mnt/user-data/outputs/reports/2026-08-business-review/charts/revenue_by_period.png",
    );
  });

  it("names each render the way the skill writes it", () => {
    expect(reportRenderPath(report, "pdf")).toBe(
      "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.pdf",
    );
    expect(reportRenderPath(report, "xlsx")).toBe(
      "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.xlsx",
    );
  });

  it("offers only the renders the turn handed over", () => {
    expect(
      availableReportRenders(report, [
        report,
        reportRenderPath(report, "pdf"),
        reportRenderPath(report, "xlsx"),
      ]),
    ).toEqual(["pdf", "xlsx"]);
  });

  it("offers nothing when a render exists under another report's name", () => {
    expect(
      availableReportRenders(report, [
        "/mnt/user-data/outputs/reports/2026-07-business-review/2026-07-business-review.pdf",
      ]),
    ).toEqual([]);
  });
});

describe("recognising a report's render", () => {
  const pdf =
    "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.pdf";

  it("names the report a render belongs to", () => {
    expect(reportOfRender(pdf)).toBe(report);
    expect(reportOfRender(reportRenderPath(report, "xlsx"))).toBe(report);
  });

  it("does not take an ordinary file for a render", () => {
    expect(reportOfRender("/mnt/user-data/outputs/notes.txt")).toBeNull();
    expect(reportOfRender("/mnt/user-data/outputs/no-extension")).toBeNull();
    expect(reportOfRender(report)).toBeNull();
  });

  it("is a render only when the report it belongs to was presented", () => {
    // The name alone is a guess; a PDF that happens to sit beside nothing is
    // just a PDF, and filing it with the reports would be wrong.
    expect(isReportRender(pdf, { artifacts: [report, pdf] })).toBe(true);
    expect(isReportRender(pdf, { artifacts: [pdf] })).toBe(false);
    expect(
      isReportRender("/mnt/user-data/outputs/notes.txt", {
        artifacts: [report],
      }),
    ).toBe(false);
  });

  it("takes the word of a reader that holds the report itself", () => {
    // The card renders from the report, so it knows; it should not have to
    // find that out again from a list.
    expect(isReportRender(pdf, { artifacts: [], report })).toBe(true);
    expect(
      isReportRender(pdf, { artifacts: [], report: `${report}.other` }),
    ).toBe(false);
  });

  it("files a report's render with the reports, and leaves anything else alone", () => {
    expect(filingFolderFor(pdf, { artifacts: [report, pdf] })).toBe("Reports");
    expect(filingFolderFor(pdf, { artifacts: [pdf] })).toBeUndefined();
    expect(
      filingFolderFor("/mnt/user-data/outputs/notes.txt", { artifacts: [] }),
    ).toBeUndefined();
  });
});

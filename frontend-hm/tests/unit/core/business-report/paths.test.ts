import { describe, expect, it } from "@rstest/core";

import {
  availableReportRenders,
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

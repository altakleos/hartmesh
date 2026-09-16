import { describe, expect, it } from "@rstest/core";

import {
  isBusinessReportPath,
  parseBusinessReport,
} from "@/core/business-report";

import fixture from "../../../fixtures/business-report/2026-08-business-review.report.json";

const report = JSON.stringify(fixture);

function withMeta(changes: Record<string, unknown>) {
  return JSON.stringify({
    ...fixture,
    meta: { ...fixture.meta, ...changes },
  });
}

describe("isBusinessReportPath", () => {
  it("claims the skill's own file and nothing else", () => {
    expect(isBusinessReportPath("/o/reports/aug/aug.report.json")).toBe(true);
    expect(isBusinessReportPath("/o/reports/aug/checks.json")).toBe(false);
    expect(isBusinessReportPath("/o/report.json")).toBe(false);
  });
});

describe("parseBusinessReport", () => {
  it("reads a report the skill built", () => {
    // The fixture is a real `build` of the repository's own export fixture,
    // with the rows sheet trimmed; nothing about its shape is hand-written.
    const parsed = parseBusinessReport(report);

    expect(parsed).not.toBeNull();
    expect(parsed?.title).toBe("August 2026 Business Review");
    expect(parsed?.company).toBe("Example Services Co.");
    expect(parsed?.periodLabel).toBe("August 2026");
    expect(parsed?.draft).toBe(1);
    expect(parsed?.currency).toBe("USD");
    expect(parsed?.kpis[0]).toMatchObject({
      label: "Revenue",
      value: 74702.61,
      format: "currency",
      delta: { vs: "July 2026", pct: 38.7 },
    });
    expect(parsed?.sections.map((section) => section.id)).toContain(
      "by_person",
    );
    expect(parsed?.charts.map((chart) => chart.png)).toContain(
      "charts/jobs_by_person.png",
    );
    expect(parsed?.inputs[0]?.name).toBe("example_services_export_small.csv");
    expect(parsed?.notes).toHaveLength(1);
  });

  it("carries the per-row units a comparison table needs", () => {
    const comparison = parseBusinessReport(report)?.sections.find(
      (section) => section.id === "comparison",
    );

    expect(comparison?.table?.rowFormats?.[0]).toEqual([
      "text",
      "currency",
      "currency",
      "percent",
    ]);
  });

  it("refuses anything that is not one of these documents", () => {
    expect(parseBusinessReport("not json")).toBeNull();
    expect(parseBusinessReport("[]")).toBeNull();
    expect(parseBusinessReport('{"version": 1}')).toBeNull();
    // A future version may mean anything; the card is written for version 1.
    expect(
      parseBusinessReport(JSON.stringify({ ...fixture, version: 2 })),
    ).toBeNull();
  });

  it("refuses a report whose figures are not figures", () => {
    const broken = JSON.parse(report) as { kpis: { value: unknown }[] };
    broken.kpis[0]!.value = "74,702.61";

    expect(parseBusinessReport(JSON.stringify(broken))).toBeNull();
  });

  it("refuses a table whose cells are not cells", () => {
    const broken = JSON.parse(report) as {
      sections: { id: string; table?: { rows: unknown[][] } }[];
    };
    const section = broken.sections.find((entry) => entry.table)!;
    section.table!.rows[0]![1] = { total: 1 };

    expect(parseBusinessReport(JSON.stringify(broken))).toBeNull();
  });

  it("drops a picture it cannot address but keeps the figures", () => {
    // The pattern is what stops a path inside the file from becoming an
    // artifact URL, and a missing picture is no reason to withhold numbers.
    const tampered = JSON.parse(report) as {
      charts: { id: string; png: string }[];
    };
    tampered.charts[0]!.png = "../../../etc/passwd";

    const parsed = parseBusinessReport(JSON.stringify(tampered));

    expect(parsed?.charts).toHaveLength(tampered.charts.length - 1);
    expect(parsed?.kpis).toHaveLength(5);
  });

  it("only takes a brand colour it can put in a style", () => {
    const branded = withMeta({
      brand: {
        company: "Cedar",
        primary: "#0a6b3d",
        secondary: "#fff",
        logo: null,
      },
    });
    const injected = withMeta({
      brand: {
        company: "Cedar",
        primary: "red; background-image: url(https://example.test/pixel)",
        secondary: "#fff",
        logo: null,
      },
    });

    expect(parseBusinessReport(branded)?.primaryColor).toBe("#0a6b3d");
    expect(parseBusinessReport(injected)?.primaryColor).toBe("#1F4E79");
  });

  it("falls back to dollars when the currency is not a currency", () => {
    expect(
      parseBusinessReport(
        withMeta({ currency: { code: "dollars", source: "assumed" } }),
      )?.currency,
    ).toBe("USD");
  });
  it("refuses a draft number the contract does not allow", () => {
    // The value is printed as-is, so `Draft Infinity` is a visible defect.
    for (const draft of [0, -3, 1.5, Number.POSITIVE_INFINITY]) {
      expect(parseBusinessReport(withMeta({ draft }))).toBeNull();
    }
  });

  it("refuses an identifier the contract does not allow", () => {
    // Section ids reach a DOM id and every id is a React key.
    const broken = JSON.parse(report) as { sections: { id: string }[] };
    broken.sections[0]!.id = "Summary; drop";

    expect(parseBusinessReport(JSON.stringify(broken))).toBeNull();
  });

  it("refuses a document written to be expensive to draw", () => {
    // Nothing the skill builds comes close to these; `Load full file` clears
    // the byte cap with one click, so the parse is the only other ceiling.
    const huge = JSON.parse(report) as {
      sections: unknown[];
      charts: unknown[];
    };
    huge.sections = Array.from({ length: 200 }, (_, index) => ({
      id: `s${index}`,
      heading: "Section",
    }));

    expect(parseBusinessReport(JSON.stringify(huge))).toBeNull();

    const wide = JSON.parse(report) as {
      sections: { id: string; table?: { rows: unknown[] } }[];
    };
    const section = wide.sections.find((entry) => entry.table)!;
    section.table!.rows = Array.from({ length: 5000 }, () => ["x", 1, 1, 1]);

    expect(parseBusinessReport(JSON.stringify(wide))).toBeNull();
  });
});

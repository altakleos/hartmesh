import { describe, expect, it } from "@rstest/core";

import {
  cellFormat,
  checksLine,
  formatValue,
  toText,
  type ReportCell,
  type ReportFormat,
} from "@/core/business-report";

import parity from "../../../fixtures/business-report/format-parity.json";

type ParityCase = {
  value: ReportCell | boolean;
  format: ReportFormat;
  currency: string;
  expected: string;
};

describe("formatValue", () => {
  // The design's promise is that a figure reads the same in the card, the
  // PDF, the Word file and the workbook. These expectations are generated
  // from the skill's own `format_value` (see the fixture's description), so
  // this suite fails the moment the two spellings part company.
  it.each((parity.cases as ParityCase[]).map((entry) => [entry] as const))(
    "agrees with the document renders",
    ({ value, format, currency, expected }) => {
      expect(formatValue(value as ReportCell, format, currency)).toBe(expected);
    },
  );

  it("rounds the decimal spelling rather than the binary double", () => {
    // 8.35 is stored as 8.3499999999999996…, so rounding the double gives
    // 8.3 while the document says 8.4.
    expect(formatValue(8.35, "percent")).toBe("8.4%");
    expect(formatValue(2.675, "currency")).toBe("$2.68");
    expect(formatValue(1000000.005, "number")).toBe("1,000,000.01");
  });

  it("names a currency it has no symbol for", () => {
    expect(formatValue(1234.5, "currency", "CHF")).toBe("CHF 1,234.50");
  });

  it("shows a missing number as a dash and a missing text as nothing", () => {
    expect(formatValue(null, "currency")).toBe("—");
    expect(formatValue(null, "text")).toBe("");
  });

  it("shows a cell the documents could not have rendered at all", () => {
    // `Decimal("n/a")` raises, so the skill never produced a document from
    // this report; the card still shows what the cell holds.
    expect(formatValue("n/a", "currency")).toBe("n/a");
  });
});

describe("toText", () => {
  it("keeps an identifier out of float spelling", () => {
    expect(toText(10001)).toBe("10001");
    expect(toText("  J-10001 ")).toBe("J-10001");
  });
});

describe("checksLine", () => {
  const reconcile = {
    id: "totals_reconcile",
    status: "pass" as const,
    text: "Totals match your file.",
  };
  const warning = {
    id: "unmapped_rows",
    status: "warn" as const,
    text: "2 jobs had no technician.",
  };
  const quiet = {
    id: "duplicate_ids",
    status: "pass" as const,
    text: "No job ID appears more than once.",
  };

  it("leads with the reconciliation and then every warning", () => {
    expect(checksLine([quiet, warning, reconcile])).toBe(
      "Totals match your file. 2 jobs had no technician.",
    );
  });

  it("says nothing when every check passed but the reconciliation is absent", () => {
    expect(checksLine([quiet])).toBe("");
  });
});

describe("cellFormat", () => {
  const table = {
    columns: ["Metric", "August"],
    formats: ["text", "number"] as ReportFormat[],
    rows: [
      ["Revenue", 1],
      ["Jobs", 2],
    ] as ReportCell[][],
    totals: null,
    rowFormats: [["text", "currency"]] as ReportFormat[][],
  };

  it("prefers the row's own units over the column's", () => {
    expect(cellFormat(table, 0, 1)).toBe("currency");
  });

  it("falls back to the column for a row the list does not cover", () => {
    expect(cellFormat(table, 1, 1)).toBe("number");
  });
});

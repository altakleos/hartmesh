/**
 * `report.json`, the one document the business-report skill's renders read.
 *
 * The shape is `contracts/business_report/report.schema.json`; this module
 * carries only what the card draws, and `parseBusinessReport` is the gate that
 * decides a file is one of these at all. It is deliberately strict about the
 * parts it will render and silent about the parts it will not: a report whose
 * `rows` sheet or `build` provenance is missing still draws correctly, so
 * demanding them would refuse a card over something nobody looks at.
 */

import type { ReportCell, ReportFormat } from "./format";

export type ReportCheckStatus = "pass" | "warn" | "fail" | "not_checked";

export type ReportTable = {
  columns: string[];
  formats: ReportFormat[];
  rows: ReportCell[][];
  totals: ReportCell[] | null;
  /** Per-row formats for tables whose rows carry different units. */
  rowFormats?: ReportFormat[][];
};

export type ReportKpi = {
  id: string;
  label: string;
  value: number | null;
  format: ReportFormat;
  delta?: { vs: string; pct: number | null };
};

export type ReportSection = {
  id: string;
  heading: string;
  paragraphs?: string[];
  bullets?: string[];
  table?: ReportTable;
  charts?: string[];
  note?: string;
};

export type ReportChart = {
  id: string;
  /** Always `charts/<id>.png`; anything else is dropped as unrenderable. */
  png: string;
  title?: string;
};

export type ReportCheck = {
  id: string;
  status: ReportCheckStatus;
  text: string;
};

export type ReportInput = {
  name: string;
  uploaded: string;
};

export type BusinessReport = {
  title: string;
  company: string;
  periodLabel: string;
  draft: number;
  currency: string;
  /** `#rrggbb`, already validated, so it is safe to put in a style. */
  primaryColor: string;
  inputs: ReportInput[];
  kpis: ReportKpi[];
  sections: ReportSection[];
  charts: ReportChart[];
  checks: ReportCheck[];
  notes: string[];
};

const FORMATS: ReadonlySet<string> = new Set([
  "currency",
  "integer",
  "number",
  "percent",
  "text",
  "date",
]);
const CHECK_STATUSES: ReadonlySet<string> = new Set([
  "pass",
  "warn",
  "fail",
  "not_checked",
]);
const CHART_PNG = /^charts\/[a-z0-9_]+\.png$/;
/** `$defs/identifier` in the contract. */
const IDENTIFIER = /^[a-z][a-z0-9_]*$/;
const HEX_COLOR = /^#[0-9a-fA-F]{6}$/;
const DEFAULT_PRIMARY = "#1F4E79";

/**
 * How much of a `report.json` the panel fetches before calling it truncated.
 *
 * The file embeds every cleaned in-period row (about 242 bytes each), which
 * the card never draws but the renders need, so under the text preview's
 * 1 MiB budget a report of roughly 4,300 rows stopped being a card and became
 * JSON. Sixteen MiB is about 69,000 rows — a year of a busy small business —
 * and still a size a phone parses; past it, *Load full file* remains.
 */
export const REPORT_PREVIEW_MAX_BYTES = 16 * 1024 * 1024;

/**
 * Ceilings on how much document the card will draw.
 *
 * A skill-built report is far below all of these — its section tables stop at
 * `TABLE_ROW_LIMIT` (25) and its charts are a handful — so they only refuse a
 * file written to be expensive.
 */
const MAX_SECTIONS = 64;
const MAX_TABLE_ROWS = 2000;
const MAX_TABLE_COLUMNS = 64;
const MAX_CHARTS = 64;
const MAX_CHART_REFERENCES = 16;
const MAX_CHECKS = 64;
const MAX_KPIS = 32;

/** A report artifact is the file the card is selected for. */
export function isBusinessReportPath(filepath: string) {
  return filepath.endsWith(".report.json");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function asStringList(value: unknown) {
  if (!Array.isArray(value)) {
    return undefined;
  }
  return value.every((entry) => typeof entry === "string") ? value : undefined;
}

function asCell(value: unknown): ReportCell | undefined {
  if (value === null || typeof value === "string") {
    return value;
  }
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function asFormats(value: unknown) {
  const names = asStringList(value);
  return names?.every((name) => FORMATS.has(name))
    ? (names as ReportFormat[])
    : undefined;
}

function asRow(value: unknown) {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const cells = value.map(asCell);
  return cells.every((cell) => cell !== undefined) ? cells : undefined;
}

function parseTable(value: unknown): ReportTable | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const columns = asStringList(value.columns);
  const formats = asFormats(value.formats);
  if (
    !columns?.length ||
    !formats?.length ||
    !Array.isArray(value.rows) ||
    columns.length > MAX_TABLE_COLUMNS ||
    value.rows.length > MAX_TABLE_ROWS
  ) {
    return undefined;
  }
  const rows = value.rows.map(asRow);
  if (rows.some((row) => row === undefined)) {
    return undefined;
  }
  const totals =
    value.totals === null || value.totals === undefined
      ? null
      : asRow(value.totals);
  if (totals === undefined) {
    return undefined;
  }
  const rowFormats = Array.isArray(value.row_formats)
    ? value.row_formats.map(asFormats)
    : undefined;
  return {
    columns,
    formats,
    rows: rows as ReportCell[][],
    totals,
    rowFormats: rowFormats?.every((entry) => entry !== undefined)
      ? rowFormats
      : undefined,
  };
}

function parseKpis(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_KPIS) {
    return undefined;
  }
  const kpis: ReportKpi[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) {
      return undefined;
    }
    const id = asString(entry.id);
    const label = asString(entry.label);
    const format = asString(entry.format);
    if (
      !id ||
      !IDENTIFIER.test(id) ||
      !label ||
      !format ||
      !FORMATS.has(format)
    ) {
      return undefined;
    }
    const raw = entry.value;
    if (raw !== null && (typeof raw !== "number" || !Number.isFinite(raw))) {
      return undefined;
    }
    let delta: ReportKpi["delta"];
    if (isRecord(entry.delta)) {
      const vs = asString(entry.delta.vs);
      const pct = entry.delta.pct;
      if (
        vs &&
        (pct === null || (typeof pct === "number" && Number.isFinite(pct)))
      ) {
        delta = { vs, pct: pct };
      }
    }
    kpis.push({ id, label, value: raw, format: format as ReportFormat, delta });
  }
  return kpis;
}

function parseSections(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_SECTIONS) {
    return undefined;
  }
  const sections: ReportSection[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) {
      return undefined;
    }
    const id = asString(entry.id);
    const heading = asString(entry.heading);
    if (!id || !IDENTIFIER.test(id) || !heading) {
      return undefined;
    }
    const table =
      entry.table === undefined ? undefined : parseTable(entry.table);
    if (entry.table !== undefined && !table) {
      return undefined;
    }
    const charts = asStringList(entry.charts);
    if (charts && charts.length > MAX_CHART_REFERENCES) {
      return undefined;
    }
    sections.push({
      id,
      heading,
      paragraphs: asStringList(entry.paragraphs),
      bullets: asStringList(entry.bullets),
      table,
      charts,
      note: asString(entry.note),
    });
  }
  return sections;
}

/**
 * Charts are filtered rather than fatal: the pattern is what keeps an
 * artifact URL from being built out of a path the file chose, and a report
 * whose numbers are intact should still be read when one picture is not.
 */
function parseCharts(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_CHARTS) {
    return undefined;
  }
  const charts: ReportChart[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) {
      continue;
    }
    const id = asString(entry.id);
    const png = asString(entry.png);
    if (!id || !IDENTIFIER.test(id) || !png || !CHART_PNG.test(png)) {
      continue;
    }
    const title = isRecord(entry.spec) ? asString(entry.spec.title) : undefined;
    charts.push({ id, png, title });
  }
  return charts;
}

function parseChecks(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_CHECKS) {
    return undefined;
  }
  const checks: ReportCheck[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) {
      return undefined;
    }
    const id = asString(entry.id);
    const status = asString(entry.status);
    const text = asString(entry.text);
    if (
      !id ||
      !IDENTIFIER.test(id) ||
      !status ||
      !CHECK_STATUSES.has(status) ||
      !text
    ) {
      return undefined;
    }
    checks.push({ id, status: status as ReportCheckStatus, text });
  }
  return checks;
}

function parseInputs(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }
  const inputs: ReportInput[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) {
      continue;
    }
    const name = asString(entry.name);
    if (name) {
      inputs.push({ name, uploaded: asString(entry.uploaded) ?? "" });
    }
  }
  return inputs;
}

/**
 * The report a card can be drawn from, or `null` for anything else — a JSON
 * file that merely ends in `.report.json`, a future `version`, a draft whose
 * numbers did not survive whatever wrote them.
 */
export function parseBusinessReport(content: string): BusinessReport | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || parsed.version !== 1 || !isRecord(parsed.meta)) {
    return null;
  }
  const meta = parsed.meta;
  const title = asString(meta.title);
  const period = isRecord(meta.period)
    ? asString(meta.period.label)
    : undefined;
  // The contract is `integer, minimum 1`, and the value is printed as-is.
  const draft =
    typeof meta.draft === "number" &&
    Number.isInteger(meta.draft) &&
    meta.draft >= 1
      ? meta.draft
      : undefined;
  if (!title || !period || draft === undefined) {
    return null;
  }
  const kpis = parseKpis(parsed.kpis);
  const sections = parseSections(parsed.sections);
  const charts = parseCharts(parsed.charts);
  const checks = parseChecks(parsed.checks);
  if (!kpis || !sections || !charts || !checks) {
    return null;
  }
  const brand = isRecord(meta.brand) ? meta.brand : {};
  const primary = asString(brand.primary);
  const currency = isRecord(meta.currency)
    ? asString(meta.currency.code)
    : undefined;
  return {
    title,
    company: asString(brand.company) ?? asString(meta.company) ?? "",
    periodLabel: period,
    draft,
    currency: currency && /^[A-Z]{3}$/.test(currency) ? currency : "USD",
    primaryColor:
      primary && HEX_COLOR.test(primary) ? primary : DEFAULT_PRIMARY,
    inputs: parseInputs(meta.inputs),
    kpis,
    sections,
    charts,
    checks,
    notes: asStringList(parsed.notes) ?? [],
  };
}

/** `checks_line`: the reconciliation, then every warning or failure. */
export function checksLine(checks: readonly ReportCheck[]) {
  const reconciliation = checks.filter(
    (check) => check.id === "totals_reconcile",
  );
  const problems = checks.filter(
    (check) =>
      check.id !== "totals_reconcile" &&
      (check.status === "warn" || check.status === "fail"),
  );
  return [...reconciliation, ...problems].map((check) => check.text).join(" ");
}

/** The format of one cell: the row's own list when the table has one. */
export function cellFormat(
  table: ReportTable,
  rowIndex: number,
  columnIndex: number,
) {
  const rowFormat = table.rowFormats?.[rowIndex]?.[columnIndex];
  return rowFormat ?? table.formats[columnIndex] ?? "text";
}

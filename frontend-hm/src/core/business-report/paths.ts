/**
 * Where a report's pictures and downloads sit.
 *
 * The skill writes one directory per report — `<name>.report.json`,
 * `charts/*.png` and the renders as `<name>.pdf`, `.docx` and `.xlsx` — so
 * every companion file is addressed relative to the report artifact the panel
 * already has open.
 */

export const REPORT_RENDER_KINDS = ["pdf", "docx", "xlsx"] as const;

export type ReportRenderKind = (typeof REPORT_RENDER_KINDS)[number];

const REPORT_SUFFIX = ".report.json";

function splitReportPath(reportPath: string) {
  const separator = reportPath.lastIndexOf("/");
  return {
    directory: separator === -1 ? "" : reportPath.slice(0, separator),
    name: reportPath.slice(
      separator + 1,
      reportPath.length - REPORT_SUFFIX.length,
    ),
  };
}

/** A file in the report's own directory. */
export function reportSiblingPath(reportPath: string, relative: string) {
  const { directory } = splitReportPath(reportPath);
  return directory ? `${directory}/${relative}` : relative;
}

export function reportRenderPath(reportPath: string, kind: ReportRenderKind) {
  const { name } = splitReportPath(reportPath);
  return reportSiblingPath(reportPath, `${name}.${kind}`);
}

/**
 * The renders this turn actually handed over.
 *
 * `artifacts` is what `present_files` presented, so a button appears only for
 * a file the person can really open; an unrendered format is left out rather
 * than offered as a link that would 404.
 */
export function availableReportRenders(
  reportPath: string,
  artifacts: readonly string[],
): ReportRenderKind[] {
  const presented = new Set(artifacts);
  return REPORT_RENDER_KINDS.filter((kind) =>
    presented.has(reportRenderPath(reportPath, kind)),
  );
}

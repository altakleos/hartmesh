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
    // The separator stays with the directory, so a report sitting directly
    // under the root addresses `/charts/a.png` rather than splicing
    // `charts/a.png` onto the end of the artifacts route.
    directory: reportPath.slice(0, separator + 1),
    name: reportPath.slice(
      separator + 1,
      reportPath.length - REPORT_SUFFIX.length,
    ),
  };
}

/** A file in the report's own directory. */
export function reportSiblingPath(reportPath: string, relative: string) {
  return `${splitReportPath(reportPath).directory}${relative}`;
}

export function reportRenderPath(reportPath: string, kind: ReportRenderKind) {
  const { name } = splitReportPath(reportPath);
  return reportSiblingPath(reportPath, `${name}.${kind}`);
}

/**
 * The renders the thread has presented.
 *
 * `artifacts` is the thread's cumulative `present_files` list, so a format
 * that was never rendered is left out rather than offered as a link that would
 * 404. It is not per-turn: a rebuild deletes the previous draft's renders and
 * their paths stay in the list, so a format that has not been rendered again
 * yet is still offered until it is.
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

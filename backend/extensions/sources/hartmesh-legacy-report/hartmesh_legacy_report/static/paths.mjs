/**
 * Where a report's pictures and downloads sit.
 *
 * The skill writes one directory per report — `<name>.report.json`,
 * `charts/*.png` and the renders as `<name>.pdf`, `.docx` and `.xlsx` — so
 * every companion file is addressed relative to the report artifact the panel
 * already has open.
 */
export const REPORT_RENDER_KINDS = ["pdf", "docx", "xlsx"];
/**
 * The folder a report's downloads are filed under, in the person's own files
 * and in the company's Shared area alike. Not a translated string: a folder
 * name is data, and Shared is one directory for everyone.
 */
export const REPORTS_FOLDER = "Reports";
const REPORT_SUFFIX = ".report.json";
function splitReportPath(reportPath) {
    const separator = reportPath.lastIndexOf("/");
    return {
        // The separator stays with the directory, so a report sitting directly
        // under the root addresses `/charts/a.png` rather than splicing
        // `charts/a.png` onto the end of the artifacts route.
        directory: reportPath.slice(0, separator + 1),
        name: reportPath.slice(separator + 1, reportPath.length - REPORT_SUFFIX.length),
    };
}
/** A file in the report's own directory. */
export function reportSiblingPath(reportPath, relative) {
    return `${splitReportPath(reportPath).directory}${relative}`;
}
export function reportRenderPath(reportPath, kind) {
    const { name } = splitReportPath(reportPath);
    return reportSiblingPath(reportPath, `${name}.${kind}`);
}
/**
 * The renders the thread has presented — eligibility, not availability.
 *
 * `artifacts` is the thread's cumulative presented-files list (from
 * `present_files`, or a tool call that presented the files it made), so a format
 * that was never rendered is left out rather than offered as a link that would
 * 404. It is not per-turn: a rebuild deletes the previous draft's renders and
 * their paths stay in the list, so this still names a format whose file is
 * gone.
 *
 * That is the delivery fence and it stays: presentation is what earns a
 * format the right to be offered at all. Whether the file is *there* is the
 * second question, asked by `useLiveReportRenders` against the same
 * authenticated artifact route the download link uses. Callers that draw
 * download links must ask both; this answer alone is history.
 */
export function availableReportRenders(reportPath, artifacts) {
    const presented = new Set(artifacts);
    return REPORT_RENDER_KINDS.filter((kind) => presented.has(reportRenderPath(reportPath, kind)));
}
/**
 * The report a render belongs to, or `null` when *path* is not a render.
 *
 * The inverse of {@link reportRenderPath}: the skill writes `<name>.pdf`
 * beside `<name>.report.json`, so the report is named by the render.
 */
export function reportOfRender(path) {
    const dot = path.lastIndexOf(".");
    if (dot < 0) {
        return null;
    }
    const kind = path.slice(dot + 1);
    if (!REPORT_RENDER_KINDS.includes(kind)) {
        return null;
    }
    return `${path.slice(0, dot)}${REPORT_SUFFIX}`;
}
/**
 * Whether *path* is a render of a report that was actually presented.
 *
 * The name alone is a guess — any PDF can sit next to nothing — so the report
 * it claims to belong to must be the one in hand or among the artifacts.
 */
export function isReportRender(path, presented) {
    const report = reportOfRender(path);
    if (report === null) {
        return false;
    }
    return report === presented.report || presented.artifacts.includes(report);
}
/**
 * The folder a file belongs in when it is filed away, or `undefined` for the
 * root. Asked by every action that files something — keeping it in My files,
 * sharing it with the company — so where a file lands is decided by what it
 * is, never by which button the person happened to press.
 */
export function filingFolderFor(path, presented) {
    return isReportRender(path, presented) ? REPORTS_FOLDER : undefined;
}

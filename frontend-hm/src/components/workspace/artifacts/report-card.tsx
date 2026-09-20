"use client";

import {
  DownloadIcon,
  FolderPlusIcon,
  LoaderIcon,
  UsersIcon,
} from "lucide-react";
import { useMemo } from "react";

import { Button } from "@/components/ui/button";
import { urlOfArtifact } from "@/core/artifacts/utils";
import {
  availableReportRenders,
  cellFormat,
  checksLine,
  formatValue,
  reportRenderPath,
  reportSiblingPath,
  useLiveReportRenders,
  type BusinessReport,
  type ReportChart,
  type ReportCheckStatus,
  type ReportRenderKind,
  type ReportSection,
  type ReportTable,
} from "@/core/business-report";
import { useSaveToMyFiles } from "@/core/files";
import { useI18n } from "@/core/i18n/hooks";
import type { Translations } from "@/core/i18n/locales/types";
import { useShareWithEveryone } from "@/core/shared";
import { cn } from "@/lib/utils";

/** Columns of these units are read down a column, so they line up right. */
const NUMERIC_FORMATS = new Set(["currency", "integer", "number", "percent"]);

const STATUS_GLYPHS: Record<ReportCheckStatus, string> = {
  pass: "✓",
  warn: "!",
  fail: "✕",
  not_checked: "–",
};

const STATUS_CLASSES: Record<ReportCheckStatus, string> = {
  pass: "text-emerald-700 dark:text-emerald-400",
  warn: "text-amber-700 dark:text-amber-500",
  fail: "text-rose-700 dark:text-rose-400",
  not_checked: "text-muted-foreground",
};

function statusLabel(status: ReportCheckStatus, t: Translations) {
  return t.businessReport.checkStatus[status];
}

function DeltaLine({
  delta,
  t,
}: {
  delta: NonNullable<BusinessReport["kpis"][number]["delta"]>;
  t: Translations;
}) {
  if (delta.pct === null) {
    return null;
  }
  const direction = delta.pct > 0 ? "up" : delta.pct < 0 ? "down" : "flat";
  const glyph = direction === "up" ? "▲" : direction === "down" ? "▼" : "=";
  return (
    <div
      className={cn(
        "mt-1 text-xs tabular-nums",
        direction === "up" && "text-emerald-700 dark:text-emerald-400",
        direction === "down" && "text-rose-700 dark:text-rose-400",
        direction === "flat" && "text-muted-foreground",
      )}
    >
      {glyph}{" "}
      {/* `-0.0 >= 0` is true in Python too, so a drop under 0.05% keeps the
          sign the PDF and the Word file both print. */}
      {formatValue(delta.pct >= 0 ? delta.pct : -delta.pct, "percent")}{" "}
      {t.businessReport.comparedWith(delta.vs)}
    </div>
  );
}

function ReportDataTable({
  table,
  caption,
  currency,
  accent,
}: {
  table: ReportTable;
  caption: string;
  currency: string;
  accent: string;
}) {
  return (
    // Wide tables scroll inside their own box; the card never scrolls
    // sideways, which is what makes it usable at 360px. The box is focusable
    // so a keyboard-only reader can scroll it at all.
    <div
      className="border-border focus-visible:ring-ring mt-3 overflow-x-auto rounded-lg border focus-visible:ring-2 focus-visible:outline-none"
      role="region"
      tabIndex={0}
    >
      <table className="w-full border-collapse text-sm">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr className="bg-muted/50">
            {table.columns.map((column, index) => (
              <th
                className={cn(
                  "text-muted-foreground border-b px-3 py-2 text-xs font-medium whitespace-nowrap",
                  NUMERIC_FORMATS.has(table.formats[index] ?? "text")
                    ? "text-right"
                    : "text-left",
                )}
                key={column + String(index)}
                scope="col"
                style={{ borderBottomColor: accent }}
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((row, rowIndex) => (
            <tr
              className="border-border/60 border-b last:border-b-0"
              key={rowIndex}
            >
              {row.map((cell, columnIndex) => {
                const format = cellFormat(table, rowIndex, columnIndex);
                const className = cn(
                  "px-3 py-1.5 whitespace-nowrap",
                  NUMERIC_FORMATS.has(format)
                    ? "text-right tabular-nums"
                    : "text-left",
                );
                const text = formatValue(cell, format, currency);
                // The leading label names its row, which is how a screen
                // reader announces the figures that follow it.
                return columnIndex === 0 && format === "text" ? (
                  <th
                    className={cn(className, "font-normal")}
                    key={columnIndex}
                    scope="row"
                  >
                    {text}
                  </th>
                ) : (
                  <td className={className} key={columnIndex}>
                    {text}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
        {table.totals && table.totals.length > 0 && (
          <tfoot>
            <tr
              className="border-t-2 font-semibold"
              style={{ borderTopColor: accent }}
            >
              {table.totals.map((cell, columnIndex) => {
                const format = table.formats[columnIndex] ?? "text";
                return (
                  <td
                    className={cn(
                      "px-3 py-2 whitespace-nowrap",
                      NUMERIC_FORMATS.has(format)
                        ? "text-right tabular-nums"
                        : "text-left",
                    )}
                    key={columnIndex}
                  >
                    {formatValue(cell, format, currency)}
                  </td>
                );
              })}
            </tr>
          </tfoot>
        )}
      </table>
    </div>
  );
}

function ReportSectionBlock({
  section,
  chartsById,
  currency,
  chartURL,
  accent,
}: {
  section: ReportSection;
  chartsById: ReadonlyMap<string, ReportChart>;
  currency: string;
  chartURL: (png: string) => string;
  accent: string;
}) {
  const charts = (section.charts ?? [])
    .map((id) => chartsById.get(id))
    .filter((chart) => chart !== undefined);

  return (
    // The report's own ids share the document with the app's, so they are
    // prefixed rather than taken as written.
    <section className="mt-6" id={`report-${section.id}`}>
      <h3
        className="border-b pb-1 text-base font-semibold"
        style={{ borderBottomColor: accent }}
      >
        {section.heading}
      </h3>
      {section.paragraphs?.map((paragraph, index) => (
        <p className="mt-2 text-sm leading-relaxed" key={index}>
          {paragraph}
        </p>
      ))}
      {section.bullets && section.bullets.length > 0 && (
        <ul className="mt-2 list-disc space-y-1 pl-5 text-sm leading-relaxed">
          {section.bullets.map((bullet, index) => (
            <li key={index}>{bullet}</li>
          ))}
        </ul>
      )}
      {section.table && (
        <ReportDataTable
          accent={accent}
          caption={section.heading}
          currency={currency}
          table={section.table}
        />
      )}
      {charts.map((chart) => (
        <figure className="mt-3" key={chart.id}>
          <img
            alt={chart.title ?? chart.id.replaceAll("_", " ")}
            className="border-border w-full rounded-lg border bg-white"
            loading="lazy"
            onError={(event) => {
              // A chart the report names but the turn never wrote; the
              // document simply omits it rather than framing a broken image.
              event.currentTarget.closest("figure")?.remove();
            }}
            src={chartURL(chart.png)}
          />
        </figure>
      ))}
      {section.note && (
        <p className="text-muted-foreground mt-2 text-xs">{section.note}</p>
      )}
    </section>
  );
}

/**
 * A built report, read the way the downloads read it.
 *
 * Everything here comes from `report.json`, through the same formatting rules
 * the PDF, Word and Excel renders use, so the figures on screen are the
 * figures in the files. The brand colour is spent only on rules and borders:
 * the document prints on white and can afford to colour its headings, while
 * this card renders on whichever ground the viewer's theme paints.
 */
export function ReportCard({
  artifacts,
  className,
  filepath,
  isMock,
  presentedKnown = true,
  report,
  reportRevision,
  runSettled = true,
  threadId,
}: {
  artifacts: readonly string[];
  className?: string;
  filepath: string;
  isMock?: boolean;
  /**
   * Whether the thread has finished telling us what it presented. Until it
   * has, an empty `artifacts` means "not known yet", not "never rendered" —
   * and saying the second while the first is true is how this card spent a
   * whole release telling people to ask for files they already had.
   */
  presentedKnown?: boolean;
  report: BusinessReport;
  /**
   * The selected report's own content digest. Every draft rewrites the same
   * filenames, so this is what tells one draft's download verdict from
   * another's; without it a late probe from the previous draft could restore
   * a link to a file that draft's rebuild deleted.
   */
  reportRevision?: string;
  /** False while a run is still writing; settling re-asks what is on disk. */
  runSettled?: boolean;
  threadId: string;
}) {
  const { t } = useI18n();
  const accent = report.primaryColor;
  // Two questions, in order. Presentation earns a format the right to be
  // offered; the probe decides whether the file is still there. A render
  // whose file a rebuild deleted is eligible and not available, which is the
  // whole of DF16.
  const eligible = availableReportRenders(filepath, artifacts);
  const live = useLiveReportRenders({
    threadId,
    filepath,
    eligible,
    revision: reportRevision,
    isMock,
    runSettled,
  });
  const renders = live.kinds;
  const line = checksLine(report.checks);
  // The documents print each check's own status and compute no summary, so
  // this glyph is the card's own claim and must not outrun the checks: a
  // report that checked nothing does not earn a tick.
  const worst: ReportCheckStatus = report.checks.some(
    (check) => check.status === "fail",
  )
    ? "fail"
    : report.checks.some((check) => check.status === "warn")
      ? "warn"
      : report.checks.some((check) => check.status === "pass")
        ? "pass"
        : "not_checked";
  const chartsById = useMemo(
    () => new Map(report.charts.map((chart) => [chart.id, chart])),
    [report.charts],
  );
  // The downloads are what a person keeps: the documents, not the JSON the
  // card is drawn from. The showcase has no files to keep them in.
  const myFiles = useSaveToMyFiles(threadId);
  // Sharing hands the same documents to everyone at the company.
  const everyone = useShareWithEveryone(threadId);
  const renderPaths = renders.map((kind) => reportRenderPath(filepath, kind));

  const chartURL = (png: string) =>
    urlOfArtifact({
      filepath: reportSiblingPath(filepath, png),
      threadId,
      isMock,
    });

  const renderLabel: Record<ReportRenderKind, string> = {
    pdf: t.businessReport.downloadPdf,
    docx: t.businessReport.downloadWord,
    xlsx: t.businessReport.downloadExcel,
  };

  return (
    <div
      className={cn("size-full overflow-auto", className)}
      data-testid="business-report-card"
    >
      <article className="mx-auto max-w-3xl px-4 py-5 sm:px-6">
        <header className="border-l-4 pl-3" style={{ borderLeftColor: accent }}>
          <h2 className="text-xl leading-tight font-semibold text-balance break-words">
            {report.title}
          </h2>
          <p className="text-muted-foreground mt-1 text-sm">
            {[
              report.company,
              report.periodLabel,
              t.businessReport.draft(report.draft),
            ]
              .filter(Boolean)
              .join(" · ")}
          </p>
        </header>

        {/* Only once the verdict is final. "No file to download yet" while a
            probe is still out is a false statement the person acts on, and
            it is the one thing worse than the stale link this replaced. */}
        {renders.length === 0 && presentedKnown && live.isSettled && (
          <p className="text-muted-foreground mt-4 text-sm">
            {t.businessReport.noRenders}
          </p>
        )}
        {renders.length > 0 && (
          <div className="mt-4 flex flex-wrap gap-2">
            {renders.map((kind) => (
              <Button asChild key={kind} size="sm" variant="outline">
                <a
                  aria-label={t.businessReport.downloadRender(
                    renderLabel[kind],
                  )}
                  href={urlOfArtifact({
                    filepath: reportRenderPath(filepath, kind),
                    threadId,
                    download: true,
                    isMock,
                  })}
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  <DownloadIcon className="size-4" />
                  {renderLabel[kind]}
                </a>
              </Button>
            ))}
            {!isMock && (
              <Button
                disabled={myFiles.isPending}
                onClick={() => void myFiles.save(renderPaths)}
                size="sm"
                variant="outline"
              >
                {myFiles.isPending ? (
                  <LoaderIcon className="size-4 animate-spin" />
                ) : (
                  <FolderPlusIcon className="size-4" />
                )}
                {myFiles.isPending ? t.files.saving : t.files.saveToMyFiles}
              </Button>
            )}
            {!isMock && (
              <Button
                disabled={everyone.isPending || everyone.hasShared(renderPaths)}
                onClick={() =>
                  void everyone.share(renderPaths, t.shared.reportsFolder)
                }
                size="sm"
                variant="outline"
              >
                {everyone.isPending ? (
                  <LoaderIcon className="size-4 animate-spin" />
                ) : (
                  <UsersIcon className="size-4" />
                )}
                {everyone.isPending
                  ? t.shared.sharing
                  : everyone.hasShared(renderPaths)
                    ? t.shared.alreadyShared
                    : t.shared.shareWithEveryone}
              </Button>
            )}
          </div>
        )}

        {report.kpis.length > 0 && (
          // Sized by the container, never by the window: the card's widest
          // home is a full page and its narrowest is the artifact side panel,
          // and a viewport breakpoint cannot tell them apart. The 10rem floor
          // is set by the figures rather than by how many tiles would fit --
          // see the frontend guide, "KPI tiles".
          <div
            className="mt-4 grid grid-cols-[repeat(auto-fit,minmax(min(100%,10rem),1fr))] gap-3"
            data-testid="business-report-kpis"
          >
            {report.kpis.map((kpi) => (
              <div
                className="bg-muted/40 rounded-lg border-t-[3px] px-3 py-2"
                key={kpi.id}
                style={{ borderTopColor: accent }}
              >
                <div className="text-muted-foreground text-[11px] font-medium tracking-wide break-words uppercase">
                  {kpi.label}
                </div>
                {/* A figure wider than its tile breaks inside it rather than
                    across its neighbour. The sizing above is what keeps that
                    from happening; this is the floor under it. */}
                <div
                  className="mt-0.5 text-lg font-semibold break-words tabular-nums"
                  data-testid="business-report-kpi-value"
                >
                  {formatValue(kpi.value, kpi.format, report.currency)}
                </div>
                {kpi.delta && <DeltaLine delta={kpi.delta} t={t} />}
              </div>
            ))}
          </div>
        )}

        {line && (
          // The one line the assistant repeats in the chat, where a person
          // looking at the figures can see it without scrolling past them.
          <p
            className={cn(
              "border-border bg-muted/20 mt-4 rounded-lg border px-3 py-2 text-sm",
            )}
            data-testid="business-report-checks-line"
          >
            <span className={cn("mr-1.5 font-semibold", STATUS_CLASSES[worst])}>
              <span aria-hidden="true">{STATUS_GLYPHS[worst]}</span>
              <span className="sr-only">{statusLabel(worst, t)}: </span>
            </span>
            {line}
          </p>
        )}

        {report.sections.map((section) => (
          <ReportSectionBlock
            accent={accent}
            chartsById={chartsById}
            chartURL={chartURL}
            currency={report.currency}
            key={section.id}
            section={section}
          />
        ))}

        {(report.checks.length > 0 || report.notes.length > 0) && (
          <section className="mt-6">
            <h3
              className="border-b pb-1 text-base font-semibold"
              style={{ borderBottomColor: accent }}
            >
              {t.businessReport.checksHeading}
            </h3>
            {report.checks.length > 0 && (
              <ul className="mt-2 space-y-1.5 text-sm">
                {report.checks.map((check) => (
                  // The status reads inline rather than in a fixed gutter: a
                  // 360px screen cannot spare 96px to one word, and the check
                  // text has to wrap under it, not beside it.
                  <li key={check.id}>
                    <span
                      className={cn(
                        "mr-1.5 text-[11px] tracking-wide uppercase",
                        STATUS_CLASSES[check.status],
                      )}
                    >
                      <span aria-hidden="true">
                        {STATUS_GLYPHS[check.status]}
                      </span>{" "}
                      {statusLabel(check.status, t)}
                    </span>
                    {check.text}
                  </li>
                ))}
              </ul>
            )}
            {report.notes.length > 0 && (
              <>
                <h4 className="mt-3 text-sm font-semibold">
                  {t.businessReport.notIncludedHeading}
                </h4>
                <ul className="text-muted-foreground mt-1 space-y-1 text-sm">
                  {report.notes.map((note, index) => (
                    <li key={index}>{note}</li>
                  ))}
                </ul>
              </>
            )}
          </section>
        )}

        {report.inputs.length > 0 && (
          <footer className="border-border text-muted-foreground mt-6 border-t pt-3 text-xs">
            {t.businessReport.builtFrom(
              report.inputs.map((input) =>
                input.uploaded
                  ? t.businessReport.uploadedOn(input.name, input.uploaded)
                  : input.name,
              ),
            )}
          </footer>
        )}
      </article>
    </div>
  );
}

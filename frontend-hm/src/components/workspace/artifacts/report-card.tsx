"use client";

import { DownloadIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { urlOfArtifact } from "@/core/artifacts/utils";
import {
  availableReportRenders,
  cellFormat,
  checksLine,
  formatValue,
  reportRenderPath,
  reportSiblingPath,
  type BusinessReport,
  type ReportCheckStatus,
  type ReportRenderKind,
  type ReportSection,
  type ReportTable,
} from "@/core/business-report";
import { useI18n } from "@/core/i18n/hooks";
import type { Translations } from "@/core/i18n/locales/types";
import { cn } from "@/lib/utils";

/** Columns of these units are read down a column, so they line up right. */
const NUMERIC_FORMATS = new Set(["currency", "integer", "number", "percent"]);

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
      {glyph} {formatValue(Math.abs(delta.pct), "percent")}{" "}
      {t.businessReport.comparedWith(delta.vs)}
    </div>
  );
}

function ReportDataTable({
  table,
  currency,
  accent,
}: {
  table: ReportTable;
  currency: string;
  accent: string;
}) {
  return (
    // Wide tables scroll inside their own box; the card never scrolls
    // sideways, which is what makes it usable at 360px.
    <div className="border-border mt-3 overflow-x-auto rounded-lg border">
      <table className="w-full border-collapse text-sm">
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
                return (
                  <td
                    className={cn(
                      "px-3 py-1.5 whitespace-nowrap",
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
          ))}
        </tbody>
        {table.totals && (
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
  report,
  chartURL,
  accent,
}: {
  section: ReportSection;
  report: BusinessReport;
  chartURL: (png: string) => string;
  accent: string;
}) {
  const charts = (section.charts ?? [])
    .map((id) => report.charts.find((chart) => chart.id === id))
    .filter((chart) => chart !== undefined);

  return (
    <section className="mt-6" id={section.id}>
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
          currency={report.currency}
          table={section.table}
        />
      )}
      {charts.map((chart) => (
        <figure className="mt-3" key={chart.id}>
          <img
            alt={chart.title ?? chart.id.replaceAll("_", " ")}
            className="border-border w-full rounded-lg border bg-white"
            loading="lazy"
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
  report,
  threadId,
}: {
  artifacts: readonly string[];
  className?: string;
  filepath: string;
  isMock?: boolean;
  report: BusinessReport;
  threadId: string;
}) {
  const { t } = useI18n();
  const accent = report.primaryColor;
  const renders = availableReportRenders(filepath, artifacts);
  const line = checksLine(report.checks);
  const worst = report.checks.some((check) => check.status === "fail")
    ? "fail"
    : report.checks.some((check) => check.status === "warn")
      ? "warn"
      : "pass";

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
          <h2 className="text-xl leading-tight font-semibold text-balance">
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

        {renders.length > 0 && (
          <div className="mt-4 flex flex-wrap gap-2">
            {renders.map((kind) => (
              <Button asChild key={kind} size="sm" variant="outline">
                <a
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
          </div>
        )}

        {report.kpis.length > 0 && (
          <div
            className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5"
            data-testid="business-report-kpis"
          >
            {report.kpis.map((kpi) => (
              <div
                className="bg-muted/40 rounded-lg border-t-[3px] px-3 py-2"
                key={kpi.id}
                style={{ borderTopColor: accent }}
              >
                <div className="text-muted-foreground text-[11px] font-medium tracking-wide uppercase">
                  {kpi.label}
                </div>
                <div className="mt-0.5 text-lg font-semibold tabular-nums">
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
              {worst === "pass" ? "✓" : "!"}
            </span>
            {line}
          </p>
        )}

        {report.sections.map((section) => (
          <ReportSectionBlock
            accent={accent}
            chartURL={chartURL}
            key={section.id}
            report={report}
            section={section}
          />
        ))}

        <section className="mt-6">
          <h3
            className="border-b pb-1 text-base font-semibold"
            style={{ borderBottomColor: accent }}
          >
            {t.businessReport.checksHeading}
          </h3>
          <ul className="mt-2 space-y-1 text-sm">
            {report.checks.map((check) => (
              <li className="flex gap-2" key={check.id}>
                <span
                  className={cn(
                    "shrink-0 text-[11px] tracking-wide uppercase",
                    STATUS_CLASSES[check.status],
                    "w-24 pt-0.5",
                  )}
                >
                  {statusLabel(check.status, t)}
                </span>
                <span className="min-w-0">{check.text}</span>
              </li>
            ))}
          </ul>
          {report.notes.length > 0 && (
            <>
              <p className="mt-3 text-sm font-semibold">
                {t.businessReport.notIncludedHeading}
              </p>
              <ul className="text-muted-foreground mt-1 space-y-1 text-sm">
                {report.notes.map((note, index) => (
                  <li key={index}>{note}</li>
                ))}
              </ul>
            </>
          )}
        </section>

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

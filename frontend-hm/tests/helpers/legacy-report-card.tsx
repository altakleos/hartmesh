import { useLayoutEffect, useMemo, useRef, useState } from "react";

import { ArtifactFileControls } from "@/components/workspace/artifacts/artifact-file-controls";
import { urlOfArtifact } from "@/core/artifacts/utils";
import { useI18n } from "@/core/i18n/hooks";

import type { BusinessReport } from "../../../backend/extensions/sources/hartmesh-legacy-report/browser/report";

import { legacyReport } from "./legacy-report";

/** Exercise the package's real independent mount alongside the real host controls.
 * The light DOM makes existing semantic assertions visible to Testing Library;
 * production Shadow DOM integration and geometry remain browser assertions.
 */
export function ReportCard({
  report,
  filepath,
  threadId,
  artifacts,
  reportRevision,
  isMock,
  presentedKnown = true,
  runSettled = true,
}: {
  report: BusinessReport;
  filepath: string;
  threadId: string;
  artifacts: readonly string[];
  reportRevision?: string;
  isMock?: boolean;
  presentedKnown?: boolean;
  runSettled?: boolean;
}) {
  const { locale } = useI18n();
  const root = useRef<HTMLDivElement>(null);
  const presentedIdentity = JSON.stringify(artifacts);
  const presented = useMemo(
    () => JSON.parse(presentedIdentity) as string[],
    [presentedIdentity],
  );
  const [files, setFiles] = useState<{
    exports: { path: string; label: string }[];
    collection?: string;
  } | null>(null);
  const content = useMemo(
    () =>
      JSON.stringify(
        {
          version: 1,
          meta: {
            title: report.title,
            company: report.company,
            period: { label: report.periodLabel },
            draft: report.draft,
            currency: { code: report.currency },
            brand: { primary: report.primaryColor },
            inputs: report.inputs,
          },
          kpis: report.kpis,
          sections: report.sections.map((section) => ({
            ...section,
            ...(section.table
              ? {
                  table: {
                    ...section.table,
                    row_formats: section.table.rowFormats,
                  },
                }
              : {}),
          })),
          charts: report.charts.map((chart) => ({
            ...chart,
            spec: { title: chart.title },
          })),
          checks: report.checks,
          notes: report.notes,
        },
        (_key, value) =>
          Object.is(value, -0) ? "__test_negative_zero__" : value,
      ).replaceAll('"__test_negative_zero__"', "-0.0"),
    [report],
  );
  useLayoutEffect(() => {
    const controller = new AbortController();
    const surface = legacyReport.artifacts![0]!;
    if (surface.kind !== "native")
      throw new Error("Expected native report package");
    const mounted = surface.mount(root.current!, {
      namespace: "hartmesh.legacy-report",
      locale,
      theme: "light",
      settings: { enabled: true },
      signal: controller.signal,
      artifact: {
        filepath,
        threadId,
        revision: reportRevision ?? String(report.draft),
        content,
        projected: false,
        presented,
      },
      callBackend: async () => {
        throw new Error("No backend action required");
      },
      loadRaster: async (relative) =>
        `${urlOfArtifact({ filepath: filepath.slice(0, filepath.lastIndexOf("/") + 1) + relative, threadId, isMock })}?revision=${encodeURIComponent(reportRevision ?? String(report.draft))}`,
    });
    setFiles(mounted.files ?? null);
    return () => {
      controller.abort();
      mounted.dispose();
    };
  }, [
    content,
    filepath,
    threadId,
    reportRevision,
    locale,
    isMock,
    report.draft,
    presented,
  ]);
  const view = useMemo(
    () =>
      files
        ? {
            exports: files.exports,
            destination: files.collection
              ? { collection: files.collection }
              : undefined,
          }
        : null,
    [files],
  );
  return (
    <>
      {view && (
        <ArtifactFileControls
          view={view}
          filepath={filepath}
          threadId={threadId}
          revision={reportRevision}
          artifacts={presented}
          presentedKnown={presentedKnown}
          runSettled={runSettled}
          isMock={isMock}
        />
      )}
      <div ref={root} />
    </>
  );
}

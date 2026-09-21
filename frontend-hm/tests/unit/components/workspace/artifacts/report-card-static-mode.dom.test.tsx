import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

/**
 * The static build skips the liveness probe, and that has to stay deliberate.
 *
 * `useLiveReportRenders` has two independent reasons to skip probing: the
 * mock flag a showcase page passes, and a static-only build, where the
 * fixtures under `public/demo` *are* the filesystem. The mock branch is
 * covered next door; this pins the other one, because they are reachable
 * separately and a silent regression in either would put the DF16 dead link
 * back on a page nobody probes.
 */

const myFiles = rs.hoisted(() => ({
  save: rs.fn<(paths: readonly string[]) => Promise<unknown[]>>(),
  isPending: false,
}));
rs.mock("@/core/files", () => ({
  useSaveToMyFiles: () => ({
    save: myFiles.save,
    isPending: myFiles.isPending,
  }),
}));
const shareWithEveryone = rs.hoisted(() => rs.fn());
rs.mock("@/core/shared/hooks", () => ({
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    isPending: false,
    openShared: rs.fn(),
  }),
}));

const fetchWithAuth = rs.hoisted(() => rs.fn());
rs.mock("@/core/api/fetcher", () => ({ fetch: fetchWithAuth }));

// The one thing this file changes: the build reports itself as static.
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => true,
}));

import { ReportCard } from "@/components/workspace/artifacts/report-card";
import { parseBusinessReport } from "@/core/business-report";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import fixture from "../../../../fixtures/business-report/2026-08-business-review.report.json";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const NAME = "2026-08-business-review";
const REPORT = `${DIRECTORY}/${NAME}.report.json`;
const PDF = `${DIRECTORY}/${NAME}.pdf`;
const XLSX = `${DIRECTORY}/${NAME}.xlsx`;
const THREAD = "thread-static";

const report = parseBusinessReport(JSON.stringify(fixture))!;

beforeEach(() => {
  fetchWithAuth.mockReset();
  myFiles.save.mockReset();
});

afterEach(() => cleanup());

it("shows the presented renders without probing in a static build", async () => {
  render(
    <QueryClientProvider
      client={
        new QueryClient({
          defaultOptions: { queries: { retry: false, gcTime: 0 } },
        })
      }
    >
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <ReportCard
          artifacts={[REPORT, PDF, XLSX]}
          filepath={REPORT}
          report={report}
          reportRevision="sha-static"
          threadId={THREAD}
        />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );

  await waitFor(() =>
    expect(screen.getByRole("link", { name: "Download the PDF" })).toBeTruthy(),
  );
  expect(screen.getByRole("link", { name: "Download the Excel" })).toBeTruthy();
  expect(screen.queryByRole("link", { name: "Download the Word" })).toBeNull();
  // The verdict is settled from the fixtures alone: no request is made, and
  // the false-empty notice never appears on the way.
  expect(fetchWithAuth).not.toHaveBeenCalled();
  expect(screen.queryByText(enUS.businessReport.noRenders)).toBeNull();
});

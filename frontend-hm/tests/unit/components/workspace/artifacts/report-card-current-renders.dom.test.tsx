import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

/**
 * DF16: the card must offer a render only while its file is actually there.
 *
 * A rebuild deletes the previous draft's renders, but their paths stay in the
 * thread's cumulative presented list — which is history and stays that way. A
 * tenant reopened a revised report and was shown three download links, all of
 * which 404'd. These tests pin the two-question decision: presentation makes a
 * format eligible, and a bounded probe against the same authenticated artifact
 * route decides whether it is available.
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
rs.mock("@/core/shared", () => ({
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    isPending: false,
    openShared: rs.fn(),
  }),
}));

const fetchWithAuth = rs.hoisted(() => rs.fn());
rs.mock("@/core/api/fetcher", () => ({ fetch: fetchWithAuth }));

import { ReportCard } from "@/components/workspace/artifacts/report-card";
import { parseBusinessReport } from "@/core/business-report";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import fixture from "../../../../fixtures/business-report/2026-08-business-review.report.json";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const NAME = "2026-08-business-review";
const REPORT = `${DIRECTORY}/${NAME}.report.json`;
const PDF = `${DIRECTORY}/${NAME}.pdf`;
const DOCX = `${DIRECTORY}/${NAME}.docx`;
const XLSX = `${DIRECTORY}/${NAME}.xlsx`;
const THREAD = "thread-1";

const report = parseBusinessReport(JSON.stringify(fixture))!;

/** A probe answer keyed by the path the card asks about. */
function serveByPath(live: Record<string, number>) {
  fetchWithAuth.mockImplementation(async (url: string) => {
    const match = Object.keys(live).find((path) =>
      url.includes(path.split("/").pop()!),
    );
    return {
      status: match ? live[match] : 404,
      body: { cancel: async () => undefined },
    };
  });
}

function renderCard({
  artifacts = [REPORT, PDF, DOCX, XLSX],
  reportRevision = "sha-draft-3",
  runSettled = true,
  isMock = false,
  client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  }),
}: {
  artifacts?: string[];
  reportRevision?: string;
  runSettled?: boolean;
  isMock?: boolean;
  client?: QueryClient;
} = {}) {
  return render(
    <QueryClientProvider client={client}>
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <ReportCard
          artifacts={artifacts}
          filepath={REPORT}
          isMock={isMock}
          report={report}
          reportRevision={reportRevision}
          runSettled={runSettled}
          threadId={THREAD}
        />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
}

function downloadLinks() {
  return screen
    .queryAllByRole("link")
    .map((link) => link.getAttribute("href") ?? "")
    .filter((href) => href.includes("download=true"));
}

beforeEach(() => {
  fetchWithAuth.mockReset();
  myFiles.save.mockReset();
});

afterEach(() => cleanup());

describe("ReportCard current renders", () => {
  it("offers exactly the three presented renders when all three are live", async () => {
    serveByPath({ [PDF]: 206, [DOCX]: 206, [XLSX]: 206 });

    renderCard();

    await waitFor(() => expect(downloadLinks()).toHaveLength(3));
    const hrefs = downloadLinks().join(" ");
    expect(hrefs).toContain(`${NAME}.pdf`);
    expect(hrefs).toContain(`${NAME}.docx`);
    expect(hrefs).toContain(`${NAME}.xlsx`);
  });

  it("offers PDF alone when only PDF was presented, and invents no other path", async () => {
    serveByPath({ [PDF]: 206 });

    renderCard({ artifacts: [REPORT, PDF] });

    await waitFor(() => expect(downloadLinks()).toHaveLength(1));
    expect(downloadLinks()[0]).toContain(`${NAME}.pdf`);
    // The two formats nobody presented are never even asked about: the
    // delivery fence decides eligibility before any probe runs.
    const asked = fetchWithAuth.mock.calls
      .map(([url]) => String(url))
      .join(" ");
    expect(asked).not.toContain(`${NAME}.docx`);
    expect(asked).not.toContain(`${NAME}.xlsx`);
  });

  it("offers nothing, and says so, when an invalidating rebuild deleted every render", async () => {
    // Exactly the reproduced defect: the cumulative list still names all
    // three, and all three 404.
    serveByPath({});

    renderCard();

    await waitFor(() =>
      expect(screen.getByText(enUS.businessReport.noRenders)).toBeTruthy(),
    );
    expect(downloadLinks()).toHaveLength(0);
  });

  it("offers only the render that is current when two siblings are stale", async () => {
    serveByPath({ [PDF]: 206, [DOCX]: 404, [XLSX]: 404 });

    renderCard();

    await waitFor(() => expect(downloadLinks()).toHaveLength(1));
    expect(downloadLinks()[0]).toContain(`${NAME}.pdf`);
    expect(screen.queryByText(enUS.businessReport.noRenders)).toBeNull();
  });

  it("is unchanged by a later unrelated presented file", async () => {
    serveByPath({ [PDF]: 206 });

    renderCard({
      artifacts: [REPORT, PDF, "/mnt/user-data/outputs/notes/unrelated.txt"],
    });

    await waitFor(() => expect(downloadLinks()).toHaveLength(1));
    expect(downloadLinks()[0]).toContain(`${NAME}.pdf`);
    const asked = fetchWithAuth.mock.calls
      .map(([url]) => String(url))
      .join(" ");
    expect(asked).not.toContain("unrelated.txt");
  });

  it("shows neither a link nor the empty notice while the verdict is unknown", async () => {
    // A probe that never settles: the card is mid-decision for the whole of
    // this assertion, which is the state the false-empty flash lived in.
    fetchWithAuth.mockImplementation(() => new Promise(() => undefined));

    renderCard();

    await waitFor(() =>
      expect(screen.getByRole("heading", { level: 2 })).toBeTruthy(),
    );
    expect(downloadLinks()).toHaveLength(0);
    expect(screen.queryByText(enUS.businessReport.noRenders)).toBeNull();
  });

  it("does not let an older draft's late probe restore a deleted link", async () => {
    // Draft 2 asks and is slow. Draft 3 asks and answers 404 first. The old
    // answer must not win when it finally lands, even though every path,
    // filename and cumulative artifact entry is identical.
    let releaseDraft2: (value: {
      status: number;
      body: undefined;
    }) => void = () => undefined;
    const draft2Answer = new Promise<{ status: number; body: undefined }>(
      (resolve) => {
        releaseDraft2 = resolve;
      },
    );
    fetchWithAuth.mockImplementation(() => draft2Answer);

    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 0 } },
    });
    const view = renderCard({ reportRevision: "sha-draft-2", client });

    // The rebuild lands: same filenames, new body.
    serveByPath({});
    view.rerender(
      <QueryClientProvider client={client}>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          <ReportCard
            artifacts={[REPORT, PDF, DOCX, XLSX]}
            filepath={REPORT}
            report={report}
            reportRevision="sha-draft-3"
            runSettled
            threadId={THREAD}
          />
        </I18nContext.Provider>
      </QueryClientProvider>,
    );

    await waitFor(() =>
      expect(screen.getByText(enUS.businessReport.noRenders)).toBeTruthy(),
    );

    // Draft 2's probe finally succeeds, for files that no longer exist.
    releaseDraft2({ status: 206, body: undefined });
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(downloadLinks()).toHaveLength(0);
    expect(screen.getByText(enUS.businessReport.noRenders)).toBeTruthy();
  });

  it("keeps confirmed links on screen when an unrelated run settles, then re-asks", async () => {
    // A run finishing anywhere in the thread is what rewrites these files, so
    // the card re-asks. It must not blink the answer away first: the links
    // were confirmed, and an unrelated turn ending is no reason to un-confirm
    // them while the new probe is in flight.
    serveByPath({ [PDF]: 206, [DOCX]: 206, [XLSX]: 206 });
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 0 } },
    });
    const view = renderCard({ runSettled: false, client });
    await waitFor(() => expect(downloadLinks()).toHaveLength(3));
    const probesBefore = fetchWithAuth.mock.calls.length;

    // The run ends. The rebuild deleted Word and Excel, but the report body
    // is unchanged, so only the settle can reveal it.
    serveByPath({ [PDF]: 206 });
    view.rerender(
      <QueryClientProvider client={client}>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          <ReportCard
            artifacts={[REPORT, PDF, DOCX, XLSX]}
            filepath={REPORT}
            report={report}
            reportRevision="sha-draft-3"
            runSettled
            threadId={THREAD}
          />
        </I18nContext.Provider>
      </QueryClientProvider>,
    );

    // No blink: the confirmed links are still there while the re-probe runs.
    expect(downloadLinks()).toHaveLength(3);
    expect(screen.queryByText(enUS.businessReport.noRenders)).toBeNull();

    // ...and the settle really did re-ask, arriving at the new answer.
    await waitFor(() => expect(downloadLinks()).toHaveLength(1));
    expect(downloadLinks()[0]).toContain(`${NAME}.pdf`);
    expect(fetchWithAuth.mock.calls.length).toBeGreaterThan(probesBefore);
  });

  it("keeps Save to my files to the renders that still exist", async () => {
    serveByPath({ [PDF]: 206, [DOCX]: 404, [XLSX]: 404 });

    renderCard();

    await waitFor(() => expect(downloadLinks()).toHaveLength(1));
    screen.getByRole("button", { name: enUS.files.saveToMyFiles }).click();
    await waitFor(() => expect(myFiles.save).toHaveBeenCalledTimes(1));
    expect(myFiles.save.mock.calls[0]![0]).toEqual([PDF]);
  });

  it("stays deterministic in mock mode without probing at all", async () => {
    renderCard({ isMock: true });

    await waitFor(() => expect(downloadLinks()).toHaveLength(3));
    expect(fetchWithAuth).not.toHaveBeenCalled();
  });

  it("asks nothing and says nothing is there when no render was presented", async () => {
    renderCard({ artifacts: [REPORT] });

    await waitFor(() =>
      expect(screen.getByText(enUS.businessReport.noRenders)).toBeTruthy(),
    );
    expect(fetchWithAuth).not.toHaveBeenCalled();
  });
});

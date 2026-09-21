import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

// The card's *Save to my files* goes through the shared hook, which needs a
// query client and a router the card itself does not; what the card owns is
// which paths it hands over.
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
// The hook is stood in for; the Shared module's constants stay real, so the
// folder this test reads is the folder the product shares into.
rs.mock("@/core/shared/hooks", () => ({
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    isPending: false,
    openShared: rs.fn(),
  }),
}));

// The card proves a render is still there before offering it, so these tests
// answer every probe as a live file; which renders are *offered* is what they
// are about, and the proving itself is covered in
// `report-card-current-renders.dom.test.tsx`.
const fetchWithAuth = rs.hoisted(() => rs.fn());
rs.mock("@/core/api/fetcher", () => ({ fetch: fetchWithAuth }));

import { ReportCard } from "@/components/workspace/artifacts/report-card";
import { parseBusinessReport } from "@/core/business-report";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

import fixture from "../../../../fixtures/business-report/2026-08-business-review.report.json";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const REPORT = `${DIRECTORY}/2026-08-business-review.report.json`;
const THREAD = "thread-1";

const report = parseBusinessReport(JSON.stringify(fixture))!;

function renderCard(
  artifacts: string[] = [REPORT],
  override: Partial<typeof report> = {},
  {
    isMock = false,
    locale = "en-US" as const,
    t = enUS,
  }: { isMock?: boolean; locale?: "en-US" | "zh-CN"; t?: typeof enUS } = {},
) {
  return render(
    <QueryClientProvider
      client={
        new QueryClient({
          defaultOptions: { queries: { retry: false, gcTime: 0 } },
        })
      }
    >
      <I18nContext.Provider value={{ locale, setLocale: () => undefined, t }}>
        <ReportCard
          artifacts={artifacts}
          filepath={REPORT}
          isMock={isMock}
          report={{ ...report, ...override }}
          reportRevision="sha-fixture"
          threadId={THREAD}
        />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  myFiles.save.mockReset();
  shareWithEveryone.mockReset();
});

beforeEach(() => {
  fetchWithAuth.mockReset();
  fetchWithAuth.mockResolvedValue({
    status: 206,
    body: { cancel: async () => undefined },
  });
});

describe("ReportCard", () => {
  it("puts a shared report in the same folder whatever language the person reads", async () => {
    // Shared is one directory for the whole company, so the folder is data,
    // not words on this person's screen: a colleague reading another
    // language must land in the same place, not beside it.
    const rendered = [REPORT, `${DIRECTORY}/2026-08-business-review.pdf`];
    renderCard(rendered);
    await screen.findByRole("button", { name: "Share with everyone" });
    fireEvent.click(
      screen.getByRole("button", { name: "Share with everyone" }),
    );
    const [, englishFolder] = shareWithEveryone.mock.calls[0]!;

    cleanup();
    renderCard(rendered, {}, { locale: "zh-CN", t: zhCN });
    await screen.findByRole("button", { name: zhCN.shared.shareWithEveryone });
    fireEvent.click(
      screen.getByRole("button", { name: zhCN.shared.shareWithEveryone }),
    );
    const [, chineseFolder] = shareWithEveryone.mock.calls[1]!;

    expect(englishFolder).toBe("Reports");
    expect(chineseFolder).toBe(englishFolder);
  });

  it("names the report, the company, the period and the draft", () => {
    renderCard();

    expect(
      screen.getByRole("heading", { name: "August 2026 Business Review" }),
    ).toBeTruthy();
    expect(
      screen.getByText("Example Services Co. · August 2026 · Draft 1"),
    ).toBeTruthy();
  });

  it("shows the figures the way the downloads show them", () => {
    renderCard();

    // The same spellings the PDF, Word and Excel renders produce.
    const kpis = screen.getByTestId("business-report-kpis");
    expect(within(kpis).getByText("$74,702.61")).toBeTruthy();
    expect(within(kpis).getByText("164")).toBeTruthy();
    expect(within(kpis).getByText("$455.50")).toBeTruthy();
    expect(within(kpis).getByText("28.6%")).toBeTruthy();
    expect(kpis.textContent).toContain("▲ 38.7% vs July 2026");
  });

  it("sizes the KPI tiles by the space they have, not by the window", () => {
    // The side panel is around 480px wide inside a 1440px window, so a
    // viewport breakpoint put five tiles in it: `$74,702.61` was about 115px
    // of text in a 61px track, and 53px of it printed across the number next
    // to it. Nobody reading the panel could tell which figure belonged to
    // which label.
    //
    // `auto-fit` asks the available width instead of the window. What that
    // actually buys is measured end to end, in
    // `tests/e2e/business-report-card.spec.ts`; here it is only that no
    // viewport-keyed column count survives beside it, because the two would
    // fight and the viewport one wins at exactly the width that breaks.
    renderCard();

    const kpis = screen.getByTestId("business-report-kpis");
    expect(kpis.className).toContain("auto-fit");
    expect(kpis.className).toContain("minmax(");
    expect(kpis.className).not.toMatch(
      /(^|[\s:])(sm:|md:|lg:|xl:)?grid-cols-\d/,
    );
  });

  it("keeps a figure too wide for its tile inside that tile", () => {
    // The sizing above is what makes the tiles readable; this is the floor
    // under it. A long enough figure — a seven-figure revenue, a narrow
    // window — still has to break inside its own tile rather than print
    // across its neighbour.
    renderCard();

    const value = screen
      .getByTestId("business-report-kpis")
      .querySelector("[data-testid='business-report-kpi-value']")!;
    expect(value.className).toContain("break-words");
  });

  it("reads a comparison row in its own units", () => {
    const { container } = renderCard();

    // `row_formats` makes the revenue row currency and the jobs row a count,
    // in a table whose column format is a plain number: without it both would
    // read as 53,864.48 and 136 with no currency and no thousands agreement.
    const comparison = within(container.querySelector("#report-comparison")!);
    const revenueRow = comparison.getByText("Revenue").closest("tr")!;
    expect(within(revenueRow).getByText("$53,864.48")).toBeTruthy();
    const jobsRow = comparison.getByText("Jobs").closest("tr")!;
    expect(within(jobsRow).getByText("136")).toBeTruthy();
  });

  it("carries the one line the assistant repeats", () => {
    renderCard();

    const line = screen.getByTestId("business-report-checks-line");
    expect(line.textContent).toContain(
      "Totals match your file: $74,702.61 across 164 jobs.",
    );
    expect(line.textContent).toContain(
      "2 jobs had no technician and are listed as Unassigned.",
    );
  });

  it("lists every check and what was left out", () => {
    renderCard();

    expect(screen.getByText("No job ID appears more than once.")).toBeTruthy();
    expect(screen.getByText("Not included")).toBeTruthy();
    expect(
      screen.getByText(
        "Comparison with August 2025: not included, the files have no rows for that period.",
      ),
    ).toBeTruthy();
  });

  it("addresses each picture inside the report's own directory", () => {
    renderCard();

    const chart = screen.getByAltText("Revenue by week");
    expect(chart.getAttribute("src")).toBe(
      `/api/threads/${THREAD}/artifacts${DIRECTORY}/charts/revenue_by_period.png`,
    );
  });

  it("offers the renders the thread has presented, and only those", async () => {
    renderCard([
      REPORT,
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);

    // Presented *and* proven to still be there: the link appears once the
    // probe has answered, not on the strength of the list alone.
    const pdf = await screen.findByRole("link", { name: "Download the PDF" });
    expect(pdf.getAttribute("href")).toBe(
      `/api/threads/${THREAD}/artifacts${DIRECTORY}/2026-08-business-review.pdf?download=true`,
    );
    expect(
      screen.getByRole("link", { name: "Download the Excel" }),
    ).toBeTruthy();
    expect(
      screen.queryByRole("link", { name: "Download the Word" }),
    ).toBeNull();
  });

  it("offers no download when nothing was rendered", () => {
    renderCard();

    expect(screen.queryByRole("link", { name: "Download the PDF" })).toBeNull();
    // Nothing to keep either: the button belongs to the downloads.
    expect(
      screen.queryByRole("button", { name: "Save to My files" }),
    ).toBeNull();
  });

  it("keeps the documents the thread has rendered, and only those", async () => {
    myFiles.save.mockResolvedValue([]);
    renderCard([
      REPORT,
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);

    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Save to My files" }),
      ).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Save to My files" }));

    // The documents, not the JSON the card is drawn from.
    expect(myFiles.save).toHaveBeenCalledWith([
      `${DIRECTORY}/2026-08-business-review.pdf`,
      `${DIRECTORY}/2026-08-business-review.xlsx`,
    ]);
  });

  it("has nowhere to keep a showcase report", () => {
    renderCard(
      [REPORT, `${DIRECTORY}/2026-08-business-review.pdf`],
      {},
      { isMock: true },
    );

    expect(screen.getByRole("link", { name: "Download the PDF" })).toBeTruthy();
    expect(
      screen.queryByRole("button", { name: "Save to My files" }),
    ).toBeNull();
  });

  it("says which file the figures came from", () => {
    renderCard();

    expect(
      screen.getByText(
        "Built from example_services_export_small.csv (uploaded 2026-09-15). Checked by the report script.",
      ),
    ).toBeTruthy();
  });
  it("keeps the sign on a fall too small to show", () => {
    // `_pct_change` returns -0.0 for a drop under 0.05%, and the PDF and the
    // Word file both print -0.0%; `Math.abs` would have lost it.
    renderCard([REPORT], {
      kpis: [
        {
          id: "revenue",
          label: "Revenue",
          value: 74702.61,
          format: "currency",
          delta: { vs: "July 2026", pct: -0 },
        },
      ],
    });

    expect(screen.getByTestId("business-report-kpis").textContent).toContain(
      "-0.0% vs July 2026",
    );
  });

  it("does not award a tick no check earned", () => {
    // The documents print each check's own status and compute no summary, so
    // the glyph is the card's own claim about the file.
    renderCard([REPORT], {
      checks: [
        {
          id: "totals_reconcile",
          status: "not_checked",
          text: "Totals were not checked against your file.",
        },
      ],
    });

    const line = screen.getByTestId("business-report-checks-line");
    expect(line.textContent).not.toContain("✓");
    expect(line.textContent).toContain("not checked");
  });

  it("says why there is nothing to download", () => {
    renderCard();

    expect(
      screen.getByText(
        "No file to download yet — ask for the PDF, Word or Excel version.",
      ),
    ).toBeTruthy();
  });

  it("leaves out a checks section it has nothing to put in", () => {
    renderCard([REPORT], { checks: [], notes: [] });

    expect(screen.queryByText("Checks")).toBeNull();
    expect(screen.queryByTestId("business-report-checks-line")).toBeNull();
  });
});

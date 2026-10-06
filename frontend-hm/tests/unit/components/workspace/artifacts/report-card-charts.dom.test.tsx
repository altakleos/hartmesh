import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";

rs.mock("@/core/files/hooks", () => ({
  useSaveToMyFiles: () => ({ save: rs.fn(), isPending: false }),
}));
rs.mock("@/core/shared/hooks", () => ({
  useShareWithEveryone: () => ({
    share: rs.fn(),
    isPending: false,
    openShared: rs.fn(),
  }),
}));

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import type { BusinessReport } from "../../../../../../backend/extensions/sources/hartmesh-legacy-report/browser/index";
import { ReportCard } from "../../../../helpers/legacy-report-card";

const REPORT = "/mnt/user-data/outputs/review/review.report.json";
const report: BusinessReport = {
  title: "Business review",
  company: "Example",
  periodLabel: "August 2026",
  draft: 1,
  currency: "USD",
  primaryColor: "#334455",
  inputs: [],
  kpis: [],
  sections: [
    {
      id: "revenue",
      heading: "Revenue",
      charts: ["weekly", "monthly"],
    },
  ],
  charts: [
    { id: "weekly", png: "charts/weekly.png", title: "Revenue by week" },
    { id: "monthly", png: "charts/monthly.png", title: "Revenue by month" },
  ],
  checks: [],
  notes: [],
};

function renderCard() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  const card = ({
    body = report,
    revision = "draft-1",
    filepath = REPORT,
    threadId = "thread-1",
  }: {
    body?: BusinessReport;
    revision?: string | null;
    filepath?: string;
    threadId?: string;
  } = {}) => (
    <QueryClientProvider client={client}>
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <ReportCard
          artifacts={[filepath]}
          filepath={filepath}
          report={body}
          reportRevision={revision ?? undefined}
          threadId={threadId}
        />
      </I18nContext.Provider>
    </QueryClientProvider>
  );
  const view = render(card());
  return {
    ...view,
    update: (options: Parameters<typeof card>[0] = {}) =>
      view.rerender(card(options)),
  };
}

afterEach(() => cleanup());

describe("ReportCard chart lifecycle", () => {
  it("retries charts when changed source bytes receive a new revision", async () => {
    const view = renderCard();
    await act(async () => undefined);
    fireEvent.error(screen.getByRole("img", { name: "Revenue by week" }));
    expect(screen.queryByRole("img", { name: "Revenue by week" })).toBeNull();
    view.update({
      body: { ...report, title: "Updated title" },
      revision: "changed-source",
    });
    await act(async () => undefined);
    expect(screen.getByRole("img", { name: "Revenue by week" })).not.toBeNull();
    expect(
      screen.getByRole("heading", { name: "Updated title" }),
    ).not.toBeNull();
  });
  it("keeps loaded charts and omits only the failed figure for this revision", async () => {
    const view = renderCard();
    await act(async () => undefined);
    const weekly = screen.getByRole("img", { name: "Revenue by week" });
    const monthly = screen.getByRole("img", { name: "Revenue by month" });

    fireEvent.load(monthly);
    expect(screen.getByRole("img", { name: "Revenue by week" })).toBe(weekly);
    expect(weekly.isConnected).toBe(true);
    fireEvent.error(weekly);

    expect(screen.queryByRole("img", { name: "Revenue by week" })).toBeNull();
    expect(screen.getByRole("img", { name: "Revenue by month" })).toBe(monthly);
    expect(view.container.querySelectorAll("figure")).toHaveLength(1);
    view.update();
    expect(
      screen.getByRole("heading", { name: "Business review" }),
    ).toBeTruthy();
    expect(screen.queryByRole("img", { name: "Revenue by week" })).toBeNull();
    expect(screen.getByRole("img", { name: "Revenue by month" })).toBe(monthly);
  });

  it("can remove a failed chart from a later report without a DOM ownership error", async () => {
    const view = renderCard();
    await act(async () => undefined);
    fireEvent.error(screen.getByRole("img", { name: "Revenue by week" }));

    expect(() =>
      view.update({
        revision: "draft-2",
        body: {
          ...report,
          sections: [
            { id: "revenue", heading: "Revised revenue", charts: ["monthly"] },
          ],
        },
      }),
    ).not.toThrow();
    expect(
      screen.getByRole("heading", { name: "Revised revenue" }),
    ).toBeTruthy();
    expect(screen.getAllByRole("img")).toHaveLength(1);
    expect(screen.getByRole("img", { name: "Revenue by month" })).toBeTruthy();
  });

  it("retries a failed chart at the same filename when the report revision changes", async () => {
    const view = renderCard();
    await act(async () => undefined);
    const failedImage = screen.getByRole("img", { name: "Revenue by week" });
    fireEvent.error(failedImage);

    view.update({ revision: "draft-2" });

    const retriedImage = screen.getByRole("img", { name: "Revenue by week" });
    expect(retriedImage).not.toBe(failedImage);
    const oldURL = new URL(
      failedImage.getAttribute("src")!,
      window.location.href,
    );
    await act(async () => undefined);
    const newURL = new URL(
      retriedImage.getAttribute("src")!,
      window.location.href,
    );
    expect(newURL.pathname).toBe(oldURL.pathname);
    expect(oldURL.searchParams.get("revision")).toBe("draft-1");
    expect(newURL.searchParams.get("revision")).toBe("draft-2");
    fireEvent.load(retriedImage);
    expect(view.container.querySelectorAll("figure")).toHaveLength(2);
  });

  it("uses the report draft to retry when no content digest is available", async () => {
    const view = renderCard();
    await act(async () => undefined);
    view.update({ revision: null });
    const failedImage = screen.getByRole("img", { name: "Revenue by week" });
    fireEvent.error(failedImage);

    view.update({ revision: null, body: { ...report, draft: 2 } });

    await act(async () => undefined);
    const newURL = new URL(
      screen.getByRole("img", { name: "Revenue by week" }).getAttribute("src")!,
      window.location.href,
    );
    expect(newURL.searchParams.get("revision")).toBe("2");
  });

  it("encodes a revision as one query parameter", async () => {
    const view = renderCard();
    await act(async () => undefined);
    const revision = "sha?draft=2&other#section";
    view.update({ revision });

    await act(async () => undefined);
    const url = new URL(
      screen.getByRole("img", { name: "Revenue by week" }).getAttribute("src")!,
      window.location.href,
    );
    expect([...url.searchParams.entries()]).toEqual([["revision", revision]]);
    expect(url.hash).toBe("");
  });

  it("ignores an old image error after a newer report revision mounts", async () => {
    const view = renderCard();
    await act(async () => undefined);
    const oldImage = screen.getByRole("img", { name: "Revenue by week" });

    view.update({ revision: "draft-2" });
    const currentImage = screen.getByRole("img", { name: "Revenue by week" });
    fireEvent.error(oldImage);

    expect(screen.getByRole("img", { name: "Revenue by week" })).toBe(
      currentImage,
    );
    expect(view.container.querySelectorAll("figure")).toHaveLength(2);
  });

  it.each([
    { filepath: "/mnt/user-data/outputs/other/other.report.json" },
    { threadId: "thread-2" },
  ])("retries charts for a different report identity: %j", (identity) => {
    const view = renderCard();
    fireEvent.error(screen.getByRole("img", { name: "Revenue by week" }));

    view.update(identity);

    expect(screen.getByRole("img", { name: "Revenue by week" })).toBeTruthy();
    expect(view.container.querySelectorAll("figure")).toHaveLength(2);
  });
});

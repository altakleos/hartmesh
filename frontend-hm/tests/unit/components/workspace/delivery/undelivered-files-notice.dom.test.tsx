import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: null }),
}));
rs.mock("@/components/workspace/artifacts/context", () => ({
  useArtifacts: () => ({ select: rs.fn(), setOpen: rs.fn() }),
}));
rs.mock("@/core/artifacts/api", () => ({
  ArtifactRequestError: class extends Error {},
  downloadArtifactArchive: rs.fn(),
  getArtifactArchiveManifest: rs.fn(),
  MAX_ARTIFACT_ARCHIVE_FILES: 50,
}));
rs.mock("sonner", () => ({
  toast: { error: rs.fn(), success: rs.fn() },
}));

const fetchRunDelivery = rs.fn();
const fetchThreadDeliveryFailures = rs.fn();
rs.mock("@/core/artifact-delivery/api", () => ({
  fetchRunDelivery: (...args: unknown[]) => fetchRunDelivery(...args),
  fetchThreadDeliveryFailures: (...args: unknown[]) =>
    fetchThreadDeliveryFailures(...args),
}));

import { UndeliveredFilesNotice } from "@/components/workspace/delivery";
import {
  ArtifactDeliveryContext,
  type ArtifactDeliveryFailure,
} from "@/core/artifact-delivery";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

const failure: ArtifactDeliveryFailure = {
  runId: "run-1",
  message:
    "Artifact delivery incomplete: no produced output artifact was presented",
  undeliveredPaths: ["/mnt/user-data/outputs/report.md"],
  undeliveredCount: 1,
};

function renderNotice(
  {
    failures = { "run-1": failure },
    runId,
    disabled,
    // Shared across two renders where a test needs the cache to outlive one
    // mount — which is what the virtualized message list does to every anchor.
    client,
  }: {
    failures?: Record<string, ArtifactDeliveryFailure>;
    // Absent means the turn's messages do not carry a run id yet, which is
    // distinct from omitting the option (JS defaults cannot tell them apart).
    runId?: string;
    disabled?: boolean;
    client?: QueryClient;
  } = { runId: "run-1" },
) {
  const queryClient =
    client ??
    new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <ArtifactDeliveryContext.Provider
          value={{
            failuresByRunId: failures,
            recordFailure: () => undefined,
          }}
        >
          <UndeliveredFilesNotice
            runId={runId}
            threadId="thread-1"
            disabled={disabled}
          />
        </ArtifactDeliveryContext.Provider>
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  fetchRunDelivery.mockReset();
  fetchThreadDeliveryFailures.mockReset();
});

describe("UndeliveredFilesNotice", () => {
  it("offers the file the run produced and never presented", () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set());
    fetchRunDelivery.mockResolvedValue(null);
    renderNotice();

    expect(
      screen.getByText("1 file wasn't attached to the reply above"),
    ).toBeTruthy();
    expect(screen.getByText("report.md")).toBeTruthy();
  });

  it("says how many of the total it is showing when the list was bounded", () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set());
    fetchRunDelivery.mockResolvedValue(null);
    renderNotice({
      failures: {
        "run-1": { ...failure, undeliveredCount: 34 },
      },
      runId: "run-1",
    });

    expect(screen.getByText(/Showing the first 1 of 34\./)).toBeTruthy();
  });

  it("renders nothing for a run that delivered what it produced", async () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set());
    fetchRunDelivery.mockResolvedValue(null);
    const { container } = renderNotice({ failures: {}, runId: "run-1" });

    await waitFor(() =>
      expect(fetchThreadDeliveryFailures).toHaveBeenCalledWith("thread-1"),
    );
    expect(
      container.querySelector("[data-testid='undelivered-files-notice']"),
    ).toBeNull();
    // The thread said this run delivered, so nothing asks it for paths. That
    // is the whole request budget for a healthy thread, however long it is.
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });

  it("renders nothing before the turn's messages carry a run id", () => {
    const { container } = renderNotice({});

    expect(
      container.querySelector("[data-testid='undelivered-files-notice']"),
    ).toBeNull();
    // No run id is also no authorized call, which is what keeps this component
    // inert on static demo threads.
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });

  // The live frame is page-local state, so the reader
  // who reloads is the reader this notice exists for.
  it("restores the correction from durable state after a reload", async () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set(["run-1"]));
    fetchRunDelivery.mockResolvedValue({
      ...failure,
      undeliveredPaths: ["/mnt/user-data/outputs/2026-08-review.pdf"],
      undeliveredCount: 3,
    });

    renderNotice({ failures: {}, runId: "run-1" });

    expect(
      await screen.findByText("3 files weren't attached to the reply above"),
    ).toBeTruthy();
    expect(screen.getByText("2026-08-review.pdf")).toBeTruthy();
    expect(fetchRunDelivery).toHaveBeenCalledWith({
      threadId: "thread-1",
      runId: "run-1",
    });
  });

  it("does not re-read what this page already heard on the stream", async () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set());
    fetchRunDelivery.mockResolvedValue(null);
    renderNotice();

    await waitFor(() =>
      expect(
        screen.getByText("1 file wasn't attached to the reply above"),
      ).toBeTruthy(),
    );
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });

  // A 502 on a refetch must not read as "this run delivered fine": that is the
  // one way a standing correction could vanish mid-session, into exactly the
  // silence it exists to end.
  it("keeps the correction up when a refetch cannot reach an answer", async () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set(["run-1"]));
    fetchRunDelivery
      .mockResolvedValueOnce({ ...failure, undeliveredCount: 2 })
      .mockRejectedValue(new Error("502 Bad Gateway"));
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { unmount } = renderNotice({ failures: {}, runId: "run-1", client });

    expect(
      await screen.findByText("2 files weren't attached to the reply above"),
    ).toBeTruthy();

    // The message list virtualizes, so an anchor remounts whenever it scrolls
    // back into view. The cached verdict is what it renders from.
    unmount();
    renderNotice({ failures: {}, runId: "run-1", client });
    await waitFor(() =>
      expect(
        screen.getByText("2 files weren't attached to the reply above"),
      ).toBeTruthy(),
    );
  });

  // `disabled` is the *thread's* loading state, not the last group's: while
  // any run is in flight its verdict does not exist yet, and caching the
  // all-clear it would answer with is how a failure that lands a moment later
  // goes unmentioned.
  it("waits for the thread to be idle before asking for a terminal verdict", async () => {
    fetchThreadDeliveryFailures.mockResolvedValue(new Set());
    fetchRunDelivery.mockResolvedValue(null);
    const { container } = renderNotice({
      failures: {},
      runId: "run-1",
      disabled: true,
    });

    await waitFor(() =>
      expect(
        container.querySelector("[data-testid='undelivered-files-notice']"),
      ).toBeNull(),
    );
    expect(fetchThreadDeliveryFailures).not.toHaveBeenCalled();
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });
});

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
rs.mock("@/core/artifact-delivery/api", () => ({
  fetchRunDelivery: (...args: unknown[]) => fetchRunDelivery(...args),
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
  }: {
    failures?: Record<string, ArtifactDeliveryFailure>;
    // Absent means the turn's messages do not carry a run id yet, which is
    // distinct from omitting the option (JS defaults cannot tell them apart).
    runId?: string;
    disabled?: boolean;
  } = { runId: "run-1" },
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
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
});

describe("UndeliveredFilesNotice", () => {
  it("offers the file the run produced and never presented", () => {
    fetchRunDelivery.mockResolvedValue(null);
    renderNotice();

    expect(
      screen.getByText("1 file wasn't attached to the reply above"),
    ).toBeTruthy();
    expect(screen.getByText("report.md")).toBeTruthy();
  });

  it("says how many of the total it is showing when the list was bounded", () => {
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
    fetchRunDelivery.mockResolvedValue(null);
    const { container } = renderNotice({ failures: {}, runId: "run-1" });

    await waitFor(() => expect(fetchRunDelivery).toHaveBeenCalledTimes(1));
    expect(
      container.querySelector("[data-testid='undelivered-files-notice']"),
    ).toBeNull();
  });

  it("renders nothing before the turn's messages carry a run id", () => {
    const { container } = renderNotice({});

    expect(
      container.querySelector("[data-testid='undelivered-files-notice']"),
    ).toBeNull();
    // No run id is also no authorized call, which is what keeps this component
    // inert on the public showcase's static threads.
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });

  // hartmesh-tenancy/DF14: the live frame is page-local state, so the reader
  // who reloads is the reader this notice exists for.
  it("restores the correction from durable state after a reload", async () => {
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
    fetchRunDelivery.mockResolvedValue(null);
    renderNotice();

    await waitFor(() =>
      expect(
        screen.getByText("1 file wasn't attached to the reply above"),
      ).toBeTruthy(),
    );
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });

  it("waits for the turn to finish before asking for a terminal verdict", async () => {
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
    expect(fetchRunDelivery).not.toHaveBeenCalled();
  });
});

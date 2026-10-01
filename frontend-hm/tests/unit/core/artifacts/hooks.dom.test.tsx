import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";

rs.mock("@/components/workspace/messages/context", () => ({
  useThread: rs.fn(),
}));

rs.mock("@/core/artifacts/loader", () => ({
  loadArtifactContent: rs.fn(),
  loadArtifactContentFromToolCall: rs.fn(),
}));

import { useThread } from "@/components/workspace/messages/context";
import { useArtifactContent } from "@/core/artifacts/hooks";
import { loadArtifactContent } from "@/core/artifacts/loader";
import { REPORT_PREVIEW_MAX_BYTES } from "@/core/business-report";

const mockedUseThread = rs.mocked(useThread);
const mockedLoadArtifactContent = rs.mocked(loadArtifactContent);
const filepath = "/mnt/user-data/outputs/report.md";

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return function QueryWrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

describe("useArtifactContent", () => {
  beforeEach(() => {
    mockedUseThread.mockReturnValue({
      thread: { isLoading: false, messages: [] },
      isMock: false,
    } as never);
    mockedLoadArtifactContent.mockImplementation(async ({ full }) => ({
      content: full ? "complete report" : "preview",
      url: filepath,
      sha256: undefined,
      truncated: !full,
      previewBytes: full ? 15 : 7,
      totalBytes: 15,
    }));
  });

  afterEach(() => {
    cleanup();
    mockedUseThread.mockReset();
    mockedLoadArtifactContent.mockReset();
  });

  it("keeps a full-content request scoped to its thread", async () => {
    const { result, rerender } = renderHook(
      ({ threadId }: { threadId: string }) =>
        useArtifactContent({ filepath, threadId, enabled: true }),
      {
        initialProps: { threadId: "thread-a" },
        wrapper: createWrapper(),
      },
    );

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath,
        threadId: "thread-a",
        isMock: false,
        full: false,
      });
    });

    act(() => {
      result.current.loadFullContent();
    });

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath,
        threadId: "thread-a",
        isMock: false,
        full: true,
      });
    });

    rerender({ threadId: "thread-b" });

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath,
        threadId: "thread-b",
        isMock: false,
        full: false,
      });
    });
  });

  it("gives a report the report's preview budget, not the text preview's", async () => {
    // The card is drawn from the whole body, rows included; under the 1 MiB
    // text budget a report of some 4,300 rows stopped being a card (C8).
    const reportPath =
      "/mnt/user-data/outputs/reports/august/august.report.json";
    renderHook(
      () =>
        useArtifactContent({
          filepath: reportPath,
          threadId: "thread-a",
          enabled: true,
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath: reportPath,
        threadId: "thread-a",
        isMock: false,
        full: false,
        previewMaxBytes: REPORT_PREVIEW_MAX_BYTES,
      });
    });
    expect(REPORT_PREVIEW_MAX_BYTES).toBeGreaterThan(1024 * 1024);
  });
});

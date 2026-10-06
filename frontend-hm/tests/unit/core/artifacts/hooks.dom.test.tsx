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

import { REPORT_PREVIEW_MAX_BYTES } from "../../../../../backend/extensions/sources/hartmesh-legacy-report/browser/index";

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
  it("keeps renderer fallback bounded and scoped separately from a full-file request", async () => {
    const presentation = {
      namespace: "example.summary",
      id: "summary",
      sourceMaxBytes: 4096,
      previewMaxBytes: 256,
      marker: "example-summary-v1",
    };
    const { result, rerender } = renderHook(
      ({ threadId }) =>
        useArtifactContent({ filepath, threadId, enabled: true, presentation }),
      { initialProps: { threadId: "thread-a" }, wrapper: createWrapper() },
    );
    await waitFor(() =>
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith(
        expect.objectContaining({ presentation, full: false }),
      ),
    );
    act(() => {
      result.current.loadSourcePreview();
    });
    await waitFor(() =>
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith(
        expect.objectContaining({
          presentation: { ...presentation, marker: undefined },
          full: false,
        }),
      ),
    );
    expect(result.current.fullContentRequested).toBe(false);
    rerender({ threadId: "thread-b" });
    await waitFor(() =>
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith(
        expect.objectContaining({
          presentation,
          threadId: "thread-b",
          full: false,
        }),
      ),
    );
  });
  beforeEach(() => {
    mockedUseThread.mockReturnValue({
      thread: { isLoading: false, messages: [] },
      isMock: false,
    } as never);
    mockedLoadArtifactContent.mockImplementation(async ({ full }) => ({
      content: full ? "complete report" : "preview",
      url: filepath,
      sha256: undefined,
      projected: false,
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
        signal: expect.any(AbortSignal),
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
        signal: expect.any(AbortSignal),
        full: true,
      });
    });

    rerender({ threadId: "thread-b" });

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath,
        threadId: "thread-b",
        isMock: false,
        signal: expect.any(AbortSignal),
        full: false,
      });
    });
  });

  it("requests a report projection with a bounded source fallback", async () => {
    const reportPath =
      "/mnt/user-data/outputs/reports/august/august.report.json";
    renderHook(
      () =>
        useArtifactContent({
          filepath: reportPath,
          threadId: "thread-a",
          enabled: true,
          presentation: {
            namespace: "hartmesh.legacy-report",
            id: "report",
            sourceMaxBytes: 16 * 1024 * 1024,
            previewMaxBytes: 1024 * 1024,
            marker: "business-report-v1",
          },
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => {
      expect(mockedLoadArtifactContent).toHaveBeenLastCalledWith({
        filepath: reportPath,
        threadId: "thread-a",
        isMock: false,
        signal: expect.any(AbortSignal),
        full: false,
        presentation: {
          namespace: "hartmesh.legacy-report",
          id: "report",
          sourceMaxBytes: 16 * 1024 * 1024,
          previewMaxBytes: 1024 * 1024,
          marker: "business-report-v1",
        },
      });
    });
    expect(REPORT_PREVIEW_MAX_BYTES).toBeGreaterThan(1024 * 1024);
  });
});

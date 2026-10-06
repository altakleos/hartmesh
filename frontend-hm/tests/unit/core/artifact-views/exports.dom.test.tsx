import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";

import { useLiveArtifactExports } from "@/core/artifact-views/exports";

const path = "/mnt/user-data/outputs/results/a.pdf";
const options = {
  eligible: [{ path, label: "PDF" }],
  filepath: "/mnt/user-data/outputs/results/a.view.json",
  threadId: "thread-1",
  runSettled: true,
};

function context() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return { client, wrapper };
}

describe("view export query ownership", () => {
  afterEach(() => {
    cleanup();
    rs.restoreAllMocks();
  });

  it("fences a late previous-revision result", async () => {
    let finish: ((response: Response) => void) | undefined;
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            finish = resolve;
          }),
      )
      .mockResolvedValue(new Response(null, { status: 404 }));
    const { wrapper } = context();
    const { result, rerender } = renderHook(
      ({ revision }) => useLiveArtifactExports({ ...options, revision }),
      { initialProps: { revision: "a".repeat(64) }, wrapper },
    );
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    const oldSignal = fetch.mock.calls[0]![1]!.signal!;
    rerender({ revision: "b".repeat(64) });
    await waitFor(() => expect(result.current.settled).toBe(true));
    expect(oldSignal.aborted).toBe(true);
    finish!(new Response("x", { status: 206 }));
    await waitFor(() => expect(result.current.files).toEqual([]));
    expect(result.current.uncertain).toBe(false);
  });

  it("limits probes to four and does not start queued reads after unmount", async () => {
    const fetch = rs.spyOn(globalThis, "fetch").mockImplementation(
      (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init!.signal!.addEventListener(
            "abort",
            () => reject(new DOMException("Retired", "AbortError")),
            { once: true },
          );
        }),
    );
    const { wrapper } = context();
    const { unmount } = renderHook(
      () =>
        useLiveArtifactExports({
          ...options,
          eligible: Array.from({ length: 16 }, (_, index) => ({
            path: `/mnt/user-data/outputs/${index}.pdf`,
            label: "PDF",
          })),
          revision: "a".repeat(64),
        }),
      { wrapper },
    );
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(4));
    unmount();
    await waitFor(() =>
      expect(fetch.mock.calls.every((call) => call[1]!.signal!.aborted)).toBe(
        true,
      ),
    );
    expect(fetch).toHaveBeenCalledTimes(4);
  });

  it("discards unused availability rather than retaining another view's verdict", async () => {
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(null, { status: 200 }),
    );
    const { client, wrapper } = context();
    const { result, unmount } = renderHook(
      () => useLiveArtifactExports({ ...options, revision: "a".repeat(64) }),
      { wrapper },
    );
    await waitFor(() => expect(result.current.files).toHaveLength(1));
    unmount();
    await waitFor(() =>
      expect(
        client
          .getQueryCache()
          .findAll({ queryKey: ["artifact-export-availability"] }),
      ).toHaveLength(0),
    );
  });
});

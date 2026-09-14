import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";

rs.mock("@/core/api/fetcher", () => ({
  fetch: rs.fn(),
}));

import { fetch } from "@/core/api/fetcher";
import { useToolPlaneGovernance } from "@/core/tool-plane/hooks";

const mockedFetch = rs.mocked(fetch);

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retryDelay: 0 },
    },
  });
  return function QueryWrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

afterEach(() => {
  cleanup();
  mockedFetch.mockReset();
});

describe("useToolPlaneGovernance fail-closed behavior", () => {
  it("blocks legacy mutation while governance is loading", () => {
    mockedFetch.mockImplementation(
      () => new Promise<Response>(() => undefined),
    );

    const { result } = renderHook(
      () => useToolPlaneGovernance("deployment_base"),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(true);
    expect(result.current.legacyMutationBlocked).toBe(true);
    expect(result.current.serviceUnavailable).toBe(false);
  });

  it("keeps legacy mutation blocked after an unexpected network failure", async () => {
    mockedFetch.mockRejectedValue(new Error("network failed"));

    const { result } = renderHook(
      () => useToolPlaneGovernance("deployment_base"),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.error).toBeInstanceOf(Error));
    expect(mockedFetch).toHaveBeenCalledTimes(8);
    expect(result.current.legacyMutationBlocked).toBe(true);
    expect(result.current.serviceUnavailable).toBe(false);
  });

  it("opts into legacy mutation only for the explicit unavailable response", async () => {
    mockedFetch.mockResolvedValue(
      new Response(
        JSON.stringify({
          detail: {
            code: "tool_plane_unavailable",
            message: "Governance is not installed",
          },
        }),
        { status: 503, headers: { "Content-Type": "application/json" } },
      ),
    );

    const { result } = renderHook(
      () => useToolPlaneGovernance("deployment_base"),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.serviceUnavailable).toBe(true));
    expect(mockedFetch).toHaveBeenCalledTimes(2);
    expect(result.current.legacyMutationBlocked).toBe(false);
  });
});

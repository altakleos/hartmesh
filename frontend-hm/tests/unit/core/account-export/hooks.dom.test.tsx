import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";

const fetchMock = rs.hoisted(() => rs.fn());
rs.mock("@/core/api/fetcher", () => ({ fetch: fetchMock }));

import {
  BUILDING_POLL_MS,
  READY_POLL_MS,
  useAccountExport,
} from "@/core/account-export";

function reply(status: number, body?: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

function withState(state: string) {
  return {
    state,
    started_at: "2026-09-27T10:30:00+00:00",
    progress: {
      conversations_total: 1,
      conversations_done: 0,
      files_total: 0,
      files_done: 0,
      bytes_total: 0,
      bytes_done: 0,
    },
    parts: [],
    skipped: 0,
    expires_at: null,
  };
}

function follow(enabled: boolean) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return renderHook(() => useAccountExport(enabled), { wrapper });
}

beforeEach(() => {
  fetchMock.mockReset();
  rs.useFakeTimers();
});

afterEach(() => {
  cleanup();
  rs.useRealTimers();
});

describe("how often the dialog asks", () => {
  it("asks nothing while the dialog is closed", async () => {
    fetchMock.mockResolvedValue(reply(404));
    follow(false);
    await rs.advanceTimersByTimeAsync(20_000);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("follows one being prepared closely", async () => {
    fetchMock.mockResolvedValue(reply(200, withState("building")));
    follow(true);
    await rs.advanceTimersByTimeAsync(0);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await rs.advanceTimersByTimeAsync(BUILDING_POLL_MS);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  for (const state of ["ready", "downloaded"]) {
    it(`follows one ${state} loosely`, async () => {
      fetchMock.mockResolvedValue(reply(200, withState(state)));
      follow(true);
      await rs.advanceTimersByTimeAsync(0);
      await rs.advanceTimersByTimeAsync(READY_POLL_MS - 1);
      expect(fetchMock).toHaveBeenCalledTimes(1);
      await rs.advanceTimersByTimeAsync(1);
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  }

  for (const [label, response] of [
    ["none", () => reply(404)],
    ["a failed one", () => reply(200, withState("failed"))],
  ] as const) {
    it(`stops asking about ${label}`, async () => {
      fetchMock.mockImplementation(async () => response());
      follow(true);
      await rs.advanceTimersByTimeAsync(20_000);
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });
  }
});

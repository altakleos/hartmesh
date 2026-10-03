import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  render,
  renderHook,
  waitFor,
} from "@testing-library/react";
import type { PropsWithChildren } from "react";

const api = rs.hoisted(() => ({
  threads: { search: rs.fn(), delete: rs.fn() },
}));
const localDelete = rs.hoisted(() => rs.fn());
rs.mock("@/core/api", () => ({
  getAPIClient: () => api,
  cancelActiveThreadRun: rs.fn(),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: localDelete }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));

import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { useDeleteThread } from "@/core/threads/hooks";

const clients = new Set<QueryClient>();
function wrapper() {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  clients.add(client);
  const invalidate = rs.spyOn(client, "invalidateQueries");
  const update = rs.spyOn(client, "setQueriesData");
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <FileActionLifetimeProvider>{children}</FileActionLifetimeProvider>
      </QueryClientProvider>
    );
  }
  return { Wrapper, invalidate, update };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const sidecar = {
  thread_id: "child",
  metadata: { deerflow_sidecar: true, parent_thread_id: "parent" },
};

beforeEach(() => {
  api.threads.search.mockReset().mockResolvedValue([]);
  api.threads.delete.mockReset().mockResolvedValue(undefined);
  localDelete
    .mockReset()
    .mockResolvedValue(new Response(null, { status: 204 }));
});
afterEach(() => {
  cleanup();
  clients.forEach((client) => client.clear());
  clients.clear();
});

describe("conversation deletion lifetime", () => {
  it.each([
    "first search",
    "next search",
    "child remote",
    "child local",
    "parent remote",
    "parent local",
  ])(
    "retires after %s without any further requests or UI/cache callbacks",
    async (boundary) => {
      const gate = deferred<unknown>();
      if (boundary === "first search")
        api.threads.search.mockReturnValueOnce(gate.promise);
      if (boundary === "next search") {
        api.threads.search
          .mockResolvedValueOnce(
            Array.from({ length: 100 }, (_, i) => ({
              thread_id: `other-${i}`,
              metadata: {},
            })),
          )
          .mockReturnValueOnce(gate.promise);
      }
      if (boundary.startsWith("child"))
        api.threads.search.mockResolvedValueOnce([sidecar]);
      if (boundary.endsWith("remote"))
        api.threads.delete.mockReturnValueOnce(gate.promise);
      if (boundary.endsWith("local"))
        localDelete.mockReturnValueOnce(gate.promise);
      const { Wrapper, invalidate, update } = wrapper();
      const hook = renderHook(() => useDeleteThread(), { wrapper: Wrapper });
      const onRemoteDeleted = rs.fn();
      let outcome!: Promise<unknown>;
      act(() => {
        outcome = hook.result.current
          .mutateAsync({ threadId: "parent", onRemoteDeleted })
          .catch((error: unknown) => error);
      });
      await waitFor(() => {
        if (boundary === "next search")
          expect(api.threads.search).toHaveBeenCalledTimes(2);
        else if (boundary.endsWith("remote"))
          expect(api.threads.delete).toHaveBeenCalledTimes(1);
        else if (boundary.endsWith("local"))
          expect(localDelete).toHaveBeenCalledTimes(1);
        else expect(api.threads.search).toHaveBeenCalledTimes(1);
      });
      const before = [
        api.threads.search.mock.calls.length,
        api.threads.delete.mock.calls.length,
        localDelete.mock.calls.length,
        onRemoteDeleted.mock.calls.length,
      ];
      const signals = [
        ...api.threads.search.mock.calls.map(
          (call) => (call[0] as { signal: AbortSignal }).signal,
        ),
        ...api.threads.delete.mock.calls.map(
          (call) => (call[1] as { signal: AbortSignal }).signal,
        ),
        ...localDelete.mock.calls.map(
          (call) => (call[1] as { signal: AbortSignal }).signal,
        ),
      ];
      expect(
        signals.every(
          (signal) => signal instanceof AbortSignal && !signal.aborted,
        ),
      ).toBe(true);
      hook.unmount();
      expect(signals.every((signal) => signal.aborted)).toBe(true);
      gate.resolve(
        boundary.includes("search")
          ? []
          : boundary.endsWith("local")
            ? new Response(null, { status: 204 })
            : undefined,
      );
      await act(async () => {
        await outcome;
      });
      expect([
        api.threads.search.mock.calls.length,
        api.threads.delete.mock.calls.length,
        localDelete.mock.calls.length,
        onRemoteDeleted.mock.calls.length,
      ]).toEqual(before);
      expect(invalidate).not.toHaveBeenCalled();
      expect(update).not.toHaveBeenCalled();
    },
  );

  it("finishes cleanup across same-account component navigation", async () => {
    const gate = deferred<unknown>();
    api.threads.delete.mockReturnValueOnce(gate.promise);
    const { Wrapper } = wrapper();
    let remove!: ReturnType<typeof useDeleteThread>["mutateAsync"];
    function Caller() {
      remove = useDeleteThread().mutateAsync;
      return null;
    }
    const app = render(
      <Wrapper>
        <Caller />
      </Wrapper>,
    );
    let done!: Promise<unknown>;
    act(() => {
      done = remove({ threadId: "parent" });
    });
    await waitFor(() => expect(api.threads.delete).toHaveBeenCalledTimes(1));
    app.rerender(
      <Wrapper>
        <span>Another page</span>
      </Wrapper>,
    );
    gate.resolve(undefined);
    await act(async () => {
      await done;
    });
    expect(localDelete).toHaveBeenCalledTimes(1);
  });
});

describe("idempotent retry", () => {
  it("retries local cleanup when the remote thread is already deleted", async () => {
    api.threads.delete.mockRejectedValueOnce(
      Object.assign(new Error("HTTP 404"), { status: 404 }),
    );
    const { Wrapper } = wrapper();
    const hook = renderHook(() => useDeleteThread(), { wrapper: Wrapper });
    const onRemoteDeleted = rs.fn();
    await act(async () => {
      await hook.result.current.mutateAsync({
        threadId: "parent",
        onRemoteDeleted,
      });
    });
    expect(localDelete).toHaveBeenCalledTimes(1);
    expect(onRemoteDeleted).toHaveBeenCalledTimes(1);
  });

  it.each([401, 403, 500, "404", undefined])(
    "does not swallow remote errors with status %s",
    async (status) => {
      const error = Object.assign(new Error("404 is only text here"), {
        status,
      });
      api.threads.delete.mockRejectedValueOnce(error);
      const { Wrapper } = wrapper();
      const hook = renderHook(() => useDeleteThread(), { wrapper: Wrapper });
      const onRemoteDeleted = rs.fn();
      await act(async () => {
        await expect(
          hook.result.current.mutateAsync({
            threadId: "parent",
            onRemoteDeleted,
          }),
        ).rejects.toBe(error);
      });
      expect(localDelete).not.toHaveBeenCalled();
      expect(onRemoteDeleted).not.toHaveBeenCalled();
    },
  );
});

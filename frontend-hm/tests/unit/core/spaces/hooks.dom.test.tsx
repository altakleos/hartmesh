import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, renderHook, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  owner: "alice",
  list: rs.fn(),
  get: rs.fn(),
  files: rs.fn(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.owner } }),
}));
rs.mock("@/core/spaces/api", () => ({
  listSpaces: state.list,
  getSpace: state.get,
  listSpaceFiles: state.files,
}));

import { useSpace, useSpaceFiles, useSpaces } from "@/core/spaces/hooks";

function wrapper({ children }: { children: React.ReactNode }) {
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
let client: QueryClient;

beforeEach(() => {
  state.owner = "alice";
  state.list.mockReset();
  state.get.mockReset();
  state.files.mockReset();
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
});
afterEach(() => {
  cleanup();
  client.clear();
});

it("does not request storage when the host capability is unavailable", () => {
  renderHook(
    () => {
      useSpaces(false);
      useSpace("a".repeat(32), false);
      useSpaceFiles("a".repeat(32), "", false);
    },
    { wrapper },
  );
  expect(state.list).not.toHaveBeenCalled();
  expect(state.get).not.toHaveBeenCalled();
  expect(state.files).not.toHaveBeenCalled();
});

it("changes account caches and aborts the old resource discovery", async () => {
  let finishOld!: (value: unknown) => void;
  let oldSignal: AbortSignal | undefined;
  state.list.mockImplementationOnce((signal: AbortSignal) => {
    oldSignal = signal;
    return new Promise((resolve) => {
      finishOld = resolve;
    });
  });
  state.list.mockResolvedValueOnce({ spaces: [{ id: "bob-space" }] });
  const hook = renderHook(() => useSpaces(true), { wrapper });
  await waitFor(() => expect(state.list).toHaveBeenCalledTimes(1));
  state.owner = "bob";
  hook.rerender();
  await waitFor(() =>
    expect(hook.result.current.data?.spaces[0]?.id).toBe("bob-space"),
  );
  expect(oldSignal?.aborted).toBe(true);
  finishOld({ spaces: [{ id: "alice-private" }] });
  await waitFor(() =>
    expect(hook.result.current.data?.spaces[0]?.id).toBe("bob-space"),
  );
});

import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));

import { fetch } from "@/core/api/fetcher";
import { useSeats, useStorage } from "@/core/features/hooks";

const clients: QueryClient[] = [];
const ready = {
  agents_api: { enabled: true },
  storage_spaces: { enabled: true, state: "ready", own: "ready" },
  accounts: { seats: { allowed: 3, in_use: 2 } },
};
function Probe() {
  const storage = useStorage();
  const seats = useSeats();
  return (
    <output data-testid="storage">{JSON.stringify({ storage, seats })}</output>
  );
}
function start() {
  const client = new QueryClient({
    defaultOptions: { queries: { retryDelay: 0 } },
  });
  clients.push(client);
  const tree = () => (
    <QueryClientProvider client={client}>
      <Probe />
    </QueryClientProvider>
  );
  return { client, tree, view: render(tree()) };
}
function state() {
  return JSON.parse(screen.getByTestId("storage").textContent ?? "{}");
}
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.mocked(fetch).mockReset();
  rs.useRealTimers();
  auth.user = { id: "alice", system_role: "user" };
});

describe("current-account storage observers", () => {
  it("shares feature transport and preserves explicit off seats", async () => {
    rs.mocked(fetch).mockResolvedValue(
      Response.json({
        ...ready,
        storage_spaces: { enabled: false, state: "off", own: "ready" },
        accounts: { seats: { allowed: 0, in_use: 0 } },
      }),
    );
    start();
    await waitFor(() => expect(state().storage.state).toBe("off"));
    expect(state().seats).toBeNull();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("fails closed after a current read fails even with cached ready data", async () => {
    rs.mocked(fetch).mockResolvedValue(Response.json(ready));
    const { client } = start();
    await waitFor(() => expect(state().storage.state).toBe("ready"));
    rs.mocked(fetch).mockRejectedValue(new Error("Gateway unavailable"));
    await act(async () => {
      await client.invalidateQueries({
        queryKey: ["features", "alice", "user"],
      });
    });
    await waitFor(() => expect(state().storage.state).toBe("unavailable"));
    expect(state().seats).toBeNull();
  });

  it("does not show a previous account's seat and file state during a new read", async () => {
    rs.mocked(fetch).mockResolvedValue(Response.json(ready));
    const { tree, view } = start();
    await waitFor(() => expect(state().seats.inUse).toBe(2));
    rs.mocked(fetch).mockImplementation(
      () => new Promise<Response>(() => undefined),
    );
    auth.user = { id: "bob", system_role: "admin" };
    view.rerender(tree());
    expect(state().storage.known).toBe(false);
    expect(state().seats).toBeNull();
  });

  it("polls preparation at fifteen seconds and retires polling after readiness and unmount", async () => {
    rs.useFakeTimers();
    rs.mocked(fetch).mockImplementation(() =>
      Promise.resolve(
        Response.json({
          ...ready,
          storage_spaces: { ...ready.storage_spaces, own: "not-assigned" },
        }),
      ),
    );
    const { view } = start();
    await act(async () => {
      await rs.advanceTimersByTimeAsync(1);
    });
    expect(state().storage.own).toBe("not-assigned");
    expect(fetch).toHaveBeenCalledTimes(1);
    rs.mocked(fetch).mockImplementation(() =>
      Promise.resolve(Response.json(ready)),
    );
    await act(async () => {
      await rs.advanceTimersByTimeAsync(15_000);
    });
    expect(state().storage.own).toBe("ready");
    expect(fetch).toHaveBeenCalledTimes(2);
    await act(async () => {
      await rs.advanceTimersByTimeAsync(30_000);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
    view.unmount();
    await act(async () => {
      await rs.advanceTimersByTimeAsync(30_000);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

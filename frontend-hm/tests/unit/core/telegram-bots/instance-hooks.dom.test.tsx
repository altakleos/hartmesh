import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  user: { id: "alice", system_role: "user", permissions: ["agents:read"] },
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/features/hooks", () => ({
  useTelegram: () => ({ known: true }),
  useAgentInstances: () => ({ known: true, enabled: true }),
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
import { useTelegramInstanceDiscovery } from "@/core/telegram-bots/instance-hooks";

import {
  clearClients,
  json,
  queries,
  renderWithClient,
} from "../../components/workspace/telegram/support";
function Discovery() {
  const read = useTelegramInstanceDiscovery();
  return (
    <output>
      {read.complete
        ? read.instances.map((entry) => entry.name).join(",")
        : read.error
          ? "failed"
          : "waiting"}
    </output>
  );
}
beforeEach(() => {
  state.user = {
    id: "alice",
    system_role: "user",
    permissions: ["agents:read"],
  };
});
afterEach(() => {
  cleanup();
  clearClients();
  rs.unstubAllGlobals();
});
it("publishes both complete paginated lists through the authenticated API client", async () => {
  rs.stubGlobal(
    "fetch",
    rs.fn(async (url: string) => {
      const p = new URL(url, "http://app.test").searchParams;
      const instances =
        p.get("custody") === "company"
          ? [{ id: "company", name: "Employee", permissions: 1 }]
          : p.get("offset") === "0"
            ? Array.from({ length: 100 }, (_, i) => ({
                id: String(i),
                name: `Assistant${i}`,
                permissions: 1,
              }))
            : [{ id: "last", name: "Last assistant", permissions: 1 }];
      return json({ instances });
    }),
  );
  renderWithClient(<Discovery />);
  await waitFor(() =>
    expect(screen.getByRole("status").textContent).toContain(
      "Last assistant,Employee",
    ),
  );
  expect(fetch).toHaveBeenCalledTimes(3);
  expect(
    rs
      .mocked(fetch)
      .mock.calls.every(([, init]) => init?.credentials === "include"),
  ).toBe(true);
});
it("does not publish a partial list and retires both readers on account change", async () => {
  const held: { signal: AbortSignal; resolve: (response: Response) => void }[] =
    [];
  rs.stubGlobal(
    "fetch",
    rs.fn(
      async (_url: string, init?: RequestInit) =>
        new Promise<Response>((resolve) => {
          held.push({ signal: init!.signal!, resolve });
        }),
    ),
  );
  const tree = renderWithClient(<Discovery />);
  await waitFor(() => expect(held).toHaveLength(2));
  await act(async () =>
    held[0]!.resolve(
      json({
        instances: [
          { id: "private", name: "Private assistant", permissions: 1 },
        ],
      }),
    ),
  );
  expect(screen.queryByText(/Private assistant/)).toBeNull();
  state.user = { ...state.user, id: "bob" };
  tree.rerender(
    <QueryClientProvider client={queries()}>
      <Discovery />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(held).toHaveLength(4));
  expect(held[1]!.signal.aborted).toBe(true);
  await act(async () => {
    held[1]!.resolve(
      json({
        instances: [{ id: "late", name: "Old employee", permissions: 1 }],
      }),
    );
    held[2]!.resolve(json({ instances: [] }));
    held[3]!.resolve(json({ instances: [] }));
  });
  expect(screen.queryByText(/Private assistant|Old employee/)).toBeNull();
});
it("fails closed without the read ceiling and makes no discovery request", async () => {
  state.user.permissions = [];
  rs.stubGlobal("fetch", rs.fn());
  renderWithClient(<Discovery />);
  await act(async () => undefined);
  expect(fetch).not.toHaveBeenCalled();
});

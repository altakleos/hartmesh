import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({ user: "reader" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: state.user, system_role: "user", permissions: ["agents:read"] },
  }),
}));
rs.mock("@/core/people/hooks", () => ({ usePeople: () => new Map() }));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getInstanceGrants: rs.fn(),
}));
import * as api from "@/core/agent-instances/api";
import type { InputRequest } from "@/core/attention/api";
import { useInputRecipients } from "@/core/attention/recipients";

const clients: QueryClient[] = [];
const instance = "a".repeat(32);
function show() {
  const client = new QueryClient();
  clients.push(client);
  return renderHook(
    ({ enabled, purpose }) => useInputRecipients(instance, purpose, enabled),
    {
      initialProps: {
        enabled: true,
        purpose: "information" as InputRequest["purpose"],
      },
      wrapper: ({ children }) => (
        <QueryClientProvider client={client}>{children}</QueryClientProvider>
      ),
    },
  );
}
beforeEach(() => {
  state.user = "reader";
  rs.mocked(api.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({
      grants: [
        { user_id: "starter", permissions: 3 },
        { user_id: "manager", permissions: 6 },
        { user_id: "chat", permissions: 1 },
      ],
    });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("purpose changes use current grant bits without widening the eligible directory", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.known).toBe(true));
  expect(view.result.current.grants.map((row) => row.user_id)).toEqual([
    "starter",
  ]);
  view.rerender({ enabled: true, purpose: "decision" });
  expect(view.result.current.grants.map((row) => row.user_id)).toEqual([
    "manager",
  ]);
});
test("retired access hides previously loaded recipients and aborts a held grant read", async () => {
  let resolve!: (value: {
    grants: { user_id: string; permissions: number }[];
  }) => void;
  const held = new Promise<{
    grants: { user_id: string; permissions: number }[];
  }>((done) => {
    resolve = done;
  });
  rs.mocked(api.getInstanceGrants).mockReturnValue(held);
  const view = show();
  await waitFor(() => expect(api.getInstanceGrants).toHaveBeenCalledTimes(1));
  const signal = rs.mocked(api.getInstanceGrants).mock.calls[0]![1]!;
  view.rerender({ enabled: false, purpose: "information" });
  expect(signal.aborted).toBe(true);
  await act(async () => {
    resolve({ grants: [{ user_id: "old", permissions: 7 }] });
  });
  expect(view.result.current.grants).toEqual([]);
  expect(view.result.current.known).toBe(false);
});
test("a failed current account read cannot borrow a previous account's recipients", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.known).toBe(true));
  state.user = "other";
  rs.mocked(api.getInstanceGrants).mockRejectedValue(new Error("PRIVATE"));
  view.rerender({ enabled: true, purpose: "information" });
  await waitFor(() => expect(view.result.current.error).toBe(true));
  expect(view.result.current.grants).toEqual([]);
});

import { afterEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

const auth = rs.hoisted(() => ({
  user: { id: "alice", system_role: "user", permissions: ["agents:read"] },
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/agent-instances/api", { spy: true });
import {
  listInstances,
  type AgentInstance,
  type AgentListEntry,
} from "@/core/agent-instances/api";
import { useAgentHomes } from "@/core/agent-instances/hooks";

const clients: QueryClient[] = [];
const counts = { per_user: 1, allowed: 10, in_use: 2, mine: 1 };
function member(id: string): AgentInstance {
  return {
    id,
    home_id: id,
    name: id,
    permissions: 7,
    status: "active",
  } as AgentInstance;
}
function start(enabled = true) {
  const client = new QueryClient();
  clients.push(client);
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return renderHook(() => useAgentHomes(enabled), { wrapper });
}
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.mocked(listInstances).mockReset();
  auth.user = {
    id: "alice",
    system_role: "user",
    permissions: ["agents:read"],
  };
});

test("reads both custodies through every page, retaining only granted live agents", async () => {
  rs.mocked(listInstances).mockImplementation(async (custody, offset) => {
    let instances: AgentListEntry[] = [];
    if (custody === "personal" && offset === 0)
      instances = Array.from({ length: 100 }, (_, i) => member(`p${i}`));
    if (custody === "personal" && offset === 100) instances = [member("tail")];
    if (custody === "company")
      instances = [
        member("company"),
        { ...member("deleted"), status: "deleted" },
        {
          id: "summary",
          name: "Inventory only",
          supervisor_name: null,
          status: "active",
        },
      ];
    return { instances, ai_assistants: counts, unfinished: [] };
  });
  const query = start();
  await waitFor(() => expect(query.result.current.isSuccess).toBe(true));
  expect(query.result.current.data).toHaveLength(102);
  expect(query.result.current.data?.map((agent) => agent.id)).toContain("tail");
  expect(query.result.current.data?.map((agent) => agent.id)).not.toContain(
    "summary",
  );
  expect(listInstances).toHaveBeenCalledTimes(3);
});

test("makes no discovery request when the feature is unavailable", () => {
  const query = start(false);
  expect(listInstances).not.toHaveBeenCalled();
  expect(query.result.current.data).toBeUndefined();
});

test("fences late discovery when the account changes", async () => {
  const pending: ((
    value: Awaited<ReturnType<typeof listInstances>>,
  ) => void)[] = [];
  const signals: AbortSignal[] = [];
  rs.mocked(listInstances)
    .mockImplementationOnce((_custody, _offset, signal) => {
      signals.push(signal!);
      return new Promise((resolve) => pending.push(resolve));
    })
    .mockImplementationOnce((_custody, _offset, signal) => {
      signals.push(signal!);
      return new Promise((resolve) => pending.push(resolve));
    })
    .mockResolvedValue({
      instances: [],
      ai_assistants: counts,
      unfinished: [],
    });
  const query = start();
  await waitFor(() => expect(listInstances).toHaveBeenCalledTimes(2));
  auth.user = { ...auth.user, id: "bob" };
  query.rerender();
  expect(query.result.current.data).toBeUndefined();
  await waitFor(() => expect(query.result.current.isSuccess).toBe(true));
  expect(signals.every((signal) => signal.aborted)).toBe(true);
  await act(async () =>
    pending.forEach((finish) =>
      finish({
        instances: [member("old private")],
        ai_assistants: counts,
        unfinished: [],
      }),
    ),
  );
  expect(query.result.current.data).toEqual([]);
});

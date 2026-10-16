import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({ user: "reader" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: state.user, system_role: "user", permissions: ["agents:read"] },
  }),
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return { ...actual, getInstance: rs.fn(), getInstanceLifecycle: rs.fn() };
});

import * as api from "@/core/agent-instances/api";
import { useConversationDetails } from "@/core/agent-instances/conversation-details";
const id = "a".repeat(32);
const detail: api.AgentInstanceDetail = {
  id,
  name: "Researcher",
  principal: { kind: "nonhuman", subject_id: "ai" },
  custody: "personal",
  owner_id: "reader",
  creator_id: "reader",
  supervisor: { kind: "human", subject_id: "reader" },
  definition_revision: "revision",
  generation: 1,
  home_id: "b".repeat(32),
  status: "active",
  permissions: 7,
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 1,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
const clients: QueryClient[] = [];
function show(
  instance: string | null = id,
  enabled = true,
  refreshKey = "idle",
) {
  const client = new QueryClient();
  clients.push(client);
  return renderHook(
    ({ instance, enabled, refreshKey }) =>
      useConversationDetails(instance, { enabled, refreshKey }),
    {
      initialProps: { instance, enabled, refreshKey },
      wrapper: ({ children }) => (
        <QueryClientProvider client={client}>{children}</QueryClientProvider>
      ),
    },
  );
}
beforeEach(() => {
  state.user = "reader";
  rs.mocked(api.getInstance).mockReset().mockResolvedValue(detail);
  rs.mocked(api.getInstanceLifecycle)
    .mockReset()
    .mockResolvedValue({ operations: [] });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("opens one current detail read and refreshes on reply transitions without duplicate initial requests", async () => {
  const view = show();
  await waitFor(() =>
    expect(view.result.current.detail?.name).toBe("Researcher"),
  );
  expect(api.getInstance).toHaveBeenCalledTimes(1);
  view.rerender({ instance: id, enabled: true, refreshKey: "running" });
  await waitFor(() => expect(api.getInstance).toHaveBeenCalledTimes(2));
  view.rerender({ instance: id, enabled: true, refreshKey: "idle" });
  await waitFor(() => expect(api.getInstance).toHaveBeenCalledTimes(3));
});
test.each([null, id])(
  "no agent detail is requested without an enabled current binding: %s",
  (instance) => {
    show(instance, false);
    expect(api.getInstance).not.toHaveBeenCalled();
  },
);
test.each([
  [403, "forbidden"],
  [404, "not-found"],
  [503, "error"],
] as const)(
  "preserves status %s as %s instead of inventing withdrawal",
  async (status, expected) => {
    rs.mocked(api.getInstance).mockRejectedValue(
      new api.InstanceApiError(status, "private"),
    );
    const view = show();
    await waitFor(() => expect(view.result.current.status).toBe(expected));
    expect(view.result.current.detail).toBeNull();
  },
);
test("reads lifecycle only for suspended agents with Inspect or Manage", async () => {
  rs.mocked(api.getInstance).mockResolvedValue({
    ...detail,
    status: "suspended",
    permissions: 2,
  });
  const view = show();
  await waitFor(() =>
    expect(api.getInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(view.result.current.pending).toBe(false);
});
test.each(["active", "suspended"] as const)(
  "observed lifecycle completion refreshes authoritative status, including %s",
  async (status) => {
    const operation: api.LifecycleOperation = {
      operation_id: "observed-operation",
      generation: 1,
      action: "grant",
      complete: false,
    };
    rs.mocked(api.getInstance)
      .mockResolvedValueOnce({ ...detail, status: "suspended" })
      .mockResolvedValue({ ...detail, generation: 2, status });
    rs.mocked(api.getInstanceLifecycle)
      .mockResolvedValueOnce({ operations: [operation] })
      .mockResolvedValue({ operations: [{ ...operation, complete: true }] });
    const view = show();
    await waitFor(() => expect(view.result.current.pending).toBe(true));
    expect(api.getInstance).toHaveBeenCalledTimes(1);
    await act(async () => {
      await clients.at(-1)!.invalidateQueries({
        predicate: (query) => query.queryKey.includes("lifecycle"),
      });
    });
    await waitFor(() => expect(api.getInstance).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(view.result.current.detail?.generation).toBe(2));
    expect(view.result.current.detail?.status).toBe(status);
    expect(view.result.current.pending).toBe(false);
  },
);
test("a late prior binding cannot supply current details", async () => {
  let finish!: (value: api.AgentInstanceDetail) => void;
  rs.mocked(api.getInstance).mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  await waitFor(() => expect(api.getInstance).toHaveBeenCalled());
  view.rerender({
    instance: "c".repeat(32),
    enabled: true,
    refreshKey: "idle",
  });
  await waitFor(() => expect(api.getInstance).toHaveBeenCalledTimes(2));
  finish({ ...detail, name: "Old identity" });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(view.result.current.detail?.name).toBe("Researcher");
});

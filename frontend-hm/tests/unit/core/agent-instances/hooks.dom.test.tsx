import { afterEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

const state = rs.hoisted(() => ({ userId: "alice" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.userId, system_role: "user" } }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
import { getConversationInstance } from "@/core/agent-instances/api";
import { useConversationInstance } from "@/core/agent-instances/hooks";

afterEach(cleanup);
it("resolves the server binding and changes cache ownership with the account", async () => {
  const accounts: string[] = [];
  rs.mocked(getConversationInstance).mockImplementation(async () => {
    accounts.push(state.userId);
    return { instance: null };
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const query = renderHook(
    () => useConversationInstance("canonical-thread", true),
    { wrapper },
  );
  await waitFor(() => expect(query.result.current.isSuccess).toBe(true));
  state.userId = "bob";
  query.rerender();
  await waitFor(() => expect(accounts).toEqual(["alice", "bob"]));
});

import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));

import { fetch } from "@/core/api/fetcher";
import {
  useAgentInstances,
  useStorage,
  useSpacesPageVisible,
} from "@/core/features/hooks";

const clients: QueryClient[] = [];
const ready = {
  agents_api: { enabled: true },
  storage_spaces: { enabled: true, state: "ready", own: "ready" },
  agent_instances: {
    enabled: true,
    execution: "ready",
    reason: null,
    ai_assistants: { per_user: 2, allowed: 6, in_use: 4, mine: 1 },
    ai_employees: { allowed: 10, in_use: 5 },
  },
};

function Probe() {
  const agents = useAgentInstances();
  const storage = useStorage();
  const spacesVisible = useSpacesPageVisible();
  return (
    <output data-testid="features">
      {JSON.stringify({ agents, storage, spacesVisible })}
    </output>
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
  return JSON.parse(screen.getByTestId("features").textContent ?? "{}");
}

afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.mocked(fetch).mockReset();
  auth.user = { id: "alice", system_role: "user" };
});

describe("current-account agent discovery", () => {
  it.each([
    ["business", "user", true, false],
    ["business", "admin", true, false],
    ["developer", "user", true, true],
    ["developer", "admin", true, true],
    ["developer", "admin", false, false],
  ] as const)(
    "gates Spaces for profile %s, role %s and storage %s",
    async (profile, role, enabled, visible) => {
      auth.user.system_role = role;
      rs.mocked(fetch).mockResolvedValue(
        Response.json({
          ...ready,
          ui: { profile },
          storage_spaces: { ...ready.storage_spaces, enabled },
        }),
      );
      start();
      await waitFor(() => expect(state().agents.known).toBe(true));
      expect(state().spacesVisible).toBe(visible);
    },
  );
  it("starts closed and shares one feature transport with storage", async () => {
    let finish!: (response: Response) => void;
    rs.mocked(fetch).mockImplementation(
      () =>
        new Promise<Response>((resolve) => {
          finish = resolve;
        }),
    );
    start();
    expect(state().agents).toMatchObject({
      known: false,
      enabled: false,
      execution: "unavailable",
      isLoading: true,
    });
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    await act(async () => {
      finish(Response.json(ready));
    });
    await waitFor(() => expect(state().agents.assistants.mine).toBe(1));
    expect(state().storage.state).toBe("ready");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("closes after a failed current refresh despite a cached enabled answer and recovers", async () => {
    rs.mocked(fetch).mockResolvedValue(Response.json(ready));
    const { client } = start();
    await waitFor(() => expect(state().agents.enabled).toBe(true));
    rs.mocked(fetch).mockRejectedValue(new Error("Gateway unavailable"));
    await act(async () => {
      await client.invalidateQueries({
        queryKey: ["features", "alice", "user"],
      });
    });
    await waitFor(() => expect(state().agents.known).toBe(false));
    expect(state().agents).toMatchObject({
      enabled: false,
      execution: "unavailable",
      isLoading: false,
      assistants: { perUser: 0, allowed: 0, inUse: 0, mine: 0 },
      employees: { allowed: 0, inUse: 0 },
    });
    expect(state().storage.known).toBe(false);
    expect(fetch).toHaveBeenCalledTimes(4);
    rs.mocked(fetch).mockResolvedValue(Response.json(ready));
    await act(async () => {
      await client.invalidateQueries({
        queryKey: ["features", "alice", "user"],
      });
    });
    await waitFor(() => expect(state().agents.known).toBe(true));
    expect(state().agents.assistants.mine).toBe(1);
  });

  it.each([
    { id: "bob", system_role: "user" },
    { id: "alice", system_role: "admin" },
  ])(
    "never shows a previous identity's counts during a new read: %j",
    async (next) => {
      rs.mocked(fetch).mockResolvedValue(Response.json(ready));
      const { tree, view } = start();
      await waitFor(() => expect(state().agents.assistants.mine).toBe(1));
      rs.mocked(fetch).mockImplementation(
        () => new Promise<Response>(() => undefined),
      );
      auth.user = next;
      view.rerender(tree());
      expect(state().agents.known).toBe(false);
      expect(state().agents.assistants.mine).toBe(0);
      expect(state().agents.isLoading).toBe(true);
    },
  );

  it("cannot publish an old account's late response into the next account", async () => {
    let alice!: (response: Response) => void;
    rs.mocked(fetch)
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            alice = resolve;
          }),
      )
      .mockResolvedValue(
        Response.json({
          ...ready,
          agent_instances: {
            enabled: false,
            execution: "unavailable",
            reason: null,
          },
        }),
      );
    const { tree, view } = start();
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    auth.user = { id: "bob", system_role: "user" };
    view.rerender(tree());
    await waitFor(() => expect(state().agents.known).toBe(true));
    expect(state().agents.enabled).toBe(false);
    await act(async () => {
      alice(Response.json(ready));
    });
    expect(state().agents.enabled).toBe(false);
    expect(state().agents.assistants.mine).toBe(0);
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

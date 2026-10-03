import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { system_role: "user" } }),
}));

import { useAgentsApiEnabled } from "@/core/agents/hooks";
import { fetch } from "@/core/api/fetcher";
import {
  useBranding,
  useBrowserControlEnabled,
  useDeveloperSurfacesVisible,
  useMcpTasksEnabled,
  useSubagentBatchesCapability,
  useWorkspacePresentation,
} from "@/core/features/hooks";

const mockedFetch = rs.mocked(fetch);
const clients = new Set<QueryClient>();
const response = {
  agents_api: { enabled: false },
  browser_control: { enabled: true },
  mcp_tasks: { enabled: true },
  subagent_batches: {
    repository_available: true,
    worker_running: false,
    max_running: 3,
  },
  branding: {
    company_name: "  Example Company  ",
    colors: { primary: "#123456", secondary: "invalid" },
    has_logo: true,
  },
  ui: {
    profile: "business",
    starters: [{ id: "review", title: "Monthly review", prompt: "Review it." }],
  },
};

function FeatureConsumers() {
  const branding = useBranding();
  const browser = useBrowserControlEnabled();
  const tasks = useMcpTasksEnabled();
  const batches = useSubagentBatchesCapability();
  const presentation = useWorkspacePresentation();
  const agents = useAgentsApiEnabled();
  const developer = useDeveloperSurfacesVisible();
  return (
    <output data-testid="features">
      {JSON.stringify({
        branding,
        browser,
        tasks,
        batches,
        presentation,
        agents,
        developer,
      })}
    </output>
  );
}

function state() {
  return JSON.parse(screen.getByTestId("features").textContent ?? "{}") as {
    branding: ReturnType<typeof useBranding>;
    browser: ReturnType<typeof useBrowserControlEnabled>;
    tasks: ReturnType<typeof useMcpTasksEnabled>;
    batches: ReturnType<typeof useSubagentBatchesCapability>;
    presentation: ReturnType<typeof useWorkspacePresentation>;
    agents: ReturnType<typeof useAgentsApiEnabled>;
    developer: boolean;
  };
}

function newClient() {
  const client = new QueryClient({
    defaultOptions: { queries: { retryDelay: 0 } },
  });
  clients.add(client);
  return client;
}

function App({ client, show = true }: { client: QueryClient; show?: boolean }) {
  return (
    <QueryClientProvider client={client}>
      {show && <FeatureConsumers />}
    </QueryClientProvider>
  );
}

beforeEach(() => {
  mockedFetch.mockReset();
  localStorage.clear();
});

afterEach(() => {
  cleanup();
  clients.forEach((client) => client.clear());
  clients.clear();
  localStorage.clear();
});

describe("shared feature discovery", () => {
  it("fetches once for concurrent consumers and preserves their selected values", async () => {
    let resolve!: () => void;
    const pending = new Promise<void>((done) => {
      resolve = done;
    });
    mockedFetch.mockImplementation(async () => {
      await pending;
      return Response.json(response);
    });
    render(<App client={newClient()} />);

    expect(state().branding.isLoading).toBe(true);
    expect(state().browser).toEqual({ enabled: false, isLoading: true });
    expect(state().tasks).toEqual({ enabled: false, isLoading: true });
    expect(state().batches.repositoryAvailable).toBe(false);
    expect(state().presentation.profile).toBeUndefined();
    expect(state().developer).toBe(true);
    expect(mockedFetch).toHaveBeenCalledTimes(1);
    expect(mockedFetch).toHaveBeenCalledWith("/api/features");

    await act(async () => resolve());
    await waitFor(() => expect(state().branding.isLoading).toBe(false));
    expect(state()).toEqual({
      branding: {
        companyName: "Example Company",
        primary: "#123456",
        secondary: null,
        hasLogo: true,
        providerName: null,
        supportURL: null,
        isLoading: false,
      },
      browser: { enabled: true, isLoading: false },
      tasks: { enabled: true, isLoading: false },
      batches: {
        repositoryAvailable: true,
        workerRunning: false,
        maxRunning: 3,
        isLoading: false,
      },
      presentation: {
        profile: "business",
        starters: response.ui.starters,
        isLoading: false,
      },
      agents: { enabled: false, isLoading: false },
      developer: false,
    });
    expect(mockedFetch).toHaveBeenCalledTimes(1);
  });

  it("keeps missing optional capabilities closed and legacy presentation open", async () => {
    mockedFetch.mockImplementation(async () =>
      Response.json({ agents_api: { enabled: true } }),
    );
    render(<App client={newClient()} />);
    await waitFor(() => expect(state().branding.isLoading).toBe(false));
    expect(state().branding).toEqual({
      companyName: null,
      primary: null,
      secondary: null,
      hasLogo: false,
      providerName: null,
      supportURL: null,
      isLoading: false,
    });
    expect(state().browser.enabled).toBe(false);
    expect(state().tasks.enabled).toBe(false);
    expect(state().batches).toEqual({
      repositoryAvailable: false,
      workerRunning: false,
      maxRunning: 0,
      isLoading: false,
    });
    expect(state().presentation).toEqual({
      profile: "developer",
      starters: [],
      isLoading: false,
    });
    expect(state().developer).toBe(true);
  });

  it("shares bounded retries, keeps failure defaults, and recovers on the next mount", async () => {
    const client = newClient();
    mockedFetch.mockRejectedValue(new Error("Offline"));
    const app = render(<App client={client} />);
    await waitFor(() => expect(state().presentation.isLoading).toBe(false));
    expect(mockedFetch).toHaveBeenCalledTimes(3);
    expect(state().branding.isLoading).toBe(true);
    expect(state().browser).toEqual({ enabled: false, isLoading: false });
    expect(state().tasks).toEqual({ enabled: false, isLoading: false });
    expect(state().batches.repositoryAvailable).toBe(false);
    expect(state().presentation.profile).toBeUndefined();
    expect(state().agents).toEqual({ enabled: true, isLoading: false });
    expect(state().developer).toBe(true);

    app.rerender(<App client={client} show={false} />);
    mockedFetch.mockImplementation(async () => Response.json(response));
    app.rerender(<App client={client} />);
    await waitFor(() =>
      expect(state().branding.companyName).toBe("Example Company"),
    );
    expect(mockedFetch).toHaveBeenCalledTimes(4);
    expect(state().developer).toBe(false);
  });

  it("retains an accepted answer during refetch failure and the agents fallback on a cold mount", async () => {
    const client = newClient();
    mockedFetch.mockImplementation(async () => Response.json(response));
    const app = render(<App client={client} />);
    await waitFor(() =>
      expect(state().branding.companyName).toBe("Example Company"),
    );
    app.rerender(<App client={client} show={false} />);
    mockedFetch.mockRejectedValue(new Error("Offline"));
    app.rerender(<App client={client} />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(4));
    await waitFor(() => expect(client.isFetching()).toBe(0));
    expect(state().branding.companyName).toBe("Example Company");
    expect(state().browser.enabled).toBe(true);
    expect(state().developer).toBe(false);

    app.unmount();
    mockedFetch.mockClear();
    render(<App client={newClient()} />);
    await waitFor(() => expect(state().presentation.isLoading).toBe(false));
    expect(state().branding.companyName).toBeNull();
    expect(state().branding.isLoading).toBe(true);
    expect(state().agents).toEqual({ enabled: false, isLoading: false });
    expect(state().developer).toBe(true);
  });
});

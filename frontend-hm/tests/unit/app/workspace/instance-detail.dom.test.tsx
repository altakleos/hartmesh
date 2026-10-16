import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  query: "instance=" + "a".repeat(32),
  systemRole: "user",
  agentPermissions: 7,
  routePermissions: [
    "agents:read",
    "agents:write",
    "threads:write",
    "memory:read",
  ],
  enabled: true,
  replace: rs.fn(),
  work: rs.fn(),
}));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(state.query),
  useRouter: () => ({ replace: state.replace, push: rs.fn() }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: state.systemRole,
      permissions: state.routePermissions,
    },
  }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => ({
    known: true,
    enabled: state.enabled,
    isLoading: false,
    execution: "ready",
    reason: null,
  }),
  useStorage: () => null,
  useBranding: () => ({}),
  useDocumentTitle: () => undefined,
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: () => "Colleague",
  useSupervisorName: () => "a manager",
}));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: ({
    breadcrumbSection,
  }: {
    breadcrumbSection?: { label: string; href: string };
  }) =>
    breadcrumbSection ? (
      <nav>
        <a href={breadcrumbSection.href}>{breadcrumbSection.label}</a>
      </nav>
    ) : null,
}));
rs.mock("@/components/workspace/instances/work-panel", () => ({
  WorkPanel: (props: unknown) => {
    state.work(props);
    return <p>Assignments shown</p>;
  },
}));
rs.mock("@/components/workspace/instances/conversations-tab", () => ({
  ConversationsTab: () => <p>Conversation history shown</p>,
}));
rs.mock("@/components/workspace/instances/memory-tab", () => ({
  MemoryTab: () => <p>Memory shown</p>,
}));
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/agents/api", () => ({
  listAgents: rs.fn().mockResolvedValue([]),
}));
import InstancesPage from "@/app/workspace/instances/page";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
const clients: QueryClient[] = [];
const id = "a".repeat(32);
function instance(): api.AgentInstanceDetail {
  return {
    id,
    name: "Researcher",
    custody: "personal",
    owner_id: "reader",
    creator_id: "reader",
    principal: { kind: "nonhuman", subject_id: "agent" },
    supervisor: { kind: "human", subject_id: "reader" },
    definition_revision: "d".repeat(64),
    home_id: "b".repeat(32),
    status: "active",
    permissions: state.agentPermissions,
    generation: 1,
    activity: {
      running_runs: 0,
      running: [],
      last_active_at: null,
      open_conversations: 0,
    },
    home: { state: "available", workspace: "absent" },
    execution: { execution: "ready", reason: null },
  };
}
function show() {
  const client = new QueryClient();
  clients.push(client);
  return {
    ...render(
      <QueryClientProvider client={client}>
        <I18nProvider initialLocale="en-US">
          <FileActionLifetimeProvider>
            <InstancesPage />
          </FileActionLifetimeProvider>
        </I18nProvider>
      </QueryClientProvider>,
    ),
    client,
  };
}
beforeEach(() => {
  state.query = `instance=${id}`;
  state.systemRole = "user";
  state.agentPermissions = 7;
  state.routePermissions = [
    "agents:read",
    "agents:write",
    "threads:write",
    "memory:read",
  ];
  state.enabled = true;
  state.replace.mockReset();
  state.work.mockReset();
  rs.mocked(api.getInstance)
    .mockReset()
    .mockImplementation(async () => instance());
  rs.mocked(api.getInstanceDefinition)
    .mockReset()
    .mockResolvedValue({
      revision: "d".repeat(64),
      config: { name: "source" },
      soul: "Instructions",
    });
  rs.mocked(api.getInstanceLifecycle)
    .mockReset()
    .mockResolvedValue({ operations: [] });
  rs.mocked(api.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({ grants: [] });
  rs.mocked(api.listInstances).mockClear();
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test.each(["user", "admin"])(
  "redirects the old no-id list by %s role without fetching inventory",
  async (role) => {
    state.query = "";
    state.systemRole = role;
    show();
    await waitFor(() =>
      expect(state.replace).toHaveBeenCalledWith(
        role === "admin" ? "/workspace/employees" : "/workspace/assistants",
      ),
    );
    expect(api.listInstances).not.toHaveBeenCalled();
  },
);
test("opens conversation history by default and exposes six permitted tabs", async () => {
  show();
  await screen.findByText("Conversation history shown");
  expect(screen.getAllByRole("tab")).toHaveLength(6);
  expect(
    screen
      .getByRole("tab", { name: "Conversations" })
      .getAttribute("aria-selected"),
  ).toBe("true");
});
test("an inaccessible tab falls back to Conversations", async () => {
  state.query += "&tab=memory";
  state.agentPermissions = 1;
  show();
  await screen.findByText("Conversation history shown");
  expect(screen.queryByRole("tab", { name: "Memory" })).toBeNull();
  expect(api.getInstanceDefinition).not.toHaveBeenCalled();
});
test("a Work deep link overrides the tab and carries its assignment", async () => {
  state.query += "&tab=memory&work=" + "c".repeat(32);
  show();
  await screen.findByText("Assignments shown");
  expect(state.work.mock.calls[0]![0]).toMatchObject({
    initialWork: "c".repeat(32),
  });
  expect(screen.queryByText("Memory shown")).toBeNull();
});
test("reads pending lifecycle for Inspect even without the route write ceiling", async () => {
  state.agentPermissions = 2;
  state.routePermissions = ["agents:read", "threads:write"];
  rs.mocked(api.getInstanceLifecycle).mockResolvedValue({
    operations: [
      {
        operation_id: "c".repeat(32),
        generation: 1,
        action: "adopt",
        complete: false,
      },
    ],
  });
  show();
  await screen.findByText("Change in progress");
  expect(api.getInstanceLifecycle).toHaveBeenCalled();
});
test("a deleted agent shows the named notice and return link without tabs", async () => {
  rs.mocked(api.getInstance).mockResolvedValue({
    ...instance(),
    status: "deleted",
  });
  show();
  await screen.findByText("Researcher was deleted.");
  expect(
    screen.getByRole("link", { name: "Back to My assistants" }),
  ).toBeDefined();
  expect(screen.queryByRole("tab")).toBeNull();
  expect(api.getInstanceDefinition).not.toHaveBeenCalled();
});
test("a failed detail read does not expose diagnostics or inventory", async () => {
  rs.mocked(api.getInstance).mockRejectedValue(new Error("private detail"));
  show();
  await screen.findByText(
    "This AI assistant or AI employee is not available to you.",
  );
  expect(screen.queryByText("private detail")).toBeNull();
  expect(api.listInstances).not.toHaveBeenCalled();
});

test("a later failed read retains only the known employee return-list kind", async () => {
  rs.mocked(api.getInstance).mockResolvedValue({
    ...instance(),
    custody: "company",
    owner_id: null,
  });
  const view = show();
  await screen.findByText("Conversation history shown");
  rs.mocked(api.getInstance).mockRejectedValue(new Error("private"));
  await view.client.invalidateQueries({ queryKey: ["agent-instances"] });
  await screen.findByText(
    "This AI assistant or AI employee is not available to you.",
  );
  expect(
    screen.getByRole("link", { name: "Back to AI employees" }),
  ).toBeDefined();
  expect(screen.queryByText("Researcher")).toBeNull();
});

for (const [custody, label, href] of [
  ["personal", "My assistants", "/workspace/assistants"],
  ["company", "AI employees", "/workspace/employees"],
] as const) {
  test(`the ${custody} detail breadcrumb uses verified custody without another read`, async () => {
    rs.mocked(api.getInstance).mockResolvedValue({
      ...instance(),
      custody,
      owner_id: custody === "personal" ? "reader" : null,
    });
    show();
    await screen.findByText("Conversation history shown");
    expect(screen.getByRole("link", { name: label }).getAttribute("href")).toBe(
      href,
    );
    expect(api.getInstance).toHaveBeenCalledTimes(1);
  });
}

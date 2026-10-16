import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

rs.mock("next/navigation", () => ({ useRouter: () => ({ push: state.push }) }));
const state = rs.hoisted(() => ({
  held: false,
  allowed: 2,
  inUse: 0,
  push: rs.fn(),
  refresh: rs.fn(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "admin",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => ({
    known: true,
    enabled: true,
    execution: state.held ? "unavailable" : "ready",
    reason: state.held ? "operator-hold" : null,
    employees: { allowed: state.allowed, inUse: state.inUse },
  }),
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
  useStorage: () => null,
}));
rs.mock("@/core/people/hooks", () => ({
  usePeople: (ids: string[]) =>
    new Map(
      ids.map((id) => [
        id,
        {
          status: "ready",
          person: {
            id,
            name: id === "colleague" ? "Alex" : "New colleague",
            email: id + "@example.test",
            active: true,
          },
        },
      ]),
    ),
}));
rs.mock("@/components/workspace/people-picker", () => ({
  PeoplePicker: ({
    onChange,
    disabled,
  }: {
    onChange: (person: unknown) => void;
    disabled?: boolean;
  }) => (
    <button
      disabled={disabled}
      onClick={() =>
        onChange({
          id: "new",
          name: "New colleague",
          email: "new@example.test",
          active: true,
        })
      }
    >
      Choose new colleague
    </button>
  ),
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return {
    ...actual,
    createCompanyCopy: rs.fn(),
    renameInstance: rs.fn(),
    getInstanceDefinition: rs.fn(),
    getInstanceGrants: rs.fn(),
    changeInstanceLifecycle: rs.fn(),
  };
});

import { CompanyCopyAction } from "@/components/workspace/instances/company-copy";
import {
  LifecycleActionsProvider,
  LifecycleState,
} from "@/components/workspace/instances/lifecycle-actions";
import * as api from "@/core/agent-instances/api";
import { readCreation, clearCreation } from "@/core/agent-instances/unfinished";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

const agent: api.AgentInstanceDetail = {
  id: "a".repeat(32),
  name: "Researcher",
  principal: { kind: "nonhuman", subject_id: "ai" },
  custody: "personal",
  owner_id: "reader",
  creator_id: "reader",
  supervisor: { kind: "human", subject_id: "reader" },
  definition_revision: "revision",
  home_id: "b".repeat(32),
  status: "active",
  generation: 1,
  permissions: 7,
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 0,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
const clients: QueryClient[] = [];
function show(permissions = 7, status: api.AgentInstance["status"] = "active") {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <LifecycleActionsProvider
            instance={{ ...agent, permissions, status }}
            operations={[]}
            refresh={state.refresh}
          >
            <LifecycleState />
            <CompanyCopyAction disabled={false} />
          </LifecycleActionsProvider>
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  state.held = false;
  state.allowed = 2;
  state.inUse = 0;
  clearCreation("reader");
  rs.mocked(api.createCompanyCopy)
    .mockReset()
    .mockResolvedValue({ ...agent, id: "c".repeat(32), custody: "company" });
  state.push.mockReset();
  rs.mocked(api.renameInstance).mockReset().mockResolvedValue(agent);
  rs.mocked(api.getInstanceDefinition)
    .mockReset()
    .mockResolvedValue({
      revision: "revision",
      config: { name: "researcher", work_policy: { enabled: true } },
      soul: "Research carefully",
    });
  state.refresh.mockReset();
  rs.mocked(api.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({
      grants: [
        { user_id: "reader", permissions: 7 },
        { user_id: "colleague", permissions: 3 },
      ],
    });
  rs.mocked(api.changeInstanceLifecycle).mockReset().mockResolvedValue({
    instance: agent,
    complete: true,
    operation_id: "token",
  });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("copy preserves an exact remembered request across an unanswered response, without assistant allowances", async () => {
  rs.mocked(api.createCompanyCopy).mockRejectedValueOnce(
    new TypeError("private transport"),
  );
  show();
  fireEvent.click(
    screen.getByRole("button", { name: "Make an AI employee with this brief" }),
  );
  const dialog = within(screen.getByRole("dialog"));
  fireEvent.click(dialog.getByRole("button", { name: "Add AI employee" }));
  await dialog.findByText(
    "It is not confirmed that Researcher was added. Press Try again; a second one is not added.",
  );
  const captured = rs.mocked(api.createCompanyCopy).mock.calls[0]![1];
  expect(captured).toMatchObject({
    generation: 1,
    name: "Researcher",
    supervisor_id: "reader",
  });
  expect(readCreation("reader")).toEqual({
    body: captured,
    definition_name: null,
    copy_of: agent.id,
  });
  fireEvent.click(dialog.getByRole("button", { name: "Try again" }));
  await waitFor(() => expect(api.createCompanyCopy).toHaveBeenCalledTimes(2));
  expect(rs.mocked(api.createCompanyCopy).mock.calls[1]![1]).toEqual(captured);
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith(
      `/workspace/instances?instance=${"c".repeat(32)}&employeeAdded=1`,
    ),
  );
  expect(readCreation("reader")).toBeNull();
});
test.each([0, 2])(
  "company allowances disable copying when allowed=%s",
  async (allowed) => {
    state.allowed = allowed;
    state.inUse = 2;
    show();
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Make an AI employee with this brief",
      }).disabled,
    ).toBe(true);
    expect(api.createCompanyCopy).not.toHaveBeenCalled();
  },
);
test("a hold disables copying but leaves its configured explanation readable", () => {
  state.held = true;
  show();
  expect(
    screen.getByRole<HTMLButtonElement>("button", {
      name: "Make an AI employee with this brief",
    }).disabled,
  ).toBe(true);
  expect(
    screen.getByText("AI assistants and AI employees are paused by Provider."),
  ).toBeDefined();
});

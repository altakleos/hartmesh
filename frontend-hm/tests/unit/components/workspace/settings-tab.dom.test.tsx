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
    employees: { allowed: 2, inUse: 0 },
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
    renameInstance: rs.fn(),
    getInstanceDefinition: rs.fn(),
    getInstanceGrants: rs.fn(),
    changeInstanceLifecycle: rs.fn(),
  };
});

rs.mock("@/core/agents/api", () => {
  const actual = rs.requireActual<typeof definitions>("@/core/agents/api");
  return {
    ...actual,
    getAgent: rs.fn(),
    updateAgent: rs.fn(),
    createAgent: rs.fn(),
    listAgents: rs.fn(),
  };
});
import { DeletedAgentNavigation } from "@/components/workspace/instances/delete-agent-dialog";
import {
  LifecycleActionsProvider,
  LifecycleState,
} from "@/components/workspace/instances/lifecycle-actions";
import { SettingsTab } from "@/components/workspace/instances/settings-tab";
import * as api from "@/core/agent-instances/api";
import * as definitions from "@/core/agents/api";
import type { Agent as definitionsAgent } from "@/core/agents/types";
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
function show(
  permissions = 7,
  status: api.AgentInstance["status"] = "active",
  overrides: Partial<api.AgentInstanceDetail> = {},
) {
  const client = new QueryClient();
  clients.push(client);
  const view = render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <LifecycleActionsProvider
            instance={{ ...agent, permissions, status, ...overrides }}
            operations={[]}
            refresh={state.refresh}
          >
            <LifecycleState />
            <DeletedAgentNavigation />
            <SettingsTab />
          </LifecycleActionsProvider>
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
  return { ...view, client };
}
beforeEach(() => {
  state.held = false;
  rs.mocked(definitions.getAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as definitionsAgent);
  rs.mocked(definitions.updateAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as definitionsAgent);
  rs.mocked(definitions.createAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as definitionsAgent);
  rs.mocked(definitions.listAgents)
    .mockReset()
    .mockResolvedValue([{ name: "custom" } as definitionsAgent]);
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
test("rename validates the name and remains usable during a deployment hold", async () => {
  state.held = true;
  show();
  const field = screen.getByRole<HTMLInputElement>("textbox", { name: "Name" });
  fireEvent.change(field, { target: { value: " " } });
  fireEvent.click(screen.getByRole("button", { name: "Rename" }));
  expect(screen.getByText("Use a name of 1 to 128 characters.")).toBeDefined();
  expect(api.renameInstance).not.toHaveBeenCalled();
  fireEvent.change(field, { target: { value: "New name" } });
  fireEvent.click(screen.getByRole("button", { name: "Rename" }));
  await waitFor(() =>
    expect(api.renameInstance).toHaveBeenCalledWith(
      agent.id,
      1,
      "New name",
      expect.any(AbortSignal),
    ),
  );
  await screen.findByText("Renamed.");
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Pause" }).disabled,
  ).toBe(true);
});
test("pause asks for specific consent and sends one lifecycle action without idle negotiation", async () => {
  show();
  fireEvent.click(screen.getByRole("button", { name: "Pause" }));
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
  expect(
    screen.getByText(
      "This stops Researcher from working. Conversations, files and memory stay. You can resume Researcher at any time.",
    ),
  ).toBeDefined();
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Pause" }),
  );
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  const body = rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1];
  expect(body.action).toBe("suspend");
  expect(body).not.toHaveProperty("only_if_idle");
});
test("resume is available for paused agents without the shared confirmation checkbox", async () => {
  show(7, "suspended");
  fireEvent.click(screen.getByRole("button", { name: "Resume" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1].action).toBe(
    "restore",
  );
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(screen.queryByText("Confirm action")).toBeNull();
  expect(screen.queryByRole("button", { name: "Pause" })).toBeNull();
});
test("delete requires the exact agent name and redirects using the server's released-space answer", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockResolvedValue({
    ...{ instance: agent, complete: true, operation_id: "token" },
    space_released: false,
  });
  show();
  fireEvent.click(screen.getByRole("button", { name: "Delete" }));
  const modal = within(screen.getByRole("dialog"));
  const field = modal.getByRole<HTMLInputElement>("textbox", {
    name: "Type Researcher to confirm",
  });
  fireEvent.change(field, { target: { value: "researcher" } });
  expect(
    modal.getByRole<HTMLButtonElement>("button", { name: "Delete" }).disabled,
  ).toBe(true);
  fireEvent.change(field, { target: { value: "Researcher" } });
  fireEvent.click(modal.getByRole("button", { name: "Delete" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1].action).toBe(
    "delete",
  );
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith(
      "/workspace/assistants?deleted=Researcher&clearing=1",
    ),
  );
});
test("an unanswered delete stays in its dialog and retries the captured operation only", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new TypeError("private transport"),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Delete" }));
  fireEvent.change(screen.getByLabelText("Type Researcher to confirm"), {
    target: { value: "Researcher" },
  });
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }),
  );
  await within(screen.getByRole("dialog")).findByText(
    "It is not confirmed that the change was saved. Press Try again; nothing is done twice.",
  );
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Try again",
    }),
  );
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[1]![1]).toEqual(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  );
  expect(screen.queryByText("private transport")).toBeNull();
});
test("manage without inspect exposes no settings or definition reads", () => {
  show(4);
  expect(
    screen.getByText(
      "To manage Researcher you also need “Can see all work”. Ask another manager to give it to you.",
    ),
  ).toBeDefined();
  expect(api.getInstanceDefinition).not.toHaveBeenCalled();
  expect(screen.queryByRole("textbox")).toBeNull();
});

test("edited brief writes the owned source before adopting it with idle protection", async () => {
  show();
  const field = await screen.findByRole("textbox", { name: "Brief" });
  fireEvent.change(field, { target: { value: "New brief" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply updated brief" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(definitions.updateAgent).toHaveBeenCalledWith(
    "researcher",
    expect.objectContaining({
      soul: "New brief",
      work_policy: { enabled: true },
    }),
    expect.any(AbortSignal),
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  ).toMatchObject({
    action: "adopt",
    definition_name: "researcher",
    only_if_idle: true,
  });
  expect(definitions.createAgent).not.toHaveBeenCalled();
  await screen.findByText("The brief was applied.");
});
test("a known source-read refusal neither creates a source nor adopts an old one", async () => {
  rs.mocked(definitions.getAgent).mockRejectedValue(
    new definitions.AgentDefinitionError(403, "private source"),
  );
  show();
  fireEvent.change(await screen.findByRole("textbox", { name: "Brief" }), {
    target: { value: "New brief" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply updated brief" }));
  await screen.findByText("The brief could not be saved. Try again.");
  expect(definitions.createAgent).not.toHaveBeenCalled();
  expect(definitions.updateAgent).not.toHaveBeenCalled();
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
  expect(screen.queryByText("private source")).toBeNull();
});
test("custom adoption skips all definition writes", async () => {
  show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Apply one of your custom agents instead",
    }),
  );
  const select = await screen.findByLabelText("Custom agent");
  await waitFor(() =>
    expect(
      within(select).getByRole("option", { name: "custom" }),
    ).toBeDefined(),
  );
  fireEvent.change(select, { target: { value: "custom" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply updated brief" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  ).toMatchObject({
    action: "adopt",
    definition_name: "custom",
    only_if_idle: true,
  });
  expect(definitions.updateAgent).not.toHaveBeenCalled();
  expect(definitions.createAgent).not.toHaveBeenCalled();
});
test("leaving Settings during a source read retires every follow-up write and adoption", async () => {
  let finish!: (value: definitionsAgent) => void;
  rs.mocked(definitions.getAgent).mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  fireEvent.change(await screen.findByRole("textbox", { name: "Brief" }), {
    target: { value: "New brief" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply updated brief" }));
  await waitFor(() => expect(definitions.getAgent).toHaveBeenCalled());
  view.unmount();
  finish({ name: "researcher" } as definitionsAgent);
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(definitions.updateAgent).not.toHaveBeenCalled();
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
});
test("unverified files permit Delete and the final server answer does not claim an automatically reusable place", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockResolvedValue({
    instance: agent,
    complete: true,
    operation_id: "token",
    space_released: false,
  });
  show(7, "active", {
    execution: { execution: "unavailable", reason: "space-unverified" },
  });
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Pause" }).disabled,
  ).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Delete" }));
  expect(
    screen.getByText(
      "Researcher's files could not be checked, so they cannot be removed now; Provider can remove them later. Researcher will be deleted and will no longer count toward the number allowed.",
    ),
  ).toBeDefined();
  fireEvent.change(screen.getByLabelText("Type Researcher to confirm"), {
    target: { value: "Researcher" },
  });
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }),
  );
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith(
      "/workspace/assistants?deleted=Researcher",
    ),
  );
});

test("retiring a held brief editor releases only its busy state and prevents late adoption", async () => {
  let finish!: (value: definitionsAgent) => void;
  rs.mocked(definitions.getAgent).mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  fireEvent.change(await screen.findByRole("textbox", { name: "Brief" }), {
    target: { value: "New brief" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply updated brief" }));
  await waitFor(() => expect(definitions.getAgent).toHaveBeenCalled());
  rs.mocked(api.getInstanceDefinition).mockResolvedValue({
    revision: "changed-revision",
    config: { name: "researcher", work_policy: { enabled: true } },
    soul: "Current brief",
  });
  await view.client.invalidateQueries({ queryKey: ["agent-instances"] });
  await screen.findByDisplayValue("Current brief");
  await waitFor(() =>
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Rename" })
        .disabled,
    ).toBe(false),
  );
  finish({ name: "researcher" } as definitionsAgent);
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
});
test("a successful retry of Pause closes that exact confirmation", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new TypeError("network"),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Pause" }));
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Pause" }),
  );
  await within(screen.getByRole("dialog")).findByText(
    "It is not confirmed that the change was saved. Press Try again; nothing is done twice.",
  );
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Try again",
    }),
  );
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[1]![1]).toEqual(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  );
});

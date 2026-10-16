import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const mocks = rs.hoisted(() => ({
  push: rs.fn(),
  close: rs.fn(),
  state: { known: true, enabled: true, execution: "ready", reason: null },
  user: {
    id: "reader",
    email: "reader@example.test",
    system_role: "admin",
    permissions: ["agents:read", "agents:write"],
  },
}));
rs.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push }) }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: mocks.user }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => mocks.state,
  useStorage: () => null,
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return {
    ...actual,
    listInstanceRoles: rs.fn(),
    createInstance: rs.fn(),
    createInstanceConversation: rs.fn(),
  };
});
rs.mock("@/core/agents/api", { spy: true });
rs.mock("@/components/workspace/people-picker", () => ({
  PeoplePicker: ({ onChange }: { onChange: (person: unknown) => void }) => (
    <button
      onClick={() =>
        onChange({
          id: "colleague",
          email: "colleague@example.test",
          name: "Colleague",
          active: true,
        })
      }
    >
      Choose colleague
    </button>
  ),
}));

import {
  CreateAssistant,
  CreateEmployee,
} from "@/components/workspace/instances/create-agent";
import * as api from "@/core/agent-instances/api";
import { readCreation } from "@/core/agent-instances/unfinished";
import * as definitions from "@/core/agents/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

const clients: QueryClient[] = [];
const id = "a".repeat(32);
const instance = {
  id,
  name: "Helper",
  custody: "personal",
  owner_id: "reader",
  generation: 1,
  permissions: 7,
  status: "active",
} as api.AgentInstance;
function show(
  employee = false,
  extra: { ownName?: string; unfinishedName?: string } = {},
) {
  const client = new QueryClient();
  clients.push(client);
  const tree = () => (
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          {employee ? (
            <CreateEmployee open onOpenChange={mocks.close} {...extra} />
          ) : (
            <CreateAssistant open onOpenChange={mocks.close} {...extra} />
          )}
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>
  );
  const view = render(tree());
  return { ...view, updateIdentity: () => view.rerender(tree()) };
}
function fill() {
  fireEvent.change(screen.getByLabelText("Name"), {
    target: { value: "Helper" },
  });
  fireEvent.change(screen.getByRole("textbox", { name: /responsible for/ }), {
    target: { value: "Keep useful notes\nAnd ask when needed" },
  });
}
beforeEach(() => {
  localStorage.clear();
  mocks.user.id = "reader";
  mocks.user.permissions = ["agents:read", "agents:write"];
  mocks.push.mockReset();
  mocks.close.mockReset();
  mocks.state.execution = "ready";
  mocks.state.known = true;
  mocks.state.enabled = true;
  rs.mocked(api.listInstanceRoles).mockReset().mockResolvedValue({ roles: [] });
  rs.mocked(api.createInstance).mockReset().mockResolvedValue(instance);
  rs.mocked(api.createInstanceConversation)
    .mockReset()
    .mockResolvedValue({ thread_id: "thread-one" });
  rs.mocked(definitions.createAgent)
    .mockReset()
    .mockImplementation(async (body) => ({
      ...body,
      description: body.description ?? "",
      model: null,
      skills: null,
      tool_groups: null,
    }));
  rs.mocked(definitions.deleteAgent).mockReset().mockResolvedValue();
  rs.mocked(definitions.listAgents)
    .mockReset()
    .mockResolvedValue([
      {
        name: "custom",
        description: "Mine",
        model: null,
        skills: null,
        tool_groups: null,
      },
    ]);
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});

test("creates an AI assistant from a brief with a usable default work policy and opens its unsent welcome", async () => {
  show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await waitFor(() =>
    expect(mocks.push).toHaveBeenCalledWith(
      "/workspace/chats/thread-one?welcome=1",
    ),
  );
  const definition = rs.mocked(definitions.createAgent).mock.calls[0]![0];
  expect(definition.name).toMatch(/^agent-[0-9a-f]{8}$/);
  expect(definition.description).toBe("Keep useful notes");
  expect(definition.soul).toBe("Keep useful notes\nAnd ask when needed");
  expect(definition).not.toHaveProperty("skills");
  expect(definition.work_policy).toEqual({
    enabled: true,
    default_priority: "normal",
    review_required: true,
    allow_derived: false,
    max_derived_per_activation: 3,
    responsibilities: [],
  });
  expect(rs.mocked(api.createInstance).mock.calls[0]![0]).toEqual({
    creation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
    name: "Helper",
    definition_name: definition.name,
    custody: "personal",
  });
  expect(readCreation("reader")).toBeNull();
  expect(definitions.deleteAgent).not.toHaveBeenCalled();
});
test("selects a supervisor by person, includes self by default, and only employees have the second step", async () => {
  show(true);
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(screen.getByText("Who is responsible for Helper")).toBeDefined();
  fireEvent.click(screen.getByRole("button", { name: "Choose colleague" }));
  fireEvent.click(screen.getByRole("button", { name: "Add AI employee" }));
  await waitFor(() => expect(api.createInstance).toHaveBeenCalled());
  expect(rs.mocked(api.createInstance).mock.calls[0]![0]).toMatchObject({
    custody: "company",
    supervisor_id: "colleague",
  });
});
test("keeps the exact unanswered request and resends it without a second definition", async () => {
  rs.mocked(api.createInstance).mockRejectedValueOnce(
    new TypeError("private transport detail"),
  );
  show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await screen.findByText(
    "It is not confirmed that Helper was added. Press Try again; a second one is not added.",
  );
  const first = rs.mocked(api.createInstance).mock.calls[0]![0];
  expect(readCreation("reader")?.body).toEqual(first);
  expect(screen.queryByText("private transport detail")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await waitFor(() => expect(mocks.push).toHaveBeenCalled());
  expect(rs.mocked(api.createInstance).mock.calls[1]![0]).toEqual(first);
  expect(definitions.createAgent).toHaveBeenCalledTimes(1);
});
test("clears a refused request and deletes only the definition made by the dialog", async () => {
  rs.mocked(api.createInstance).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private refusal", "another-refusal"),
  );
  show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await screen.findByText("Helper could not be added. Try again.");
  expect(readCreation("reader")).toBeNull();
  expect(definitions.deleteAgent).toHaveBeenCalledTimes(1);
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await waitFor(() => expect(mocks.push).toHaveBeenCalled());
  expect(definitions.createAgent).toHaveBeenCalledTimes(2);
  expect(rs.mocked(api.createInstance).mock.calls[1]![0].creation_id).not.toBe(
    rs.mocked(api.createInstance).mock.calls[0]![0].creation_id,
  );
});
test("never creates or removes a selected custom definition", async () => {
  rs.mocked(api.createInstance).mockRejectedValue(
    new api.InstanceApiError(409, "private refusal", "another-refusal"),
  );
  show();
  fireEvent.change(screen.getByLabelText("Name"), {
    target: { value: "Helper" },
  });
  fireEvent.click(
    screen.getByRole("button", { name: "Use one of your custom agents" }),
  );
  await screen.findByRole("option", { name: "custom" });
  fireEvent.change(screen.getByLabelText("Custom agent"), {
    target: { value: "custom" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await screen.findByText("Helper could not be added. Try again.");
  expect(definitions.createAgent).not.toHaveBeenCalled();
  expect(definitions.deleteAgent).not.toHaveBeenCalled();
});
test("opens the created agent with a clear notice if its first conversation fails", async () => {
  rs.mocked(api.createInstanceConversation).mockRejectedValue(
    new Error("private diagnostic"),
  );
  show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await waitFor(() =>
    expect(mocks.push).toHaveBeenCalledWith(
      `/workspace/instances?instance=${id}&added=1`,
    ),
  );
  expect(definitions.deleteAgent).not.toHaveBeenCalled();
});
test("does not continue from a late definition after unmount", async () => {
  let resolve!: (
    value: Awaited<ReturnType<typeof definitions.createAgent>>,
  ) => void;
  rs.mocked(definitions.createAgent).mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const view = show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await waitFor(() => expect(definitions.createAgent).toHaveBeenCalled());
  view.unmount();
  resolve({
    name: "late",
    description: "",
    model: null,
    skills: null,
    tool_groups: null,
  });
  await new Promise((done) => setTimeout(done, 10));
  expect(api.createInstance).not.toHaveBeenCalled();
  expect(definitions.deleteAgent).not.toHaveBeenCalled();
  expect(mocks.push).not.toHaveBeenCalled();
});
test("cancel after an unanswered request clears its record and its owned definition", async () => {
  rs.mocked(api.createInstance).mockRejectedValue(new TypeError("offline"));
  show();
  fill();
  fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
  await screen.findByText(
    "It is not confirmed that Helper was added. Press Try again; a second one is not added.",
  );
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  await waitFor(() => expect(mocks.close).toHaveBeenCalledWith(false));
  expect(readCreation("reader")).toBeNull();
  expect(definitions.deleteAgent).toHaveBeenCalledTimes(1);
});

test.each(["account", "permission"])(
  "retires a held definition on %s change",
  async (change) => {
    let resolve!: (
      value: Awaited<ReturnType<typeof definitions.createAgent>>,
    ) => void;
    rs.mocked(definitions.createAgent).mockImplementation(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const view = show();
    fill();
    fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
    await waitFor(() => expect(definitions.createAgent).toHaveBeenCalled());
    if (change === "account") mocks.user.id = "other";
    else mocks.user.permissions = ["agents:read"];
    view.updateIdentity();
    resolve({
      name: "late",
      description: "",
      model: null,
      skills: null,
      tool_groups: null,
    });
    await new Promise((done) => setTimeout(done, 10));
    expect(api.createInstance).not.toHaveBeenCalled();
    expect(definitions.deleteAgent).not.toHaveBeenCalled();
    expect(mocks.push).not.toHaveBeenCalled();
  },
);
test.each(["account", "permission"])(
  "retires a held creation on %s change",
  async (change) => {
    let resolve!: (value: api.AgentInstance) => void;
    rs.mocked(api.createInstance).mockImplementation(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const view = show();
    fill();
    fireEvent.click(screen.getByRole("button", { name: "Add AI assistant" }));
    await waitFor(() => expect(api.createInstance).toHaveBeenCalled());
    if (change === "account") mocks.user.id = "other";
    else mocks.user.permissions = ["agents:read"];
    view.updateIdentity();
    resolve(instance);
    await new Promise((done) => setTimeout(done, 10));
    expect(api.createInstanceConversation).not.toHaveBeenCalled();
    expect(definitions.deleteAgent).not.toHaveBeenCalled();
    expect(mocks.push).not.toHaveBeenCalled();
    expect(readCreation("reader")).not.toBeNull();
    expect(readCreation("other")).toBeNull();
  },
);
test.each([
  {
    employee: false,
    status: 409,
    reason: "ai-assistant-limit",
    extra: { per_user: 0 },
    expected: "AI assistants are not offered here.",
  },
  {
    employee: false,
    status: 409,
    reason: "ai-assistant-limit",
    extra: { per_user: 1, mine: 1 },
    expected:
      "You already have an AI assistant, Existing, and each person can have one. Delete Existing on Existing's Settings tab to add another, or ask an administrator for an AI employee.",
  },
  {
    employee: false,
    status: 409,
    reason: "ai-assistant-limit",
    extra: { per_user: 1, mine: 1 },
    unfinished: true,
    expected:
      "Setting up Pending did not finish, and Pending counts as one of your AI assistants. Finish or remove Pending under “Not finished” first.",
  },
  {
    employee: false,
    status: 409,
    reason: "ai-assistant-none-free",
    extra: {},
    expected:
      "No AI assistant can be added right now: all the AI assistants allowed here are in use. Ask your service provider to allow more.",
  },
  {
    employee: true,
    status: 409,
    reason: "ai-employee-limit",
    extra: { allowed: 0 },
    expected: "AI employees are not offered here.",
  },
  {
    employee: true,
    status: 409,
    reason: "ai-employee-limit",
    extra: { allowed: 2 },
    expected:
      "All 2 AI employees allowed here are taken. Delete one that is no longer needed, or ask your service provider to allow more.",
  },
  {
    employee: true,
    status: 403,
    reason: "permission-denied",
    extra: {},
    expected: "Only an administrator can add an AI employee.",
  },
  {
    employee: false,
    status: 409,
    reason: "execution-unavailable",
    extra: {},
    executionReason: "operator-hold",
    expected:
      "AI assistants and AI employees are paused by your service provider.",
  },
])("localizes the $reason refusal without server prose", async (row) => {
  rs.mocked(api.createInstance).mockRejectedValue(
    new api.InstanceApiError(
      row.status,
      "private diagnostic",
      row.reason,
      row.executionReason,
      row.extra,
    ),
  );
  show(
    row.employee,
    row.unfinished ? { unfinishedName: "Pending" } : { ownName: "Existing" },
  );
  fill();
  if (row.employee)
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
  fireEvent.click(
    screen.getByRole("button", {
      name: row.employee ? "Add AI employee" : "Add AI assistant",
    }),
  );
  await screen.findByText(row.expected);
  expect(screen.queryByText("private diagnostic")).toBeNull();
  expect(definitions.deleteAgent).toHaveBeenCalledTimes(1);
  expect(readCreation("reader")).toBeNull();
  expect(screen.queryByRole("button", { name: "Try again" })).toBeNull();
});

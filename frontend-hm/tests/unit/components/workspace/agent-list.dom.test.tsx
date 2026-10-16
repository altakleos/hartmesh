import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  user: {
    id: "reader",
    email: "reader@example.test",
    system_role: "admin",
    permissions: ["agents:read", "agents:write"],
  },
  feature: {
    known: true,
    enabled: true,
    isLoading: false,
    execution: "ready",
    reason: null as string | null,
    assistants: { perUser: 1, allowed: 3, inUse: 0, mine: 0 },
    employees: { allowed: 2, inUse: 0 },
  },
  push: rs.fn(),
}));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: state.push }),
  useSearchParams: () => new URLSearchParams(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => state.feature,
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
  useStorage: () => null,
  useDocumentTitle: () => undefined,
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return {
    ...actual,
    listInstanceRoles: rs.fn(),
    listInstances: rs.fn(),
    removeUnfinished: rs.fn(),
    createInstance: rs.fn(),
    createCompanyCopy: rs.fn(),
    createInstanceConversation: rs.fn(),
  };
});
rs.mock("@/core/agents/api", { spy: true });
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: (id: string) => (id === "reader" ? "You" : "Colleague"),
  PersonName: ({ id }: { id: string }) => (
    <span>{id === "reader" ? "You" : "Colleague"}</span>
  ),
}));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));
import { AgentList } from "@/components/workspace/instances/agent-list";
import * as api from "@/core/agent-instances/api";
import {
  rememberCreation,
  readCreation,
} from "@/core/agent-instances/unfinished";
import { createAgent, deleteAgent } from "@/core/agents/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
const clients: QueryClient[] = [];
const personal = {
  id: "a".repeat(32),
  name: "Researcher",
  custody: "personal",
  owner_id: "reader",
  supervisor: { kind: "human", subject_id: "reader" },
  status: "active",
  generation: 1,
  permissions: 7,
} as api.AgentInstance;
const unfinished = {
  id: "c".repeat(32),
  name: "Not ready",
  custody: "personal" as const,
  created_at: "2026-10-15T00:00:00Z",
};
function show(kind: api.Custody = "personal") {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <AgentList kind={kind} />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
function page(
  instances: api.AgentListEntry[] = [],
  unfinishedRows: api.UnfinishedCreation[] = [],
): api.InstanceList {
  return {
    instances,
    ai_assistants: {
      per_user: 1,
      allowed: 3,
      in_use: instances.length,
      mine:
        unfinishedRows.length +
        instances.filter(
          (row) => api.isFullInstance(row) && row.owner_id === "reader",
        ).length,
    },
    unfinished: unfinishedRows,
  };
}
beforeEach(() => {
  localStorage.clear();
  state.user.system_role = "admin";
  state.user.permissions = ["agents:read", "agents:write"];
  Object.assign(state.feature, {
    known: true,
    enabled: true,
    isLoading: false,
    execution: "ready",
    reason: null,
    assistants: { perUser: 1, allowed: 3, inUse: 0, mine: 0 },
    employees: { allowed: 2, inUse: 0 },
  });
  state.push.mockReset();
  rs.mocked(api.listInstanceRoles).mockReset().mockResolvedValue({ roles: [] });
  rs.mocked(api.listInstances).mockReset().mockResolvedValue(page());
  rs.mocked(api.removeUnfinished).mockReset().mockResolvedValue();
  rs.mocked(deleteAgent).mockReset().mockResolvedValue();
  rs.mocked(api.createInstance).mockReset().mockResolvedValue(personal);
  rs.mocked(createAgent)
    .mockReset()
    .mockImplementation(async (body) => ({
      ...body,
      description: body.description ?? "",
      model: null,
      skills: null,
      tool_groups: null,
    }));
  rs.mocked(api.createInstanceConversation)
    .mockReset()
    .mockResolvedValue({ thread_id: "first" });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("names the assistant at the per-person limit and keeps its detail link", async () => {
  rs.mocked(api.listInstances).mockResolvedValue(page([personal]));
  state.feature.assistants.mine = 1;
  show();
  await screen.findByRole("link", { name: "Researcher" });
  expect(
    screen.getByText(
      "Researcher is your AI assistant. Each person can have 1.",
    ),
  ).toBeDefined();
  expect(
    screen.getByText(
      "You already have an AI assistant, Researcher, and each person can have one. Delete Researcher on Researcher's Settings tab to add another, or ask an administrator for an AI employee.",
    ),
  ).toBeDefined();
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Add AI assistant" })
      .disabled,
  ).toBe(true);
  expect(rs.mocked(api.listInstances).mock.calls[0]?.slice(0, 2)).toEqual([
    "personal",
    0,
  ]);
});
test("company inventory summaries never become a detail link or a grant-bearing card", async () => {
  rs.mocked(api.listInstances).mockResolvedValue(
    page([
      {
        id: "b".repeat(32),
        name: "Company researcher",
        supervisor_name: "Manager",
        status: "active",
      },
    ]),
  );
  show("company");
  await screen.findByText("Company researcher");
  expect(screen.queryByRole("link", { name: "Company researcher" })).toBeNull();
  expect(screen.getByText("supervised by Manager")).toBeDefined();
});
test("keeps rows readable during an execution hold without suggesting an available allowance", async () => {
  state.feature.execution = "unavailable";
  state.feature.reason = "operator-hold";
  rs.mocked(api.listInstances).mockResolvedValue(page([personal]));
  show();
  await screen.findByRole("link", { name: "Researcher" });
  expect(
    screen.queryByText(
      "Researcher is your AI assistant. Each person can have 1.",
    ),
  ).toBeNull();
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Add AI assistant" })
      .disabled,
  ).toBe(true);
  expect(
    screen.getAllByText(
      "AI assistants and AI employees are paused by Provider.",
    ).length,
  ).toBeGreaterThan(0);
});
test("does not request disabled instance routes", () => {
  state.feature.enabled = false;
  show();
  expect(api.listInstances).not.toHaveBeenCalled();
  expect(
    screen.getByText(
      "AI assistants and AI employees are not available right now.",
    ),
  ).toBeDefined();
});
test("shows the administrator instruction to a non-administrator on the company list", async () => {
  state.user.system_role = "user";
  show("company");
  await screen.findByText("No AI employees yet.");
  expect(screen.getByText("Administrators add AI employees.")).toBeDefined();
  expect(screen.queryByRole("button", { name: "Add AI employee" })).toBeNull();
});
test("uses the unfinished name at the limit and removes it with its remembered owned definition", async () => {
  rs.mocked(api.listInstances).mockResolvedValue(page([], [unfinished]));
  state.feature.assistants.mine = 1;
  rememberCreation("reader", {
    body: {
      creation_id: "d".repeat(32),
      name: unfinished.name,
      custody: "personal",
      definition_name: "owned",
    },
    definition_name: "owned",
    copy_of: null,
  });
  show();
  await screen.findByText("Setting up Not ready did not finish.");
  expect(screen.getByText(/counts as one of your AI assistants/)).toBeDefined();
  fireEvent.click(screen.getByRole("button", { name: "Remove" }));
  await waitFor(() => expect(api.removeUnfinished).toHaveBeenCalled());
  await waitFor(() =>
    expect(deleteAgent).toHaveBeenCalledWith("owned", expect.any(AbortSignal)),
  );
  expect(readCreation("reader")).toBeNull();
});
test("resumes an unfinished request unchanged then opens its first welcome conversation", async () => {
  const body = {
    creation_id: "d".repeat(32),
    name: unfinished.name,
    custody: "personal" as const,
    definition_name: "owned",
  };
  rememberCreation("reader", { body, definition_name: "owned", copy_of: null });
  rs.mocked(api.listInstances).mockResolvedValue(page([], [unfinished]));
  show();
  fireEvent.click(
    await screen.findByRole("button", { name: "Finish setting up" }),
  );
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith("/workspace/chats/first?welcome=1"),
  );
  expect(api.createInstance).toHaveBeenCalledWith(
    body,
    expect.any(AbortSignal),
  );
  expect(deleteAgent).not.toHaveBeenCalled();
});
test("a concurrent completed setup produces a named message and refreshes instead of claiming removal", async () => {
  rs.mocked(api.listInstances).mockResolvedValue(page([], [unfinished]));
  rs.mocked(api.removeUnfinished).mockRejectedValue(
    new api.InstanceApiError(409, "private", "not-removable"),
  );
  show();
  fireEvent.click(await screen.findByRole("button", { name: "Remove" }));
  await screen.findByText(
    "Not ready has been set up in the meantime, so nothing was removed.",
  );
  expect(api.listInstances).toHaveBeenCalledTimes(2);
  expect(deleteAgent).not.toHaveBeenCalled();
});

test("keeps the creation dialog alive when the post-creation refresh reaches the limit", async () => {
  rs.mocked(api.listInstances)
    .mockResolvedValueOnce(page())
    .mockResolvedValue(page([personal]));
  let resolve!: (value: { thread_id: string }) => void;
  rs.mocked(api.createInstanceConversation).mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  show();
  await waitFor(() =>
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Add AI assistant",
      }).disabled,
    ).toBe(false),
  );
  fireEvent.click(
    await screen.findByRole("button", { name: "Add AI assistant" }),
  );
  fireEvent.change(screen.getByLabelText("Name"), {
    target: { value: "Researcher" },
  });
  fireEvent.change(screen.getByRole("textbox", { name: /responsible for/ }), {
    target: { value: "Research carefully" },
  });
  fireEvent.click(
    screen.getByRole("dialog").querySelector('button[data-variant="default"]')!,
  );
  await waitFor(() =>
    expect(api.createInstanceConversation).toHaveBeenCalled(),
  );
  await waitFor(() => expect(api.listInstances).toHaveBeenCalledTimes(2));
  expect(
    rs.mocked(api.createInstanceConversation).mock.calls[0]![2]?.aborted,
  ).toBe(false);
  resolve({ thread_id: "created" });
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith(
      "/workspace/chats/created?welcome=1",
    ),
  );
});

test.each(["personal", "company", "copy"] as const)(
  "visiting the other custody preserves an unanswered %s creation",
  async (kind) => {
    const remembered =
      kind === "copy"
        ? {
            body: {
              creation_id: "d".repeat(32),
              generation: 1,
              name: "Pending",
            },
            definition_name: null,
            copy_of: "e".repeat(32),
          }
        : {
            body: {
              creation_id: "d".repeat(32),
              name: "Pending",
              custody: kind,
              definition_name: "owned",
            },
            definition_name: "owned",
            copy_of: null,
          };
    rememberCreation("reader", remembered);
    show(kind === "personal" ? "company" : "personal");
    await screen.findByText(
      kind === "personal" ? "No AI employees yet." : "No AI assistants yet.",
    );
    expect(readCreation("reader")).toEqual(remembered);
  },
);
test("an unanswered unfinished resume keeps the exact body and shows its named retry guidance", async () => {
  const body = {
    creation_id: "d".repeat(32),
    name: unfinished.name,
    custody: "personal" as const,
    definition_name: "owned",
  };
  rememberCreation("reader", { body, definition_name: "owned", copy_of: null });
  rs.mocked(api.listInstances).mockResolvedValue(page([], [unfinished]));
  rs.mocked(api.createInstance).mockRejectedValue(new TypeError("private"));
  show();
  fireEvent.click(
    await screen.findByRole("button", { name: "Finish setting up" }),
  );
  await screen.findByText(
    "It is not confirmed that Not ready was added. Press Try again; a second one is not added.",
  );
  expect(readCreation("reader")?.body).toEqual(body);
  expect(deleteAgent).not.toHaveBeenCalled();
});
test.each(["ai-assistant-limit", "execution-unavailable"])(
  "a refused unfinished resume clears its owned request and refreshes for %s",
  async (reason) => {
    rememberCreation("reader", {
      body: {
        creation_id: "d".repeat(32),
        name: unfinished.name,
        custody: "personal",
        definition_name: "owned",
      },
      definition_name: "owned",
      copy_of: null,
    });
    rs.mocked(api.listInstances).mockResolvedValue(page([], [unfinished]));
    rs.mocked(api.createInstance).mockRejectedValue(
      new api.InstanceApiError(
        409,
        "private refusal",
        reason,
        "operator-hold",
        { per_user: 0 },
      ),
    );
    show();
    fireEvent.click(
      await screen.findByRole("button", { name: "Finish setting up" }),
    );
    await screen.findByText(
      reason === "ai-assistant-limit"
        ? "AI assistants are not offered here."
        : "AI assistants and AI employees are paused by Provider.",
    );
    expect(readCreation("reader")).toBeNull();
    expect(deleteAgent).toHaveBeenCalledTimes(1);
    expect(api.listInstances).toHaveBeenCalledTimes(2);
    expect(screen.queryByText("private refusal")).toBeNull();
  },
);

test.each(["network", "refused"] as const)(
  "company-copy resumption handles %s without deleting the source definition",
  async (failure) => {
    const entry = { ...unfinished, custody: "company" as const };
    const body = {
      creation_id: "d".repeat(32),
      generation: 3,
      name: entry.name,
    };
    const source = "e".repeat(32);
    rememberCreation("reader", {
      body,
      definition_name: null,
      copy_of: source,
    });
    rs.mocked(api.listInstances).mockResolvedValue(page([], [entry]));
    rs.mocked(api.createCompanyCopy)
      .mockReset()
      .mockRejectedValue(
        failure === "network"
          ? new TypeError("offline")
          : new api.InstanceApiError(403, "private"),
      );
    show("company");
    fireEvent.click(
      await screen.findByRole("button", { name: "Finish setting up" }),
    );
    await screen.findByText(
      failure === "network"
        ? "It is not confirmed that Not ready was added. Press Try again; a second one is not added."
        : "Only an administrator can add an AI employee.",
    );
    expect(api.createCompanyCopy).toHaveBeenCalledWith(
      source,
      body,
      expect.any(AbortSignal),
    );
    expect(deleteAgent).not.toHaveBeenCalled();
    expect(readCreation("reader") !== null).toBe(failure === "network");
  },
);

test.each([
  { kind: "personal", case: "loading", show: true, disabled: true, text: null },
  {
    kind: "personal",
    case: "unknown",
    show: true,
    disabled: true,
    text: "AI assistants and AI employees are not available right now.",
  },
  {
    kind: "personal",
    case: "zero",
    show: true,
    disabled: true,
    text: "AI assistants are not offered here.",
  },
  {
    kind: "personal",
    case: "permission",
    show: false,
    disabled: true,
    text: "You do not have permission to add an AI assistant. Ask an administrator.",
  },
  {
    kind: "personal",
    case: "full",
    show: true,
    disabled: true,
    text: "No AI assistant can be added right now: all the AI assistants allowed here are in use. Ask Provider to allow more.",
  },
  {
    kind: "personal",
    case: "available",
    show: true,
    disabled: false,
    text: "You can add 1 AI assistant.",
  },
  { kind: "company", case: "loading", show: true, disabled: true, text: null },
  {
    kind: "company",
    case: "unknown",
    show: true,
    disabled: true,
    text: "AI assistants and AI employees are not available right now.",
  },
  {
    kind: "company",
    case: "zero",
    show: true,
    disabled: true,
    text: "AI employees are not offered here.",
  },
  {
    kind: "company",
    case: "permission",
    show: false,
    disabled: true,
    text: null,
  },
  {
    kind: "company",
    case: "full",
    show: true,
    disabled: true,
    text: "All 2 AI employees allowed here are taken. Delete one that is no longer needed, or ask Provider to allow more.",
  },
  {
    kind: "company",
    case: "available",
    show: true,
    disabled: false,
    text: "0 of 2 AI employees",
  },
] as const)("the $kind Add priority table handles $case", async (row) => {
  if (row.case === "loading") {
    state.feature.known = false;
    state.feature.isLoading = true;
  }
  if (row.case === "unknown") state.feature.known = false;
  if (row.case === "zero") {
    if (row.kind === "personal") state.feature.assistants.perUser = 0;
    else state.feature.employees.allowed = 0;
  }
  if (row.case === "permission") state.user.permissions = ["agents:read"];
  if (row.case === "full") {
    if (row.kind === "personal") state.feature.assistants.inUse = 3;
    else state.feature.employees.inUse = 2;
  }
  show(row.kind);
  if (state.feature.known)
    await screen.findByText(
      row.kind === "personal"
        ? "No AI assistants yet."
        : "No AI employees yet.",
    );
  const button = screen.queryByRole<HTMLButtonElement>("button", {
    name: row.kind === "personal" ? "Add AI assistant" : "Add AI employee",
  });
  expect(button !== null).toBe(row.show);
  if (button) expect(button.disabled).toBe(row.disabled);
  if (row.text) expect(screen.getByText(row.text)).toBeDefined();
  if (row.case === "loading")
    expect(
      screen.queryByText(
        "AI assistants and AI employees are not available right now.",
      ),
    ).toBeNull();
});

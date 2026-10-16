import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  feature: {
    known: true,
    enabled: true,
    execution: "ready",
    reason: null as string | null,
  },
  push: rs.fn(),
}));
rs.mock("next/navigation", () => ({ useRouter: () => ({ push: state.push }) }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["threads:write"] },
  }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => state.feature,
  useStorage: () => null,
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: (id: string) => (id === "reader" ? "you" : "Colleague"),
  useSupervisorName: () => "a manager",
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return { ...actual, createInstanceConversation: rs.fn() };
});
import { AgentHeader } from "@/components/workspace/instances/agent-header";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
function instance(): api.AgentInstanceDetail {
  return {
    id: "a".repeat(32),
    name: "Researcher",
    principal: { kind: "nonhuman", subject_id: "agent" },
    custody: "personal",
    owner_id: "reader",
    creator_id: "reader",
    supervisor: { kind: "human", subject_id: "reader" },
    definition_revision: "d".repeat(64),
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
}
function show(agent = instance(), pending = false) {
  return render(
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <AgentHeader instance={agent} lifecyclePending={pending} />
      </FileActionLifetimeProvider>
    </I18nProvider>,
  );
}
beforeEach(() => {
  state.feature = {
    known: true,
    enabled: true,
    execution: "ready",
    reason: null,
  };
  state.push.mockReset();
  rs.mocked(api.createInstanceConversation)
    .mockReset()
    .mockResolvedValue({ thread_id: "new" });
});
afterEach(cleanup);
test("shows a named AI identity, readable access levels and idle state", () => {
  show();
  expect(screen.getByText("Your AI assistant")).toBeDefined();
  expect(screen.getByText(/Idle.*not used yet/)).toBeDefined();
  expect(
    screen.getByText(/Can chat, Can see all work, Can manage/),
  ).toBeDefined();
  expect(screen.getByRole("img", { name: "AI assistant" })).toBeDefined();
});
test.each(["starting", "absent"] as const)(
  "workspace %s does not disable a new conversation",
  async (workspace) => {
    const agent = instance();
    agent.home!.workspace = workspace;
    show(agent);
    fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
    await waitFor(() =>
      expect(state.push).toHaveBeenCalledWith("/workspace/chats/new"),
    );
  },
);
test.each([
  { status: "provisioning", pending: false, expected: "Not finished" },
  { status: "deleted", pending: false, expected: "Deleted" },
  { status: "archived", pending: true, expected: "Retired" },
  { status: "suspended", pending: true, expected: "Change in progress" },
  { status: "suspended", pending: false, expected: "Paused" },
] as const)("uses status priority for $status / $pending", (row) => {
  const agent = instance();
  agent.status = row.status;
  show(agent, row.pending);
  expect(screen.getByText(row.expected)).toBeDefined();
});
test("deployment holds take priority over local verification and busy files", () => {
  const agent = instance();
  agent.execution = { execution: "unavailable", reason: "space-unverified" };
  agent.home!.state = "busy";
  state.feature.execution = "unavailable";
  state.feature.reason = "operator-hold";
  show(agent);
  expect(screen.getByText("Cannot work right now")).toBeDefined();
  expect(
    screen.getAllByText(
      "AI assistants and AI employees are paused by Provider.",
    ).length,
  ).toBeGreaterThan(0);
  expect(screen.queryByText(/files did not pass/)).toBeNull();
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "New conversation" })
      .disabled,
  ).toBe(true);
});
test("shows working status from the server running-run count", () => {
  const agent = instance();
  agent.activity.running_runs = 1;
  show(agent);
  expect(screen.getByText("Working now")).toBeDefined();
});
test("keeps the same conversation identity for an unanswered retry", async () => {
  rs.mocked(api.createInstanceConversation).mockRejectedValueOnce(
    new TypeError("offline"),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
  await screen.findByRole("alert");
  fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
  await waitFor(() => expect(state.push).toHaveBeenCalled());
  expect(rs.mocked(api.createInstanceConversation).mock.calls[0]![1]).toBe(
    rs.mocked(api.createInstanceConversation).mock.calls[1]![1],
  );
});

test("a grant change retires a held start without leaving a still-permitted reader busy", async () => {
  let resolve!: (value: { thread_id: string }) => void;
  rs.mocked(api.createInstanceConversation).mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const agent = instance();
  const view = show(agent);
  fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
  await waitFor(() =>
    expect(api.createInstanceConversation).toHaveBeenCalled(),
  );
  view.rerender(
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <AgentHeader instance={{ ...agent, permissions: 3 }} />
      </FileActionLifetimeProvider>
    </I18nProvider>,
  );
  resolve({ thread_id: "old" });
  await new Promise((done) => setTimeout(done, 10));
  expect(state.push).not.toHaveBeenCalled();
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "New conversation" })
      .disabled,
  ).toBe(false);
  fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
  await waitFor(() =>
    expect(state.push).toHaveBeenCalledWith("/workspace/chats/new"),
  );
});
test("a refused new conversation displays its execution reason and keeps diagnostics private", async () => {
  rs.mocked(api.createInstanceConversation).mockRejectedValue(
    new api.InstanceApiError(
      409,
      "private diagnostic",
      "execution-unavailable",
      "operator-hold",
    ),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "New conversation" }));
  await screen.findByText(
    "AI assistants and AI employees are paused by Provider.",
  );
  expect(screen.queryByText("private diagnostic")).toBeNull();
});

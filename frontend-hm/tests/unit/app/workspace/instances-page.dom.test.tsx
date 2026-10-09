import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  enabled: true,
  userId: "alice",
  permissions: 7,
  push: rs.fn(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: state.userId,
      system_role: "user",
      permissions: [
        "agents:read",
        "agents:write",
        "threads:write",
        "memory:read",
        "memory:write",
      ],
    },
  }),
}));
rs.mock("@/core/features", () => ({
  useStorageSpacesEnabled: () => ({ enabled: state.enabled, isLoading: false }),
  useDocumentTitle: () => undefined,
}));
rs.mock("@/core/agents", () => ({
  useAgentsApiEnabled: () => ({ enabled: state.enabled, isLoading: false }),
}));
rs.mock("@/core/agents/api", () => ({
  listAgents: rs.fn().mockResolvedValue([]),
}));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams("instance=" + "a".repeat(32)),
  useRouter: () => ({ push: state.push, replace: rs.fn() }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
}));

import InstancesPage from "@/app/workspace/instances/page";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

function instance(): api.AgentInstance {
  return {
    id: "a".repeat(32),
    name: "Resident analyst",
    principal: { kind: "nonhuman", subject_id: "agent:" + "a".repeat(32) },
    custody: "company",
    owner_id: null,
    creator_id: "alice",
    supervisor: { kind: "human", subject_id: "bob" },
    definition_revision: "d".repeat(64),
    home_id: "b".repeat(32),
    status: "active",
    generation: 7,
    permissions: state.permissions,
  };
}
function show() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const ui = () => (
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <InstancesPage />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>
  );
  return { ...render(ui()), ui };
}
beforeEach(() => {
  rs.mocked(api.listWork).mockResolvedValue({ work: [] });
  state.enabled = true;
  state.userId = "alice";
  state.permissions = 7;
  state.push.mockReset();
  rs.mocked(api.listInstances).mockResolvedValue({ instances: [instance()] });
  rs.mocked(api.getInstance).mockImplementation(async () => instance());
  rs.mocked(api.getInstanceDefinition).mockResolvedValue({
    revision: "d".repeat(64),
    config: { name: "analyst" },
    soul: "Business instructions",
  });
  rs.mocked(api.getInstanceGrants).mockResolvedValue({ grants: [] });
  rs.mocked(api.getInstanceLifecycle).mockResolvedValue({ operations: [] });
  rs.mocked(api.getInstanceMemory).mockRejectedValue(
    new api.InstanceApiError(501, "Scoped memory unavailable"),
  );
  rs.mocked(api.createInstanceConversation).mockReset();
});
afterEach(cleanup);
it("does not load instances when the deployment capability is disabled", async () => {
  state.enabled = false;
  rs.mocked(api.listInstances).mockClear();
  show();
  expect(
    await screen.findByText(
      "Persistent instances require enabled agent management and qualified Storage Spaces.",
    ),
  ).toBeTruthy();
  expect(api.listInstances).not.toHaveBeenCalled();
});
it("shows identity and Home while withholding Manage and memory controls from Use-only staff", async () => {
  state.permissions = 1;
  rs.mocked(api.getInstanceDefinition).mockClear();
  show();
  expect(
    await screen.findByRole("button", { name: "Start conversation" }),
  ).toBeTruthy();
  expect(screen.queryByRole("button", { name: "Suspend" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Clear memory" })).toBeNull();
  expect(
    screen.getByRole("link", { name: "Open Home" }).getAttribute("href"),
  ).toContain("space=" + "b".repeat(32));
  expect(api.getInstanceDefinition).not.toHaveBeenCalled();
});
it("fences late conversation navigation when the account changes", async () => {
  let resolve!: (value: { thread_id: string }) => void;
  rs.mocked(api.createInstanceConversation).mockImplementationOnce(
    () =>
      new Promise((r) => {
        resolve = r;
      }),
  );
  const view = show();
  fireEvent.click(
    await screen.findByRole("button", { name: "Start conversation" }),
  );
  await waitFor(() =>
    expect(api.createInstanceConversation).toHaveBeenCalledTimes(1),
  );
  state.userId = "bob";
  view.rerender(view.ui());
  await act(async () => resolve({ thread_id: "old-account-thread" }));
  expect(state.push).not.toHaveBeenCalled();
});

it("abandons a captured obsolete operation and leaves restoration explicit", async () => {
  const request: api.LifecycleChange = {
    operation_id: "c".repeat(32),
    generation: 6,
    action: "supervise",
    supervisor_id: "departed",
  };
  let abandoned = false;
  rs.mocked(api.getInstance).mockImplementation(async () => ({
    ...instance(),
    status: "suspended",
  }));
  rs.mocked(api.getInstanceLifecycle).mockImplementation(async () => ({
    operations: [
      { ...request, complete: abandoned, abandoned, retry: request },
    ],
  }));
  rs.mocked(api.abandonInstanceLifecycle).mockImplementation(
    async (_id, body) => {
      expect(body).toEqual({
        operation_id: request.operation_id,
        generation: 7,
      });
      abandoned = true;
      return {
        instance: { ...instance(), status: "suspended" },
        complete: true,
        abandoned: true,
      };
    },
  );
  show();
  const button = await screen.findByRole("button", {
    name: "Abandon contained operation",
  });
  fireEvent.click(screen.getByLabelText("Confirm action", { exact: true }));
  fireEvent.click(button);
  await waitFor(() =>
    expect(api.abandonInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(
    await screen.findByText(
      "An obsolete operation was abandoned without changing grants or reactivating the agent.",
    ),
  ).toBeTruthy();
  expect(
    screen.queryByRole("button", { name: "Start conversation" }),
  ).toBeNull();
  expect(screen.getByRole("button", { name: "Restore" })).toBeTruthy();
});

it("shows historical abandonment accurately after a later restoration", async () => {
  rs.mocked(api.getInstanceLifecycle).mockResolvedValue({
    operations: [
      {
        operation_id: "c".repeat(32),
        generation: 6,
        action: "supervise",
        complete: true,
        abandoned: true,
      },
    ],
  });
  show();
  expect(
    await screen.findByRole("button", { name: "Start conversation" }),
  ).toBeTruthy();
  expect(
    await screen.findByText(
      "An obsolete operation was abandoned without changing grants or reactivating the agent.",
    ),
  ).toBeTruthy();
  expect(
    screen.queryByText(
      "An obsolete operation was abandoned. The agent remains suspended until explicitly restored.",
    ),
  ).toBeNull();
});

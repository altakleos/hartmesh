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

const state = rs.hoisted(() => ({ held: false, refresh: rs.fn() }));
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
rs.mock("@/core/agent-instances/api", { spy: true });

import { AccessTab } from "@/components/workspace/instances/access-tab";
import {
  LifecycleActionsProvider,
  LifecycleState,
} from "@/components/workspace/instances/lifecycle-actions";
import * as api from "@/core/agent-instances/api";
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
function show(permissions = 7) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <LifecycleActionsProvider
            instance={{ ...agent, permissions }}
            operations={[]}
            refresh={state.refresh}
          >
            <LifecycleState />
            <AccessTab />
          </LifecycleActionsProvider>
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  state.held = false;
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
test("renders human names and emails with the three access levels and locks own management", async () => {
  show();
  await screen.findByText("colleague@example.test");
  expect(screen.getByText("Who can work with Researcher")).toBeDefined();
  expect(
    screen.getByText(
      "You cannot remove your own “Can manage”. Ask another manager.",
    ),
  ).toBeDefined();
  const own = screen.getByRole("group", { name: "You" });
  expect(
    within(own).getByRole<HTMLInputElement>("checkbox", { name: "Can manage" })
      .disabled,
  ).toBe(true);
  const other = screen.getByRole("group", { name: "Alex" });
  fireEvent.click(
    within(other).getByRole<HTMLInputElement>("checkbox", {
      name: "Can manage",
    }),
  );
  expect(
    within(other).getByRole<HTMLInputElement>("checkbox", {
      name: "Can see all work",
    }).disabled,
  ).toBe(true);
  fireEvent.click(within(other).getByRole("button", { name: "Save" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  ).toMatchObject({
    action: "grant",
    member_id: "colleague",
    permissions: 7,
    only_if_idle: true,
    generation: 1,
  });
});
test("new colleague starts with chat and inspect selected", async () => {
  show();
  await screen.findByText("colleague@example.test");
  fireEvent.click(screen.getByRole("button", { name: "Add a colleague" }));
  fireEvent.click(screen.getByRole("button", { name: "Choose new colleague" }));
  const row = screen.getByRole("group", { name: "New colleague" });
  expect(
    within(row).getByRole<HTMLInputElement>("checkbox", { name: "Can chat" })
      .checked,
  ).toBe(true);
  expect(
    within(row).getByRole<HTMLInputElement>("checkbox", {
      name: "Can see all work",
    }).checked,
  ).toBe(true);
  fireEvent.click(within(row).getByRole("button", { name: "Save" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  ).toMatchObject({
    action: "grant",
    member_id: "new",
    permissions: 3,
    only_if_idle: true,
  });
});
test("removal has its own named confirmation and sends a zero grant only after consent", async () => {
  show();
  await screen.findByText("colleague@example.test");
  fireEvent.click(
    within(screen.getByRole("group", { name: "Alex" })).getByRole("button", {
      name: "Remove access",
    }),
  );
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
  expect(
    screen.getByText(
      "Remove Alex's access to Researcher? Alex will no longer see Researcher's conversations or files.",
    ),
  ).toBeDefined();
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Remove access",
    }),
  );
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1),
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  ).toMatchObject({
    action: "grant",
    member_id: "colleague",
    permissions: 0,
    only_if_idle: true,
  });
});
test("management without inspect does not read grants or expose mutation controls", () => {
  show(4);
  expect(
    screen.getByText(
      "To manage Researcher you also need “Can see all work”. Ask another manager to give it to you.",
    ),
  ).toBeDefined();
  expect(api.getInstanceGrants).not.toHaveBeenCalled();
  expect(screen.queryByRole("checkbox")).toBeNull();
});
test("execution hold retains readable grants and disables saves", async () => {
  state.held = true;
  show();
  await screen.findByText("colleague@example.test");
  const other = screen.getByRole("group", { name: "Alex" });
  expect(
    within(other).getByRole<HTMLInputElement>("checkbox", { name: "Can chat" })
      .disabled,
  ).toBe(true);
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
});
test("supervisor choices are confined to existing managers and never grant access", async () => {
  show();
  await screen.findByText("colleague@example.test");
  fireEvent.click(screen.getByRole("button", { name: "Change supervisor" }));
  const options = within(screen.getByRole("dialog")).getByRole("combobox");
  expect(within(options).queryByRole("option", { name: "Alex" })).toBeNull();
  expect(within(options).getByRole("option", { name: "You" })).toBeDefined();
  expect(api.changeInstanceLifecycle).not.toHaveBeenCalled();
});

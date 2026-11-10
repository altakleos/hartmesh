import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";

const state = rs.hoisted(() => ({ held: false, connected: 1 }));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: rs.fn() }),
  usePathname: () => "/workspace/instances",
  useSearchParams: () => new URLSearchParams(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "admin",
      permissions: ["agents:read", "agents:write", "threads:write"],
    },
  }),
}));
rs.mock("@/core/features/hooks", () => ({
  useAgentInstances: () => ({
    known: true,
    enabled: true,
    execution: state.held ? "unavailable" : "ready",
    reason: state.held ? "operator-hold" : null,
    assistants: { perUser: 3 },
    employees: { allowed: 3 },
  }),
  useTelegram: () => ({
    known: true,
    enabled: true,
    bots: "ready",
    reason: null,
  }),
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
  useStorage: () => ({ reason: null }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => ({
    known: true,
    enabled: true,
    execution: state.held ? "unavailable" : "ready",
    reason: state.held ? "operator-hold" : null,
    assistants: { perUser: 3 },
    employees: { allowed: 3 },
  }),
  useTelegram: () => ({
    known: true,
    enabled: true,
    bots: "ready",
    reason: null,
  }),
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
  useStorage: () => ({ reason: null }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: () => "Reader",
  useSupervisorName: () => "Reader",
  usePersonNames: () => new Map([["reader", "Reader"]]),
}));
rs.mock("@/core/people/hooks", () => ({ usePeople: () => new Map() }));
rs.mock("@/components/workspace/people-picker", () => ({
  PeoplePicker: () => null,
}));
rs.mock("@/components/ai-elements/prompt-input", () => ({
  usePromptInputController: () => ({
    textInput: { value: "", setInput: rs.fn() },
  }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/telegram-bots/api", { spy: true });

import { AgentWelcome } from "@/components/workspace/chats/agent-welcome";
import { AccessTab } from "@/components/workspace/instances/access-tab";
import { AgentHeader } from "@/components/workspace/instances/agent-header";
import { LifecycleActionsProvider } from "@/components/workspace/instances/lifecycle-actions";
import { SettingsTab } from "@/components/workspace/instances/settings-tab";
import * as instances from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import * as telegram from "@/core/telegram-bots/api";

import { bot, listOf } from "./telegram/support";

const instance: instances.AgentInstanceDetail = {
  id: "a".repeat(32),
  name: "Mira",
  custody: "company",
  owner_id: null,
  creator_id: "reader",
  principal: { kind: "nonhuman", subject_id: "agent" },
  supervisor: { kind: "human", subject_id: "reader" },
  definition_revision: "revision",
  home_id: "home",
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
function show(children: ReactNode, detail = instance) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <LifecycleActionsProvider
            instance={detail}
            operations={[]}
            refresh={rs.fn()}
          >
            {children}
          </LifecycleActionsProvider>
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  state.held = false;
  state.connected = 1;
  rs.mocked(instances.getInstanceGrants).mockResolvedValue({
    grants: [{ user_id: "reader", permissions: 7 }],
  });
  rs.mocked(instances.getInstanceDefinition).mockResolvedValue({
    revision: "revision",
    config: { name: "analyst" },
    soul: "Brief",
  });
  rs.mocked(telegram.listTelegramBots).mockResolvedValue(listOf([]));
  rs.mocked(telegram.getTelegramPolicy).mockImplementation(async () => ({
    policy: "allowed",
    people: state.connected,
    connected: state.connected,
  }));
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.clearAllMocks();
});

it("mounts a header Connect independently of paused work and busy files", async () => {
  const paused = {
    ...instance,
    status: "suspended" as const,
    home: { workspace: "absent" as const, state: "busy" as const },
  };
  show(<AgentHeader instance={paused} />);
  await waitFor(() =>
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Connect Telegram",
      }).disabled,
    ).toBe(false),
  );
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "New conversation" })
      .disabled,
  ).toBe(true);
  expect(
    screen
      .getByRole("button", { name: "New conversation" })
      .compareDocumentPosition(
        screen.getByRole("button", { name: "Connect Telegram" }),
      ) & Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
});
it("mounts the header bot link and current Telegram state", async () => {
  rs.mocked(telegram.listTelegramBots).mockResolvedValue(
    listOf([
      bot({
        agent_kind: "instance",
        agent_ref: instance.id,
        agent_name: "Mira",
      }),
    ]),
  );
  show(<AgentHeader instance={instance} />);
  await screen.findByRole("link", { name: "Open in Telegram" });
  await screen.findByText("Connected as Rosa");
});
it("mounts company policy after Add a colleague, and keeps it editable during an execution hold", async () => {
  state.held = true;
  show(<AccessTab />);
  const add = await screen.findByRole<HTMLButtonElement>("button", {
    name: "Add a colleague",
  });
  const allowed = await screen.findByRole<HTMLInputElement>("radio", {
    name: "Yes",
  });
  expect(
    add.compareDocumentPosition(allowed) & Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
  expect(add.disabled).toBe(true);
  expect(allowed.disabled).toBe(false);
});
it("does not mount company policy on a personal assistant", async () => {
  show(<AccessTab />, { ...instance, custody: "personal", owner_id: "reader" });
  await screen.findByRole("button", { name: "Add a colleague" });
  expect(telegram.getTelegramPolicy).not.toHaveBeenCalled();
  expect(screen.queryByRole("radio", { name: "Yes" })).toBeNull();
});
it("mounts rename and delete notes at their actual decisions", async () => {
  show(<SettingsTab />);
  await screen.findByText(/Renaming Mira also renames Mira's Telegram bots/);
  fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
  await screen.findByText(/Telegram bot connected to Mira stops answering/);
});
it("mounts the welcome Telegram offer after Work and Access", async () => {
  show(
    <AgentWelcome
      binding={{
        id: instance.id,
        name: instance.name,
        custody: instance.custody,
        status: "active",
        requester_id: "reader",
      }}
      detail={instance}
      threadId="thread"
    />,
  );
  const offer = await screen.findByRole("button", { name: /Telegram/ });
  const access = screen.getByRole("link", {
    name: "Add colleagues on the Access tab.",
  });
  expect(
    access.compareDocumentPosition(offer) & Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
});

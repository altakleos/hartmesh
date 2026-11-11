import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  t: {} as unknown,
  known: true,
  push: rs.fn(),
  user: {
    id: "alice",
    system_role: "user",
    permissions: ["agents:read", "agents:write"],
  },
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: state.t }) }));
rs.mock("@/core/features/hooks", () => ({
  useTelegram: () => ({
    known: state.known,
    enabled: true,
    bots: "ready",
    reason: null,
  }),
  useAgentInstances: () => ({
    known: true,
    enabled: true,
    execution: "ready",
    reason: null,
    assistants: { perUser: 3 },
    employees: { allowed: 5 },
  }),
  useBranding: () => ({ providerName: "Acme" }),
  useStorage: () => ({ reason: null }),
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: state.push }),
  usePathname: () => "/workspace/instances",
  useSearchParams: () => new URLSearchParams("instance=agent-a&tab=settings"),
}));
import {
  TelegramInstanceConnect,
  TelegramInstanceState,
  TelegramInstanceNote,
  TelegramInstanceTip,
} from "@/components/workspace/telegram/instance-controls";
import {
  resetTelegramPageState,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { createEnUS } from "@/core/i18n/locales/en-US";

import { bot, clearClients, json, listOf, renderWithClient } from "./support";
const instance: AgentInstanceDetail = {
  id: "agent-a",
  name: "Mira",
  custody: "company",
  owner_id: null,
  creator_id: "alice",
  principal: { kind: "nonhuman", subject_id: "agent-a" },
  supervisor: { kind: "human", subject_id: "alice" },
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
let bots = listOf([]);
let connected = 2;
function Opened() {
  const { setup } = useTelegramPageState();
  return (
    <output data-testid="target">
      {setup?.kind === "agent" ? setup.id : ""}
    </output>
  );
}
const render = (part: React.ReactNode) =>
  renderWithClient(
    <FileActionLifetimeProvider>
      {part}
      <Opened />
    </FileActionLifetimeProvider>,
  );
beforeEach(() => {
  state.t = createEnUS("HartMesh");
  state.known = true;
  state.user.permissions = ["agents:read", "agents:write"];
  bots = listOf([]);
  connected = 2;
  rs.stubGlobal(
    "fetch",
    rs.fn(async (url: string) =>
      url.endsWith("/policy")
        ? json({ policy: "allowed", people: 3, connected })
        : json(bots),
    ),
  );
});
afterEach(() => {
  cleanup();
  clearClients();
  resetTelegramPageState();
  rs.unstubAllGlobals();
  rs.clearAllMocks();
});
it("offers Connect and the welcome tip only with current readiness and opens one target", async () => {
  render(
    <>
      <TelegramInstanceConnect instance={instance} />
      <TelegramInstanceTip instance={instance} />
    </>,
  );
  await waitFor(() =>
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Connect Telegram",
      }).disabled,
    ).toBe(false),
  );
  fireEvent.click(
    screen.getByRole("button", { name: "Chat with Mira on Telegram." }),
  );
  expect(screen.getByTestId("target").textContent).toBe("agent-a");
});
it("opens a healthy bot and shows its protected state", async () => {
  bots = listOf([
    bot({
      agent_kind: "instance",
      agent_ref: instance.id,
      policy: "protected",
    }),
  ]);
  render(
    <>
      <TelegramInstanceConnect instance={instance} />
      <TelegramInstanceState instance={instance} />
      <TelegramInstanceTip instance={instance} />
    </>,
  );
  expect(
    (
      await screen.findByRole("link", { name: "Open in Telegram" })
    ).getAttribute("href"),
  ).toBe("https://t.me/chat_abc_bot");
  await screen.findByText(
    "Connected as Rosa. What Mira sends cannot be forwarded or saved.",
  );
  expect(screen.queryByText("Chat with Mira on Telegram.")).toBeNull();
});
it("opens settings for a bot needing attention and keeps the instance page query", async () => {
  bots = listOf([
    bot({
      agent_kind: "instance",
      agent_ref: instance.id,
      health: "token_rejected",
    }),
  ]);
  render(<TelegramInstanceConnect instance={instance} />);
  fireEvent.click(
    await screen.findByRole("button", { name: "Telegram settings" }),
  );
  expect(state.push).toHaveBeenCalledWith(
    "/workspace/instances?instance=agent-a&tab=settings&settings=channels&telegram=agent.agent-a",
  );
});
it("disables Connect and hides the tip under the employee policy", async () => {
  bots = listOf([], {
    policies: [{ instance_id: instance.id, policy: "off" }],
  });
  render(
    <>
      <TelegramInstanceConnect instance={instance} />
      <TelegramInstanceTip instance={instance} />
    </>,
  );
  await screen.findByText(
    "Telegram is turned off for Mira by the people who manage Mira.",
  );
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Connect Telegram" })
      .disabled,
  ).toBe(true);
  expect(screen.queryByText("Chat with Mira on Telegram.")).toBeNull();
});
it("reads company counts for the rename/delete notes without needing an own bot", async () => {
  render(
    <>
      <TelegramInstanceNote instance={instance} kind="rename" />
      <TelegramInstanceNote instance={instance} kind="delete" />
    </>,
  );
  await screen.findByText("Renaming Mira also renames Mira's Telegram bots.");
  await screen.findByText("2 Telegram bots connected to Mira stop answering.");
  expect(
    rs
      .mocked(fetch)
      .mock.calls.filter(
        ([url]) => typeof url === "string" && url.endsWith("/policy"),
      ),
  ).toHaveLength(1);
});
it("uses only the reader's connected personal bot and never calls the company policy route", async () => {
  bots = listOf([bot({ agent_kind: "instance", agent_ref: instance.id })]);
  render(
    <TelegramInstanceNote
      instance={{ ...instance, custody: "personal", owner_id: "alice" }}
      kind="delete"
    />,
  );
  await screen.findByText("1 Telegram bot connected to Mira stops answering.");
  expect(
    rs
      .mocked(fetch)
      .mock.calls.every(
        ([url]) => typeof url === "string" && !url.endsWith("/policy"),
      ),
  ).toBe(true);
});
it("does not offer Telegram when feature discovery is unknown", async () => {
  state.known = false;
  render(
    <>
      <TelegramInstanceConnect instance={instance} />
      <TelegramInstanceState instance={instance} />
      <TelegramInstanceTip instance={instance} />
      <TelegramInstanceNote instance={instance} kind="rename" />
    </>,
  );
  await act(async () => undefined);
  expect(screen.queryByRole("button")).toBeNull();
  expect(fetch).not.toHaveBeenCalled();
});

it("shows a load failure instead of leaving Connect on a permanent loading hint", async () => {
  rs.mocked(fetch).mockImplementation(async () =>
    json({ reason: "unavailable" }, 503),
  );
  render(<TelegramInstanceConnect instance={instance} />);
  await screen.findByText(
    "Your Telegram bots could not be loaded. Check your connection and refresh.",
  );
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Connect Telegram" })
      .disabled,
  ).toBe(true);
});
it("omits management notes and policy reads after Manage is withdrawn", async () => {
  render(
    <TelegramInstanceNote
      instance={{ ...instance, permissions: 3 }}
      kind="rename"
    />,
  );
  await act(async () => undefined);
  expect(screen.queryByText(/Renaming Mira/)).toBeNull();
  expect(
    rs
      .mocked(fetch)
      .mock.calls.every(
        ([url]) => typeof url === "string" && !url.endsWith("/policy"),
      ),
  ).toBe(true);
});

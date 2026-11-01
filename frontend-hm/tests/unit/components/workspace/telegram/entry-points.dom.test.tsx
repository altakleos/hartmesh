import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
const local = rs.hoisted(() => ({
  dismissed: false,
  set: rs.fn(),
  push: rs.fn(),
  openSettings: rs.fn(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: i18n.t }) }));
rs.mock("@/core/settings", () => ({
  useLocalSettings: () => [
    { telegram: { offerDismissed: local.dismissed } },
    local.set,
  ],
}));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: local.push }),
}));
rs.mock("@/components/ui/sidebar", () => ({
  SidebarMenuItem: ({ children }: { children: ReactNode }) => <>{children}</>,
}));
rs.mock("@/components/workspace/settings/settings-dialog-store", () => ({
  openSettingsDialog: local.openSettings,
}));

import { TelegramAgentCardButton } from "@/components/workspace/telegram/agent-card-button";
import {
  resetTelegramPageState,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import { TelegramSidebarRow } from "@/components/workspace/telegram/sidebar-row";
import { TelegramWelcomeCard } from "@/components/workspace/telegram/welcome-card";
import { fetch } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { BotList } from "@/core/telegram-bots/api";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import {
  bot,
  clearClients,
  json,
  listOf,
  renderWithClient,
  server,
} from "./support";

i18n.t = createEnUS("HartMesh");

const READY = { enabled: true, bots: "ready", reason: null };
const OFF = { enabled: false, bots: "unavailable", reason: "off" };
const MISSING = {
  enabled: true,
  bots: "unavailable",
  reason: "secret-missing",
};

function serve(telegram: unknown, list: BotList = listOf([])) {
  const { handle } = server({
    "GET /api/features": () =>
      json({ agents_api: { enabled: true }, telegram }),
    "GET /api/telegram-bots": () => json(list),
  });
  rs.mocked(fetch).mockImplementation(handle);
}

function Setup() {
  const { setup } = useTelegramPageState();
  return setup ? (
    <output data-testid="set-up-for">{telegramTargetOf(setup)}</output>
  ) : null;
}

afterEach(() => {
  cleanup();
  clearClients();
  resetTelegramPageState();
  rs.mocked(fetch).mockReset();
  local.dismissed = false;
  local.set.mockReset();
  local.push.mockReset();
  local.openSettings.mockReset();
});

const settle = () => new Promise((resolve) => setTimeout(resolve, 30));

describe("the welcome card", () => {
  it("is shown until Not now, which is remembered", async () => {
    serve(READY);
    renderWithClient(<TelegramWelcomeCard />);
    fireEvent.click(await screen.findByRole("button", { name: "Not now" }));
    expect(local.set).toHaveBeenCalledWith("telegram", {
      offerDismissed: true,
    });
    cleanup();
    clearClients();

    local.dismissed = true;
    serve(READY);
    renderWithClient(<TelegramWelcomeCard />);
    await settle();
    expect(screen.queryByTestId("telegram-welcome-card")).toBeNull();
  });

  it("opens the set-up for the ordinary chat", async () => {
    serve(READY);
    render(<Setup />);
    renderWithClient(<TelegramWelcomeCard />);
    fireEvent.click(
      await screen.findByRole("button", { name: "Connect Telegram" }),
    );
    expect(await screen.findByTestId("set-up-for")).toBeTruthy();
    expect(screen.getByTestId("set-up-for").textContent).toBe("chat");
  });

  it("is absent when the ordinary chat has a bot or Telegram is switched off", async () => {
    serve(READY, listOf([bot()]));
    renderWithClient(<TelegramWelcomeCard />);
    await settle();
    expect(screen.queryByTestId("telegram-welcome-card")).toBeNull();
    cleanup();
    clearClients();

    serve(OFF);
    renderWithClient(<TelegramWelcomeCard />);
    await settle();
    expect(screen.queryByTestId("telegram-welcome-card")).toBeNull();
  });

  it("disables Connect with its sentence while Telegram cannot run", async () => {
    serve(MISSING);
    renderWithClient(<TelegramWelcomeCard />);
    const connect = await screen.findByRole("button", {
      name: "Connect Telegram",
    });
    expect((connect as HTMLButtonElement).disabled).toBe(true);
    screen.getByText(/the setting that encrypts their tokens is missing/);
  });
});

describe("the sidebar row", () => {
  const text = async (list: BotList) => {
    serve(READY, list);
    renderWithClient(<TelegramSidebarRow />);
    await settle();
    return screen.getByRole("button").textContent;
  };

  it("reads Connect, the number connected, or the number that need attention", async () => {
    expect(await text(listOf([]))).toBe("Connect");
    cleanup();
    clearClients();

    expect(await text(listOf([bot()]))).toBe("1 connected");
    cleanup();
    clearClients();

    expect(await text(listOf([bot({ state: "held" })]))).toBe(
      "1 needs attention",
    );
    cleanup();
    clearClients();

    expect(
      await text(
        listOf([
          bot({ id: "a", state: "blocked" }),
          bot({ id: "b", health: "stopped" }),
          bot({ id: "c" }),
        ]),
      ),
    ).toBe("2 need attention");
  });

  it("opens the section", async () => {
    serve(READY);
    renderWithClient(<TelegramSidebarRow />);
    fireEvent.click(await screen.findByRole("button", { name: "Connect" }));
    expect(local.openSettings).toHaveBeenCalledWith("channels");
  });
});

describe("the agent card's button", () => {
  const custom = bot({
    agent_kind: "custom_agent",
    agent_ref: "helper",
    agent_name: "helper",
  });
  const button = async () => {
    await settle();
    await waitFor(() => expect(screen.getByRole("button")).toBeTruthy());
    return screen.getByRole<HTMLButtonElement>("button");
  };

  it("the card's button reads Connect Telegram, or the short state of the bot", async () => {
    serve(READY);
    render(<Setup />);
    renderWithClient(<TelegramAgentCardButton name="helper" />);
    await settle();
    const connect = await screen.findByTitle("Connect Telegram");
    fireEvent.click(connect);
    expect(await screen.findByTestId("set-up-for")).toBeTruthy();
    expect(screen.getByTestId("set-up-for").textContent).toBe(
      "custom_agent.helper",
    );
    cleanup();
    clearClients();

    serve(READY, listOf([custom]));
    renderWithClient(<TelegramAgentCardButton name="helper" />);
    fireEvent.click(await screen.findByTitle("Telegram: connected"));
    expect(local.push).toHaveBeenCalledWith(
      "/workspace/agents?settings=channels&telegram=custom_agent.helper",
    );
  });

  it("is disabled where a bot cannot be connected, and says why", async () => {
    serve(OFF);
    renderWithClient(<TelegramAgentCardButton name="helper" />);
    await screen.findByTitle("Telegram: turned off");
    expect((await button()).disabled).toBe(true);
    cleanup();
    clearClients();

    serve(READY, listOf([bot()], { limits: { per_person: 1, used: 1 } }));
    renderWithClient(<TelegramAgentCardButton name="helper" />);
    await screen.findByTitle("Telegram: not connected");
    expect((await button()).disabled).toBe(true);
  });

  it("is absent without an answer about Telegram", async () => {
    serve(null);
    renderWithClient(<TelegramAgentCardButton name="helper" />);
    await settle();
    expect(screen.queryByRole("button")).toBeNull();
  });
});

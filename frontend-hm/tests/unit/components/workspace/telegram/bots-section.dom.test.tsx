import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
const toast = rs.hoisted(() => ({ success: rs.fn(), error: rs.fn() }));
const agents = rs.hoisted(() => ({
  list: [] as { name: string }[],
  isLoading: false,
}));
const navigation = rs.hoisted(() => ({
  query: "",
  replace: rs.fn(),
}));
rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace",
  useRouter: () => ({ replace: navigation.replace }),
  useSearchParams: () => new URLSearchParams(navigation.query),
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("sonner", () => ({ toast }));
rs.mock("@/core/agents/hooks", () => ({
  useAgents: () => ({ agents: agents.list, isLoading: agents.isLoading }),
}));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: i18n.t, locale: "en-US" }),
}));
rs.mock("@/components/ui/dropdown-menu", () => ({
  DropdownMenu: ({ children }: { children: React.ReactNode }) => children,
  DropdownMenuTrigger: () => null,
  DropdownMenuContent: ({ children }: { children: React.ReactNode }) =>
    children,
  DropdownMenuItem: ({
    children,
    onSelect,
  }: {
    children: React.ReactNode;
    onSelect: () => void;
  }) => <button onClick={onSelect}>{children}</button>,
}));

import { setSettingsDialogOpen } from "@/components/workspace/settings";
import { TelegramBotsSection } from "@/components/workspace/telegram/bots-section";
import {
  discardTelegramConnects,
  holdTelegramConnect,
  resetTelegramPageState,
  telegramConnectEpoch,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import { WorkspaceSettingsDeepLink } from "@/components/workspace/workspace-settings-deep-link";
import { fetch } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { BotList } from "@/core/telegram-bots/api";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import {
  ago,
  bot,
  clearClients,
  json,
  listOf,
  queries,
  renderWithClient,
  server,
} from "./support";

i18n.t = createEnUS("HartMesh");

const READY = { enabled: true, bots: "ready", reason: null };
const LIST = "GET /api/telegram-bots";
const ADMIN = "GET /api/telegram-bots/admin/summary";

function start(
  telegram: unknown,
  list: BotList = listOf([]),
  routes: Record<string, () => Response> = {},
) {
  const { calls, handle } = server({
    "GET /api/features": () =>
      json({ agents_api: { enabled: true }, telegram }),
    [LIST]: () => json(list),
    ...routes,
  });
  rs.mocked(fetch).mockImplementation(handle);
  renderWithClient(<TelegramBotsSection />);
  return calls;
}

function OpenedSetup() {
  const { setup } = useTelegramPageState();
  return setup ? (
    <output data-testid="set-up-for">{telegramTargetOf(setup)}</output>
  ) : null;
}
const renderOpened = () => render(<OpenedSetup />);

const rowsShown = () =>
  Array.from(document.querySelectorAll("[data-telegram-row] .break-words")).map(
    (element) => element.textContent,
  );

afterEach(() => {
  cleanup();
  clearClients();
  resetTelegramPageState();
  setSettingsDialogOpen(false);
  rs.restoreAllMocks();
  rs.mocked(fetch).mockReset();
  toast.success.mockReset();
  toast.error.mockReset();
  auth.user = { id: "alice", system_role: "user" };
  agents.list = [];
  agents.isLoading = false;
  navigation.query = "";
  navigation.replace.mockReset();
});

describe("the Telegram section", () => {
  it("lists the ordinary chat, then custom agents, then what has gone", async () => {
    agents.list = [{ name: "zeta" }, { name: "alpha" }];
    start(
      READY,
      listOf([
        bot({
          id: "gone",
          agent_kind: "custom_agent",
          agent_ref: "deleted",
          agent_name: "deleted",
          health: "agent_unavailable",
        }),
      ]),
    );
    await waitFor(() =>
      expect(rowsShown()).toEqual(["HartMesh", "alpha", "zeta", "deleted"]),
    );
  });

  it("shows the notice of each reason and disables every Connect", async () => {
    const reasons: [string, string][] = [
      ["secret-missing", "the setting that encrypts their tokens is missing"],
      ["secret-invalid", "the setting that encrypts their tokens is not valid"],
      ["host-changed", "the service moved to a new server"],
      ["polled-elsewhere", "several bots conflict with another program"],
      ["misconfigured", "Telegram is not available right now."],
    ];
    for (const [reason, sentence] of reasons) {
      const calls = start({ enabled: true, bots: "unavailable", reason });
      const status = await screen.findByRole("status");
      expect(status.textContent).toContain(sentence);
      expect(status.textContent).toContain("Ask an administrator.");
      expect(status.textContent).not.toContain(reason);
      const connect = await screen.findByRole("button", {
        name: "Connect Telegram",
      });
      expect((connect as HTMLButtonElement).disabled).toBe(true);
      expect(calls.some(({ url }) => url.endsWith("/admin/summary"))).toBe(
        false,
      );
      cleanup();
      clearClients();
    }
  });

  it("tells an administrator what to quote and whom to ask", async () => {
    auth.user = { id: "alice", system_role: "admin" };
    start(
      { enabled: true, bots: "unavailable", reason: "secret-missing" },
      undefined,
      { [ADMIN]: () => new Response("{}", { status: 403 }) },
    );
    const status = await screen.findByRole("status");
    expect(status.textContent).toContain(
      "Reference for support: secret-missing",
    );
    expect(status.textContent).toContain("to fix this.");
    expect(status.textContent).not.toContain("Ask an administrator.");
  });

  it("says Telegram is switched off, and that bots are kept when there are some", async () => {
    start(
      { enabled: false, bots: "unavailable", reason: "off" },
      listOf([bot()]),
    );
    await waitFor(() =>
      expect(screen.getByRole("status").textContent).toContain(
        "Your bots are kept",
      ),
    );
    expect(screen.getByRole("status").textContent).toContain(
      "Ask an administrator to switch Telegram on.",
    );
    expect(screen.getByText("Paused")).toBeTruthy();
  });

  it("renders nothing without an answer about Telegram", async () => {
    const calls = start(null);
    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(document.body.textContent).toBe("");
    expect(calls.some(({ url }) => url === "/api/telegram-bots")).toBe(false);
  });

  it("shows the count from two bots before the limit, and the limit's sentence at the limit", async () => {
    start(READY, listOf([bot()], { limits: { per_person: 3, used: 1 } }));
    await screen.findByText("Your Telegram bots");
    await screen.findByText("1 of 3 bots");
    cleanup();
    clearClients();

    agents.list = [{ name: "alpha" }];
    start(READY, listOf([bot()], { limits: { per_person: 1, used: 1 } }));
    await screen.findByText("1 of 1 bots");
    const connect = await screen.findByRole("button", {
      name: "Connect Telegram",
    });
    expect((connect as HTMLButtonElement).disabled).toBe(true);
    expect(
      screen.getAllByText(/You have 1 Telegram bots/).length,
    ).toBeGreaterThan(0);
  });

  it("shows no count while more than two bots can be added", async () => {
    start(READY, listOf([bot()], { limits: { per_person: 4, used: 1 } }));
    await screen.findByText("Your Telegram bots");
    await screen.findByText("HartMesh");
    expect(screen.queryByText(/of 4 bots/)).toBeNull();
  });

  it("says nothing about a row until the list has answered", async () => {
    const { handle } = server({
      "GET /api/features": () =>
        json({ agents_api: { enabled: true }, telegram: READY }),
    });
    rs.mocked(fetch).mockImplementation((url, init) =>
      url === LIST.slice(4) ? new Promise(() => undefined) : handle(url, init),
    );
    renderWithClient(<TelegramBotsSection />);
    await screen.findByText("Your Telegram bots");
    expect(screen.queryByText("Not connected")).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Connect Telegram" }),
    ).toBeNull();
    screen.getByText("Loading...");
  });

  it("keeps one row per target when a custom agent has the product's name", async () => {
    agents.list = [{ name: "HartMesh" }];
    start(READY, listOf([]));
    await waitFor(() => expect(rowsShown()).toEqual(["HartMesh", "HartMesh"]));
  });

  it("says when the list could not be read, and reads it again on a press", async () => {
    let fail = true;
    start(READY, undefined, {
      [LIST]: () =>
        fail ? new Response("{}", { status: 500 }) : json(listOf([])),
    });
    const retry = await screen.findByRole("button", { name: "Try again" });
    screen.getByText(/could not be loaded/);
    fail = false;
    fireEvent.click(retry);
    await screen.findByRole("button", { name: "Connect Telegram" });
  });

  it("offers That was me only after the conflict ended", async () => {
    start(READY, listOf([bot({ health: "conflict" })]));
    await screen.findByText(/Another program is using this bot's token/);
    expect(screen.queryByRole("button", { name: "That was me" })).toBeNull();
    cleanup();
    clearClients();

    start(READY, listOf([bot({ acts_paused_since: ago(60) })]));
    await screen.findByRole("button", { name: "That was me" });
  });

  it("Remove, Connect a different account and Disconnect all call their routes after the confirmation", async () => {
    const connect = { url: "https://t.me/x?start=code", expires_at: ago(-600) };
    const calls = start(READY, listOf([bot()]), {
      "DELETE /api/telegram-bots/link-a": () =>
        new Response(null, { status: 204 }),
      "POST /api/telegram-bots/link-a/disconnect": () =>
        json({ bot: bot(), connect }),
      "POST /api/telegram-bots/disconnect-all": () => json({ disconnected: 1 }),
    });
    const posted = () =>
      calls.filter(({ method }) => method !== "GET").map(({ url }) => url);

    fireEvent.click(await screen.findByText("Remove bot"));
    expect(posted()).toEqual([]);
    fireEvent.click(
      await screen.findByRole("button", { name: "Remove bot", hidden: false }),
    );
    await waitFor(() => expect(toast.success).toHaveBeenCalledTimes(1));
    expect(posted()).toEqual(["/api/telegram-bots/link-a"]);

    fireEvent.click(
      await screen.findByText("Connect a different Telegram account"),
    );
    fireEvent.click(
      await screen.findByRole("button", {
        name: "Disconnect and get a new link",
      }),
    );
    await waitFor(() => expect(toast.success).toHaveBeenCalledTimes(2));
    expect(posted()[1]).toBe("/api/telegram-bots/link-a/disconnect");

    fireEvent.click(await screen.findByText("Disconnect all bots"));
    await screen.findByText("Disconnect all 1 bots?");
    expect(posted()).toHaveLength(2);
    const buttons = screen.getAllByRole("button", {
      name: "Disconnect all bots",
    });
    fireEvent.click(buttons[buttons.length - 1]!);
    await waitFor(() => expect(toast.success).toHaveBeenCalledTimes(3));
    expect(posted()[2]).toBe("/api/telegram-bots/disconnect-all");
  });

  it("the administrator's block shows counts and no username", async () => {
    auth.user = { id: "alice", system_role: "admin" };
    start(READY, listOf([]), {
      [ADMIN]: () =>
        json({
          enabled: true,
          reason: null,
          bots: 2,
          people: 1,
          polling: 2,
          failing: 0,
          held: 0,
          limits: { per_person: 10, deployment: 100 },
          voice: "available",
          voice_provider: "gemini",
        }),
    });
    await screen.findByText(/2 bots for 1 person: 2 working/);
    screen.getByText("One person can connect up to 10 bots; 100 bots in all.");
    screen.getByText(/Voice notes are turned into text by Google Gemini/);
    expect(document.body.textContent).not.toContain("@");
  });

  it("the administrator's block says who can switch voice notes on", async () => {
    auth.user = { id: "alice", system_role: "admin" };
    start(READY, listOf([]), {
      [ADMIN]: () =>
        json({
          enabled: true,
          reason: null,
          bots: 0,
          people: 0,
          polling: 0,
          failing: 0,
          held: 0,
          limits: { per_person: 10, deployment: 100 },
          voice: "unavailable",
          voice_provider: null,
        }),
    });
    await screen.findByText("Nobody has connected a Telegram bot yet.");
    screen.getByText(/Voice notes are not switched on here/);
  });

  it("focuses the row an address names and takes the name out of the address", async () => {
    navigation.query = "settings=channels&telegram=chat";
    const scroll = rs.fn();
    window.HTMLElement.prototype.scrollIntoView = scroll;
    start(READY, listOf([bot()]));
    await waitFor(() => expect(navigation.replace).toHaveBeenCalled());
    expect(navigation.replace).toHaveBeenCalledWith(
      "/workspace?settings=channels",
      { scroll: false },
    );
    expect(document.activeElement?.getAttribute("data-telegram-target")).toBe(
      "chat",
    );
    expect(scroll).toHaveBeenCalled();
  });

  it("opens the set-up after Settings took its own word off the address first", async () => {
    navigation.query = "settings=channels&telegram=chat";
    navigation.replace.mockImplementation((href: string) => {
      navigation.query = href.split("?")[1] ?? "";
    });
    renderOpened();
    render(<WorkspaceSettingsDeepLink />);
    // The router answers before the two lazy chunks that hold the section.
    await waitFor(() => expect(navigation.query).toBe("telegram=chat"));
    start(READY, listOf([]));
    await screen.findByTestId("set-up-for");
    expect(screen.getByTestId("set-up-for").textContent).toBe("chat");
    await waitFor(() => expect(navigation.query).toBe(""));
  });

  it("keeps the rows when a later read fails", async () => {
    let healthy = true;
    start(READY, undefined, {
      [LIST]: () =>
        healthy ? json(listOf([bot()])) : new Response("{}", { status: 500 }),
    });
    await waitFor(() => expect(rowsShown()).toEqual(["HartMesh"]));
    healthy = false;
    await act(async () => {
      await queries().refetchQueries({ queryKey: ["telegram-bots"] });
    });
    expect(rowsShown()).toEqual(["HartMesh"]);
    expect(
      screen.queryByText(
        "Your Telegram bots could not be loaded. Check your connection and refresh.",
      ),
    ).toBeNull();
  });

  it("says so when the first read fails, and offers another", async () => {
    let healthy = false;
    start(READY, undefined, {
      [LIST]: () =>
        healthy ? json(listOf([bot()])) : new Response("{}", { status: 500 }),
    });
    await screen.findByText(
      "Your Telegram bots could not be loaded. Check your connection and refresh.",
    );
    healthy = true;
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    await waitFor(() => expect(rowsShown()).toEqual(["HartMesh"]));
  });

  it("opens the set-up for an address that names a row without a bot", async () => {
    navigation.query = "settings=channels&telegram=custom_agent.alpha";
    agents.list = [{ name: "alpha" }];
    renderOpened();
    start(READY, listOf([]));
    await screen.findByTestId("set-up-for");
    expect(screen.getByTestId("set-up-for").textContent).toBe(
      "custom_agent.alpha",
    );
  });

  it("waits for the custom agents before reading an address", async () => {
    navigation.query = "settings=channels&telegram=custom_agent.alpha";
    agents.isLoading = true;
    renderOpened();
    start(READY, listOf([]));
    await screen.findByRole("button", { name: "Connect Telegram" });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(navigation.replace).not.toHaveBeenCalled();
    expect(screen.queryByTestId("set-up-for")).toBeNull();
  });

  it("does not open a set-up that could not work", async () => {
    navigation.query = "settings=channels&telegram=chat";
    renderOpened();
    start(
      { enabled: true, bots: "unavailable", reason: "secret-missing" },
      listOf([]),
    );
    await waitFor(() => expect(navigation.replace).toHaveBeenCalled());
    expect(screen.queryByTestId("set-up-for")).toBeNull();
  });

  it("ignores an address that names no row", async () => {
    navigation.query = "settings=channels&telegram=custom_agent.nobody";
    renderOpened();
    start(READY, listOf([]));
    await waitFor(() => expect(navigation.replace).toHaveBeenCalled());
    expect(screen.queryByTestId("set-up-for")).toBeNull();
  });

  it("offers nothing held here after Disconnect all", async () => {
    let state = listOf([bot({ state: "waiting_connect", connected_as: null })]);
    start(READY, undefined, {
      [LIST]: () => json(state),
      "POST /api/telegram-bots/link-a/connect": () =>
        json({ url: "https://t.me/x?start=c", expires_at: ago(-600) }),
      "POST /api/telegram-bots/disconnect-all": () => {
        state = listOf([bot({ state: "waiting_connect", connected_as: null })]);
        return json({ disconnected: 1 });
      },
    });
    fireEvent.click(
      await screen.findByRole("button", { name: "Disconnect all bots" }),
    );
    await screen.findByText("Disconnect all 1 bots?");
    holdTelegramConnect(
      "link-a",
      { url: "https://t.me/x?start=old", expires_at: ago(-600) },
      telegramConnectEpoch(),
    );
    await screen.findByRole("link", {
      name: "Open chat with HartMesh",
      hidden: true,
    });
    const buttons = screen.getAllByRole("button", {
      name: "Disconnect all bots",
    });
    fireEvent.click(buttons[buttons.length - 1]!);
    await waitFor(() => expect(toast.success).toHaveBeenCalled());
    await screen.findByRole("button", { name: "Open chat with HartMesh" });
    expect(
      screen.queryByRole("link", { name: "Open chat with HartMesh" }),
    ).toBeNull();
  });

  it("drops an address that arrives after Disconnect all", async () => {
    let answer: (response: Response) => void = () => undefined;
    const pending = new Promise<Response>((resolve) => {
      answer = resolve;
    });
    start(
      READY,
      listOf([bot({ state: "waiting_connect", connected_as: null })]),
    );
    const route = rs.mocked(fetch).getMockImplementation()!;
    rs.mocked(fetch).mockImplementation((url, init) =>
      (url as string).endsWith("/connect") ? pending : route(url, init),
    );
    const opened = {
      closed: false,
      close: rs.fn(),
      location: { replace: rs.fn() },
      opener: {},
    };
    rs.spyOn(window, "open").mockReturnValue(opened as unknown as Window);
    fireEvent.click(
      await screen.findByRole("button", { name: "Open chat with HartMesh" }),
    );
    await waitFor(() =>
      expect(
        rs
          .mocked(fetch)
          .mock.calls.some(([url]) => (url as string).endsWith("/connect")),
      ).toBe(true),
    );
    discardTelegramConnects();
    answer(json({ url: "https://t.me/x?start=dead", expires_at: ago(-600) }));
    await waitFor(() => expect(opened.close).toHaveBeenCalled());
    expect(opened.location.replace).not.toHaveBeenCalled();
    expect(
      screen.queryByRole("link", { name: "Open chat with HartMesh" }),
    ).toBeNull();
  });

  it("lets a late answer close only its own dialog", async () => {
    let answer: (response: Response) => void = () => undefined;
    const pending = new Promise<Response>((resolve) => {
      answer = resolve;
    });
    agents.list = [{ name: "alpha" }];
    start(
      READY,
      listOf([
        bot(),
        bot({
          id: "link-b",
          agent_kind: "custom_agent",
          agent_ref: "alpha",
          agent_name: "alpha",
        }),
      ]),
    );
    const route = rs.mocked(fetch).getMockImplementation()!;
    rs.mocked(fetch).mockImplementation((url, init) =>
      init?.method === "DELETE" ? pending : route(url, init),
    );
    const removes = await screen.findAllByText("Remove bot");
    fireEvent.click(removes[0]!);
    await screen.findByText("Remove HartMesh's Telegram bot?");
    const confirm = screen.getAllByRole("button", { name: "Remove bot" });
    fireEvent.click(confirm[confirm.length - 1]!);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() =>
      expect(screen.queryByText("Remove HartMesh's Telegram bot?")).toBeNull(),
    );
    fireEvent.click(screen.getAllByText("Remove bot")[1]!);
    await screen.findByText("Remove alpha's Telegram bot?");
    answer(new Response(null, { status: 204 }));
    await waitFor(() => expect(toast.success).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 20));
    screen.getByText("Remove alpha's Telegram bot?");
  });

  it("reads a waiting row as expired when its time passes, with no new answer", async () => {
    const intervals = rs.spyOn(globalThis, "setInterval");
    const now = Date.now();
    start(
      READY,
      listOf([
        bot({
          state: "waiting_connect",
          connected_as: null,
          connect_expires_at: new Date(now + 60_000).toISOString(),
        }),
      ]),
    );
    await screen.findByText("Waiting for you to press Start");
    const tick = intervals.mock.calls.find(([, delay]) => delay === 5000)?.[0];
    rs.spyOn(Date, "now").mockReturnValue(now + 120_000);
    act(() => (tick as () => void)());
    await screen.findByText("The link expired");
  });

  it("Try now calls its route and shows no message", async () => {
    const calls = start(READY, listOf([bot({ health: "stopped" })]), {
      "POST /api/telegram-bots/link-a/retry": () => json({ bot: bot() }),
    });
    fireEvent.click(await screen.findByRole("button", { name: "Try now" }));
    await waitFor(() =>
      expect(calls.some(({ url }) => url.endsWith("/retry"))).toBe(true),
    );
    expect(
      calls.find(({ url }) => url.endsWith("/retry"))?.body,
    ).toBeUndefined();
    expect(toast.success).not.toHaveBeenCalled();
    expect(toast.error).not.toHaveBeenCalled();
  });

  it("Apply name and picture says it was applied, and Telegram being out of reach", async () => {
    let answer = () => json({ bot: bot() });
    start(READY, listOf([bot({ profile_synced: false })]), {
      "POST /api/telegram-bots/link-a/profile": () => answer(),
    });
    const buttons = await screen.findAllByRole("button", {
      name: "Apply name and picture again",
    });
    fireEvent.click(buttons[0]!);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith("Name and picture applied."),
    );
    answer = () =>
      json({ detail: "not shown", reason: "telegram-unreachable" }, 502);
    fireEvent.click(buttons[0]!);
    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith(
        "HartMesh could not reach Telegram. Nothing was saved. Try again in a minute.",
      ),
    );
  });

  it("Use it here on a row asks first, then takes the webhook over", async () => {
    const calls = start(
      READY,
      listOf([bot({ health: "conflict", webhook_host: "x.test" })]),
      {
        "POST /api/telegram-bots/link-a/retry": () => json({ bot: bot() }),
      },
    );
    fireEvent.click(await screen.findByRole("button", { name: "Use it here" }));
    await screen.findByText("Use this bot here?");
    expect(calls.some(({ url }) => url.endsWith("/retry"))).toBe(false);
    const buttons = screen.getAllByRole("button", { name: "Use it here" });
    fireEvent.click(buttons[buttons.length - 1]!);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "Done. This bot now answers here.",
      ),
    );
    expect(calls.find(({ url }) => url.endsWith("/retry"))?.body).toEqual({
      take_over_webhook: true,
    });
  });

  it("That was me says nothing when another tab already did it", async () => {
    start(READY, listOf([bot({ acts_paused_since: ago(60) })]), {
      "POST /api/telegram-bots/link-a/acknowledge-conflict": () =>
        json({ detail: "not shown", reason: "no-conflict" }, 409),
    });
    fireEvent.click(await screen.findByRole("button", { name: "That was me" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "Yes, that was me" }),
    );
    await waitFor(() =>
      expect(screen.queryByText("Was the other program yours?")).toBeNull(),
    );
    expect(toast.error).not.toHaveBeenCalled();
    expect(toast.success).not.toHaveBeenCalled();
  });

  it("offers the replacement beside That was me", async () => {
    start(READY, listOf([bot({ acts_paused_since: ago(60) })]));
    fireEvent.click(await screen.findByRole("button", { name: "That was me" }));
    await screen.findByText("Was the other program yours?");
    const paste = screen.getAllByRole("button", { name: "Paste a new token" });
    fireEvent.click(paste[paste.length - 1]!);
    await screen.findByText("Paste a new token for HartMesh's bot");
  });

  it("says when too many links wait, and nothing when the bot is already connected", async () => {
    let reason = "too-many-codes";
    start(READY, listOf([bot({ state: "held" })]), {
      "POST /api/telegram-bots/link-a/connect": () =>
        json({ detail: "not shown", reason }, 409),
    });
    const closed = rs.fn();
    rs.spyOn(window, "open").mockReturnValue({
      closed: false,
      close: closed,
      opener: {},
    } as unknown as Window);
    const reconnect = await screen.findByRole("button", { name: "Reconnect" });
    fireEvent.click(reconnect);
    await waitFor(() => expect(toast.error).toHaveBeenCalledTimes(1));
    expect(toast.error.mock.calls[0]?.[0]).toMatch(/Five of your bots/);
    expect(closed).toHaveBeenCalledTimes(1);

    reason = "already-connected";
    fireEvent.click(await screen.findByRole("button", { name: "Reconnect" }));
    await waitFor(() => expect(closed).toHaveBeenCalledTimes(2));
    expect(toast.error).toHaveBeenCalledTimes(1);
  });

  it("offers Open chat as a link only while the page holds a live address", async () => {
    const calls = start(
      READY,
      listOf([bot({ state: "waiting_connect", connected_as: null })]),
      {
        "POST /api/telegram-bots/link-a/connect": () =>
          json({ url: "https://t.me/x?start=c", expires_at: ago(-600) }),
      },
    );
    const open = await screen.findByRole("button", {
      name: "Open chat with HartMesh",
    });
    const opened = {
      closed: false,
      location: { replace: rs.fn() },
      opener: {},
    };
    rs.spyOn(window, "open").mockReturnValue(opened as unknown as Window);
    fireEvent.click(open);
    await waitFor(() =>
      expect(opened.location.replace).toHaveBeenCalledWith(
        "https://t.me/x?start=c",
      ),
    );
    expect(calls.some(({ url }) => url.endsWith("/link-a/connect"))).toBe(true);
    const link = await screen.findByRole("link", {
      name: "Open chat with HartMesh",
    });
    expect(link.getAttribute("href")).toBe("https://t.me/x?start=c");
  });
});

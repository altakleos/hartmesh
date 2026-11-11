import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({
  user: {
    id: "alice",
    system_role: "user",
    permissions: ["agents:read"] as string[],
  },
}));
const page = rs.hoisted(() => ({
  mobile: false,
  agents: [] as { name: string }[],
  chooseZone: (_zone: string) => undefined as void,
}));
const toast = rs.hoisted(() => ({ success: rs.fn(), error: rs.fn() }));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
const navigation = rs.hoisted(() => ({ push: rs.fn() }));
rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace/chats/new",
  useRouter: () => ({ push: navigation.push }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("sonner", () => ({ toast }));
rs.mock("@/core/agents/hooks", () => ({
  useAgents: () => ({ agents: page.agents }),
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: i18n.t, locale: "en-US" }),
}));
rs.mock("@/hooks/use-mobile", () => ({ useIsMobile: () => page.mobile }));
rs.mock("@/components/workspace/telegram/qr-code", () => ({
  TelegramQrCode: ({ value, label }: { value: string; label: string }) => (
    <svg data-testid="qr" data-value={value} aria-label={label} />
  ),
}));
const settingsDialog = rs.hoisted(() => ({ open: rs.fn() }));
rs.mock("@/components/workspace/settings/settings-dialog-store", () => ({
  openSettingsDialog: settingsDialog.open,
}));
rs.mock("@/components/ui/select", () => ({
  Select: ({
    onValueChange,
    children,
  }: {
    onValueChange: (value: string) => void;
    children: ReactNode;
  }) => {
    page.chooseZone = onValueChange;
    return <>{children}</>;
  },
  SelectTrigger: () => null,
  SelectValue: () => null,
  SelectContent: () => null,
  SelectItem: () => null,
}));

import { TelegramSetupDialog } from "@/components/workspace/telegram/setup-dialog";
import {
  openTelegramSetup,
  resetTelegramPageState,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import { fetch } from "@/core/api/fetcher";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { Bot, BotList, Settings } from "@/core/telegram-bots/api";
import type { AgentTarget } from "@/core/telegram-bots/web-pages";

import {
  ago,
  instance,
  INSTANCE_FEATURE,
  bot,
  clearClients,
  json,
  listOf,
  queries,
  renderWithClient,
  server,
} from "./support";

i18n.t = createEnUS("HartMesh");

const ADD = "POST /api/telegram-bots";
const LIST = "GET /api/telegram-bots";
const settings: Settings = {
  timezone: "Europe/Lisbon",
  locale: "en-US",
  quiet_enabled: true,
  quiet_start: "21:00",
  quiet_end: "07:00",
  notify: "all",
  summary_enabled: true,
  summary_time: "08:00",
};
// Built at run time: no tracked file may hold text of a token's shape.
const token = `${"1".repeat(9)}:${"A".repeat(35)}`;
const connect = { url: "https://t.me/x?start=code", expires_at: ago(-600) };
const waiting = (overrides: Partial<Bot> = {}) =>
  bot({
    state: "waiting_connect",
    connected_as: null,
    connect_expires_at: connect.expires_at,
    ...overrides,
  });
let state = listOf([]);

interface Start {
  list?: () => BotList;
  telegram?: Record<string, unknown>;
  routes?: Record<string, () => Response | Promise<Response>>;
  target?: AgentTarget;
}

function start({
  list = () => listOf([]),
  telegram,
  routes,
  target = { kind: "chat" },
}: Start = {}) {
  const { calls, handle } = server({
    "GET /api/v1/auth/users?ids=alice": () =>
      json({
        users: [
          {
            id: "alice",
            name: "Alice",
            email: "alice@example.test",
            active: true,
          },
        ],
      }),
    "GET /api/v1/auth/users?ids=bob": () =>
      json({
        users: [
          { id: "bob", name: "Bob", email: "bob@example.test", active: true },
        ],
      }),
    "GET /api/features": () =>
      json({
        agents_api: { enabled: true },
        ...(target.kind === "agent"
          ? { agent_instances: INSTANCE_FEATURE }
          : {}),
        telegram: { enabled: true, bots: "ready", reason: null, ...telegram },
      }),
    [LIST]: () => json(list()),
    ...(target.kind === "agent"
      ? {
          [`GET /api/agent-instances/${target.id}?include_deleted=true`]: () =>
            json(instance({ id: target.id, name: target.name })),
        }
      : {}),
    ...routes,
  });
  rs.mocked(fetch).mockImplementation(handle);
  const tree = renderWithClient(
    <FileActionLifetimeProvider>
      <TelegramSetupDialog target={target} />
    </FileActionLifetimeProvider>,
  );
  return {
    tree,
    calls,
    added: () =>
      calls.filter(
        ({ url, method }) => url === "/api/telegram-bots" && method === "POST",
      ),
  };
}

function OpenedSetup() {
  const { setup } = useTelegramPageState();
  return <output data-testid="opened">{setup ? "open" : "closed"}</output>;
}

const field = async () => screen.findByLabelText<HTMLInputElement>("Bot token");
const paste = async (text: string) =>
  fireEvent.paste(await field(), { clipboardData: { getData: () => text } });
const refusal = (status: number, reason: string, extra = {}) =>
  json({ detail: "not shown", reason, ...extra }, status);

beforeEach(() => {
  state = listOf([]);
  page.mobile = false;
  page.agents = [];
});

afterEach(() => {
  cleanup();
  clearClients();
  resetTelegramPageState();
  rs.restoreAllMocks();
  rs.mocked(fetch).mockReset();
  rs.unstubAllGlobals();
});

describe("the set-up dialog", () => {
  it("shows the paragraph in full without a bot and one line afterwards", async () => {
    start();
    await screen.findByText("Before you connect");
    screen.getByText(/Bot chats are not end-to-end encrypted/);
    cleanup();
    clearClients();

    start({ list: () => listOf([bot({ agent_kind: "custom_agent" })]) });
    await screen.findByText(/What Telegram can read is explained/);
    expect(screen.queryByText(/Bot chats are not end-to-end/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Read it" }));
    screen.getByText(/Bot chats are not end-to-end encrypted/);
  });

  it("shows the voice line only when voice notes are available", async () => {
    start({ telegram: { voice: "available", voice_provider: "openai" } });
    await screen.findByText(/Voice notes are turned into text by OpenAI/);
    cleanup();
    clearClients();

    start();
    await screen.findByText("Before you connect");
    expect(screen.queryByText(/Voice notes are turned into text/)).toBeNull();
  });

  it("copies the suggested username and opens BotFather", async () => {
    const writeText = rs.fn(async () => undefined);
    rs.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    const open = rs.spyOn(window, "open").mockReturnValue(null);
    start();
    fireEvent.click(
      await screen.findByRole("button", {
        name: "Copy username and open BotFather",
      }),
    );
    expect(open.mock.calls[0]?.[0]).toBe("https://t.me/BotFather");
    await screen.findByText(/^Copied chat_/);
    expect(writeText.mock.calls[0]?.[0 as never]).toMatch(/^chat_.+_bot$/);
  });

  it("sends a pasted token at once and empties the field", async () => {
    const { added } = start({
      routes: { [ADD]: () => json({ bot: waiting(), connect }, 201) },
    });
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(1));
    expect(added()[0]?.body).toMatchObject({
      agent_kind: "lead_agent",
      agent_ref: "lead_agent",
      token,
      locale: "en-US",
    });
    expect((added()[0]?.body as { timezone: string }).timezone).toBeTruthy();
    expect((await field()).value).toBe("");
  });

  it("accepts BotFather's whole message", async () => {
    const { added } = start({
      routes: { [ADD]: () => json({ bot: waiting(), connect }, 201) },
    });
    await paste(`Done! Use this token: ${token} Keep it safe.`);
    await waitFor(() => expect(added()).toHaveLength(1));
    expect(added()[0]?.body).toMatchObject({ token });
  });

  it("sends nothing while a token is typed or on leaving the field, and the whole token on Enter", async () => {
    const { added } = start({
      routes: { [ADD]: () => json({ bot: waiting(), connect }, 201) },
    });
    const input = await field();
    for (let length = 1; length <= token.length; length += 9) {
      fireEvent.change(input, { target: { value: token.slice(0, length) } });
    }
    fireEvent.change(input, { target: { value: token } });
    fireEvent.blur(input);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(added()).toEqual([]);
    fireEvent.submit(input.closest("form")!);
    await waitFor(() => expect(added()).toHaveLength(1));
    expect(added()[0]?.body).toMatchObject({ token });
  });

  it("shows refusal.format for text without a token and sends nothing", async () => {
    const { added } = start();
    await paste("hello");
    await screen.findByText(/That is not a bot token/);
    expect(added()).toEqual([]);
  });

  it("shows the sentence of each refusal", async () => {
    const cases: [number, string, Record<string, unknown>, RegExp][] = [
      [400, "token-invalid", {}, /does not recognise this token/],
      [
        409,
        "bot-linked",
        { own: true, agent_name: "Maya" },
        /already connected to Maya/,
      ],
      [409, "bot-linked", { own: false }, /Someone else at your company/],
      [
        409,
        "agent-linked",
        { link_id: "other" },
        /already have a bot for HartMesh/,
      ],
      [502, "telegram-unreachable", {}, /could not reach Telegram/],
      [429, "too-many-codes", {}, /Five of your bots/],
      [404, "agent-not-found", {}, /no longer available to you/],
      [403, "ceiling", {}, /may not connect a bot/],
      [400, "invalid-timezone", {}, /Choose your time zone/],
      [409, "limit-person", {}, /the most one person can have here/],
      [409, "limit-deployment", {}, /the limit is reached/],
      [409, "secret-missing", {}, /is missing/],
      [500, "something-new", {}, /That did not work/],
    ];
    for (const [status, reason, extra, sentence] of cases) {
      start({ routes: { [ADD]: () => refusal(status, reason, extra) } });
      await paste(token);
      await screen.findByText(sentence);
      expect(document.body.textContent).not.toContain("not shown");
      cleanup();
      clearClients();
    }
  });

  it("says a request failed when it gets no answer", async () => {
    start({
      routes: {
        [ADD]: () => {
          throw new TypeError("network");
        },
      },
    });
    await paste(token);
    await screen.findByText("That did not work. Try again.");
  });

  it("offers Use it here for a bot with a webhook", async () => {
    let answer = () => refusal(409, "webhook-set", { webhook_host: "x.test" });
    const { added } = start({ routes: { [ADD]: () => answer() } });
    await paste(token);
    fireEvent.click(await screen.findByRole("button", { name: "Use it here" }));
    expect(added()).toHaveLength(1);
    const asked = await screen.findByText(/Paste the same token once more/);
    expect(asked.closest('[role="status"]')).not.toBeNull();
    expect(screen.queryByRole("alert")).toBeNull();
    expect((await field()).value).toBe("");
    answer = () => json({ bot: waiting(), connect }, 201);
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(2));
    expect(added()[1]?.body).toMatchObject({ token, take_over_webhook: true });
    expect(added()[0]?.body).not.toHaveProperty("take_over_webhook");
  });

  it("takes over no webhook but the one the person agreed to", async () => {
    const other = `${"2".repeat(9)}:${"B".repeat(35)}`;
    let answer = () => refusal(409, "webhook-set", { webhook_host: "x.test" });
    const { added } = start({ routes: { [ADD]: () => answer() } });
    await paste(token);
    fireEvent.click(await screen.findByRole("button", { name: "Use it here" }));
    await screen.findByText(/Paste the same token once more/);
    answer = () => refusal(400, "token-invalid");
    await paste(other);
    await waitFor(() => expect(added()).toHaveLength(2));
    expect(added()[1]?.body).not.toHaveProperty("take_over_webhook");
    await screen.findByText(/does not recognise this token/);
    expect(screen.queryByRole("button", { name: "Use it here" })).toBeNull();
  });

  it("offers no button that would only clear the line when Telegram cannot be reached", async () => {
    start({ routes: { [ADD]: () => refusal(502, "telegram-unreachable") } });
    await paste(token);
    await screen.findByText(/could not reach Telegram/);
    expect(screen.queryByRole("button", { name: "Try again" })).toBeNull();
    expect((await field()).disabled).toBe(false);
  });

  it("moves on when a lost answer turns out to have added the bot", async () => {
    let state = listOf([]);
    start({
      list: () => state,
      routes: {
        [ADD]: () => {
          state = listOf([waiting()]);
          throw new TypeError("network");
        },
      },
    });
    await paste(token);
    await screen.findByText("3. Open the chat and press Start");
    expect(screen.queryByText("That did not work. Try again.")).toBeNull();
  });

  it("asks for the time zone when the browser reports none", async () => {
    const formatter = Intl.DateTimeFormat();
    rs.spyOn(Intl, "DateTimeFormat").mockReturnValue({
      ...formatter,
      resolvedOptions: () => ({ ...formatter.resolvedOptions(), timeZone: "" }),
    } as Intl.DateTimeFormat);
    const { added } = start({
      routes: { [ADD]: () => json({ bot: waiting(), connect }, 201) },
    });
    await screen.findByText(/did not say which time zone/);
    expect((await field()).disabled).toBe(true);
    act(() => page.chooseZone("Asia/Tokyo"));
    await waitFor(async () => expect((await field()).disabled).toBe(false));
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(1));
    expect(added()[0]?.body).toMatchObject({ timezone: "Asia/Tokyo" });
  });

  it("sends the chosen zone when the server refused the browser's", async () => {
    let answer = () => refusal(400, "invalid-timezone");
    const { added } = start({ routes: { [ADD]: () => answer() } });
    await paste(token);
    await screen.findByText("Choose your time zone, then try again.");
    expect((await field()).disabled).toBe(true);
    act(() => page.chooseZone("Asia/Tokyo"));
    await waitFor(async () => expect((await field()).disabled).toBe(false));
    answer = () => json({ bot: waiting(), connect }, 201);
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(2));
    expect(added()[1]?.body).toMatchObject({ timezone: "Asia/Tokyo" });
  });

  it("offers the bot a person already has for this target", async () => {
    let state = listOf([]);
    start({
      list: () => state,
      routes: {
        [ADD]: () => {
          state = listOf([bot({ id: "other", agent_kind: "custom_agent" })]);
          return refusal(409, "agent-linked", { link_id: "other" });
        },
      },
    });
    await paste(token);
    const open = await screen.findByRole("link", { name: "Open in Telegram" });
    expect(open.getAttribute("href")).toBe("https://t.me/chat_abc_bot");
  });

  it("does not ask a later bot for a time zone", async () => {
    const formatter = Intl.DateTimeFormat();
    rs.spyOn(Intl, "DateTimeFormat").mockReturnValue({
      ...formatter,
      resolvedOptions: () => ({ ...formatter.resolvedOptions(), timeZone: "" }),
    } as Intl.DateTimeFormat);
    start({ list: () => listOf([], { settings }) });
    await field();
    expect(screen.queryByText(/did not say which time zone/)).toBeNull();
  });

  it("waits for Start, shows the hint after 60 seconds, then Done", async () => {
    let state = listOf([waiting({ profile_synced: false })]);
    const timers = rs.spyOn(globalThis, "setTimeout");
    const { calls } = start({ list: () => state });
    await screen.findByText("Waiting for you to press Start in Telegram…");
    screen.getByText(/This is @chat_abc_bot/);
    expect(screen.queryByText(/Nothing happening\?/)).toBeNull();
    const hint = timers.mock.calls.find(([, delay]) => delay === 60_000);
    act(() => (hint?.[0] as () => void)());
    screen.getByText(/Nothing happening\?/);

    const reads = () =>
      calls.filter(
        ({ url, method }) => url === "/api/telegram-bots" && method === "GET",
      ).length;
    const before = reads();
    state = listOf([bot()]);
    await screen.findByText("HartMesh is on Telegram", undefined, {
      timeout: 4000,
    });
    expect(reads()).toBeGreaterThan(before);
    screen.getByText(/Connected as Rosa/);
  });

  it.each([
    [
      "a conflict",
      { health: "conflict", connect_expires_at: null },
      /Another program is using this bot's token/,
    ],
    [
      "a stop",
      { health: "stopped" },
      /This bot stopped after an internal error/,
    ],
    [
      "a refused token",
      { health: "token_rejected" },
      /Telegram no longer accepts this bot's token/,
    ],
  ])(
    "says why a bot cannot hear Start after %s",
    async (_name, fault, sentence) => {
      openTelegramSetup({ kind: "chat" });
      render(<OpenedSetup />);
      start({ list: () => listOf([waiting(fault)]) });
      await screen.findByText(sentence);
      expect(screen.queryByText(/Waiting for you to press Start/)).toBeNull();
      expect(screen.queryByTestId("qr")).toBeNull();
      fireEvent.click(
        screen.getByRole("button", { name: "Open Telegram settings" }),
      );
      expect(screen.getByTestId("opened").textContent).toBe("closed");
      expect(navigation.push).toHaveBeenCalledWith(
        "/workspace/chats/new?settings=channels&telegram=chat",
      );
    },
  );

  it("says why from the section's notice when Telegram itself is stopped", async () => {
    start({
      list: () => listOf([waiting()]),
      telegram: { bots: "unavailable", reason: "polled-elsewhere" },
    });
    await screen.findByText(/several bots conflict with another program/);
    expect(screen.queryByText("Paused")).toBeNull();
  });

  it("leaves a bot being prepared on its steps", async () => {
    start({
      list: () =>
        listOf([waiting({ profile_synced: false, created_at: ago(5) })]),
    });
    await screen.findByText("3. Open the chat and press Start");
    expect(screen.queryByText(/This bot cannot connect/)).toBeNull();
  });

  it("notices a fault clearing behind a link that expired", async () => {
    let fault: Partial<Bot> = { health: "conflict" };
    start({
      list: () => listOf([waiting({ connect_expires_at: ago(60), ...fault })]),
    });
    await screen.findByText(/Another program is using this bot's token/);
    fault = {};
    await screen.findByText("This link has expired.", undefined, {
      timeout: 4000,
    });
  });

  it("offers Open chat for a waiting bot with no address in this page", async () => {
    const opened = {
      closed: false,
      location: { replace: rs.fn() },
      opener: {},
    };
    rs.spyOn(window, "open").mockReturnValue(opened as unknown as Window);
    start({
      list: () => listOf([waiting()]),
      routes: { "POST /api/telegram-bots/link-a/connect": () => json(connect) },
    });
    fireEvent.click(
      await screen.findByRole("button", { name: "Open chat with HartMesh" }),
    );
    await waitFor(() =>
      expect(opened.location.replace).toHaveBeenCalledWith(connect.url),
    );
    const link = await screen.findByRole("link", {
      name: "Open chat with HartMesh",
    });
    expect(link.getAttribute("href")).toBe(connect.url);
  });

  it("asks for one link however often Open chat is pressed", async () => {
    rs.spyOn(window, "open").mockReturnValue(null);
    const { calls } = start({
      list: () => listOf([waiting()]),
      routes: { "POST /api/telegram-bots/link-a/connect": () => json(connect) },
    });
    const open = await screen.findByRole("button", {
      name: "Open chat with HartMesh",
    });
    fireEvent.click(open);
    fireEvent.click(open);
    await screen.findByRole("link", { name: "Open chat with HartMesh" });
    expect(calls.filter(({ url }) => url.endsWith("/connect"))).toHaveLength(1);
  });

  it("does not offer an address the list no longer names", async () => {
    let state = listOf([waiting()]);
    start({
      list: () => state,
      routes: { "POST /api/telegram-bots/link-a/connect": () => json(connect) },
    });
    rs.spyOn(window, "open").mockReturnValue(null);
    fireEvent.click(
      await screen.findByRole("button", { name: "Open chat with HartMesh" }),
    );
    await screen.findByRole("link", { name: "Open chat with HartMesh" });
    state = listOf([waiting({ connect_expires_at: ago(-900) })]);
    await screen.findByRole(
      "button",
      { name: "Open chat with HartMesh" },
      { timeout: 4000 },
    );
    expect(
      screen.queryByRole("link", { name: "Open chat with HartMesh" }),
    ).toBeNull();
  });

  it("announces the steps that change by themselves, from one region that stays", async () => {
    let state = listOf([]);
    const { added } = start({
      list: () => state,
      routes: {
        [ADD]: () => {
          state = listOf([waiting()]);
          return json({ bot: waiting(), connect }, 201);
        },
      },
    });
    await field();
    const region = screen.getByRole("status");

    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(1));
    await screen.findByRole("link", { name: "Open chat with HartMesh" });
    state = listOf([bot()], { settings });
    const done = await screen.findByText(
      "HartMesh is on Telegram",
      {},
      { timeout: 4000 },
    );

    expect(region.isConnected).toBe(true);
    expect(region.contains(done)).toBe(true);
  });

  it("offers Get a new link when the link expired", async () => {
    let state = listOf([waiting({ connect_expires_at: ago(1) })]);
    const { calls } = start({
      list: () => state,
      routes: {
        "POST /api/telegram-bots/link-a/connect": () => {
          state = listOf([waiting()]);
          return json(connect);
        },
      },
    });
    await screen.findByText("This link has expired.");
    expect(screen.queryByTestId("qr")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Get a new link" }));
    await screen.findByRole("link", { name: "Open chat with HartMesh" });
    expect(calls.some(({ url }) => url.endsWith("/connect"))).toBe(true);
  });

  it("waits another minute for the hint after a link is renewed", async () => {
    let state = listOf([waiting()]);
    const timers = rs.spyOn(globalThis, "setTimeout");
    const intervals = rs.spyOn(globalThis, "setInterval");
    start({
      list: () => state,
      routes: {
        "POST /api/telegram-bots/link-a/connect": () => {
          state = listOf([waiting()]);
          return json(connect);
        },
      },
    });
    await screen.findByText("Waiting for you to press Start in Telegram…");
    const hint = () =>
      timers.mock.calls.filter(([, delay]) => delay === 60_000).at(-1)?.[0];
    act(() => (hint() as () => void)());
    screen.getByText(/Nothing happening\?/);

    state = listOf([waiting({ connect_expires_at: ago(1) })]);
    const tick = intervals.mock.calls.find(([, delay]) => delay === 5000)?.[0];
    await act(async () => {
      await queries().invalidateQueries();
      (tick as () => void)();
    });
    fireEvent.click(
      await screen.findByRole("button", { name: "Get a new link" }),
    );
    await screen.findByText("Waiting for you to press Start in Telegram…");
    expect(screen.queryByText(/Nothing happening\?/)).toBeNull();
  });

  it("shows the notice when the deployment refuses a new link", async () => {
    const { calls } = start({
      list: () => listOf([waiting({ connect_expires_at: ago(1) })]),
      routes: {
        "POST /api/telegram-bots/link-a/connect": () =>
          refusal(409, "polled-elsewhere"),
      },
    });
    const features = () =>
      calls.filter(({ url }) => url === "/api/features").length;
    const renew = await screen.findByRole("button", { name: "Get a new link" });
    const reads = features();
    fireEvent.click(renew);
    await screen.findByText(/several bots conflict with another program/);
    await waitFor(() => expect(features()).toBeGreaterThan(reads));
  });

  it("says when too many links are waiting", async () => {
    start({
      list: () => listOf([waiting({ connect_expires_at: ago(1) })]),
      routes: {
        "POST /api/telegram-bots/link-a/connect": () =>
          refusal(429, "too-many-codes"),
      },
    });
    fireEvent.click(
      await screen.findByRole("button", { name: "Get a new link" }),
    );
    await screen.findByText(/Five of your bots/);
  });

  it("draws the QR codes at desktop width only", async () => {
    const { added } = start({
      routes: {
        [ADD]: () => {
          state = listOf([waiting()]);
          return json({ bot: waiting(), connect }, 201);
        },
      },
      list: () => state,
    });
    expect(added()).toEqual([]);
    fireEvent.click(
      await screen.findByRole("button", { name: "Continue on your phone" }),
    );
    expect(screen.getByTestId("qr").getAttribute("data-value")).toContain(
      "/workspace?settings=channels&telegram=chat",
    );
    await paste(token);
    await waitFor(() =>
      expect(screen.getByTestId("qr").getAttribute("data-value")).toBe(
        connect.url,
      ),
    );
    cleanup();
    clearClients();
    resetTelegramPageState();

    page.mobile = true;
    state = listOf([]);
    start({ list: () => state });
    await field();
    expect(
      screen.queryByRole("button", { name: "Continue on your phone" }),
    ).toBeNull();
    expect(screen.queryByTestId("qr")).toBeNull();
  });

  it("shows language and time zone on Done for a first bot only", async () => {
    let state = listOf([], { settings: null });
    const { added } = start({
      list: () => state,
      routes: {
        [ADD]: () => {
          state = listOf([bot()], { settings });
          return json({ bot: bot(), connect }, 201);
        },
      },
    });
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(1));
    await screen.findByText(
      /Bot messages are in English; times are in Europe\/Lisbon\./,
    );
    cleanup();
    clearClients();

    start({ list: () => listOf([bot()], { settings }) });
    await screen.findByText("HartMesh is on Telegram");
    expect(screen.queryByText(/Bot messages are in/)).toBeNull();
  });

  it("Change opens the settings over the page the dialog was opened on", async () => {
    start({ list: () => listOf([bot()], { settings }) });
    await screen.findByText("HartMesh is on Telegram");
    cleanup();
    clearClients();

    let state = listOf([], { settings: null });
    const { added } = start({
      list: () => state,
      routes: {
        [ADD]: () => {
          state = listOf([bot()], { settings });
          return json({ bot: bot(), connect }, 201);
        },
      },
    });
    await paste(token);
    await waitFor(() => expect(added()).toHaveLength(1));
    fireEvent.click(await screen.findByRole("button", { name: "Change" }));
    expect(settingsDialog.open).toHaveBeenCalledWith("channels");
  });

  it("names the next target that has no bot on Done", async () => {
    page.agents = [{ name: "alpha" }];
    start({ list: () => listOf([bot()]) });
    await screen.findByRole("button", { name: "Next: alpha" });
  });

  it("offers no next target once the limit is reached", async () => {
    page.agents = [{ name: "alpha" }];
    start({
      list: () => listOf([bot()], { limits: { per_person: 1, used: 1 } }),
    });
    await screen.findByText("HartMesh is on Telegram");
    expect(screen.queryByRole("button", { name: "Next: alpha" })).toBeNull();
  });

  it("returns to waiting when Disconnect is pressed on Done", async () => {
    let state = listOf([bot()]);
    start({
      list: () => state,
      routes: {
        "POST /api/telegram-bots/link-a/disconnect": () => {
          state = listOf([waiting()]);
          return json({ bot: waiting(), connect });
        },
      },
    });
    fireEvent.click(await screen.findByRole("button", { name: "Disconnect" }));
    await screen.findByText("Waiting for you to press Start in Telegram…");
  });

  it("takes a bot already disconnected from another tab as done", async () => {
    let state = listOf([bot()]);
    start({
      list: () => state,
      routes: {
        "POST /api/telegram-bots/link-a/disconnect": () => {
          state = listOf([waiting()]);
          return refusal(409, "not-connected");
        },
      },
    });
    fireEvent.click(await screen.findByRole("button", { name: "Disconnect" }));
    await screen.findByText("3. Open the chat and press Start");
    expect(toast.error).not.toHaveBeenCalled();
  });
});

describe("instance setup privacy", () => {
  it.each([false, true])(
    "keeps the shared-reader notice visible with an existing bot=%s",
    async (hasBot) => {
      start({
        target: {
          kind: "agent",
          id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          name: "Mira",
        },
        list: () =>
          listOf(hasBot ? [bot()] : [], {
            read_by_others: ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
          }),
      });
      await screen.findByText(
        /People at your company who can see all of Mira's work/,
      );
      expect(
        screen.getByText(/Files you send are kept in Mira's files/),
      ).toBeDefined();
    },
  );
  it("explains protected delivery only when the list carries that policy", async () => {
    start({
      target: {
        kind: "agent",
        id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        name: "Mira",
      },
      list: () =>
        listOf([], {
          policies: [
            {
              instance_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
              policy: "protected",
            },
          ],
        }),
    });
    await screen.findByText(/A manager has set Telegram for Mira/);
    expect(
      screen.queryByText(
        /People at your company who can see all of Mira's work/,
      ),
    ).toBeNull();
  });
  it("does not infer sharing or protection merely from being an instance", async () => {
    start({
      target: {
        kind: "agent",
        id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        name: "Mira",
      },
    });
    await field();
    expect(screen.queryByText(/People at your company/)).toBeNull();
    expect(screen.queryByText(/A manager has set Telegram/)).toBeNull();
  });
});

it("shows current access refusal instead of success for a connected instance bot", async () => {
  start({
    target: {
      kind: "agent",
      id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      name: "Mira",
    },
    list: () =>
      listOf([
        bot({
          agent_kind: "instance",
          agent_ref: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          agent_name: "Mira",
        }),
      ]),
    routes: {
      "GET /api/agent-instances/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?include_deleted=true":
        () => json(instance({ permissions: 2 })),
    },
  });
  await screen.findByText(
    "You no longer have access to Mira, so this bot no longer answers.",
  );
  expect(screen.queryByText(/Mira is connected/)).toBeNull();
  expect(screen.queryByLabelText("Bot token")).toBeNull();
});

it("does not offer a token field when the current employee policy is off", async () => {
  start({
    target: {
      kind: "agent",
      id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      name: "Mira",
    },
    list: () =>
      listOf([], {
        policies: [
          { instance_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", policy: "off" },
        ],
      }),
  });
  await screen.findByText(/Telegram is turned off for Mira/);
  expect(screen.queryByLabelText("Bot token")).toBeNull();
});

it("explains an execution refusal returned after a token was submitted", async () => {
  start({
    target: {
      kind: "agent",
      id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      name: "Mira",
    },
    routes: {
      "POST /api/telegram-bots": () =>
        json(
          {
            reason: "execution-unavailable",
            execution_reason: "operator-hold",
            detail: "never show provider internals",
          },
          409,
        ),
    },
  });
  const input = await field();
  fireEvent.paste(input, { clipboardData: { getData: () => token } });
  await screen.findByText(/AI assistants and AI employees are paused by/i);
  expect(screen.queryByText(/never show provider internals/)).toBeNull();
});

it("offers a currently connectable instance after finishing the ordinary bot", async () => {
  start({
    list: () => listOf([bot()]),
    routes: {
      "GET /api/features": () =>
        json({
          telegram: { enabled: true, bots: "ready", reason: null },
          agent_instances: INSTANCE_FEATURE,
        }),
      "GET /api/agent-instances?limit=100&offset=0&custody=personal": () =>
        json({ instances: [] }),
      "GET /api/agent-instances?limit=100&offset=0&custody=company": () =>
        json({ instances: [instance()] }),
      [`GET /api/agent-instances/${"a".repeat(32)}?include_deleted=true`]: () =>
        json(instance()),
    },
  });
  await screen.findByRole("button", { name: /Mira/ });
});

it("retires a submitted token when current Use disappears and drops its late answer", async () => {
  let permissions = 7;
  let finish!: (answer: Response) => void;
  const { calls } = start({
    target: { kind: "agent", id: "a".repeat(32), name: "Mira" },
    routes: {
      [`GET /api/agent-instances/${"a".repeat(32)}?include_deleted=true`]: () =>
        json(instance({ permissions })),
      "POST /api/telegram-bots": () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
  });
  await paste(token);
  await waitFor(() => expect(finish).toBeDefined());
  const sent = rs
    .mocked(fetch)
    .mock.calls.find(([, init]) => init?.method === "POST")!;
  permissions = 2;
  await act(async () => {
    await queries().invalidateQueries({ queryKey: ["agent-instances"] });
  });
  await waitFor(() => expect(screen.queryByLabelText("Bot token")).toBeNull());
  expect(sent[1]?.signal?.aborted).toBe(true);
  const reads = calls.filter(
    (call) => call.url === "/api/telegram-bots",
  ).length;
  await act(async () =>
    finish(
      json({
        bot: waiting({ agent_kind: "instance", agent_ref: "a".repeat(32) }),
        connect,
      }),
    ),
  );
  expect(
    calls.filter((call) => call.url === "/api/telegram-bots"),
  ).toHaveLength(reads);
  expect(screen.queryByText("3. Open the chat and press Start")).toBeNull();
});

it("skips a next instance whose current Use was withdrawn", async () => {
  page.agents = [{ name: "Custom" }];
  start({
    list: () => listOf([bot()]),
    routes: {
      "GET /api/features": () =>
        json({
          telegram: { enabled: true, bots: "ready", reason: null },
          agent_instances: INSTANCE_FEATURE,
        }),
      "GET /api/agent-instances?limit=100&offset=0&custody=personal": () =>
        json({ instances: [] }),
      "GET /api/agent-instances?limit=100&offset=0&custody=company": () =>
        json({ instances: [instance()] }),
      [`GET /api/agent-instances/${"a".repeat(32)}?include_deleted=true`]: () =>
        json(instance({ permissions: 2 })),
    },
  });
  await screen.findByRole("button", { name: /Custom/ });
  expect(screen.queryByRole("button", { name: /Mira/ })).toBeNull();
});

it("explains a policy refusal after submit without claiming a bot was kept", async () => {
  start({
    target: { kind: "agent", id: "a".repeat(32), name: "Mira" },
    routes: {
      "POST /api/telegram-bots": () => refusal(409, "policy-off"),
    },
  });
  await paste(token);
  await screen.findByText(/Telegram is turned off for Mira/);
  expect(screen.queryByText(/The bot is kept/)).toBeNull();
});

it("names an instance that disappeared before setup without offering a token", async () => {
  start({
    target: { kind: "agent", id: "a".repeat(32), name: "Mira" },
    routes: {
      [`GET /api/agent-instances/${"a".repeat(32)}?include_deleted=true`]: () =>
        json({}, 404),
    },
  });
  await screen.findByText(
    "Mira is no longer available to you, so a bot cannot be connected.",
  );
  expect(screen.queryByLabelText("Bot token")).toBeNull();
});

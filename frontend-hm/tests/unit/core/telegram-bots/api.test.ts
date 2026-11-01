import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));

import { fetch as fetcher } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";
import { createZhCN } from "@/core/i18n/locales/zh-CN";
import {
  acknowledgeTelegramConflict,
  addTelegramBot,
  applyTelegramProfile,
  disconnectAllTelegramBots,
  disconnectTelegramBot,
  getTelegramAdminSummary,
  listTelegramBots,
  newTelegramOperationId,
  removeTelegramBot,
  replaceTelegramToken,
  requestTelegramConnect,
  retryTelegramBot,
  saveTelegramSettings,
  TelegramRefusal,
  telegramRefusalKey,
} from "@/core/telegram-bots/api";
import { type TelegramKey, telegramText } from "@/core/telegram-bots/text";

const mockedFetch = rs.mocked(fetcher);
const base = "/backend/api/telegram-bots";

function answer(status: number, body?: unknown): Response {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
  });
}

beforeEach(() => {
  mockedFetch.mockReset();
  mockedFetch.mockImplementation(async () => answer(200, {}));
});

describe("the telegram api", () => {
  it("each call uses its route and method", async () => {
    const add = {
      agent_kind: "lead_agent",
      agent_ref: "lead_agent",
      token: "t",
      timezone: "UTC",
      locale: "en-US",
    };
    const replace = { token: "t", operation_id: "o" };
    const calls: [() => Promise<unknown>, string, string, unknown][] = [
      [listTelegramBots, "", "GET", undefined],
      [() => addTelegramBot(add), "", "POST", add],
      [
        () => replaceTelegramToken("a b", replace),
        "/a%20b/token",
        "POST",
        replace,
      ],
      [() => requestTelegramConnect("x"), "/x/connect", "POST", undefined],
      [() => disconnectTelegramBot("x"), "/x/disconnect", "POST", undefined],
      [disconnectAllTelegramBots, "/disconnect-all", "POST", undefined],
      [
        () => acknowledgeTelegramConflict("x"),
        "/x/acknowledge-conflict",
        "POST",
        undefined,
      ],
      [() => retryTelegramBot("x"), "/x/retry", "POST", undefined],
      [
        () => retryTelegramBot("x", { takeOverWebhook: true }),
        "/x/retry",
        "POST",
        { take_over_webhook: true },
      ],
      [() => applyTelegramProfile("x"), "/x/profile", "POST", undefined],
      [
        () => saveTelegramSettings({ locale: "zh-CN" }),
        "/settings",
        "PUT",
        { locale: "zh-CN" },
      ],
      [getTelegramAdminSummary, "/admin/summary", "GET", undefined],
    ];
    for (const [run, path, method, body] of calls) {
      mockedFetch.mockClear();
      await run();
      const [url, init] = mockedFetch.mock.calls[0]!;
      expect(url).toBe(`${base}${path}`);
      expect(init?.method ?? "GET").toBe(method);
      expect(init?.body ? JSON.parse(init.body as string) : undefined).toEqual(
        body,
      );
    }
  });

  it("removes a bot with a 204 and no body", async () => {
    mockedFetch.mockResolvedValueOnce(answer(204));
    await expect(removeTelegramBot("x")).resolves.toBeUndefined();
    expect(mockedFetch.mock.calls[0]?.[1]?.method).toBe("DELETE");
  });

  it("a refusal keeps its status, reason and extra keys", async () => {
    mockedFetch.mockResolvedValueOnce(
      answer(409, {
        detail: "This bot is connected to another service.",
        reason: "webhook-set",
        webhook_host: "example.test",
        own: true,
        agent_name: "Maya",
        link_id: "link",
        execution_reason: "operator-hold",
      }),
    );
    const refusal = (await listTelegramBots().catch(
      (error: unknown) => error,
    )) as TelegramRefusal;
    expect(refusal).toBeInstanceOf(TelegramRefusal);
    expect(refusal).toMatchObject({
      status: 409,
      reason: "webhook-set",
      detail: "This bot is connected to another service.",
      webhook_host: "example.test",
      own: true,
      agent_name: "Maya",
      link_id: "link",
      execution_reason: "operator-hold",
    });
  });

  it("a failure without a readable body has no reason", async () => {
    mockedFetch.mockResolvedValueOnce(new Response("gateway", { status: 502 }));
    const refusal = (await listTelegramBots().catch(
      (error: unknown) => error,
    )) as TelegramRefusal;
    expect(refusal).toMatchObject({ status: 502, reason: null, detail: null });
    expect(telegramRefusalKey(refusal)).toBe("refusal.failed");
  });

  it("every reason gives its key", () => {
    const keys: [string, TelegramKey][] = [
      ["token-invalid", "refusal.rejected"],
      ["agent-linked", "refusal.agentLinked"],
      ["webhook-set", "refusal.webhook"],
      ["telegram-unreachable", "refusal.unreachable"],
      ["too-many-codes", "refusal.tooManyCodes"],
      ["agent-not-found", "refusal.agentGone"],
      ["ceiling", "refusal.ceiling"],
      ["timezone-required", "refusal.noTimezone"],
      ["invalid-timezone", "refusal.noTimezone"],
      ["limit-person", "unavailable.limitPerson"],
      ["limit-deployment", "unavailable.limitAll"],
      ["not-found", "refusal.gone"],
      ["replacement-gone", "token.completedRemoved"],
      ["telegram-off", "unavailable.off"],
      ["secret-missing", "unavailable.secretMissing"],
      ["secret-invalid", "unavailable.secretInvalid"],
      ["host-changed", "unavailable.hostChanged"],
      ["polled-elsewhere", "unavailable.polledElsewhere"],
      ["misconfigured", "unavailable.unknown"],
    ];
    for (const [reason, key] of keys) {
      expect(telegramRefusalKey({ reason })).toBe(key);
    }
    expect(telegramRefusalKey({ reason: "bot-linked", own: true })).toBe(
      "refusal.inUseSelf",
    );
    expect(telegramRefusalKey({ reason: "bot-linked", own: false })).toBe(
      "refusal.inUseOther",
    );
    expect(telegramRefusalKey({ reason: "something-new" })).toBe(
      "refusal.failed",
    );
    expect(telegramRefusalKey({ reason: "constructor" })).toBe(
      "refusal.failed",
    );
  });

  it("a token equal to the stored one selects token.same", () => {
    expect(telegramRefusalKey({ reason: "token-same" })).toBe("token.same");
  });

  it("makes an operation id the server accepts", () => {
    expect(newTelegramOperationId()).toMatch(/^[0-9a-f]{32}$/);
    expect(newTelegramOperationId()).not.toBe(newTelegramOperationId());
  });
});

describe("the telegram strings", () => {
  const values = {
    name: "Maya",
    telegramName: "Rosa",
    time: "today",
    count: 2,
    admin: false,
    provider: "Acme",
  };

  it("resolves a dotted key and fills its placeholders", () => {
    const strings = createEnUS("HartMesh").telegramBots;
    expect(telegramText(strings, "action.connect")).toBe("Connect Telegram");
    expect(telegramText(strings, "state.connected", values)).toBe(
      "Connected as Rosa",
    );
    expect(telegramText(strings, "chatName")).toBe("HartMesh");
    expect(strings.sidebar.attention({ count: 1 })).toBe("1 needs attention");
    expect(strings.sidebar.attention({ count: 3 })).toBe("3 need attention");
  });

  it("a text that says whom to ask follows the reader", () => {
    for (const create of [createEnUS, createZhCN]) {
      const strings = create("HartMesh").telegramBots;
      const user = strings.state.stopped({ admin: false, provider: "Acme" });
      const admin = strings.state.stopped({ admin: true, provider: "Acme" });
      expect(user).not.toContain("Acme");
      expect(admin).toContain("Acme");
    }
  });

  it("writes one bot and one person in the administrator's counts", () => {
    const { admin } = createEnUS("HartMesh").telegramBots;
    expect(
      admin.counts({ bots: 1, people: 1, polling: 1, failing: 0, held: 0 }),
    ).toContain("1 bot for 1 person:");
    expect(
      admin.counts({ bots: 2, people: 3, polling: 1, failing: 0, held: 0 }),
    ).toContain("2 bots for 3 people:");
  });

  it("names the product in both languages", () => {
    for (const create of [createEnUS, createZhCN]) {
      const strings = create("Acme Chat").telegramBots;
      expect(strings.chatName).toBe("Acme Chat");
      expect(strings.list.intro).toContain("Acme Chat");
    }
  });
});

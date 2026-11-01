import { expect, test, type Page, type Route } from "@playwright/test";

import { enUS } from "../../src/core/i18n/locales/en-US";

import { mockLangGraphAPI, type MockThread } from "./utils/mock-api";

const strings = enUS.telegramBots;
const READY = { enabled: true, bots: "ready", reason: null };
const NAME = strings.chatName;
const OPEN_CHAT = strings.action.openChat({ name: NAME });
const DEEP_LINK = "/workspace?settings=channels";
// Built at run time: no tracked file may hold text of a token's shape.
const TOKEN = `${"1".repeat(9)}:${"A".repeat(35)}`;
const CONNECT = {
  url: "https://t.me/fixture_bot?start=code",
  expires_at: "2099-01-01T00:00:00Z",
};

function bot(overrides: Record<string, unknown> = {}) {
  return {
    id: "link-a",
    bot_id: 1,
    agent_kind: "lead_agent",
    agent_ref: "lead_agent",
    agent_name: NAME,
    bot_username: "fixture_bot",
    bot_name: NAME,
    url: "https://t.me/fixture_bot",
    origin: "pasted",
    managed_by: null,
    can_manage_bots: false,
    bind_mode: "code",
    state: "connected",
    health: "ok",
    connected_as: "Rosa",
    connect_expires_at: null,
    profile_synced: true,
    acts_paused_since: null,
    probing_ended: false,
    limited_since: null,
    delivery_pending: 0,
    delivery_degraded: false,
    delivery_failed: 0,
    webhook_host: null,
    topics_enabled: false,
    policy: null,
    last_update_at: null,
    created_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

const WAITING = {
  state: "waiting_connect",
  connected_as: null,
  connect_expires_at: "2099-01-01T00:00:00Z",
};
const SETTINGS = {
  timezone: "Europe/Lisbon",
  locale: "en-US",
  quiet_enabled: true,
  quiet_start: "21:00",
  quiet_end: "07:00",
  notify: "all",
  summary_enabled: true,
  summary_time: "08:00",
};

interface Telegram {
  bots: ReturnType<typeof bot>[];
  settings: typeof SETTINGS | null;
  /** The answer of `METHOD /path`, the path after `/api/telegram-bots`. */
  answers: Record<string, (body: unknown) => [number, unknown]>;
  calls: { method: string; path: string; body: unknown }[];
}

function refused(status: number, reason: string, extra = {}) {
  return (): [number, unknown] => [
    status,
    { detail: "never shown", reason, ...extra },
  ];
}

async function mockTelegram(
  page: Page,
  {
    telegram = READY as unknown,
    bots = [],
    settings = null,
    answers = {},
    threads,
  }: {
    telegram?: unknown;
    bots?: ReturnType<typeof bot>[];
    settings?: typeof SETTINGS | null;
    answers?: Telegram["answers"];
    threads?: MockThread[];
  } = {},
): Promise<Telegram> {
  const state: Telegram = { bots, settings, answers, calls: [] };
  mockLangGraphAPI(page, {
    features: { telegram: telegram as Record<string, unknown> | null },
    threads,
  });
  const handle = async (route: Route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname.replace(
      "/api/telegram-bots",
      "",
    );
    const method = request.method();
    const key = `${method} ${path}`;
    const body: unknown = request.postDataJSON();
    if (key === "GET " || key === "GET /") {
      return route.fulfill({
        json: {
          bots: state.bots,
          limits: { per_person: 10, used: state.bots.length },
          settings: state.settings,
          quick_setup: {
            available: false,
            manager_link_id: null,
            manager_username: null,
          },
          pending_creation: null,
          removed_bots: [],
          policies: [],
          read_by_others: [],
        },
      });
    }
    if (key === "GET /admin/summary") {
      return route.fulfill({
        json: {
          enabled: true,
          reason: null,
          bots: 3,
          people: 2,
          polling: 2,
          failing: 1,
          held: 0,
          limits: { per_person: 10, deployment: 100 },
          voice: "available",
          voice_provider: "openai",
        },
      });
    }
    state.calls.push({ method, path, body });
    const answer = state.answers[key];
    const [status, json] = answer ? answer(body) : [200, {}];
    return route.fulfill({ status, json });
  };
  await page.route("**/api/telegram-bots", handle);
  await page.route("**/api/telegram-bots/**", handle);
  return state;
}

const section = (page: Page) =>
  page.getByRole("heading", { name: strings.list.title });
const paste = (page: Page, text: string) =>
  page.getByLabel(strings.setup.step2.label).evaluate((input, value) => {
    const data = new DataTransfer();
    data.setData("text", value);
    input.dispatchEvent(
      new ClipboardEvent("paste", {
        clipboardData: data,
        bubbles: true,
        cancelable: true,
      }),
    );
  }, text);

async function openSetup(page: Page) {
  await page.goto(DEEP_LINK);
  await expect(section(page)).toBeVisible();
  await page
    .getByRole("button", { name: strings.action.connect, exact: true })
    .first()
    .click();
}

test.describe("Telegram bots", () => {
  test("first bot: paste to Done, with the paragraph shown once", async ({
    page,
  }) => {
    const state = await mockTelegram(page, {
      telegram: { ...READY, voice: "available", voice_provider: "openai" },
      answers: {
        "POST ": () => {
          state.bots = [bot({ ...WAITING })];
          state.settings = SETTINGS;
          return [201, { bot: state.bots[0], connect: CONNECT }];
        },
      },
    });
    await openSetup(page);
    await expect(page.getByText(strings.setup.before.title)).toBeVisible();
    await expect(
      page.getByText(/after you agree to that service/),
    ).toBeVisible();

    await paste(page, `Use this token: ${TOKEN}`);
    await expect(page.getByRole("link", { name: OPEN_CHAT })).toHaveAttribute(
      "href",
      CONNECT.url,
    );
    expect(state.calls[0]?.body).toMatchObject({
      agent_kind: "lead_agent",
      agent_ref: "lead_agent",
      token: TOKEN,
      locale: "en-US",
    });

    state.bots = [bot()];
    await expect(
      page.getByRole("heading", {
        name: strings.setup.done.title({ name: NAME }),
      }),
    ).toBeVisible({ timeout: 10_000 });
    await expect(
      page.getByText(strings.setup.done.body({ telegramName: "Rosa" })),
    ).toBeVisible();
  });

  test("token refusals: no token, rejected, linked by me, linked by someone else, unreachable", async ({
    page,
  }) => {
    const answers: Telegram["answers"] = {};
    await mockTelegram(page, { answers });
    await openSetup(page);

    await paste(page, "not a token");
    await expect(page.getByText(strings.refusal.format)).toBeVisible();

    const cases: [() => [number, unknown], string][] = [
      [refused(400, "token-invalid"), strings.refusal.rejected],
      [
        refused(409, "bot-linked", { own: true, agent_name: "Maya" }),
        strings.refusal.inUseSelf({ other: "Maya", name: NAME }),
      ],
      [refused(409, "bot-linked", { own: false }), strings.refusal.inUseOther],
      [refused(502, "telegram-unreachable"), strings.refusal.unreachable],
    ];
    for (const [answer, sentence] of cases) {
      answers["POST "] = answer;
      await paste(page, TOKEN);
      await expect(page.getByText(sentence)).toBeVisible();
      await expect(page.getByText("never shown")).toHaveCount(0);
    }
  });

  test("a bot with a webhook is refused and Use it here adds it", async ({
    page,
  }) => {
    const answers: Telegram["answers"] = {
      "POST ": refused(409, "webhook-set", { webhook_host: "x.test" }),
    };
    const state = await mockTelegram(page, { answers });
    await openSetup(page);
    await paste(page, TOKEN);
    await page.getByRole("button", { name: strings.action.useHere }).click();
    await expect(page.getByText(strings.token.retryPaste)).toBeVisible();
    answers["POST "] = () => {
      state.bots = [bot({ ...WAITING })];
      return [201, { bot: state.bots[0], connect: CONNECT }];
    };
    await paste(page, TOKEN);
    await expect(page.getByRole("link", { name: OPEN_CHAT })).toBeVisible();
    expect(state.calls.at(-1)?.body).toMatchObject({
      take_over_webhook: true,
    });
  });

  test("the time zone list appears when the browser reports none", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      const original = Intl.DateTimeFormat;
      const stub = function (...args: []) {
        const formatter = new original(...args);
        const options = formatter.resolvedOptions.bind(formatter);
        formatter.resolvedOptions = () => ({ ...options(), timeZone: "" });
        return formatter;
      };
      Intl.DateTimeFormat = Object.assign(stub, original);
    });
    await mockTelegram(page);
    await openSetup(page);
    await expect(page.getByText(strings.setup.timezone.ask)).toBeVisible();
    await expect(page.getByLabel(strings.setup.step2.label)).toBeDisabled();
  });

  test("the connect link expires and is renewed", async ({ page }) => {
    const state = await mockTelegram(page, {
      bots: [bot({ ...WAITING, connect_expires_at: "2020-01-01T00:00:00Z" })],
      answers: {
        "POST /link-a/connect": () => {
          state.bots = [bot({ ...WAITING })];
          return [200, CONNECT];
        },
      },
    });
    await page.goto(DEEP_LINK);
    await expect(page.getByText(strings.state.expired)).toBeVisible();
    await page.getByRole("button", { name: strings.action.newLink }).click();
    await expect(page.getByRole("link", { name: OPEN_CHAT })).toHaveAttribute(
      "href",
      CONNECT.url,
    );
  });

  test("QR codes at desktop width only", async ({ page }) => {
    const images: string[] = [];
    page.on("request", (request) => {
      if (request.resourceType() === "image") {
        images.push(request.url());
      }
    });
    const state = await mockTelegram(page, {
      answers: {
        "POST ": () => {
          state.bots = [bot({ ...WAITING })];
          return [201, { bot: state.bots[0], connect: CONNECT }];
        },
      },
    });
    await openSetup(page);
    await page.getByRole("button", { name: strings.setup.onPhoneLink }).click();
    await expect(
      page.getByRole("img", { name: strings.setup.onPhoneQr }),
    ).toBeVisible();
    await paste(page, TOKEN);
    await expect(
      page.getByRole("img", { name: strings.setup.step3.qr }),
    ).toBeVisible();
    expect(images.filter((url) => url.includes("code"))).toEqual([]);

    await page.setViewportSize({ width: 390, height: 800 });
    await expect(
      page.getByRole("img", { name: strings.setup.step3.qr }),
    ).toHaveCount(0);
  });

  test("every row of the table shows its sentence and button", async ({
    page,
  }) => {
    const rows: [Record<string, unknown>, string, string | null][] = [
      [{ state: "held" }, strings.state.held, strings.action.reconnect],
      [
        { health: "secret_unreadable" },
        strings.state.unreadableKey,
        strings.action.pasteToken,
      ],
      [
        { health: "token_rejected" },
        strings.state.tokenRejected,
        strings.action.pasteToken,
      ],
      [
        { health: "conflict", webhook_host: "x.test" },
        strings.state.conflictWebhook,
        strings.action.useHere,
      ],
      [
        { health: "conflict" },
        strings.state.conflict,
        strings.action.pasteToken,
      ],
      [
        { health: "conflict", probing_ended: true },
        strings.state.conflictStopped,
        strings.action.tryNow,
      ],
      [{ health: "unreachable" }, strings.state.unreachable, null],
      [
        { state: "blocked" },
        strings.state.blocked({ name: NAME }),
        strings.action.open,
      ],
      [
        { ...WAITING, connect_expires_at: "2020-01-01T00:00:00Z" },
        strings.state.expired,
        strings.action.newLink,
      ],
      [{ ...WAITING }, strings.state.waiting, OPEN_CHAT],
      [
        { delivery_failed: 1 },
        strings.state.deliveryFailed,
        strings.action.open,
      ],
      [
        { profile_synced: false },
        strings.state.profileStale,
        strings.action.applyProfile,
      ],
      [
        {},
        strings.state.connected({ telegramName: "Rosa" }),
        strings.action.open,
      ],
    ];
    const state = await mockTelegram(page);
    for (const [fields, sentence, button] of rows) {
      await test.step(sentence, async () => {
        state.bots = [bot(fields)];
        await page.goto(DEEP_LINK);
        await expect(page.getByText(sentence, { exact: true })).toBeVisible();
        if (button) {
          await expect(
            page
              .locator("[data-telegram-row]")
              .getByText(button, { exact: true })
              .first(),
          ).toBeVisible();
        }
      });
    }
  });

  test("conflict: Try now after checking stopped; That was me after the conflict ended", async ({
    page,
  }) => {
    const state = await mockTelegram(page, {
      bots: [bot({ health: "conflict", probing_ended: true })],
      answers: {
        "POST /link-a/retry": () => {
          state.bots = [bot({ acts_paused_since: "2026-01-01T00:00:00Z" })];
          return [200, { bot: state.bots[0] }];
        },
        "POST /link-a/acknowledge-conflict": () => {
          state.bots = [bot()];
          return [200, { bot: state.bots[0] }];
        },
      },
    });
    await page.goto(DEEP_LINK);
    await expect(
      page.getByRole("button", { name: strings.action.thatWasMe }),
    ).toHaveCount(0);
    await page.getByRole("button", { name: strings.action.tryNow }).click();
    await page.getByRole("button", { name: strings.action.thatWasMe }).click();
    await page
      .getByRole("button", { name: strings.confirm.thatWasMe.button })
      .click();
    await expect(page.getByText(strings.toast.thatWasMe)).toBeVisible();
    await expect(
      page.getByText(strings.state.connected({ telegramName: "Rosa" })),
    ).toBeVisible();
  });

  test("new token: the same bot stays connected, another bot waits for Start", async ({
    page,
  }) => {
    const answers: Telegram["answers"] = {
      "POST /link-a/token": () => [
        200,
        { bot: bot(), managed_replaced: 0, replacement_status: "committed" },
      ],
    };
    const state = await mockTelegram(page, {
      bots: [bot({ health: "token_rejected" })],
      answers,
    });
    await page.goto(DEEP_LINK);
    const row = page.locator("[data-telegram-row]").first();
    await row
      .getByRole("button", { name: strings.action.pasteToken })
      .first()
      .click();
    await paste(page, TOKEN);
    await expect(
      page.getByText(strings.toast.tokenReplaced({ name: NAME })),
    ).toBeVisible();

    const other = bot({ id: "link-b", bot_id: 2, ...WAITING });
    answers["POST /link-a/token"] = () => {
      state.bots = [other];
      return [
        200,
        {
          bot: other,
          connect: CONNECT,
          managed_replaced: 0,
          replacement_status: "committed",
        },
      ];
    };
    await row
      .getByRole("button", { name: strings.action.pasteToken })
      .first()
      .click();
    await paste(page, TOKEN);
    await expect(page.getByText(strings.toast.botReplaced)).toBeVisible();
    await expect(
      page.getByText(strings.state.replaced({ name: NAME })),
    ).toBeVisible();
  });

  test("a token equal to the stored one shows its sentence and leaves the pause", async ({
    page,
  }) => {
    await mockTelegram(page, {
      bots: [bot({ acts_paused_since: "2026-01-01T00:00:00Z" })],
      answers: { "POST /link-a/token": refused(409, "token-same") },
    });
    await page.goto(DEEP_LINK);
    await page
      .locator("[data-telegram-row]")
      .getByRole("button", { name: strings.action.pasteToken })
      .first()
      .click();
    await paste(page, TOKEN);
    await expect(page.getByText(strings.token.same)).toBeVisible();
    await expect(page.getByLabel(strings.setup.step2.label)).toHaveValue("");
  });

  test("different account, remove, disconnect all", async ({ page }) => {
    const state = await mockTelegram(page, {
      bots: [bot()],
      answers: {
        "POST /link-a/disconnect": () => {
          state.bots = [bot({ ...WAITING })];
          return [200, { bot: state.bots[0], connect: CONNECT }];
        },
        "POST /disconnect-all": () => [200, { disconnected: 1 }],
        "DELETE /link-a": () => {
          state.bots = [];
          return [204, undefined];
        },
      },
    });
    await page.goto(DEEP_LINK);
    const menu = page.getByRole("button", {
      name: strings.row.menu({ name: NAME }),
    });

    await menu.click();
    await page
      .getByRole("menuitem", { name: strings.action.otherAccount })
      .click();
    await page
      .getByRole("button", { name: strings.confirm.otherAccount.button })
      .click();
    await expect(page.getByText(strings.toast.disconnected)).toBeVisible();
    await expect(page.getByRole("link", { name: OPEN_CHAT })).toHaveAttribute(
      "href",
      CONNECT.url,
    );

    await page.getByRole("button", { name: strings.lost.button }).click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: strings.lost.button })
      .click();
    await expect(page.getByText(strings.toast.lost)).toBeVisible();
    await expect(page.getByRole("link", { name: OPEN_CHAT })).toHaveCount(0);

    await menu.click();
    await page.getByRole("menuitem", { name: strings.action.remove }).click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: strings.confirm.remove.button })
      .click();
    await expect(page.getByText(strings.toast.removed)).toBeVisible();
    await expect(page.getByText(strings.state.none)).toBeVisible();
  });

  test("the settings block saves each control; a language change is sent", async ({
    page,
  }) => {
    const state = await mockTelegram(page, {
      bots: [bot()],
      settings: SETTINGS,
      answers: { "PUT /settings": () => [200, SETTINGS] },
    });
    await page.goto(DEEP_LINK);
    await page
      .getByRole("combobox", { name: strings.prefs.language.label })
      .click();
    await page.getByRole("option", { name: "中文" }).click();
    await expect(page.getByText(strings.prefs.saved)).toBeVisible();
    expect(state.calls.at(-1)?.body).toEqual({ locale: "zh-CN" });
  });

  test("an answer that offered a page shows the link, and the link opens the page", async ({
    page,
  }) => {
    const threadId = "00000000-0000-0000-0000-0000000000b1";
    await mockTelegram(page, {
      threads: [
        {
          thread_id: threadId,
          title: "A chat",
          messages: [
            { id: "m1", type: "human", content: "Where is it?" },
            {
              id: "m2",
              type: "ai",
              content: "",
              tool_calls: [
                { id: "call", name: "show_web_page", args: { key: "account" } },
              ],
            },
            {
              id: "m3",
              type: "tool",
              name: "show_web_page",
              tool_call_id: "call",
              content: "A button is shown under your reply.",
              artifact: {
                web_page: { version: 1, kind: "web_page", key: "account" },
              },
            },
            { id: "m4", type: "ai", content: "It is on your account page." },
          ],
        },
      ],
    });
    await page.goto(`/workspace/chats/${threadId}`);
    const links = page.getByTestId("web-page-links");
    await expect(links).toBeVisible();
    const link = links.getByRole("link", { name: strings.webPage.account });
    await expect(link).toHaveAttribute(
      "href",
      `/workspace/chats/${threadId}?settings=account`,
    );
    await expect(async () => {
      await link.click();
      await expect(page.getByRole("dialog")).toBeVisible({ timeout: 1000 });
    }).toPass();
  });

  test("off, secret missing, moved, copy running: notice and no button", async ({
    page,
  }) => {
    for (const [telegram, sentence] of [
      [
        { enabled: false, bots: "unavailable", reason: "off" },
        /Telegram is switched off for your company/,
      ],
      [
        { enabled: true, bots: "unavailable", reason: "secret-missing" },
        strings.unavailable.secretMissing,
      ],
      [
        { enabled: true, bots: "unavailable", reason: "host-changed" },
        /the service moved to a new server/,
      ],
      [
        { enabled: true, bots: "unavailable", reason: "polled-elsewhere" },
        strings.unavailable.polledElsewhere,
      ],
    ] as const) {
      await page.unrouteAll({ behavior: "ignoreErrors" });
      await mockTelegram(page, { telegram });
      await page.goto(DEEP_LINK);
      await expect(page.getByTestId("telegram-notice")).toContainText(sentence);
      await expect(
        page
          .getByRole("button", { name: strings.action.connect, exact: true })
          .first(),
      ).toBeDisabled();
    }
  });

  test("administrator sees counts and no username", async ({ page }) => {
    await mockTelegram(page);
    await page.goto(DEEP_LINK);
    await expect(
      page.getByText(
        strings.admin.counts({
          bots: 3,
          people: 2,
          polling: 2,
          failing: 1,
          held: 0,
        }),
      ),
    ).toBeVisible();
    await expect(
      page.getByText(strings.admin.limits({ perPerson: 10, limit: 100 })),
    ).toBeVisible();
    await expect(
      page.getByText(/one of the AI services already used here/),
    ).toBeVisible();
  });

  test("no features answer: nothing renders", async ({ page }) => {
    await mockTelegram(page, { telegram: null });
    await page.goto(DEEP_LINK);
    await expect(section(page)).toHaveCount(0);
    await expect(page.getByTestId("telegram-welcome-card")).toHaveCount(0);
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("telegram-welcome-card")).toHaveCount(0);
  });

  test("welcome-page card: shown until Not now", async ({ page }) => {
    await mockTelegram(page);
    await page.goto("/workspace/chats/new");
    const card = page.getByTestId("telegram-welcome-card");
    await expect(card).toBeVisible();
    await card.getByRole("button", { name: strings.offer.later }).click();
    await expect(card).toHaveCount(0);
    await page.reload();
    await expect(page.getByRole("textbox").first()).toBeVisible();
    await expect(card).toHaveCount(0);
  });
});

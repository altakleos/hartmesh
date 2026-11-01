import { describe, expect, it } from "@rstest/core";

import { CLOSED_TELEGRAM, type TelegramFeature } from "@/core/features/api";
import type { Bot } from "@/core/telegram-bots/api";
import {
  needsAttention,
  rowOf,
  type RowInput,
  shortStateOf,
} from "@/core/telegram-bots/rows";

const NOW = Date.parse("2026-11-01T12:00:00Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const ahead = (seconds: number) => new Date(NOW + seconds * 1000).toISOString();
const ready: TelegramFeature = {
  ...CLOSED_TELEGRAM,
  known: true,
  enabled: true,
  bots: "ready",
};
const unavailable = (reason: TelegramFeature["reason"]): TelegramFeature => ({
  ...ready,
  bots: "unavailable",
  reason,
});

function bot(overrides: Partial<Bot> = {}): Bot {
  return {
    id: "link",
    bot_id: 1,
    agent_kind: "lead_agent",
    agent_ref: "lead_agent",
    agent_name: "HartMesh",
    bot_username: "chat_abc_bot",
    bot_name: "HartMesh",
    url: "https://t.me/chat_abc_bot",
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
    created_at: ago(3600),
    ...overrides,
  };
}

const input = (overrides: Partial<RowInput> = {}): RowInput => ({
  name: "HartMesh",
  bot: bot(),
  feature: ready,
  reader: { admin: false, provider: "Acme" },
  now: NOW,
  ...overrides,
});
const keys = (view: ReturnType<typeof rowOf>) =>
  view.lines.map((line) => line.key);
const waiting = { state: "waiting_connect", connected_as: null };

const CASES: [number, Partial<RowInput>, string[], string[]][] = [
  [
    1,
    { feature: { ...ready, enabled: false, reason: "off" } },
    ["state.sectionPaused"],
    [],
  ],
  [2, { bot: null }, ["state.none"], ["action.connect"]],
  [4, { bot: bot({ state: "held" }) }, ["state.held"], ["action.reconnect"]],
  [5, { feature: unavailable("secret-missing") }, ["state.unreadableHost"], []],
  [
    6,
    { bot: bot({ health: "secret_unreadable" }) },
    ["state.unreadableKey"],
    ["action.pasteToken"],
  ],
  [
    7,
    { bot: bot({ health: "token_rejected" }) },
    ["state.tokenRejected"],
    ["action.pasteToken"],
  ],
  [
    10,
    { bot: bot({ health: "conflict", webhook_host: "example.test" }) },
    ["state.conflictWebhook"],
    ["action.useHere"],
  ],
  [
    11,
    { bot: bot({ health: "conflict" }) },
    ["state.conflict", "state.conflictFix.pasted"],
    ["action.pasteToken"],
  ],
  [
    12,
    { bot: bot({ health: "conflict", probing_ended: true }) },
    ["state.conflict", "state.conflictFix.pasted", "state.conflictStopped"],
    ["action.pasteToken", "action.tryNow"],
  ],
  [
    13,
    { bot: bot({ health: "stopped" }) },
    ["state.stopped"],
    ["action.tryNow"],
  ],
  [14, { bot: bot({ health: "unreachable" }) }, ["state.unreachable"], []],
  [
    17,
    { bot: bot({ health: "agent_unavailable", agent_kind: "custom_agent" }) },
    ["state.agentGone"],
    ["action.remove"],
  ],
  [20, { bot: bot({ state: "blocked" }) }, ["state.blocked"], ["action.open"]],
  [
    21,
    { bot: bot({ ...waiting, profile_synced: false, created_at: ago(30) }) },
    ["state.preparing"],
    [],
  ],
  [
    22,
    { bot: bot({ ...waiting, connect_expires_at: ago(1) }) },
    ["state.expired"],
    ["action.newLink"],
  ],
  [
    23,
    { bot: bot({ ...waiting, connect_expires_at: ahead(600) }) },
    ["state.waiting"],
    ["action.openChat"],
  ],
  [
    24,
    { bot: bot({ acts_paused_since: ago(60) }) },
    ["state.afterConflict.chat"],
    ["action.thatWasMe", "action.pasteToken"],
  ],
  [
    26,
    {
      bot: bot({
        delivery_failed: 1,
        limited_since: ago(60),
        delivery_degraded: true,
      }),
    },
    ["state.deliveryFailed", "state.limited", "state.deliveryPending"],
    ["action.open"],
  ],
  [
    27,
    { bot: bot({ profile_synced: false }) },
    ["state.profileStale"],
    ["action.applyProfile"],
  ],
  [29, {}, ["state.connected"], ["action.open"]],
];

describe("rowOf", () => {
  it("each row gives its state line and buttons", () => {
    expect(CASES).toHaveLength(20);
    for (const [row, overrides, lines, buttons] of CASES) {
      const view = rowOf(input(overrides));
      expect({
        row: view.row,
        lines: keys(view),
        buttons: view.buttons,
      }).toEqual({ row, lines, buttons });
    }
  });

  it("the first matching row wins", () => {
    expect(
      rowOf(input({ bot: bot({ state: "held", health: "conflict" }) })).row,
    ).toBe(4);
    for (const reason of ["host-changed", "polled-elsewhere"] as const) {
      for (const health of ["conflict", "stopped", "ok"]) {
        const paused = rowOf(
          input({ bot: bot({ health }), feature: unavailable(reason) }),
        );
        expect(paused.row).toBe(1);
      }
    }
  });

  it("an unknown state or health shows unavailable.unknown", () => {
    for (const fields of [{ state: "new-state" }, { health: "new-health" }]) {
      const view = rowOf(input({ bot: bot(fields) }));
      expect(keys(view)).toEqual(["unavailable.unknown"]);
      expect(view.buttons).toEqual([]);
    }
  });

  it("names the state line by the bot's kind", () => {
    const view = rowOf(
      input({
        bot: bot({
          agent_kind: "custom_agent",
          acts_paused_since: ago(60),
        }),
      }),
    );
    expect(keys(view)).toEqual(["state.afterConflict.custom_agent"]);
    expect(view.lines[0]?.values.time).toBeTruthy();
  });

  it("shows row 23 with the replaced line after a pasted token of another bot", () => {
    const view = rowOf(
      input({
        bot: bot({ ...waiting, connect_expires_at: ahead(600) }),
        replaced: true,
      }),
    );
    expect(keys(view)).toEqual(["state.replaced"]);
  });

  it("gives row 21 only to an unsynced bot under a minute old", () => {
    const waitingBot = { ...waiting, connect_expires_at: ahead(600) };
    const row = (profile_synced: boolean, seconds: number) =>
      rowOf(
        input({
          bot: bot({ ...waitingBot, profile_synced, created_at: ago(seconds) }),
        }),
      ).row;
    expect(row(false, 59)).toBe(21);
    expect(row(false, 61)).toBe(23);
    expect(row(true, 59)).toBe(23);
    expect(
      rowOf(
        input({
          bot: bot({
            ...waiting,
            profile_synced: false,
            created_at: ago(61),
            connect_expires_at: ago(1),
          }),
        }),
      ).row,
    ).toBe(22);
  });

  it("gives row 27 only after a minute", () => {
    const row = (seconds: number) =>
      rowOf(
        input({
          bot: bot({ profile_synced: false, created_at: ago(seconds) }),
        }),
      ).row;
    expect(row(59)).toBe(29);
    expect(row(61)).toBe(27);
  });

  it("offers Open chat when no connect code is live", () => {
    expect(
      rowOf(input({ bot: bot({ ...waiting, connect_expires_at: null }) })).row,
    ).toBe(23);
  });

  it("does not call a creator link expired", () => {
    expect(
      rowOf(
        input({
          bot: bot({
            ...waiting,
            bind_mode: "creator",
            connect_expires_at: ago(1),
          }),
        }),
      ).row,
    ).toBe(23);
  });

  it("shows unavailable.unknown for a value with no row in this stage", () => {
    for (const fields of [
      { health: "token_rejected", origin: "managed" },
      { health: "policy_off" },
    ]) {
      const view = rowOf(input({ bot: bot(fields) }));
      expect(view.row).toBe(0);
      expect(view.buttons).toEqual([]);
    }
  });

  it("puts a held bot before an unreadable secret, and that before a fault", () => {
    const missing = unavailable("secret-missing");
    expect(
      rowOf(input({ bot: bot({ state: "held" }), feature: missing })).row,
    ).toBe(4);
    expect(
      rowOf(input({ bot: bot({ health: "conflict" }), feature: missing })).row,
    ).toBe(5);
  });

  it("prints the time of the pause and carries the reader", () => {
    const paused = ago(60);
    const view = rowOf(input({ bot: bot({ acts_paused_since: paused }) }));
    expect(view.lines[0]?.values.time).toBe(new Date(paused).toLocaleString());
    expect(
      rowOf(input({ bot: bot({ health: "stopped" }) })).lines[0]?.values,
    ).toMatchObject({ admin: false, provider: "Acme" });
  });

  it("treats a cause that has just ended as healthy", () => {
    expect(
      rowOf(input({ bot: bot({ health: "agent_unavailable" }) })).row,
    ).toBe(29);
  });

  it("reads a bound bot's Telegram name", () => {
    expect(rowOf(input()).lines[0]?.values).toMatchObject({
      name: "HartMesh",
      telegramName: "Rosa",
    });
  });
});

describe("the rows that need attention", () => {
  it("the rows that need attention are exactly those of section 4.3", () => {
    const rows = Array.from({ length: 30 }, (_, row) => row);
    expect(rows.filter(needsAttention)).toEqual([
      4, 6, 7, 8, 10, 11, 12, 13, 16, 17, 20, 22, 24, 26, 27,
    ]);
  });

  it("every row has a short state", () => {
    expect(shortStateOf(1)).toBe("short.off");
    expect(shortStateOf(2)).toBe("short.none");
    for (const row of [3, 21, 23]) {
      expect(shortStateOf(row)).toBe("short.waiting");
    }
    for (const row of [25, 28, 29]) {
      expect(shortStateOf(row)).toBe("short.connected");
    }
    for (const row of [4, 5, 14, 15, 18, 19, 26]) {
      expect(shortStateOf(row)).toBe("short.attention");
    }
  });
});

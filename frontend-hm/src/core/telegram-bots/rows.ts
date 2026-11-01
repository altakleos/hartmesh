import type { TelegramFeature } from "@/core/features/api";

import type { Bot } from "./api";
import type { TelegramKey, TelegramReader, TelegramValues } from "./text";
import type { AgentTarget } from "./web-pages";

export interface RowInput {
  name: string;
  bot: Bot | null;
  feature: Pick<TelegramFeature, "enabled" | "bots" | "reason">;
  reader: TelegramReader;
  now: number;
  /** The row is the answer to a pasted token of another bot. */
  replaced?: boolean;
}

export interface RowLine {
  key: TelegramKey;
  values: TelegramValues;
}

export interface RowView {
  /** The row of the design's table; 0 for a value this client does not know. */
  row: number;
  lines: RowLine[];
  buttons: TelegramKey[];
}

const SETTING_UP_MS = 60_000;
const UNREADABLE_SECRET = new Set(["secret-missing", "secret-invalid"]);
const KNOWN_STATES = new Set([
  "waiting_connect",
  "connected",
  "held",
  "blocked",
]);
const KNOWN_HEALTH = new Set([
  "ok",
  "token_rejected",
  "conflict",
  "unreachable",
  "secret_unreadable",
  "agent_unavailable",
  "stopped",
]);
const ATTENTION = new Set([
  4, 6, 7, 8, 10, 11, 12, 13, 16, 17, 20, 22, 24, 26, 27,
]);
const WAITING = new Set([3, 21, 23]);
const CONNECTED = new Set([25, 28, 29]);

function view(
  row: number,
  keys: TelegramKey[],
  buttons: TelegramKey[],
  values: TelegramValues,
): RowView {
  return { row, lines: keys.map((key) => ({ key, values })), buttons };
}

function faultRow(bot: Bot, values: TelegramValues): RowView | null {
  const conflict: TelegramKey[] = [
    "state.conflict",
    "state.conflictFix.pasted",
  ];
  switch (bot.health) {
    case "secret_unreadable":
      return view(6, ["state.unreadableKey"], ["action.pasteToken"], values);
    case "token_rejected":
      return bot.origin === "pasted"
        ? view(7, ["state.tokenRejected"], ["action.pasteToken"], values)
        : null;
    case "conflict":
      if (bot.webhook_host !== null) {
        return view(10, ["state.conflictWebhook"], ["action.useHere"], values);
      }
      return bot.probing_ended
        ? view(
            12,
            [...conflict, "state.conflictStopped"],
            ["action.pasteToken", "action.tryNow"],
            values,
          )
        : view(11, conflict, ["action.pasteToken"], values);
    case "stopped":
      return view(13, ["state.stopped"], ["action.tryNow"], values);
    case "unreachable":
      return view(14, ["state.unreachable"], [], values);
    case "agent_unavailable":
      return bot.agent_kind === "custom_agent"
        ? view(17, ["state.agentGone"], ["action.remove"], values)
        : null;
    default:
      return null;
  }
}

function waitingRow(
  input: RowInput,
  bot: Bot,
  values: TelegramValues,
): RowView {
  const age = input.now - Date.parse(bot.created_at);
  if (!bot.profile_synced && age < SETTING_UP_MS) {
    return view(21, ["state.preparing"], [], values);
  }
  const expires = bot.connect_expires_at
    ? Date.parse(bot.connect_expires_at)
    : Number.POSITIVE_INFINITY;
  if (bot.bind_mode === "code" && expires <= input.now) {
    return view(22, ["state.expired"], ["action.newLink"], values);
  }
  return view(
    23,
    [input.replaced ? "state.replaced" : "state.waiting"],
    ["action.openChat"],
    values,
  );
}

function connectedRow(
  input: RowInput,
  bot: Bot,
  values: TelegramValues,
): RowView {
  if (bot.acts_paused_since !== null) {
    return view(
      24,
      [
        bot.agent_kind === "custom_agent"
          ? "state.afterConflict.custom_agent"
          : "state.afterConflict.chat",
      ],
      ["action.thatWasMe", "action.pasteToken"],
      { ...values, time: new Date(bot.acts_paused_since).toLocaleString() },
    );
  }
  const delivery: TelegramKey[] = [
    ...(bot.delivery_failed > 0 ? (["state.deliveryFailed"] as const) : []),
    ...(bot.limited_since !== null ? (["state.limited"] as const) : []),
    ...(bot.delivery_degraded ? (["state.deliveryPending"] as const) : []),
  ];
  if (delivery.length > 0) {
    return view(26, delivery, ["action.open"], values);
  }
  if (
    !bot.profile_synced &&
    input.now - Date.parse(bot.created_at) > SETTING_UP_MS
  ) {
    return view(27, ["state.profileStale"], ["action.applyProfile"], values);
  }
  return view(29, ["state.connected"], ["action.open"], values);
}

/** What a target's row shows: the first row of the design's table that matches. */
export function rowOf(input: RowInput): RowView {
  const { bot, feature, name, reader } = input;
  const values: TelegramValues = {
    ...reader,
    name,
    telegramName: bot?.connected_as ?? "",
  };
  if (bot === null) {
    return view(2, ["state.none"], ["action.connect"], values);
  }
  const unreadable =
    feature.reason !== null && UNREADABLE_SECRET.has(feature.reason);
  if ((!feature.enabled || feature.bots === "unavailable") && !unreadable) {
    return view(1, ["state.sectionPaused"], [], values);
  }
  if (!KNOWN_STATES.has(bot.state) || !KNOWN_HEALTH.has(bot.health)) {
    return view(0, ["unavailable.unknown"], [], values);
  }
  if (bot.state === "held") {
    return view(4, ["state.held"], ["action.reconnect"], values);
  }
  if (unreadable) {
    return view(5, ["state.unreadableHost"], [], values);
  }
  const fault = faultRow(bot, values);
  if (fault !== null) {
    return fault;
  }
  if (bot.health === "token_rejected") {
    return view(0, ["unavailable.unknown"], [], values);
  }
  if (bot.state === "blocked") {
    return view(20, ["state.blocked"], ["action.open"], values);
  }
  return bot.state === "waiting_connect"
    ? waitingRow(input, bot, values)
    : connectedRow(input, bot, values);
}

export type SetupStage = "steps" | "waiting" | "expired" | "done";

/** Where the set-up stands, read from the list so a reload or a second tab agrees. */
export function setupStageOf(bot: Bot | null, now: number): SetupStage {
  if (bot?.state === "connected") {
    return "done";
  }
  if (bot?.state !== "waiting_connect" || bot.bind_mode !== "code") {
    return "steps";
  }
  const expires = bot.connect_expires_at
    ? Date.parse(bot.connect_expires_at)
    : Number.POSITIVE_INFINITY;
  return expires <= now ? "expired" : "waiting";
}

/** False while the set-up's own steps describe the row: the bot can hear Start. */
export function stopsSetup(row: number): boolean {
  return row !== 21 && row !== 22 && row !== 23;
}

export interface TargetRow {
  target: AgentTarget;
  name: string;
  bot: Bot | null;
}

function byName(first: { name: string }, second: { name: string }): number {
  return first.name.localeCompare(second.name);
}

/** The ordinary chat, then custom agents by name, then bots whose target has gone. */
export function targetRowsOf({
  chatName,
  agentNames,
  bots,
}: {
  chatName: string;
  agentNames: string[];
  bots: Bot[];
}): TargetRow[] {
  const remaining = new Map(
    bots.map((bot) => [`${bot.agent_kind}.${bot.agent_ref}`, bot]),
  );
  const claim = (kind: string, reference: string) => {
    const key = `${kind}.${reference}`;
    const bot = remaining.get(key) ?? null;
    remaining.delete(key);
    return bot;
  };
  const chat: TargetRow = {
    target: { kind: "chat" },
    name: chatName,
    bot: claim("lead_agent", "lead_agent"),
  };
  const agents = agentNames
    .map(
      (name): TargetRow => ({
        target: { kind: "custom_agent", name },
        name,
        bot: claim("custom_agent", name),
      }),
    )
    .sort(byName);
  const gone = Array.from(
    remaining.values(),
    (bot): TargetRow => ({
      target: { kind: "custom_agent", name: bot.agent_ref },
      name: bot.agent_name ?? bot.bot_name ?? bot.agent_ref,
      bot,
    }),
  ).sort(byName);
  return [chat, ...agents, ...gone];
}

export interface SectionNotice {
  key: TelegramKey;
  /** False for a sentence that says whom to ask itself. */
  ending: boolean;
  /** False where the sentence already says what the provider has to do. */
  next: boolean;
}

/** The notice for the whole section, or null when Telegram can run. */
export function sectionNoticeOf(
  feature: Pick<TelegramFeature, "enabled" | "bots" | "reason">,
  hasBot: boolean,
): SectionNotice | null {
  if (!feature.enabled) {
    return {
      key: hasBot ? "unavailable.offKept" : "unavailable.off",
      ending: false,
      next: false,
    };
  }
  if (feature.bots === "ready") {
    return null;
  }
  switch (feature.reason) {
    case "secret-missing":
      return { key: "unavailable.secretMissing", ending: true, next: true };
    case "secret-invalid":
      return { key: "unavailable.secretInvalid", ending: true, next: true };
    case "host-changed":
      return { key: "unavailable.hostChanged", ending: true, next: false };
    case "polled-elsewhere":
      return { key: "unavailable.polledElsewhere", ending: true, next: true };
    default:
      return { key: "unavailable.unknown", ending: true, next: true };
  }
}

/** The person can do something on these rows, so the sidebar counts them. */
export function needsAttention(row: number): boolean {
  return ATTENTION.has(row);
}

export function shortStateOf(row: number): TelegramKey {
  if (row === 1) {
    return "short.off";
  }
  if (row === 2) {
    return "short.none";
  }
  if (WAITING.has(row)) {
    return "short.waiting";
  }
  if (CONNECTED.has(row)) {
    return "short.connected";
  }
  return "short.attention";
}

import type {
  AgentInstance,
  AgentInstanceDetail,
} from "@/core/agent-instances/api";
import { USE } from "@/core/agent-instances/presentation";
import type { TelegramFeature } from "@/core/features/api";

import type { Bot, Connect } from "./api";
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
  instance?: AgentInstance | null;
  execution?: AgentInstanceDetail["execution"];
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
  execution?: AgentInstanceDetail["execution"];
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
  "policy_off",
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
    case "policy_off":
      return view(15, ["state.notAllowed"], [], values);
    case "agent_unavailable":
      return bot.agent_kind === "custom_agent"
        ? view(17, ["state.agentGone"], ["action.remove"], values)
        : null;
    default:
      return null;
  }
}

/** Newer verified grants/lifecycle do not wait for the poller's health reconciliation. */
function instanceRow(
  input: RowInput,
  bot: Bot,
  values: TelegramValues,
): RowView | null {
  if (
    bot.agent_kind !== "instance" ||
    (input.instance === undefined && bot.health !== "agent_unavailable")
  )
    return null;
  const instance = input.instance?.id === bot.agent_ref ? input.instance : null;
  if (
    !instance ||
    !(instance.permissions & USE) ||
    instance.status === "deleted"
  )
    return view(16, ["state.noAccess"], ["action.remove"], values);
  if (instance.status === "archived")
    return view(18, ["state.retired"], ["action.remove"], values);
  if (instance.status === "suspended")
    return view(19, ["state.paused"], ["action.open"], values);
  return null;
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
        bot.agent_kind === "instance"
          ? "state.afterConflict.agent"
          : bot.agent_kind === "custom_agent"
            ? "state.afterConflict.custom_agent"
            : "state.afterConflict.chat",
      ],
      ["action.thatWasMe", "action.pasteToken"],
      { ...values, time: new Date(bot.acts_paused_since).toLocaleString() },
    );
  }
  if (
    bot.agent_kind === "instance" &&
    input.execution?.execution === "unavailable"
  )
    return {
      ...view(25, ["state.cannotWork"], ["action.open"], values),
      execution: input.execution,
    };
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
  if (bot.policy === "protected")
    return view(28, ["state.connectedProtected"], ["action.open"], values);
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
  const currentInstance = instanceRow(input, bot, values);
  if (currentInstance) return currentInstance;
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

/**
 * A held address works only while it is the one the list names: a new token, a
 * conflict, Disconnect all or a link from a second tab each end it without a word.
 */
export function liveConnectOf(
  held: Connect | undefined,
  bot: Bot | null,
  now: number,
): Connect | undefined {
  return held &&
    bot?.connect_expires_at &&
    Date.parse(held.expires_at) === Date.parse(bot.connect_expires_at) &&
    Date.parse(held.expires_at) > now
    ? held
    : undefined;
}

/** False while the set-up’s waiting or Done screen describes the row. */
export function stopsSetup(row: number): boolean {
  return row !== 21 && row !== 22 && row !== 23 && !CONNECTED.has(row);
}

export interface TargetRow {
  target: AgentTarget;
  name: string;
  bot: Bot | null;
  instance?: AgentInstance;
}

function byName(first: { name: string }, second: { name: string }): number {
  return first.name.localeCompare(second.name);
}

/** Sidebar order: chat, own/colleague assistants, employees, custom agents, inaccessible targets. */
export function targetRowsOf({
  chatName,
  agentNames,
  bots,
  instances = [],
  ownerId,
}: {
  chatName: string;
  agentNames: string[];
  bots: Bot[];
  instances?: AgentInstance[];
  ownerId?: string;
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
  const visible = instances.filter(
    (entry) =>
      (entry.permissions & USE) !== 0 &&
      entry.status !== "deleted" &&
      entry.status !== "provisioning" &&
      entry.status !== "archived",
  );
  const instanceRows = visible.map(
    (entry): TargetRow => ({
      target: {
        kind: "agent",
        id: entry.id,
        name: entry.name,
        permissions: entry.permissions,
      },
      name: entry.name,
      bot: claim("instance", entry.id),
      instance: entry,
    }),
  );
  const assistants = instanceRows
    .filter((row) => row.instance?.custody === "personal")
    .sort(
      (a, b) =>
        Number(b.instance?.owner_id === ownerId) -
          Number(a.instance?.owner_id === ownerId) || byName(a, b),
    );
  const employees = instanceRows
    .filter((row) => row.instance?.custody === "company")
    .sort(byName);
  const byId = new Map(instances.map((instance) => [instance.id, instance]));
  const gone = Array.from(remaining.values(), (bot): TargetRow => {
    const instance =
      bot.agent_kind === "instance" ? byId.get(bot.agent_ref) : undefined;
    const name =
      instance?.name ?? bot.agent_name ?? bot.bot_name ?? bot.agent_ref;
    return {
      target:
        bot.agent_kind === "instance"
          ? {
              kind: "agent",
              id: bot.agent_ref,
              name,
              permissions: instance?.permissions ?? 0,
            }
          : { kind: "custom_agent", name: bot.agent_ref },
      name,
      bot,
      ...(instance ? { instance } : {}),
    };
  }).sort(byName);
  return [chat, ...assistants, ...employees, ...agents, ...gone];
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

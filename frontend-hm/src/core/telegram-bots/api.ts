import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import type { TelegramKey } from "./text";

/** `state` and `health` stay strings: a value this client does not know has its own row. */
export interface Bot {
  id: string;
  bot_id: number;
  agent_kind: string;
  agent_ref: string;
  agent_name: string | null;
  bot_username: string;
  bot_name: string | null;
  url: string;
  origin: string;
  managed_by: string | null;
  can_manage_bots: boolean;
  bind_mode: string;
  state: string;
  health: string;
  connected_as: string | null;
  connect_expires_at: string | null;
  profile_synced: boolean;
  acts_paused_since: string | null;
  probing_ended: boolean;
  limited_since: string | null;
  delivery_pending: number;
  delivery_degraded: boolean;
  delivery_failed: number;
  webhook_host: string | null;
  topics_enabled: boolean;
  policy: string | null;
  last_update_at: string | null;
  created_at: string;
}

export interface Connect {
  url: string;
  expires_at: string;
}

export interface Settings {
  timezone: string;
  locale: string;
  quiet_enabled: boolean;
  quiet_start: string;
  quiet_end: string;
  notify: string;
  summary_enabled: boolean;
  summary_time: string;
}

export interface RemovedBot {
  id: string;
  reason: string;
  agent_kind: string;
  agent_ref: string;
  agent_name: string | null;
  bot_username: string;
  at: string;
}

export interface BotList {
  bots: Bot[];
  limits: { per_person: number; used: number };
  settings: Settings | null;
  quick_setup: {
    available: boolean;
    manager_link_id: string | null;
    manager_username: string | null;
  };
  pending_creation: unknown;
  removed_bots: RemovedBot[];
  policies: { instance_id: string; policy: string }[];
  read_by_others: string[];
}

export interface AdminSummary {
  enabled: boolean;
  reason: string | null;
  bots: number;
  people: number;
  polling: number;
  failing: number;
  held: number;
  limits: { per_person: number; deployment: number };
  voice: string;
  voice_provider: string | null;
}

export interface AddBotRequest {
  agent_kind: string;
  agent_ref: string;
  token: string;
  timezone: string;
  locale: string;
  take_over_webhook?: boolean;
}

export interface ReplaceTokenRequest {
  token: string;
  operation_id: string;
  take_over_webhook?: boolean;
}

export interface ReplaceTokenAnswer {
  bot: Bot;
  connect?: Connect;
  managed_replaced: number;
  replacement_status: "committed" | "superseded";
}

/** A refusal is shown by its `reason`; its `detail` is never shown. */
export class TelegramRefusal extends Error {
  readonly status: number;
  readonly reason: string | null;
  readonly detail: string | null;
  readonly webhook_host?: string;
  readonly own?: boolean;
  readonly agent_name?: string | null;
  readonly link_id?: string | null;
  readonly execution_reason?: string;

  constructor(status: number, body: Record<string, unknown>) {
    super(typeof body.reason === "string" ? body.reason : "telegram-refusal");
    this.name = "TelegramRefusal";
    this.status = status;
    this.reason = typeof body.reason === "string" ? body.reason : null;
    this.detail = typeof body.detail === "string" ? body.detail : null;
    Object.assign(
      this,
      Object.fromEntries(
        (
          [
            "webhook_host",
            "own",
            "agent_name",
            "link_id",
            "execution_reason",
          ] as const
        )
          .filter((key) => key in body)
          .map((key) => [key, body[key]]),
      ),
    );
  }
}

/** The six reasons of the whole section: the features answer is read again for them. */
export const TELEGRAM_SECTION_REFUSALS: ReadonlyMap<string, TelegramKey> =
  new Map<string, TelegramKey>([
    ["telegram-off", "unavailable.off"],
    ["secret-missing", "unavailable.secretMissing"],
    ["secret-invalid", "unavailable.secretInvalid"],
    ["host-changed", "unavailable.hostChanged"],
    ["polled-elsewhere", "unavailable.polledElsewhere"],
    ["misconfigured", "unavailable.unknown"],
  ]);

const REFUSAL_KEYS = new Map<string, TelegramKey>([
  ...TELEGRAM_SECTION_REFUSALS,
  ["token-invalid", "refusal.rejected"],
  ["token-same", "token.same"],
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
]);

export function telegramRefusalKey(
  refusal: Pick<TelegramRefusal, "reason" | "own">,
): TelegramKey {
  if (refusal.reason === "bot-linked") {
    return refusal.own ? "refusal.inUseSelf" : "refusal.inUseOther";
  }
  return REFUSAL_KEYS.get(refusal.reason ?? "") ?? "refusal.failed";
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/telegram-bots${path}`,
    init,
  );
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as Record<
      string,
      unknown
    >;
    throw new TelegramRefusal(response.status, body);
  }
  return (response.status === 204 ? undefined : response.json()) as Promise<T>;
}

function post<T>(path: string, body?: unknown): Promise<T> {
  return call<T>(path, {
    method: "POST",
    ...(body === undefined
      ? {}
      : {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }),
  });
}

function botPath(id: string, action: string): string {
  return `/${encodeURIComponent(id)}/${action}`;
}

export function listTelegramBots(): Promise<BotList> {
  return call("");
}

/** A plain call, never a mutation: the query library would keep the token as its variables. */
export function addTelegramBot(
  request: AddBotRequest,
): Promise<{ bot: Bot; connect: Connect }> {
  return post("", request);
}

/** A plain call for the same reason as `addTelegramBot`. */
export function replaceTelegramToken(
  id: string,
  request: ReplaceTokenRequest,
): Promise<ReplaceTokenAnswer> {
  return post(botPath(id, "token"), request);
}

export function requestTelegramConnect(id: string): Promise<Connect> {
  return post(botPath(id, "connect"));
}

export function disconnectTelegramBot(
  id: string,
): Promise<{ bot: Bot; connect: Connect }> {
  return post(botPath(id, "disconnect"));
}

export function disconnectAllTelegramBots(): Promise<{ disconnected: number }> {
  return post("/disconnect-all");
}

export function acknowledgeTelegramConflict(id: string): Promise<{ bot: Bot }> {
  return post(botPath(id, "acknowledge-conflict"));
}

export function retryTelegramBot(
  id: string,
  { takeOverWebhook = false }: { takeOverWebhook?: boolean } = {},
): Promise<{ bot: Bot }> {
  return post(
    botPath(id, "retry"),
    takeOverWebhook ? { take_over_webhook: true } : undefined,
  );
}

export function applyTelegramProfile(id: string): Promise<{ bot: Bot }> {
  return post(botPath(id, "profile"));
}

export function removeTelegramBot(id: string): Promise<void> {
  return call(`/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export function saveTelegramSettings(
  change: Partial<Settings>,
): Promise<Settings> {
  return call("/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(change),
  });
}

export function getTelegramAdminSummary(): Promise<AdminSummary> {
  return call("/admin/summary");
}

/** One logical replacement's id, kept across a retry of the same pasted token. */
export function newTelegramOperationId(): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

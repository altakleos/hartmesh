import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
import { EXECUTION_REASONS, type ExecutionReason } from "@/core/features/api";

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
  /** Only a person's first bot uses these two. */
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
  readonly execution_reason: ExecutionReason | null;

  constructor(status: number, body: Record<string, unknown>) {
    super(typeof body.reason === "string" ? body.reason : "telegram-refusal");
    this.name = "TelegramRefusal";
    this.status = status;
    this.reason = typeof body.reason === "string" ? body.reason : null;
    this.detail = typeof body.detail === "string" ? body.detail : null;
    this.webhook_host =
      typeof body.webhook_host === "string" ? body.webhook_host : undefined;
    this.own = typeof body.own === "boolean" ? body.own : undefined;
    this.agent_name =
      typeof body.agent_name === "string" ? body.agent_name : null;
    this.link_id = typeof body.link_id === "string" ? body.link_id : null;
    this.execution_reason = EXECUTION_REASONS.includes(
      body.execution_reason as ExecutionReason,
    )
      ? (body.execution_reason as ExecutionReason)
      : null;
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
  ["policy-off", "state.notAllowedConnect"],
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
  init?.signal?.throwIfAborted();
  const pending = fetch(
    `${getBackendBaseURL()}/api/telegram-bots${path}`,
    init,
  );
  const response = init?.signal
    ? await awaitAbortable(pending, init.signal, (late) => {
        void late.body?.cancel().catch(() => undefined);
      })
    : await pending;
  const read = () =>
    init?.signal
      ? awaitAbortable(response.json() as Promise<unknown>, init.signal)
      : (response.json() as Promise<unknown>);
  if (!response.ok) {
    const body = (await read().catch(() => {
      init?.signal?.throwIfAborted();
      return {};
    })) as Record<string, unknown>;
    throw new TelegramRefusal(response.status, body);
  }
  return (response.status === 204 ? undefined : read()) as Promise<T>;
}

function post<T>(
  path: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  return call<T>(path, {
    method: "POST",
    ...(signal ? { signal: policySignal(signal) } : {}),
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
  signal?: AbortSignal,
): Promise<{ bot: Bot; connect: Connect }> {
  return post("", request, signal);
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

export type TelegramPolicy = "allowed" | "protected" | "off";
export interface TelegramPolicyAnswer {
  policy: TelegramPolicy;
  people: number;
  connected: number;
}

function policyPath(instanceId: string) {
  return `/instances/${encodeURIComponent(instanceId)}/policy`;
}
function policySignal(signal: AbortSignal) {
  return AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
}
export function getTelegramPolicy(
  instanceId: string,
  signal: AbortSignal,
): Promise<TelegramPolicyAnswer> {
  return call(policyPath(instanceId), { signal: policySignal(signal) });
}
export function saveTelegramPolicy(
  instanceId: string,
  policy: TelegramPolicy,
  signal: AbortSignal,
): Promise<TelegramPolicyAnswer> {
  return call(policyPath(instanceId), {
    method: "PUT",
    signal: policySignal(signal),
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ policy }),
  });
}

/** Mark only a notice actually rendered on a visible Telegram settings page. */
export function markTelegramRemovalSeen(
  eventId: string,
  signal: AbortSignal,
): Promise<void> {
  return call(`/removed/${encodeURIComponent(eventId)}/seen`, {
    method: "POST",
    signal: policySignal(signal),
  });
}

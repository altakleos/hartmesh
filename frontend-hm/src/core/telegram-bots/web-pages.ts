/** What a bot is connected to. The server names the same thing by `serverTargetOf`. */
export type AgentTarget =
  | { kind: "chat" }
  | { kind: "custom_agent"; name: string }
  | { kind: "agent"; id: string; name: string; permissions?: number };

export type ParsedTelegramTarget =
  | { kind: "chat" }
  | { kind: "custom_agent"; name: string }
  | { kind: "agent"; id: string };

const WEB_PAGE_KEYS = [
  "access",
  "files",
  "memory",
  "conversations",
  "work",
  "agentSettings",
  "addAssistant",
  "addEmployee",
  "telegram",
  "scheduled",
  "account",
  "timezone",
] as const;

export type WebPageKey = (typeof WEB_PAGE_KEYS)[number];

export function isWebPageKey(value: unknown): value is WebPageKey {
  return WEB_PAGE_KEYS.some((key) => key === value);
}

const CAN_MANAGE = 4;
const CAN_SEE_ALL_WORK = 2;

const CUSTOM_AGENT_NAME = /^[A-Za-z0-9-]+$/;
const AGENT_ID = /^[0-9a-f]{32}$/i;

/** A dot and not a colon: the sign-in return refuses an address that holds a colon. */
export function telegramTargetOf(target: AgentTarget): string {
  if (target.kind === "chat") {
    return "chat";
  }
  return target.kind === "custom_agent"
    ? `custom_agent.${target.name}`
    : `agent.${target.id}`;
}

export function parseTelegramTarget(
  value: string | null,
): ParsedTelegramTarget | null {
  if (value === "chat") {
    return { kind: "chat" };
  }
  if (value?.startsWith("custom_agent.")) {
    const name = value.slice("custom_agent.".length);
    return CUSTOM_AGENT_NAME.test(name) ? { kind: "custom_agent", name } : null;
  }
  if (value?.startsWith("agent.")) {
    const id = value.slice("agent.".length);
    return AGENT_ID.test(id) ? { kind: "agent", id } : null;
  }
  return null;
}

/** The pair every request and every match of a `Bot` with a target uses. */
export function serverTargetOf(target: AgentTarget): {
  agent_kind: "lead_agent" | "custom_agent" | "instance";
  agent_ref: string;
} {
  if (target.kind === "chat") {
    return { agent_kind: "lead_agent", agent_ref: "lead_agent" };
  }
  return target.kind === "custom_agent"
    ? { agent_kind: "custom_agent", agent_ref: target.name }
    : { agent_kind: "instance", agent_ref: target.id };
}

function agentTab(
  target: AgentTarget,
  tab: string,
  needs: number,
): string | null {
  return target.kind === "agent" &&
    ((target.permissions ?? 0) & needs) === needs
    ? `/workspace/instances?instance=${target.id}&tab=${tab}`
    : null;
}

function pageOf(key: WebPageKey, target: AgentTarget): string | null {
  switch (key) {
    case "access":
      return agentTab(target, "access", CAN_MANAGE);
    case "agentSettings":
      return agentTab(target, "settings", CAN_MANAGE);
    case "files":
      return agentTab(target, "files", CAN_SEE_ALL_WORK);
    case "work":
      return agentTab(target, "work", CAN_SEE_ALL_WORK);
    case "memory":
      return target.kind === "chat"
        ? "?settings=memory"
        : agentTab(target, "memory", CAN_SEE_ALL_WORK);
    case "conversations":
      return target.kind === "agent"
        ? `/workspace/instances?instance=${target.id}&tab=conversations`
        : "/workspace/chats";
    case "addAssistant":
      return "/workspace/assistants";
    case "addEmployee":
      return "/workspace/employees";
    case "telegram":
      return `?settings=channels&telegram=${telegramTargetOf(target)}`;
    case "timezone":
      return "?settings=channels";
    case "scheduled":
      return "/workspace/scheduled-tasks";
    case "account":
      return "?settings=account";
  }
}

/** An address that opens the settings is added to the current path, over the conversation. */
export function webPageHref(
  key: WebPageKey,
  target: AgentTarget,
  pathname: string,
): string | null {
  const page = pageOf(key, target);
  return page?.startsWith("?") ? `${pathname}${page}` : page;
}

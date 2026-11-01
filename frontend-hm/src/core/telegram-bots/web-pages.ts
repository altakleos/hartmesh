/** What a bot is connected to. The server names the same thing by `serverTargetOf`. */
export type AgentTarget =
  | { kind: "chat" }
  | { kind: "custom_agent"; name: string }
  | { kind: "agent"; id: string; name: string; permissions?: number };

export type ParsedTelegramTarget =
  | { kind: "chat" }
  | { kind: "custom_agent"; name: string }
  | { kind: "agent"; id: string };

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

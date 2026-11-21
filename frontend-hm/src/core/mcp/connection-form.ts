import type { MCPServerConfig } from "./types";

export interface ConnectionDraft {
  name: string;
  url: string;
  description: string;
  token: string;
  tokenChanged: boolean;
  type: "http" | "sse";
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function formCanEdit(server: MCPServerConfig): boolean {
  const type = server.type ?? server.transport;
  const headers = Object.keys(record(server.headers));
  const routing = record(server.routing);
  return (
    (type === "http" || type === "sse") &&
    headers.every((name) => name.toLowerCase() === "authorization") &&
    headers.length <= 1 &&
    !server.oauth &&
    !server.user_auth &&
    !server.agent_auth &&
    !server.headers_from_context &&
    !(Array.isArray(server.task_toolsets) && server.task_toolsets.length) &&
    Object.keys(record(server.tools)).length === 0 &&
    (routing.mode == null || routing.mode === "off") &&
    !routing.priority &&
    !(Array.isArray(routing.keywords) && routing.keywords.length)
  );
}

export function connectionDraft(
  name = "",
  server?: MCPServerConfig,
): ConnectionDraft {
  return {
    name,
    description: server?.description ?? "",
    url: typeof server?.url === "string" ? server.url : "",
    token: Object.keys(record(server?.headers)).length ? "***" : "",
    tokenChanged: false,
    type: (server?.type ?? server?.transport) === "sse" ? "sse" : "http",
  };
}

type Field = "name" | "url" | "token" | "description";
export type FormErrors = Partial<
  Record<
    Field,
    "nameRule" | "duplicate" | "secureAddress" | "tokenFormat" | "literal"
  >
>;

function secureAddress(value: string) {
  try {
    const url = new URL(value);
    return (
      /^https:\/\//i.test(value) &&
      !/\s/.test(value) &&
      !!url.hostname &&
      !url.username &&
      !url.password
    );
  } catch {
    return false;
  }
}

export function formErrors(
  draft: ConnectionDraft,
  servers: Record<string, MCPServerConfig>,
  editing: boolean,
) {
  const errors: FormErrors = {};
  if (!editing) {
    if (!/^[A-Za-z0-9_-]{1,48}$/.test(draft.name)) errors.name = "nameRule";
    else if (Object.hasOwn(servers, draft.name)) errors.name = "duplicate";
  }
  if (!secureAddress(draft.url)) errors.url = "secureAddress";
  if (/[^ -~]/.test(draft.token) || draft.token.trim() !== draft.token)
    errors.token = "tokenFormat";
  for (const field of ["name", "url", "token", "description"] as const) {
    if ((field !== "name" || !editing) && draft[field].startsWith("$"))
      errors[field] = "literal";
  }
  return errors;
}

export function toServer(
  draft: ConnectionDraft,
  original?: MCPServerConfig,
): MCPServerConfig {
  const transportField =
    original &&
    !Object.hasOwn(original, "type") &&
    Object.hasOwn(original, "transport")
      ? "transport"
      : "type";
  const server: MCPServerConfig = {
    ...original,
    enabled: original?.enabled ?? true,
    description: draft.description,
    url: draft.url,
    [transportField]: draft.type,
  };
  // The Gateway restores a masked header by its exact, case-sensitive key.
  if (!original || draft.tokenChanged) {
    const key = Object.keys(record(original?.headers))[0] ?? "Authorization";
    server.headers = draft.token ? { [key]: `Bearer ${draft.token}` } : {};
  }
  return server;
}

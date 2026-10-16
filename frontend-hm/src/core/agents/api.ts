import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
export { fetchAgentsApiEnabled } from "@/core/features/api";

import type { Agent, CreateAgentRequest, UpdateAgentRequest } from "./types";

const BACKEND_UNAVAILABLE_STATUSES = new Set([502, 503, 504]);

function definitionRequest(
  url: string,
  init?: RequestInit,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  if (!signal) return init ? fetch(url, init) : fetch(url);
  return awaitAbortable(fetch(url, { ...init, signal }), signal, (late) => {
    void late.body?.cancel().catch(() => undefined);
  });
}

function definitionJSON<T>(
  response: Response,
  signal?: AbortSignal,
): Promise<T> {
  if (signal?.aborted) {
    void response.body?.cancel().catch(() => undefined);
    signal.throwIfAborted();
  }
  const reading = response.json() as Promise<T>;
  return signal ? awaitAbortable(reading, signal) : reading;
}

async function definitionFailure(
  response: Response,
  signal?: AbortSignal,
): Promise<{ detail?: string }> {
  try {
    return await definitionJSON<{ detail?: string }>(response, signal);
  } catch (error) {
    signal?.throwIfAborted();
    if (error instanceof Error && error.name === "AbortError") throw error;
    return {};
  }
}

export class AgentNameCheckError extends Error {
  constructor(
    message: string,
    public readonly reason: "backend_unreachable" | "request_failed",
    /**
     * Raw backend `detail` string when the failure came from a backend
     * response carrying one. `null` when no detail was provided (e.g.
     * network-layer failure, empty response body, unparseable body) — in
     * which case `message` is a generated fallback like "Failed to check
     * agent name: Bad Gateway" and the UI should prefer its own localized
     * fallback instead of surfacing the generated string.
     */
    public readonly detail: string | null = null,
  ) {
    super(message);
    this.name = "AgentNameCheckError";
  }
}

export class AgentsApiDisabledError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentsApiDisabledError";
  }
}

/** Adoption creates a missing source only after an authoritative 404. */
export class AgentDefinitionError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "AgentDefinitionError";
  }
}

function isAgentsApiDisabledDetail(detail: string | undefined): boolean {
  return typeof detail === "string" && detail.includes("agents_api.enabled");
}

export async function listAgents(signal?: AbortSignal): Promise<Agent[]> {
  const res = await definitionRequest(
    `${getBackendBaseURL()}/api/agents`,
    undefined,
    signal,
  );
  if (!res.ok) throw new Error(`Failed to load agents: ${res.statusText}`);
  const data = await definitionJSON<{ agents: Agent[] }>(res, signal);
  return data.agents;
}

export async function getAgent(
  name: string,
  signal?: AbortSignal,
): Promise<Agent> {
  const res = await definitionRequest(
    `${getBackendBaseURL()}/api/agents/${name}`,
    undefined,
    signal,
  );
  if (!res.ok)
    throw new AgentDefinitionError(
      res.status,
      res.status === 404
        ? `Agent '${name}' not found`
        : `Failed to load agent (${res.status})`,
    );
  return definitionJSON<Agent>(res, signal);
}

export async function createAgent(
  request: CreateAgentRequest,
  signal?: AbortSignal,
): Promise<Agent> {
  const res = await definitionRequest(
    `${getBackendBaseURL()}/api/agents`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    signal,
  );
  if (!res.ok) {
    const err = await definitionFailure(res, signal);
    if (isAgentsApiDisabledDetail(err.detail)) {
      throw new AgentsApiDisabledError(err.detail!);
    }
    throw new Error(err.detail ?? `Failed to create agent: ${res.statusText}`);
  }
  return definitionJSON<Agent>(res, signal);
}

export async function updateAgent(
  name: string,
  request: UpdateAgentRequest,
  signal?: AbortSignal,
): Promise<Agent> {
  const res = await definitionRequest(
    `${getBackendBaseURL()}/api/agents/${name}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    signal,
  );
  if (!res.ok) {
    const err = await definitionFailure(res, signal);
    throw new Error(err.detail ?? `Failed to update agent: ${res.statusText}`);
  }
  return definitionJSON<Agent>(res, signal);
}

export async function deleteAgent(
  name: string,
  signal?: AbortSignal,
): Promise<void> {
  const res = await definitionRequest(
    `${getBackendBaseURL()}/api/agents/${name}`,
    {
      method: "DELETE",
    },
    signal,
  );
  if (!res.ok) throw new Error(`Failed to delete agent: ${res.statusText}`);
}

export async function checkAgentName(
  name: string,
): Promise<{ available: boolean; name: string }> {
  let res: Response;
  try {
    res = await fetch(
      `${getBackendBaseURL()}/api/agents/check?name=${encodeURIComponent(name)}`,
    );
  } catch {
    throw new AgentNameCheckError(
      "Could not reach the server.",
      "backend_unreachable",
    );
  }

  if (!res.ok) {
    const err = (await res.json().catch(() => ({}))) as { detail?: string };
    if (isAgentsApiDisabledDetail(err.detail)) {
      throw new AgentsApiDisabledError(err.detail!);
    }
    if (BACKEND_UNAVAILABLE_STATUSES.has(res.status)) {
      throw new AgentNameCheckError(
        "Could not reach the server.",
        "backend_unreachable",
      );
    }
    const backendDetail = typeof err.detail === "string" ? err.detail : null;
    throw new AgentNameCheckError(
      backendDetail ?? `Failed to check agent name: ${res.statusText}`,
      "request_failed",
      backendDetail,
    );
  }
  return res.json() as Promise<{ available: boolean; name: string }>;
}

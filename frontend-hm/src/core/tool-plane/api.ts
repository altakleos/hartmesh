import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import type {
  ToolPlaneGovernance,
  ToolPlaneRevisionList,
  ToolPlaneScopeKind,
  ToolPlaneStatus,
} from "./types";

export class ToolPlaneRequestError extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(status: number, code: string | null, message: string) {
    super(message);
    this.name = "ToolPlaneRequestError";
    this.status = status;
    this.code = code;
  }

  get isServiceUnavailable(): boolean {
    return this.status === 503 && this.code === "tool_plane_unavailable";
  }
}

async function requestError(
  response: Response,
): Promise<ToolPlaneRequestError> {
  const body = (await response.json().catch(() => ({}))) as {
    detail?: unknown;
  };
  const detail = body.detail;
  if (typeof detail === "object" && detail !== null) {
    const record = detail as Record<string, unknown>;
    const code = typeof record.code === "string" ? record.code : null;
    const message =
      typeof record.message === "string"
        ? record.message
        : "Failed to load governed tool-plane status";
    return new ToolPlaneRequestError(response.status, code, message);
  }
  return new ToolPlaneRequestError(
    response.status,
    null,
    typeof detail === "string"
      ? detail
      : "Failed to load governed tool-plane status",
  );
}

async function loadJson<T>(path: string): Promise<T> {
  const response = await fetch(`${getBackendBaseURL()}${path}`);
  if (!response.ok) {
    throw await requestError(response);
  }
  return response.json() as Promise<T>;
}

export async function loadToolPlaneGovernance(
  scopeKind: ToolPlaneScopeKind,
): Promise<ToolPlaneGovernance> {
  const query = new URLSearchParams({ scope_kind: scopeKind });
  const [status, revisions] = await Promise.all([
    loadJson<ToolPlaneStatus>(`/api/tool-plane/status?${query}`),
    loadJson<ToolPlaneRevisionList>(
      `/api/tool-plane/revisions?${query}&limit=10`,
    ),
  ]);
  return { status, revisions: revisions.revisions };
}

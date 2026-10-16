import { request } from "@/core/agent-instances/api";
import { getBackendBaseURL } from "@/core/config";

export interface Person {
  id: string;
  email: string;
  name: string | null;
  active: boolean;
}

export function searchPeople(query: string, signal?: AbortSignal, limit = 20) {
  const params = new URLSearchParams({ query, limit: String(limit) });
  return request<{ users: Person[] }>(
    `${getBackendBaseURL()}/api/v1/auth/users?${params}`,
    { signal },
  );
}

export function getPeople(ids: string[], signal?: AbortSignal) {
  const params = new URLSearchParams();
  for (const id of ids) params.append("ids", id);
  return request<{ users: Person[] }>(
    `${getBackendBaseURL()}/api/v1/auth/users?${params}`,
    { signal },
  );
}

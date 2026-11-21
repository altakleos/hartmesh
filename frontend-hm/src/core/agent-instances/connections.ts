import { useQuery } from "@tanstack/react-query";

import { useAuth } from "@/core/auth/AuthProvider";

import { instanceBaseURL, request, type AgentInstance } from "./api";
import { hasCeiling } from "./presentation";

export type ConnectionReason =
  | "switched-off"
  | "not-remote"
  | "per-person-credentials"
  | "per-request-credentials"
  | "agent-credential"
  | "long-running"
  | "ambiguous";
interface VisibleConnection {
  name: string;
  host: string;
}
export interface ManagedConnection extends VisibleConnection {
  id: string;
  description: string;
  given: boolean;
  given_at: string | null;
  given_by: string | null;
  can_give: boolean;
  reason: ConnectionReason | null;
}
export type AgentConnection = VisibleConnection | ManagedConnection;
export interface ConnectionList {
  connections: AgentConnection[];
  chat_member_count?: number;
}
export function isManaged(row: AgentConnection): row is ManagedConnection {
  return "id" in row;
}
export function activeConnectionNames(rows: AgentConnection[]) {
  return rows
    .filter((row) => !isManaged(row) || (row.given && row.reason === null))
    .map((row) => row.name);
}
export function useInstanceConnections(
  instance: Pick<AgentInstance, "id" | "generation" | "permissions">,
  refreshWhileVisible = false,
) {
  const { user } = useAuth();
  const key = [
    "agent-instances",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance.id,
    "connections",
    instance.generation,
    instance.permissions,
  ] as const;
  const query = useQuery({
    queryKey: key,
    enabled: !!user && hasCeiling(user.permissions, "agents:read"),
    queryFn: ({ signal }) =>
      request<ConnectionList>(instanceBaseURL(instance.id) + "/connections", {
        signal,
      }),
    retry: false,
    refetchOnWindowFocus: "always",
    refetchInterval: refreshWhileVisible ? 30_000 : false,
  });
  return {
    query,
    key,
    scope: JSON.stringify(key),
    data: !query.isError ? query.data : undefined,
  };
}
export function setConnection(
  instance: string,
  id: string,
  give: boolean,
  signal: AbortSignal,
) {
  return request<void>(
    instanceBaseURL(instance) + "/connections/" + encodeURIComponent(id),
    { method: give ? "PUT" : "DELETE", signal },
  );
}

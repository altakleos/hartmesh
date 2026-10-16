"use client";

import { useQuery } from "@tanstack/react-query";

import { getInstance, type AgentInstance } from "@/core/agent-instances/api";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";

import { AgentAvatar } from "./instances/agent-avatar";
import { PersonName } from "./person-name";

type NamedAgent = Pick<AgentInstance, "id" | "name" | "custody">;
function AgentActor({ id, instance }: { id: string; instance?: NamedAgent }) {
  const { user } = useAuth();
  const { t } = useI18n();
  const known = instance?.id === id ? instance : null;
  const query = useQuery({
    queryKey: [
      "actor-agent",
      user?.id,
      user?.system_role,
      user?.permissions,
      id,
    ],
    queryFn: ({ signal }) => getInstance(id, signal),
    enabled:
      !known &&
      /^[0-9a-f]{32}$/.test(id) &&
      hasCeiling(user?.permissions, "agents:read"),
    retry: false,
    gcTime: 0,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
  const agent = known ?? (query.isError ? null : query.data);
  return agent ? (
    <span className="inline-flex items-center gap-1">
      <AgentAvatar instance={agent} className="size-6" />
      {agent.name}
    </span>
  ) : (
    <span>{query.isPending ? "…" : t.people.unknown}</span>
  );
}
export function ActorName({
  id,
  kind,
  instance,
}: {
  id: string;
  kind?: "human" | "nonhuman" | "host";
  instance?: NamedAgent;
}) {
  const { t } = useI18n();
  return kind === "host" ? (
    t.agentWork.host
  ) : id.startsWith("agent:") ? (
    <AgentActor id={id.slice(6)} instance={instance} />
  ) : (
    <PersonName id={id} />
  );
}

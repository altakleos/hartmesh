import { useQuery } from "@tanstack/react-query";

import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";

import {
  getConversationInstance,
  listInstances,
  isFullInstance,
  type AgentInstance,
  type Custody,
} from "./api";

/** The server binding owns identity; metadata and URL parameters are display data. */
export function useConversationInstance(threadId: string, enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["conversation-instance", user?.id, user?.system_role, threadId],
    queryFn: ({ signal }) => getConversationInstance(threadId, signal),
    enabled: enabled && !!user,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
}

/** Grant-bearing records only: inventory summaries convey no file authority. */
export function useAgentHomes(enabled: boolean) {
  const { user } = useAuth();
  const query = useQuery({
    queryKey: ["agent-homes", user?.id, user?.system_role, user?.permissions],
    enabled: enabled && !!user,
    queryFn: async ({ signal }) => {
      async function read(custody: Custody) {
        const agents: AgentInstance[] = [];
        for (let offset = 0; ; offset += 100) {
          const page = await awaitAbortable(
            listInstances(custody, offset, signal),
            signal,
          );
          agents.push(
            ...page.instances
              .filter(isFullInstance)
              .filter((agent) => agent.status !== "deleted"),
          );
          if (page.instances.length < 100) return agents;
        }
      }
      return (await Promise.all([read("personal"), read("company")])).flat();
    },
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  return { ...query, data: enabled && !query.isError ? query.data : undefined };
}

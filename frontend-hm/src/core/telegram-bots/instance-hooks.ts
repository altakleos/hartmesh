"use client";

import {
  useQueries,
  useQuery,
  useQueryClient,
  type QueryClient,
} from "@tanstack/react-query";

import {
  getInstance,
  InstanceApiError,
  type AgentInstance,
  type AgentInstanceDetail,
} from "@/core/agent-instances/api";
import {
  canManageInstance,
  hasCeiling,
} from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useTelegram } from "@/core/features/hooks";

import { getTelegramPolicy } from "./api";
import { telegramBotsQueryPrefix, useTelegramBots } from "./hooks";
import { InstanceReadPool, readTelegramInstances } from "./instance-reads";

const pools = new WeakMap<QueryClient, InstanceReadPool>();
function poolOf(client: QueryClient) {
  let pool = pools.get(client);
  if (!pool) {
    pool = new InstanceReadPool();
    pools.set(client, pool);
  }
  return pool;
}

/** Both lists are complete before a missing instance is interpreted as lost access. */
export function useTelegramInstanceDiscovery() {
  const { user } = useAuth();
  const telegram = useTelegram();
  const feature = useAgentInstances();
  const enabled =
    !!user &&
    telegram.known &&
    feature.known &&
    feature.enabled &&
    hasCeiling(user.permissions, "agents:read");
  const queries = useQueries({
    queries: (["personal", "company"] as const).map((custody) => ({
      queryKey: [
        "agent-instances",
        user?.id,
        user?.system_role,
        user?.permissions,
        "telegram-discovery",
        custody,
      ],
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        readTelegramInstances(custody, signal),
      enabled,
      retry: false,
      refetchInterval: 10_000,
    })),
  });
  const complete = enabled && queries.every((query) => query.isSuccess);
  return {
    feature,
    retry: () => Promise.all(queries.map((query) => query.refetch())),
    complete,
    pending: enabled && queries.some((query) => query.isPending),
    error: enabled && queries.some((query) => query.isError),
    instances: complete ? queries.flatMap((query) => query.data ?? []) : [],
  };
}

type DetailKey = Pick<AgentInstance, "id"> &
  Partial<Pick<AgentInstance, "generation" | "permissions">>;
function detailOptions(
  client: QueryClient,
  user: ReturnType<typeof useAuth>["user"],
  instance?: DetailKey,
) {
  return {
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance?.id,
      "telegram-detail",
      instance?.generation,
      instance?.permissions,
    ],
    enabled:
      !!instance && !!user && hasCeiling(user.permissions, "agents:read"),
    retry: false,
    refetchInterval: 10_000,
    queryFn: ({ signal }: { signal: AbortSignal }) =>
      poolOf(client).read(
        signal,
        async (): Promise<AgentInstanceDetail | null> => {
          try {
            return await getInstance(instance!.id, signal);
          } catch (error) {
            if (error instanceof InstanceApiError && error.status === 404)
              return null;
            throw error;
          }
        },
      ),
  };
}

/** The list projection does not carry per-instance execution. All views share four readers. */
export function useTelegramInstanceDetail(instance?: DetailKey) {
  const { user } = useAuth();
  const client = useQueryClient();
  return useQuery(detailOptions(client, user, instance));
}

/** The sidebar reads only owned bot targets, with the same cache and admission bound. */
export function useTelegramInstanceDetails(instances: DetailKey[]) {
  const { user } = useAuth();
  const client = useQueryClient();
  return useQueries({
    queries: instances.map((instance) => detailOptions(client, user, instance)),
  });
}

/** Personal assistants have no policy route: only the reader's own connected bot counts. */
export function useTelegramConnectedCount(instance: AgentInstance) {
  const { user } = useAuth();
  const telegram = useTelegram();
  const bots = useTelegramBots();
  const eligible =
    telegram.known &&
    !!user &&
    canManageInstance(instance.permissions) &&
    hasCeiling(user.permissions, "agents:read");
  const policy = useQuery({
    queryKey: [
      ...telegramBotsQueryPrefix,
      user?.id,
      user?.system_role,
      user?.permissions,
      "connected-count",
      instance.id,
      instance.generation,
      instance.permissions,
    ],
    queryFn: ({ signal }) => getTelegramPolicy(instance.id, signal),
    enabled: eligible && instance.custody === "company",
    retry: false,
  });
  if (!eligible) return 0;
  if (instance.custody === "company")
    return policy.isError ? 0 : (policy.data?.connected ?? 0);
  return bots.data?.bots.some(
    (bot) =>
      bot.agent_kind === "instance" &&
      bot.agent_ref === instance.id &&
      bot.state === "connected",
  )
    ? 1
    : 0;
}

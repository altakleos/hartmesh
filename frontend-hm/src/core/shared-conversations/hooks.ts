import {
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useFeatures } from "@/core/features";

import { snapshotDetail, snapshotList } from "./api";
import {
  sharedConversationOperations,
  type SharingOperation,
} from "./protocol";

export function useSharing() {
  const { user } = useAuth();
  const discovery = useFeatures(sharedConversationOperations);
  return {
    account: user?.id,
    agentPublishing:
      !discovery.isError && discovery.data?.includes("agent_publish") === true,
    canUndo: !!user && hasCeiling(user.permissions, "threads:delete"),
    scope: JSON.stringify([user?.id, user?.system_role, user?.permissions]),
    can: (action: SharingOperation) =>
      !!user &&
      hasCeiling(user.permissions, "threads:read") &&
      (action === "read" || hasCeiling(user.permissions, "threads:write")) &&
      (!["publish", "agent_publish"].includes(action) ||
        hasCeiling(user.permissions, "runs:read")) &&
      !discovery.isError &&
      Array.isArray(discovery.data) &&
      discovery.data.includes(action),
  };
}
export function useSnapshotList(enabled: boolean, scope: string) {
  return useInfiniteQuery({
    queryKey: ["shared-conversations", scope, "list"],
    queryFn: ({ signal, pageParam }) => snapshotList(signal, pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.next_before ?? undefined,
    select: (result) => ({
      conversations: result.pages.flatMap((page) => page.conversations),
      truncated: result.pages.at(-1)?.truncated ?? false,
    }),
    enabled,
    retry: false,
    gcTime: 0,
  });
}
export function useSnapshotDetail(
  id: string | undefined,
  enabled: boolean,
  scope: string,
) {
  return useQuery({
    queryKey: ["shared-conversations", scope, "detail", id],
    queryFn: ({ signal }) => snapshotDetail(id!, signal),
    enabled: enabled && !!id,
    retry: false,
    gcTime: 0,
  });
}
export function useRefreshShared() {
  const client = useQueryClient();
  return () =>
    Promise.all(
      [["shared-conversations"], ["files", "shared"], ["file-browser"]].map(
        (queryKey) => client.invalidateQueries({ queryKey }),
      ),
    );
}

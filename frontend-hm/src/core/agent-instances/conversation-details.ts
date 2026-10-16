import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import { useAuth } from "@/core/auth/AuthProvider";

import { getInstance, getInstanceLifecycle, InstanceApiError } from "./api";
import type { ConversationDetailStatus } from "./conversation-presentation";
import { hasCeiling, INSPECT, MANAGE } from "./presentation";

export function useConversationDetails(
  instance: string | null,
  { enabled, refreshKey }: { enabled: boolean; refreshKey: string },
) {
  const { user } = useAuth();
  const readable =
    enabled &&
    !!instance &&
    !!user &&
    hasCeiling(user.permissions, "agents:read");
  const scope = [
    "conversation-agent",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance,
  ];
  const detail = useQuery({
    queryKey: [...scope, "detail"],
    queryFn: ({ signal }) => getInstance(instance!, signal),
    enabled: readable,
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  const current = readable && !detail.isError ? (detail.data ?? null) : null;
  const previous = useRef<{ scope: string; key: string } | null>(null);
  const identity = JSON.stringify([scope, readable]);
  const refetch = detail.refetch;
  useEffect(() => {
    const last = previous.current;
    previous.current = { scope: identity, key: refreshKey };
    if (readable && last?.scope === identity && last.key !== refreshKey)
      void refetch();
  }, [identity, readable, refreshKey, refetch]);
  const lifecycle = useQuery({
    queryKey: [
      ...scope,
      "lifecycle",
      current?.generation,
      current?.permissions,
    ],
    queryFn: ({ signal }) => getInstanceLifecycle(instance!, signal),
    enabled:
      readable &&
      current?.status === "suspended" &&
      !!(current.permissions & (INSPECT | MANAGE)),
    retry: false,
    staleTime: 0,
    gcTime: 0,
    refetchInterval: (query) =>
      query.state.data?.operations.some((operation) => !operation.complete)
        ? 10_000
        : false,
    refetchIntervalInBackground: false,
  });
  const observedPending = useRef<{ scope: string; ids: Set<string> } | null>(
    null,
  );
  useEffect(() => {
    if (!readable || observedPending.current?.scope !== identity)
      observedPending.current = { scope: identity, ids: new Set() };
    if (!readable || lifecycle.isError || !lifecycle.data) return;
    const previousIds = observedPending.current.ids;
    const completed = lifecycle.data.operations.some(
      (operation) =>
        operation.complete && previousIds.has(operation.operation_id),
    );
    observedPending.current = {
      scope: identity,
      ids: new Set(
        lifecycle.data.operations
          .filter((operation) => !operation.complete)
          .map((operation) => operation.operation_id),
      ),
    };
    // A receipt proves the operation ended, not that the agent became active.
    if (completed) void refetch();
  }, [identity, lifecycle.data, lifecycle.isError, readable, refetch]);
  const status: ConversationDetailStatus = !readable
    ? "forbidden"
    : detail.isError
      ? detail.error instanceof InstanceApiError && detail.error.status === 404
        ? "not-found"
        : detail.error instanceof InstanceApiError &&
            detail.error.status === 403
          ? "forbidden"
          : "error"
      : detail.isPending
        ? "loading"
        : "ready";
  return {
    detail: current,
    status,
    pending:
      !lifecycle.isError &&
      lifecycle.data?.operations.some((operation) => !operation.complete) ===
        true,
    refresh: refetch,
  };
}

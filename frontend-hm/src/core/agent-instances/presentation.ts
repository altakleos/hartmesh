import type {
  AgentInstancesFeature,
  ExecutionReason,
} from "@/core/features/api";

import type { AgentInstanceDetail } from "./api";

export const USE = 1,
  INSPECT = 2,
  MANAGE = 4;

export function canManageInstance(permissions: number) {
  return (permissions & (MANAGE | INSPECT)) === (MANAGE | INSPECT);
}

export function hasCeiling(
  permissions: readonly string[] | null | undefined,
  permission: string,
) {
  return (
    permissions == null ||
    permissions.some(
      (p) =>
        p === "*" || p === permission || p === `${permission.split(":")[0]}:*`,
    )
  );
}

export interface AgentBlock {
  reason: ExecutionReason | null;
  busy: boolean;
}

/** Execution readiness is independent of whether a workspace is starting. */
export function agentBlock(
  instance: AgentInstanceDetail,
  feature: Pick<
    AgentInstancesFeature,
    "known" | "enabled" | "execution" | "reason"
  >,
): AgentBlock | null {
  if (!feature.known || !feature.enabled || feature.execution === "unavailable")
    return { reason: feature.reason, busy: false };
  if (instance.execution.execution === "unavailable")
    return { reason: instance.execution.reason, busy: false };
  if (instance.home?.state === "busy") return { reason: null, busy: true };
  return null;
}

import type { AgentThread } from "@/core/threads/types";

import type { AgentInstanceDetail } from "./api";
import {
  agentBlock,
  canManageInstance,
  INSPECT,
  MANAGE,
  USE,
} from "./presentation";

type Binding = NonNullable<AgentThread["agent_instance"]>;
export function conversationIdentity({
  threadId,
  isNew,
  isMock,
  createdHere,
  row,
  rowKnown,
}: {
  threadId: string;
  isNew: boolean;
  isMock: boolean;
  createdHere?: string | null;
  row?: Pick<AgentThread, "agent_instance"> | null;
  rowKnown: boolean;
}) {
  const agent = rowKnown && row ? (row.agent_instance ?? null) : null;
  return {
    agent,
    legacyCapabilities:
      isNew ||
      isMock ||
      createdHere === threadId ||
      (rowKnown && row?.agent_instance === null),
  };
}
export type ConversationDetailStatus =
  | "loading"
  | "ready"
  | "not-found"
  | "forbidden"
  | "error";
export type ConversationSendBlock =
  | "deleted"
  | "archived"
  | "suspended"
  | "changing"
  | "model-off"
  | "withdrawn"
  | "unavailable"
  | "no-use"
  | "loading";
export function conversationSendBlock({
  binding,
  detail,
  feature,
  detailStatus,
  pendingLifecycle,
}: {
  binding: Binding;
  detail: AgentInstanceDetail | null;
  feature: {
    known: boolean;
    enabled: boolean;
    execution?: "ready" | "unavailable";
    reason?: Parameters<typeof agentBlock>[1]["reason"];
  };
  detailStatus: ConversationDetailStatus;
  pendingLifecycle: boolean;
}): ConversationSendBlock | null {
  const status = detail?.status ?? binding.status;
  if (status === "deleted" || status === "archived") return status;
  if (status === "suspended")
    return pendingLifecycle &&
      !!detail &&
      !!(detail.permissions & (INSPECT | MANAGE))
      ? "changing"
      : "suspended";
  if (!feature.known || !feature.enabled) return "model-off";
  if (detailStatus === "not-found") return "withdrawn";
  if (detailStatus === "loading") return "loading";
  if (detailStatus !== "ready" || !detail) return "unavailable";
  if (
    agentBlock(detail, {
      ...feature,
      execution: feature.execution ?? "ready",
      reason: feature.reason ?? null,
    })
  )
    return "unavailable";
  if (!(detail.permissions & USE)) return "no-use";
  return null;
}
export function conversationStopAuthority({
  threadId,
  binding,
  detail,
  userId,
  sentHere,
  canCancel,
}: {
  threadId: string;
  binding: Binding;
  detail: AgentInstanceDetail | null;
  userId: string | undefined;
  sentHere: boolean;
  canCancel: boolean;
}) {
  const current = detail?.activity.running.find(
    (run) => run.thread_id === threadId,
  );
  const workId = current?.work_id ?? null;
  if (!canCancel || !detail || !userId)
    return { ...(workId ? { workId } : {}), allowed: false };
  const manager = canManageInstance(detail.permissions);
  const requester = current
    ? current.requester_id === userId
    : sentHere ||
      (detail.activity.running.length === 0 && binding.requester_id === userId);
  const allowed =
    manager ||
    (requester &&
      (!workId || (detail.permissions & (USE | INSPECT)) === (USE | INSPECT)));
  return { ...(workId ? { workId } : {}), allowed };
}

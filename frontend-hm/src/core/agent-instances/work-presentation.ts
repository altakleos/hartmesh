import type { WorkEvent, WorkRecord } from "./api";

export type AttemptLabel =
  | NonNullable<WorkRecord["attempt"]>["status"]
  | "contained"
  | "notStarted"
  | "noResult";
export function workAttemptLabel(
  record: Pick<WorkRecord, "attempt" | "outcome">,
  stopReason?: string | null,
): AttemptLabel | null {
  const attempt = record.attempt;
  if (!attempt) return null;
  if (attempt.contained)
    return ["run-never-started", "start-failed"].includes(
      attempt.contained.reason,
    )
      ? "notStarted"
      : "contained";
  if (
    attempt.status === "starting" ||
    (attempt.status === "running" && attempt.workspace === "starting")
  )
    return "starting";
  if (
    attempt.status === "failed" &&
    stopReason &&
    [
      "busy",
      "run-limit",
      "restarting",
      "starting-elsewhere",
      "start-failed",
      "files-full",
      "storage-busy",
    ].includes(stopReason)
  )
    return "notStarted";
  if (attempt.status === "succeeded" && !record.outcome && !attempt.candidate)
    return "noResult";
  return attempt.status;
}
export type StopCause =
  | { kind: "plain" | "service" }
  | { kind: "stop" | "edit" | "cancel"; actor: string; at: string };
export function workStopCause(
  attempt: string,
  events: readonly Pick<
    WorkEvent,
    "action" | "actor_id" | "created_at" | "report"
  >[],
): StopCause {
  const activation = events.findIndex((event) => event.action === "activate");
  const settlement = events.findIndex(
    (event) =>
      event.action === "attempt_settled" &&
      event.report?.attempt_id === attempt,
  );
  if (activation < 0 || settlement < 0 || settlement >= activation)
    return { kind: "plain" };
  for (let index = activation - 1; index > settlement; index--) {
    const event = events[index]!;
    if (
      event.action === "stop" ||
      event.action === "edit" ||
      event.action === "cancel"
    )
      return {
        kind: event.action,
        actor: event.actor_id,
        at: event.created_at,
      };
  }
  return { kind: "service" };
}
export function workActivationBlock(state: {
  pending: boolean;
  status: string;
  policy: boolean;
  execution: boolean;
  busy: boolean;
  reconcile: boolean;
  capacity: { max: number; in_use: number } | null;
}):
  | "pending"
  | "paused"
  | "retired"
  | "policy"
  | "execution"
  | "reconcile"
  | "capacity"
  | null {
  if (state.pending) return "pending";
  if (state.status === "suspended") return "paused";
  if (state.status === "archived" || state.status === "deleted")
    return "retired";
  if (!state.policy) return "policy";
  if (!state.execution || state.busy) return "execution";
  if (state.reconcile) return "reconcile";
  if (state.capacity && state.capacity.in_use >= state.capacity.max)
    return "capacity";
  return null;
}

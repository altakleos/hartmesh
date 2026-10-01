/**
 * What a run says about itself while the person waits.
 *
 * The worker publishes one advisory `custom` frame per stage as the phase
 * begins — `preparing` at admission (before any model token exists),
 * `workspace_starting` when a sandbox is being created, `thinking` at the
 * first model request — and nothing for tools, whose cards the client already
 * draws (`backend/.../runtime/turn_progress.py`). A stage says what the run
 * is doing now, never how long it will take; a client that missed the frame
 * loses a label and nothing else.
 */

export const TURN_PROGRESS_EVENT = "turn_progress";

export const TURN_PROGRESS_STAGES = [
  "preparing",
  "workspace_starting",
  "thinking",
] as const;

export type TurnProgressStage = (typeof TURN_PROGRESS_STAGES)[number];

export interface TurnProgress {
  runId: string;
  stage: TurnProgressStage;
  /** Milliseconds since the run was admitted, from the turn's own clock. */
  atMs: number;
}

function isStage(value: unknown): value is TurnProgressStage {
  return (
    typeof value === "string" &&
    (TURN_PROGRESS_STAGES as readonly string[]).includes(value)
  );
}

/**
 * Narrow a custom stream event into a progress frame, or `null` for any other
 * event. Refuses anything partial or with an unknown stage: a label for a
 * stage this client cannot name is worse than "Working…".
 */
export function parseTurnProgress(event: unknown): TurnProgress | null {
  if (typeof event !== "object" || event === null) return null;
  const value = event as Record<string, unknown>;
  if (value.type !== TURN_PROGRESS_EVENT) return null;
  const runId = value.run_id;
  const atMs = value.at_ms;
  if (typeof runId !== "string" || runId.length === 0) return null;
  if (!isStage(value.stage)) return null;
  if (typeof atMs !== "number" || !Number.isFinite(atMs) || atMs < 0) {
    return null;
  }
  return { runId, stage: value.stage, atMs };
}

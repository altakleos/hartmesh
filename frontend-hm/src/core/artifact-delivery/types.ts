/**
 * A run that produced files and finished without presenting any of them.
 *
 * The backend's delivery fence already failed such a run, but until
 * hartmesh-tenancy/DF13 the browser never heard about it: the stream ended
 * normally and the assistant's prose read as a success. The worker now
 * publishes the verdict as a ``custom`` frame immediately before the ``error``
 * frame, because the LangGraph SDK reduces an ``error`` frame to an ``Error``
 * carrying only ``message``/``name`` and stops reading the stream there.
 */
export interface ArtifactDeliveryFailure {
  runId: string;
  message: string;
  /** Produced but never presented. Bounded by the backend. */
  undeliveredPaths: string[];
  /** Exact total, which can exceed `undeliveredPaths.length`. */
  undeliveredCount: number;
}

export const ARTIFACT_DELIVERY_INCOMPLETE_EVENT =
  "artifact_delivery_incomplete";

function readStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const paths = value.filter(
    (entry): entry is string => typeof entry === "string" && entry.length > 0,
  );
  return paths.length === value.length ? paths : null;
}

/**
 * Narrow a custom stream event into a delivery failure, or `null` when it is
 * any other event. Nothing partial is accepted: a notice that named the wrong
 * files, or none, would be worse than the silence it replaces.
 */
export function parseArtifactDeliveryFailure(
  event: unknown,
): ArtifactDeliveryFailure | null {
  if (typeof event !== "object" || event === null) return null;
  const value = event as Record<string, unknown>;
  if (value.type !== ARTIFACT_DELIVERY_INCOMPLETE_EVENT) return null;

  const undeliveredPaths = readStringArray(value.undelivered_paths);
  if (
    typeof value.run_id !== "string" ||
    !value.run_id ||
    typeof value.message !== "string" ||
    !value.message ||
    undeliveredPaths === null ||
    undeliveredPaths.length === 0 ||
    typeof value.undelivered_count !== "number" ||
    !Number.isInteger(value.undelivered_count) ||
    value.undelivered_count < undeliveredPaths.length
  ) {
    return null;
  }

  return {
    runId: value.run_id,
    message: value.message,
    undeliveredPaths,
    undeliveredCount: value.undelivered_count,
  };
}

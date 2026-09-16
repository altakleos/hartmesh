/**
 * A run that produced files and finished without presenting any of them.
 *
 * The backend's delivery fence already failed such a run, but until
 * hartmesh-tenancy/DF13 the browser never heard about it: the stream ended
 * normally and the assistant's prose read as a success. The worker now
 * publishes the verdict as an advisory ``custom`` frame and lets the stream
 * reach ``end`` — deliberately not an ``error`` frame, which the SDK reads as
 * "this stream carries no valid turn", stops reading at, and throws on. The
 * durable half of the same verdict is the run record's ``stop_reason``
 * (``artifact_delivery_incomplete``), so a client that reloaded or never
 * negotiated ``custom`` reads it over HTTP instead.
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
 * Read the verdict's own fields, wherever they arrived from. Nothing partial is
 * accepted: a notice that named the wrong files, or none, would be worse than
 * the silence it replaces.
 */
function readFailure(
  value: Record<string, unknown>,
): ArtifactDeliveryFailure | null {
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

/**
 * Narrow a custom stream event into a delivery failure, or `null` when it is
 * any other event.
 */
export function parseArtifactDeliveryFailure(
  event: unknown,
): ArtifactDeliveryFailure | null {
  if (typeof event !== "object" || event === null) return null;
  const value = event as Record<string, unknown>;
  if (value.type !== ARTIFACT_DELIVERY_INCOMPLETE_EVENT) return null;
  return readFailure(value);
}

/**
 * Narrow the durable verdict a rejoining client reads over HTTP.
 *
 * Same fields, same strictness, deliberately the same reader: the live frame
 * and the receipt describe one run, and a reload must not change what the
 * notice says (hartmesh-tenancy/DF14). `available: false` is the ordinary
 * answer — almost every run delivered what it produced — so it is `null` here,
 * not a failure to report.
 */
export function parseArtifactDeliveryRecord(
  record: unknown,
): ArtifactDeliveryFailure | null {
  if (typeof record !== "object" || record === null) return null;
  const value = record as Record<string, unknown>;
  if (value.available !== true) return null;
  return readFailure(value);
}

export const ARTIFACT_DELIVERY_UNVERIFIED_EVENT =
  "artifact_delivery_unverified";

/**
 * The sibling verdict: the run *did* present its files, but the durable
 * delivery receipt could not be written. There is nothing to offer under the
 * turn — the files are already attached — so this carries no paths.
 */
export interface ArtifactDeliveryUnverified {
  runId: string;
  message: string;
}

export function parseArtifactDeliveryUnverified(
  event: unknown,
): ArtifactDeliveryUnverified | null {
  if (typeof event !== "object" || event === null) return null;
  const value = event as Record<string, unknown>;
  if (value.type !== ARTIFACT_DELIVERY_UNVERIFIED_EVENT) return null;
  if (
    typeof value.run_id !== "string" ||
    !value.run_id ||
    typeof value.message !== "string" ||
    !value.message
  ) {
    return null;
  }
  return { runId: value.run_id, message: value.message };
}

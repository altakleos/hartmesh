import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import {
  parseArtifactDeliveryRecord,
  type ArtifactDeliveryFailure,
} from "./types";

/**
 * The run's durable delivery verdict, or `null` when it delivered what it
 * produced — which is almost every run, and is not an error.
 *
 * A transient failure is also `null` rather than a throw: this call exists to
 * restore a correction after a reload, and a chat that refused to render
 * because the correction could not be re-read would be a worse outcome than the
 * one it is fixing.
 */
export async function fetchRunDelivery({
  threadId,
  runId,
}: {
  threadId: string;
  runId: string;
}): Promise<ArtifactDeliveryFailure | null> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(
      threadId,
    )}/runs/${encodeURIComponent(runId)}/delivery`,
  );
  if (!response.ok) {
    return null;
  }
  return parseArtifactDeliveryRecord(await response.json().catch(() => null));
}

import { throwGatewayApiError } from "@/core/api/errors";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import {
  ARTIFACT_DELIVERY_INCOMPLETE_EVENT,
  parseArtifactDeliveryRecord,
  type ArtifactDeliveryFailure,
} from "./types";

/**
 * The runs of this thread whose terminal verdict says they withheld a file.
 *
 * One read per thread, and almost always the end of it: `stop_reason` is
 * committed with the terminal status, so this names the turns worth asking
 * about, and a healthy thread names none. Asking every anchored turn "did you
 * fail?" to find the one that did costs a request per turn, which is what the
 * card beside this one does and what this avoids.
 */
export async function fetchThreadDeliveryFailures(
  threadId: string,
): Promise<Set<string>> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/runs`,
  );
  if (!response.ok) {
    await throwGatewayApiError(response, "Failed to load this chat's runs.");
  }
  const runs: unknown = await response.json();
  if (!Array.isArray(runs)) {
    throw new Error("Invalid run list");
  }
  const failed = new Set<string>();
  for (const run of runs) {
    if (
      typeof run === "object" &&
      run !== null &&
      (run as Record<string, unknown>).stop_reason ===
        ARTIFACT_DELIVERY_INCOMPLETE_EVENT &&
      typeof (run as Record<string, unknown>).run_id === "string"
    ) {
      failed.add((run as Record<string, unknown>).run_id as string);
    }
  }
  return failed;
}

/**
 * The run's durable delivery verdict, or `null` when it delivered what it
 * produced — which is almost every run, and is not an error.
 *
 * A request that did not reach an answer throws instead, and the distinction is
 * the whole point: `null` is a statement about the run, and collapsing "I could
 * not ask" into it would let one 502 on a refetch replace a standing correction
 * with the silence this exists to end. React Query keeps the last good value
 * across a rejected query and replaces it across a resolved one.
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
    await throwGatewayApiError(
      response,
      "Failed to load the delivery verdict.",
    );
  }
  return parseArtifactDeliveryRecord(await response.json());
}

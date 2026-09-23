import { useQuery } from "@tanstack/react-query";

import { fetchRunDelivery, fetchThreadDeliveryFailures } from "./api";
import { useArtifactDeliveryFailure } from "./context";
import type { ArtifactDeliveryFailure } from "./types";

export function threadDeliveryQueryKey(threadId: string | undefined) {
  return ["artifact-delivery", "thread", threadId] as const;
}

export function runDeliveryQueryKey(
  threadId: string | undefined,
  runId: string | undefined,
) {
  return ["artifact-delivery", "run", threadId, runId] as const;
}

/** Terminal either way, so both answers cache; see `fetchRunDelivery`. */
const TERMINAL_VERDICT_STALE_TIME = 5 * 60 * 1000;

/**
 * This run's delivery verdict, from the stream if this client heard it and from
 * the durable receipt otherwise.
 *
 * Two reads, and for almost every thread only the first happens. The thread's
 * runs say which turns the delivery fence failed — one request, shared by every
 * notice on the page through one query key — and only a named turn asks for the
 * paths it withheld. A healthy thread therefore costs one read, not one per
 * turn, which is what asking every turn "did you fail?" would have cost.
 *
 * The live frame wins when both exist. They carry the same fields from the same
 * bytes, so this is not a precedence rule so much as an ordering one: the frame
 * arrives at the end of the turn, the fetch a moment later, and preferring the
 * frame keeps the notice from flickering through a second identical value.
 *
 * Gated on `runId` exactly like the workspace-changes card this sits beside,
 * which is what keeps every one of these reads off the static demo threads:
 * they carry no run id, and there is no run to ask about.
 */
export function useRunArtifactDelivery(
  threadId: string | undefined,
  runId: string | undefined,
  { enabled = true }: { enabled?: boolean } = {},
): ArtifactDeliveryFailure | undefined {
  const live = useArtifactDeliveryFailure(runId);
  const wanted = enabled && Boolean(threadId) && Boolean(runId) && !live;

  const { data: failedRunIds } = useQuery<Set<string>>({
    queryKey: threadDeliveryQueryKey(threadId),
    queryFn: () => {
      if (!threadId) {
        throw new Error("threadId is required");
      }
      return fetchThreadDeliveryFailures(threadId);
    },
    enabled: wanted,
    retry: false,
    staleTime: TERMINAL_VERDICT_STALE_TIME,
    refetchOnWindowFocus: false,
  });

  const { data } = useQuery<ArtifactDeliveryFailure | null>({
    queryKey: runDeliveryQueryKey(threadId, runId),
    queryFn: () => {
      if (!threadId || !runId) {
        throw new Error("threadId and runId are required");
      }
      return fetchRunDelivery({ threadId, runId });
    },
    enabled: wanted && Boolean(runId && failedRunIds?.has(runId)),
    retry: false,
    staleTime: TERMINAL_VERDICT_STALE_TIME,
    refetchOnWindowFocus: false,
  });

  return live ?? data ?? undefined;
}

import { useQuery } from "@tanstack/react-query";

import { fetchRunDelivery } from "./api";
import { useArtifactDeliveryFailure } from "./context";
import type { ArtifactDeliveryFailure } from "./types";

export function runDeliveryQueryKey(
  threadId: string | undefined,
  runId: string | undefined,
) {
  return ["artifact-delivery", threadId, runId] as const;
}

/**
 * This run's delivery verdict, from the stream if this client heard it and from
 * the durable receipt otherwise.
 *
 * The live frame wins when both exist. They carry the same fields from the same
 * bytes, so this is not a precedence rule so much as an ordering one: the frame
 * arrives at the end of the turn, the fetch a moment later, and preferring the
 * frame keeps the notice from flickering through a second identical value.
 *
 * Keyed and gated exactly like the workspace-changes card this sits beside, and
 * for the same reason: `runId` is absent on the public showcase's static
 * threads, so the query never fires there, where an authorized call would meet
 * a 401 and send a reader who is not signed in to the login page.
 */
export function useRunArtifactDelivery(
  threadId: string | undefined,
  runId: string | undefined,
  { enabled = true }: { enabled?: boolean } = {},
): ArtifactDeliveryFailure | undefined {
  const live = useArtifactDeliveryFailure(runId);
  const { data } = useQuery<ArtifactDeliveryFailure | null>({
    queryKey: runDeliveryQueryKey(threadId, runId),
    queryFn: () => {
      if (!threadId || !runId) {
        throw new Error("threadId and runId are required");
      }
      return fetchRunDelivery({ threadId, runId });
    },
    enabled: enabled && Boolean(threadId) && Boolean(runId) && !live,
    retry: false,
    // A verdict is terminal, so once one is in hand it never needs re-reading.
    // "No verdict" is the answer that can be wrong — a transient failure reads
    // the same as a run that delivered — and caching it would hide the
    // correction for the rest of the window, which is the failure this hook
    // exists to end.
    staleTime: (query) => (query.state.data ? 5 * 60 * 1000 : 0),
    refetchOnMount: true,
    refetchOnWindowFocus: false,
  });

  return live ?? data ?? undefined;
}

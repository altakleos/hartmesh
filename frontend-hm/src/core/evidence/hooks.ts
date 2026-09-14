import { useQuery } from "@tanstack/react-query";

import { fetchThreadEvidence } from "./api";

export const evidenceKey = (threadId: string) =>
  ["run-evidence", threadId] as const;

export function useThreadEvidence(
  threadId: string,
  options: { enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: evidenceKey(threadId),
    queryFn: ({ signal }) => fetchThreadEvidence(threadId, signal),
    enabled: options.enabled !== false && Boolean(threadId),
    retry: 2,
    refetchInterval: (query) =>
      query.state.data?.summary.overview.completeness === "in_progress"
        ? 3000
        : false,
    refetchIntervalInBackground: false,
  });
}

import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import { UnauthorizedError } from "@/core/api/errors";
import { fetch } from "@/core/api/fetcher";
import { urlOfArtifact } from "@/core/artifacts/utils";
import { isStaticWebsiteOnly } from "@/core/static-mode";

import type { ArtifactViewExport } from "./contract";

export async function probeArtifactExport({
  path,
  threadId,
  signal,
  isMock,
}: {
  path: string;
  threadId: string;
  signal: AbortSignal;
  isMock?: boolean;
}): Promise<"present" | "absent" | "error"> {
  signal.throwIfAborted();
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(abort, 10_000);
  try {
    signal.throwIfAborted();
    const response = await fetch(
      urlOfArtifact({ filepath: path, threadId, isMock }),
      {
        headers: { Range: "bytes=0-0" },
        cache: "no-store",
        signal: controller.signal,
      },
    );
    void response.body?.cancel().catch(() => undefined);
    signal.throwIfAborted();
    if (response.status === 200 || response.status === 206) return "present";
    if ([400, 403, 404, 410, 415, 416, 422].includes(response.status))
      return "absent";
    return "error";
  } catch (error) {
    signal.throwIfAborted();
    if (error instanceof UnauthorizedError) throw error;
    return "error";
  } finally {
    clearTimeout(timeout);
    signal.removeEventListener("abort", abort);
  }
}

/** Eligibility comes from presentation; a live read separately establishes availability. */
export function useLiveArtifactExports({
  eligible,
  filepath,
  threadId,
  revision,
  isMock,
  runSettled,
}: {
  eligible: readonly ArtifactViewExport[];
  filepath: string;
  threadId: string;
  revision?: string;
  isMock?: boolean;
  runSettled: boolean;
}) {
  const deterministic = Boolean(isMock) || isStaticWebsiteOnly();
  const enabled = eligible.length > 0 && !deterministic;
  const query = useQuery({
    queryKey: [
      "artifact-export-availability",
      threadId,
      filepath,
      revision ?? "",
      eligible.map((item) => item.path),
    ],
    enabled,
    gcTime: 0,
    staleTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const group = new AbortController();
      const abort = () => group.abort();
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
      const verdicts: {
        path: string;
        status: "present" | "absent" | "error";
      }[] = [];
      let next = 0;
      // A document has at most 16 exports. Four probes bound concurrent reads.
      try {
        await Promise.all(
          Array.from({ length: Math.min(4, eligible.length) }, async () => {
            try {
              while (next < eligible.length) {
                group.signal.throwIfAborted();
                const item = eligible[next++]!;
                verdicts.push({
                  path: item.path,
                  status: await probeArtifactExport({
                    path: item.path,
                    threadId,
                    isMock,
                    signal: group.signal,
                  }),
                });
              }
            } catch (error) {
              group.abort();
              throw error;
            }
          }),
        );
        signal.throwIfAborted();
        return {
          paths: verdicts
            .filter((item) => item.status === "present")
            .map((item) => item.path),
          uncertain: verdicts.some((item) => item.status === "error"),
        };
      } finally {
        signal.removeEventListener("abort", abort);
      }
    },
  });
  const refetch = query.refetch;
  const previouslySettled = useRef(runSettled);
  useEffect(() => {
    const settled = runSettled && !previouslySettled.current;
    previouslySettled.current = runSettled;
    if (settled && enabled) void refetch();
  }, [enabled, refetch, runSettled]);
  return {
    files: deterministic
      ? [...eligible]
      : eligible.filter((item) => query.data?.paths.includes(item.path)),
    settled: !enabled || query.isSuccess || query.isError,
    uncertain: enabled && (query.isError || Boolean(query.data?.uncertain)),
    checking: enabled && query.isFetching,
    retry: () => {
      void refetch();
    },
  };
}

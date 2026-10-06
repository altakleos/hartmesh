"use client";

import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import { loadArtifactContent } from "@/core/artifacts/loader";
import { useAuth } from "@/core/auth/AuthProvider";

import {
  associationCandidates,
  chooseSourceView,
  SOURCE_VIEW_MAX_READERS,
  type SourceView,
} from "./associations";
import { parseArtifactView } from "./contract";

export async function readSourceViews({
  filepath,
  presented,
  threadId,
  signal,
  isMock,
}: {
  filepath: string;
  presented: readonly string[];
  threadId: string;
  signal: AbortSignal;
  isMock?: boolean;
}): Promise<SourceView | null> {
  signal.throwIfAborted();
  const { paths } = associationCandidates(filepath, presented);
  if (!paths.length) return null;
  const group = new AbortController();
  const abort = () => group.abort(signal.reason);
  signal.addEventListener("abort", abort, { once: true });
  const snapshots: SourceView[] = [];
  let next = 0;
  try {
    await Promise.all(
      Array.from(
        { length: Math.min(SOURCE_VIEW_MAX_READERS, paths.length) },
        async () => {
          try {
            while (next < paths.length) {
              group.signal.throwIfAborted();
              const path = paths[next++]!;
              const loaded = await loadArtifactContent({
                filepath: path,
                threadId,
                signal: group.signal,
                isMock,
              });
              group.signal.throwIfAborted();
              if (loaded.truncated)
                throw new Error("Incomplete source view candidate");
              const view = parseArtifactView(loaded.content);
              if (!view) throw new Error("Unsupported source view candidate");
              snapshots.push({
                filepath: path,
                revision: loaded.sha256 ?? "",
                view,
              });
            }
          } catch (error) {
            group.abort();
            throw error;
          }
        },
      ),
    );
    signal.throwIfAborted();
    return chooseSourceView(filepath, snapshots);
  } finally {
    signal.removeEventListener("abort", abort);
  }
}

/** Only the active source's bounded candidate set is read or cached. */
export function useSourceView({
  filepath,
  presented,
  threadId,
  enabled,
  isMock,
  runSettled,
}: {
  filepath: string;
  presented: readonly string[];
  threadId: string;
  enabled: boolean;
  isMock?: boolean;
  runSettled: boolean;
}) {
  const { user } = useAuth();
  const candidates = associationCandidates(filepath, presented);
  const active = enabled && !!user?.id && candidates.paths.length > 0;
  const query = useQuery({
    queryKey: [
      "artifact-source-view",
      user?.id,
      threadId,
      filepath,
      candidates.paths,
      Boolean(isMock),
    ],
    enabled: active,
    queryFn: ({ signal }) =>
      readSourceViews({ filepath, presented, threadId, signal, isMock }),
    gcTime: 0,
    staleTime: 0,
    retry: false,
    refetchOnWindowFocus: true,
  });
  const previouslySettled = useRef(runSettled);
  const refetch = query.refetch;
  useEffect(() => {
    const settled = runSettled && !previouslySettled.current;
    previouslySettled.current = runSettled;
    if (settled && active) void refetch();
  }, [active, runSettled, refetch]);
  return {
    sourceView: active && !query.isError ? (query.data ?? null) : null,
    overBudget: candidates.overBudget,
  };
}

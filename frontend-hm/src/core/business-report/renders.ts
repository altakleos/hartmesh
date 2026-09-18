"use client";

/**
 * Which of a report's renders can actually be downloaded right now.
 *
 * `availableReportRenders` answers a different question: which formats this
 * thread ever *presented*. That list is cumulative and deliberately so — it is
 * history, and the artifact panel and the file chips on earlier messages both
 * need it. But a rebuild deletes the previous draft's renders while their
 * paths stay in the list, so a card drawn from eligibility alone promises
 * downloads that 404. A tenant met exactly that: three links, three 404s,
 * after a `prose` revision that rendered nothing.
 *
 * So the card asks two questions in order, and both must answer yes:
 *
 * 1. is this exact sibling path eligible under the presentation contract?
 *    (the delivery fence: a format nobody presented is never offered, however
 *    live the file is); then
 * 2. does that exact path resolve as a regular file *now*, through the same
 *    authenticated, owner-checked thread artifact route the download link
 *    uses?
 *
 * The second question is asked with a bounded range request and the body is
 * dropped unread, so proving a 40 MiB PDF is still there costs one byte. It is
 * never asked about a path the first question did not produce: this does not
 * glob the report directory and does not infer the three conventional names.
 */

import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import { fetch as fetchWithAuth } from "../api/fetcher";
import { urlOfArtifact } from "../artifacts/utils";
import { isStaticWebsiteOnly } from "../static-mode";

import { reportRenderPath, type ReportRenderKind } from "./paths";

/**
 * One byte is enough to prove a regular file resolves.
 *
 * The artifact route serves regular files through `FileResponse`, which
 * honours RFC 9110 byte ranges, so a live render answers `206` with a
 * one-byte body. A `200` means the range was ignored somewhere in front of
 * us and the whole file is on its way — still proof the file is there, and
 * the body is cancelled either way so those bytes are never pulled.
 */
const PROBE_RANGE = "bytes=0-0";

/** Statuses that prove the exact path is a regular file we may serve. */
function isLiveStatus(status: number) {
  return status === 200 || status === 206;
}

/**
 * Whether `path` resolves as a regular file for this thread, right now.
 *
 * Fail-closed in every direction: a 400 (not a file), 403 (refused), 404
 * (gone) and a network failure all mean "not a download link". Nothing here
 * distinguishes them for the caller, and nothing is retried — a card that
 * cannot prove a file is there offers nothing, which is the whole repair.
 */
export async function probeReportRenderLive({
  threadId,
  path,
  isMock,
}: {
  threadId: string;
  path: string;
  isMock?: boolean;
}): Promise<boolean> {
  try {
    // Inside the guard with everything else: this function promises never to
    // throw, and a promise kept only for the part after URL construction
    // would fail the whole batch and drop renders that are genuinely live.
    const url = urlOfArtifact({ filepath: path, threadId, isMock });
    const response = await fetchWithAuth(url, {
      method: "GET",
      headers: { Range: PROBE_RANGE },
    });
    // Drop the body without reading it. Cancelling can itself throw on a
    // already-settled stream in some engines, which must not turn a live
    // file into a dead one.
    try {
      await response.body?.cancel();
    } catch {
      // Nothing to do: the verdict is the status, not the body.
    }
    return isLiveStatus(response.status);
  } catch {
    return false;
  }
}

export const REPORT_RENDERS_QUERY_PREFIX = "business-report-renders" as const;

/**
 * The query identity.
 *
 * `revision` is the selected report's own content digest, and it is in the key
 * for a specific reason: every draft rewrites the *same* `*.report.json` and
 * the same render filenames, and the cumulative artifact list is byte-for-byte
 * unchanged across a rebuild. Keying on the pathname alone would let Draft 3's
 * card read Draft 2's cached verdict, and would let a slow probe started under
 * Draft 2 resolve into Draft 3's view and restore a link to a file that has
 * been deleted. A distinct key per revision makes that structurally
 * impossible rather than guarded against.
 *
 * A run settling is deliberately *not* in the key. It is an event that makes
 * the answer worth re-asking, not part of what is being asked, and the
 * difference is visible: a key change resets the verdict to unknown, so any
 * run finishing anywhere in the thread — including one that never touched
 * this report — made confirmed download links vanish and reappear a round
 * trip later. Settling triggers a refetch of the same key instead, which
 * keeps the known-good answer on screen while the new one is fetched.
 */
export function reportRendersQueryKey({
  threadId,
  filepath,
  revision,
  kinds,
}: {
  threadId: string;
  filepath: string;
  revision: string | undefined;
  kinds: readonly ReportRenderKind[];
}) {
  return [
    REPORT_RENDERS_QUERY_PREFIX,
    threadId,
    filepath,
    revision ?? "",
    kinds.join("|"),
  ] as const;
}

export type LiveReportRenders = {
  /** The formats that are eligible *and* currently downloadable. */
  kinds: ReportRenderKind[];
  /**
   * Whether the verdict is final. False means "still asking" — the card must
   * not say there is nothing to download while this is false, which is the
   * false-empty flash the repair has to avoid.
   */
  isSettled: boolean;
};

/**
 * Narrow the eligible renders to the ones that currently resolve.
 *
 * With nothing eligible there is nothing to ask and the verdict is settled
 * immediately: a report that never rendered anything says so without a probe.
 * In static/mock mode the fixtures are the filesystem, so eligibility is the
 * answer and no request is made — that keeps the demo and the mocked routes
 * deterministic rather than dependent on a fixture server answering ranges.
 */
export function useLiveReportRenders({
  threadId,
  filepath,
  eligible,
  revision,
  isMock,
  runSettled,
}: {
  threadId: string;
  filepath: string;
  eligible: readonly ReportRenderKind[];
  revision: string | undefined;
  isMock?: boolean;
  runSettled: boolean;
}): LiveReportRenders {
  const deterministic = Boolean(isMock) || isStaticWebsiteOnly();
  const enabled = eligible.length > 0 && !deterministic;
  const query = useQuery({
    queryKey: reportRendersQueryKey({
      threadId,
      filepath,
      revision,
      kinds: eligible,
    }),
    enabled,
    // The filesystem under these paths changes when a run rebuilds the
    // report, which the revision in the key already captures; nothing else
    // should be served from cache after a remount.
    staleTime: 0,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async () => {
      const verdicts = await Promise.all(
        eligible.map(async (kind) => ({
          kind,
          live: await probeReportRenderLive({
            threadId,
            path: reportRenderPath(filepath, kind),
            isMock,
          }),
        })),
      );
      return verdicts.filter((entry) => entry.live).map((entry) => entry.kind);
    },
  });

  // Re-ask when a run finishes, because a run is what rewrites or deletes
  // these files. This refetches the same key rather than moving to a new one,
  // so the previous verdict stays on screen until the new one replaces it --
  // the links do not blink out every time an unrelated turn ends.
  const refetch = query.refetch;
  const wasSettled = useRef(runSettled);
  useEffect(() => {
    const justSettled = runSettled && !wasSettled.current;
    wasSettled.current = runSettled;
    if (justSettled && enabled) {
      void refetch();
    }
  }, [runSettled, enabled, refetch]);

  if (!enabled) {
    return {
      kinds: deterministic ? [...eligible] : [],
      isSettled: true,
    };
  }
  return {
    // Fail-closed while the answer is unknown *and* if it failed: `data`
    // is only a list once every probe has answered.
    kinds: query.data ?? [],
    isSettled: query.isSuccess || query.isError,
  };
}

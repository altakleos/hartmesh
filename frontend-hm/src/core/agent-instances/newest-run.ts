import type { Message } from "@langchain/langgraph-sdk";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { awaitAbortable } from "@/core/api/abort";
import { fetch as fetchWithAuth } from "@/core/api/fetcher";
import { useAuth } from "@/core/auth/AuthProvider";
import { getBackendBaseURL } from "@/core/config";
import {
  hasContent,
  hasToolCalls,
  isHiddenFromUIMessage,
  isUploadPlaceholderMessage,
} from "@/core/messages/utils";

import type { Refusal } from "./errors";
import { hasCeiling } from "./presentation";

export interface NewestAgentRun {
  run_id: string;
  thread_id: string;
  status: string;
  stop_reason: string | null;
}
export type AgentRunEnding =
  | { kind: "service" }
  | { kind: "refusal"; refusal: Refusal };
export function agentRunEnding(run: NewestAgentRun): AgentRunEnding | null {
  if (run.status === "running" || run.status === "pending") return null;
  if (
    (run.stop_reason === "gateway_stopped" && run.status === "interrupted") ||
    run.stop_reason === "orphan_recovered"
  )
    return { kind: "service" };
  return [
    "busy",
    "run-limit",
    "restarting",
    "starting-elsewhere",
    "start-failed",
    "files-full",
    "storage-busy",
  ].includes(run.stop_reason ?? "")
    ? { kind: "refusal", refusal: { reason: run.stop_reason!, extra: {} } }
    : null;
}
export function hasFinalAgentAnswer(messages: readonly Message[]) {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index]!;
    if (isHiddenFromUIMessage(message) || isUploadPlaceholderMessage(message))
      continue;
    return (
      message.type === "ai" && hasContent(message) && !hasToolCalls(message)
    );
  }
  return false;
}

export async function readNewestAgentRun(
  threadId: string,
  owner?: AbortSignal,
): Promise<NewestAgentRun | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20_000);
  const signal = owner
    ? AbortSignal.any([controller.signal, owner])
    : controller.signal;
  try {
    if (signal.aborted) throw new DOMException("Read retired", "AbortError");
    const response = await awaitAbortable(
      fetchWithAuth(
        `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/runs/page?limit=1`,
        { signal },
      ),
      signal,
      (late) => {
        void late.body?.cancel().catch(() => undefined);
      },
    );
    if (!response.ok) {
      void response.body?.cancel().catch(() => undefined);
      throw new Error("Latest run could not be read");
    }
    const value: unknown = await awaitAbortable(response.json(), signal);
    if (
      !value ||
      typeof value !== "object" ||
      !Array.isArray(Reflect.get(value, "data"))
    )
      throw new Error("Invalid latest run response");
    const data = Reflect.get(value, "data") as unknown[];
    if (!data.length) return null;
    if (data.length !== 1) throw new Error("Invalid latest run page");
    const run = data[0];
    if (
      !run ||
      typeof run !== "object" ||
      typeof Reflect.get(run, "run_id") !== "string" ||
      !Reflect.get(run, "run_id") ||
      Reflect.get(run, "thread_id") !== threadId ||
      typeof Reflect.get(run, "status") !== "string" ||
      (Reflect.get(run, "stop_reason") !== null &&
        typeof Reflect.get(run, "stop_reason") !== "string")
    )
      throw new Error("Invalid latest run record");
    return {
      run_id: Reflect.get(run, "run_id") as string,
      thread_id: threadId,
      status: Reflect.get(run, "status") as string,
      stop_reason: Reflect.get(run, "stop_reason") as string | null,
    };
  } finally {
    clearTimeout(timer);
  }
}

/** A bounded observation, separate from live frame notices and send admission. */
export function useNewestAgentRun({
  threadId,
  enabled,
  streaming,
  finalAnswer,
}: {
  threadId: string;
  enabled: boolean;
  streaming: boolean;
  finalAnswer: boolean;
}) {
  const { user } = useAuth();
  const client = useQueryClient();
  const readable =
    enabled && !!user && hasCeiling(user.permissions, "runs:read");
  const key = useMemo(
    () => [
      "conversation-newest-run",
      user?.id,
      user?.system_role,
      user?.permissions,
      threadId,
      readable,
    ],
    [user?.id, user?.system_role, user?.permissions, threadId, readable],
  );
  const identity = JSON.stringify(key);
  const active = useRef(identity);
  active.current = identity;
  const liveStream = useRef(streaming);
  liveStream.current = streaming;
  const admission = useRef<{
    scope: string;
    epoch: number;
    expected: string | null;
    waiting: boolean;
  }>({ scope: identity, epoch: 0, expected: null, waiting: false });
  if (admission.current.scope !== identity)
    admission.current = {
      scope: identity,
      epoch: 0,
      expected: null,
      waiting: false,
    };
  const episode = useRef<{ scope: string; run: string; since: number } | null>(
    null,
  );
  const [notice, setNotice] = useState<{
    scope: string;
    ending: AgentRunEnding;
  } | null>(null);
  const query = useQuery({
    queryKey: key,
    queryFn: async ({ signal }) => {
      const epoch = admission.current.epoch;
      const run = await readNewestAgentRun(threadId, signal);
      if (
        active.current === identity &&
        !liveStream.current &&
        epoch === admission.current.epoch &&
        run?.status === "running" &&
        (episode.current?.scope !== identity ||
          episode.current.run !== run.run_id)
      )
        episode.current = {
          scope: identity,
          run: run.run_id,
          since: Date.now(),
        };
      return { run, epoch };
    },
    enabled: readable,
    retry: false,
    staleTime: 0,
    gcTime: 0,
    refetchOnWindowFocus: false,
    refetchIntervalInBackground: false,
    refetchInterval: (observed) =>
      !streaming &&
      observed.state.data?.epoch === admission.current.epoch &&
      observed.state.data?.run?.status === "running" &&
      episode.current?.scope === identity &&
      Date.now() - episode.current.since < 120_000
        ? 15_000
        : false,
  });
  const last = useRef({ scope: identity, streaming });
  const refetch = query.refetch;
  useEffect(() => {
    if (streaming && episode.current?.scope === identity)
      episode.current = null;
  }, [identity, streaming]);
  useEffect(() => {
    const previous = last.current;
    last.current = { scope: identity, streaming };
    if (
      readable &&
      previous.scope === identity &&
      previous.streaming &&
      !streaming &&
      !finalAnswer
    )
      void refetch();
  }, [identity, readable, streaming, finalAnswer, refetch]);
  useEffect(() => {
    const run = query.data?.run;
    if (
      !readable ||
      !run ||
      query.isError ||
      query.data?.epoch !== admission.current.epoch ||
      admission.current.waiting ||
      (admission.current.expected && run.run_id !== admission.current.expected)
    )
      return;
    const ending = agentRunEnding(run);
    if (ending) setNotice({ scope: identity, ending });
  }, [identity, readable, query.data, query.isError]);
  const sent = useCallback(() => {
    if (active.current !== identity) return;
    admission.current = {
      scope: identity,
      epoch: admission.current.epoch + 1,
      expected: null,
      waiting: true,
    };
    setNotice(null);
    void client.cancelQueries({ queryKey: key, exact: true });
  }, [identity, client, key]);
  const admitted = useCallback(
    (thread: string, run: string) => {
      if (thread === threadId && active.current === identity)
        admission.current = {
          ...admission.current,
          expected: run,
          waiting: false,
        };
    },
    [identity, threadId],
  );
  return {
    notice: readable && notice?.scope === identity ? notice.ending : null,
    sent,
    admitted,
  };
}

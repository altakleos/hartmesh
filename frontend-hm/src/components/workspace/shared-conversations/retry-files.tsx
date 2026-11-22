"use client";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import { continueSnapshot } from "@/core/shared-conversations/api";
import {
  saveContinuedDraft,
  type ContinuedDraft,
} from "@/core/shared-conversations/handoff";
import {
  sameTarget,
  validAnswerer,
} from "@/core/shared-conversations/protocol";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { useSnapshotCopy } from "./copy";

export function RetryContinuedFiles({
  account,
  thread,
  draft,
}: {
  account: string;
  thread: string;
  draft: ContinuedDraft;
}) {
  const w = useSnapshotCopy(),
    client = useQueryClient(),
    signalFor = useSpaceActionSignal(thread);
  const [pending, setPending] = useState(false),
    [error, setError] = useState<string | null>(null);
  const retry = async () => {
    const signal = signalFor();
    setPending(true);
    setError(null);
    try {
      const result = await continueSnapshot(
        draft.publication,
        draft.request,
        signal,
        draft.answerer ?? { kind: "assistant" },
      );
      if (signal.aborted) return;
      if (
        result.thread_id !== thread ||
        !validAnswerer(result.answerer) ||
        !sameTarget(result.answerer, draft.answerer ?? { kind: "assistant" })
      )
        throw new Error("Conversation changed");
      const current = client.getQueryData<ContinuedDraft>([
        "shared-conversation-draft",
        account,
        thread,
      ]);
      if (current?.request === draft.request)
        saveContinuedDraft(
          client,
          account,
          draft.publication,
          draft.request,
          draft.title,
          result,
          current,
        );
    } catch (e) {
      if (!signal.aborted) setError(w.error(e));
    } finally {
      if (!signal.aborted) setPending(false);
    }
  };
  return (
    <div className="w-full text-sm" role="status">
      {w.failedFiles}:{" "}
      {draft.missing
        .map((f) => f.path + " (" + w.copyReason(f.reason) + ")")
        .join(", ")}{" "}
      <Button
        size="sm"
        variant="outline"
        disabled={pending}
        onClick={() => void retry()}
      >
        {w.retryFiles}
      </Button>
      {error && <p role="alert">{error}</p>}
    </div>
  );
}

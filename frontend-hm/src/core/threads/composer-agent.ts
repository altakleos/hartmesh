"use client";

import { useCallback, useEffect, useRef } from "react";

import type { FileInMessage } from "../messages/utils";

import type { AgentThreadRunRefusal } from "./hooks";

export function useComposerAgentRefusal({
  refusal,
  threadId,
  getText,
  getEditRevision,
  setText,
  restoreFiles,
}: {
  refusal?: AgentThreadRunRefusal | null;
  threadId: string;
  getText: () => string;
  getEditRevision: () => number;
  setText: (text: string) => void;
  restoreFiles?: (files: FileInMessage[]) => void;
}) {
  const active = useRef(threadId);
  active.current = threadId;
  const seen = useRef<number | null>(null);
  const submitted = useRef<{
    threadId: string;
    messageText: string;
    draft: string;
    revision: number;
  } | null>(null);
  useEffect(() => {
    seen.current = null;
    submitted.current = null;
  }, [threadId]);
  const capture = useCallback(
    (messageText: string, draft: string, revision: number) => {
      submitted.current = { threadId, messageText, draft, revision };
    },
    [threadId],
  );
  useEffect(() => {
    if (refusal?.threadId !== threadId || seen.current === refusal.sequence)
      return;
    seen.current = refusal.sequence;
    const snapshot = submitted.current;
    const restore = refusal.restore;
    if (
      snapshot?.threadId !== threadId ||
      snapshot.messageText !== restore?.text
    )
      return;
    const timer = setTimeout(() => {
      if (
        active.current !== threadId ||
        getEditRevision() !== snapshot.revision
      )
        return;
      const current = getText();
      if (current !== "" && current !== snapshot.draft) return;
      setText(snapshot.draft);
      restoreFiles?.(restore.files);
    }, 0);
    return () => clearTimeout(timer);
  }, [refusal, threadId, getText, getEditRevision, setText, restoreFiles]);
  return { capture };
}

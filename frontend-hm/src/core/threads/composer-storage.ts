"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import type { PromptInputMessage } from "@/components/ai-elements/prompt-input";
import type { StorageRefusal } from "@/core/file-areas/storage-state";
import type { PromptInputFilePart } from "@/core/uploads";

import type { ThreadRunRefusal } from "./hooks";

type AttachedPart = PromptInputFilePart & { id: string };
const sameFile = (left: PromptInputFilePart, right: PromptInputFilePart) =>
  left.file && right.file ? left.file === right.file : left.url === right.url;

/** Keep the refused snapshot distinct from later human edits and new chips. */
export function useComposerStorageRefusal({
  refusal,
  threadId,
  getText,
  getEditRevision,
  setText,
}: {
  refusal?: ThreadRunRefusal | null;
  threadId?: string;
  getText: () => string;
  getEditRevision: () => number;
  setText: (value: string) => void;
}) {
  const [failures, setFailures] = useState<Record<string, StorageRefusal>>({});
  const active = useRef(threadId);
  active.current = threadId;
  const seen = useRef<number | null>(null);
  const submitted = useRef<{
    threadId?: string;
    messageText: string;
    draft: string;
    ids: string[];
    revision: number;
  } | null>(null);
  useEffect(() => {
    submitted.current = null;
    seen.current = null;
    setFailures({});
  }, [threadId]);
  const capture = useCallback(
    (
      message: PromptInputMessage,
      draft: string,
      parts: AttachedPart[],
      revision: number,
    ) => {
      submitted.current = {
        threadId,
        messageText: message.text,
        draft,
        ids: parts
          .filter((part) => message.files.some((file) => sameFile(part, file)))
          .map((part) => part.id),
        revision,
      };
    },
    [threadId],
  );
  useEffect(() => {
    if (
      !refusal ||
      refusal.threadId !== threadId ||
      seen.current === refusal.sequence
    )
      return;
    seen.current = refusal.sequence;
    const snapshot = submitted.current;
    if (
      snapshot?.threadId !== threadId ||
      snapshot.messageText !== refusal.message?.text
    )
      return;
    if (refusal.phase === "upload")
      setFailures((previous) => ({
        ...previous,
        ...Object.fromEntries(snapshot.ids.map((id) => [id, refusal.refusal])),
      }));
    // Provider cleanup can follow the SDK error callback in the same turn.
    const timer = setTimeout(() => {
      if (
        active.current !== snapshot.threadId ||
        getEditRevision() !== snapshot.revision
      )
        return;
      const current = getText();
      if (current === "" || current === snapshot.draft) setText(snapshot.draft);
    }, 0);
    return () => clearTimeout(timer);
  }, [refusal, threadId, getText, getEditRevision, setText]);
  const eligibleFiles = useCallback(
    (files: PromptInputFilePart[], parts: AttachedPart[]) => {
      const refused = parts.filter((part) => failures[part.id]);
      return files.filter(
        (file) => !refused.some((part) => sameFile(file, part)),
      );
    },
    [failures],
  );
  return { failures, capture, eligibleFiles };
}

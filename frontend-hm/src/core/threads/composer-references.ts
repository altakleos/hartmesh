import { useLayoutEffect, useMemo, useReducer } from "react";

import {
  pastedConversationId,
  resolveConversationReference,
  type ConversationReference,
} from "./conversation-references";

/** Snapshot the selection before the browser performs its normal text paste. */
export function pasteSelection(target: EventTarget | null) {
  if (target instanceof HTMLTextAreaElement) {
    return {
      before: target.value.slice(0, target.selectionStart),
      after: target.value.slice(target.selectionEnd),
    };
  }
  const editor =
    target instanceof HTMLElement && target.closest('[contenteditable="true"]');
  const selection = window.getSelection();
  if (!editor || !selection?.rangeCount) return null;
  const range = selection.getRangeAt(0);
  if (!editor.contains(range.commonAncestorContainer)) return null;
  const prefix = range.cloneRange();
  prefix.selectNodeContents(editor);
  prefix.setEnd(range.startContainer, range.startOffset);
  const suffix = range.cloneRange();
  suffix.selectNodeContents(editor);
  suffix.setStart(range.endContainer, range.endOffset);
  return { before: prefix.toString(), after: suffix.toString() };
}

/** References last only in this account, destination and effective grant. */
export function useComposerReferences(
  account: string | undefined,
  threadId: string,
  instance: string | undefined,
  grant: string,
  limit: number,
) {
  const scope = JSON.stringify([account, threadId, instance, grant, limit]);
  const selection = useMemo(
    () => ({
      scope,
      references: [] as ConversationReference[],
      pending: new Set<AbortController>(),
    }),
    [scope],
  );
  const [, refresh] = useReducer((n: number) => n + 1, 0);
  function cancelLookups() {
    for (const controller of selection.pending) controller.abort();
    selection.pending.clear();
  }
  useLayoutEffect(
    () => () => {
      for (const controller of selection.pending) controller.abort();
    },
    [selection],
  );

  return {
    references: selection.references,
    cancelLookups,
    remove(sent: ConversationReference[]) {
      selection.references = selection.references.filter(
        (reference) => !sent.includes(reference),
      );
      refresh();
    },
    async pasted(
      text: string,
      target: EventTarget | null,
      getText: () => string,
      setText: (value: string) => void,
    ) {
      if (!account || !limit || selection.references.length >= limit) return;
      const id = pastedConversationId(text, window.location.origin);
      if (!id || id === threadId) return;
      const cursor = pasteSelection(target);
      if (!cursor) return;
      const controller = new AbortController();
      selection.pending.add(controller);
      try {
        const reference = await resolveConversationReference(
          id,
          account,
          instance,
          controller.signal,
        );
        if (
          !reference ||
          controller.signal.aborted ||
          selection.references.length >= limit
        )
          return;
        // A refusal, edited draft or intervening paste keeps the browser's original text.
        if (getText() !== cursor.before + text + cursor.after) return;
        if (!selection.references.some((item) => item.id === id))
          selection.references = [...selection.references, reference];
        setText(cursor.before + cursor.after);
        refresh();
      } catch {
        // The permission lookup can fail; the clipboard text remains editable.
      } finally {
        selection.pending.delete(controller);
      }
    },
  };
}

import type { Message } from "@langchain/langgraph-sdk";

import { getMessageRunId } from "./run-duration";
import {
  extractTextFromMessage,
  isHiddenFromUIMessage,
  stripUploadedFilesTag,
} from "./utils";

export type SentTurn = {
  optimisticId: string;
  /** Null means a new conversation; an absent ID has no safe text fallback. */
  previousHumanId?: string | null;
  text: string;
  runId?: string;
};

function matchesSentText(message: Message, turn: SentTurn) {
  return (
    stripUploadedFilesTag(extractTextFromMessage(message)) ===
    stripUploadedFilesTag(turn.text)
  );
}

/** Locate the server's row in a complete or partial view of one conversation. */
export function canonicalSentIndex<T>(
  rows: readonly T[],
  turn: SentTurn,
  read: (row: T) => Message | undefined,
) {
  if (!turn.runId) return -1;
  const human = (row: T) => {
    const message = read(row);
    return message?.type === "human" &&
      message.id !== turn.optimisticId &&
      !isHiddenFromUIMessage(message)
      ? message
      : undefined;
  };
  const previous = turn.previousHumanId
    ? rows.findIndex((row) => human(row)?.id === turn.previousHumanId)
    : -1;
  const confirmed = rows.findIndex((row, index) => {
    if (index <= previous) return false;
    const message = human(row);
    return Boolean(
      message &&
      getMessageRunId(message) === turn.runId &&
      matchesSentText(message, turn),
    );
  });
  if (confirmed >= 0) return confirmed;
  // With no previous row or known new conversation, repeated text is ambiguous.
  if (previous < 0 && turn.previousHumanId !== null) return -1;
  const first = rows.findIndex(
    (row, index) => index > previous && Boolean(human(row)),
  );
  const firstRow = rows[first];
  const message = firstRow === undefined ? undefined : read(firstRow);
  return message?.id &&
    !getMessageRunId(message) &&
    matchesSentText(message, turn)
    ? first
    : -1;
}

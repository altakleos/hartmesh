"use client";
import type { Message } from "@langchain/langgraph-sdk";
import { useQuery } from "@tanstack/react-query";
import { createContext, useContext, useMemo, type ReactNode } from "react";

import { threadSharedHistory } from "@/core/shared-conversations/api";
import {
  useSharing,
  useSnapshotDetail,
} from "@/core/shared-conversations/hooks";
import { readSharedHistory } from "@/core/shared-conversations/protocol";

import { useSnapshotCopy } from "./copy";

type Source = {
  frozen: ReadonlySet<string>;
  last: string | null;
  line: ReactNode;
};
const empty: Source = { frozen: new Set(), last: null, line: null };
const HistoryContext = createContext<Source>(empty);
export const useCopiedTurns = () => useContext(HistoryContext);
type Props = {
  thread: string;
  messages: Message[];
  agent: { id: string; name: string } | null;
  children: ReactNode;
};
export function SharedHistoryBoundary(props: Props) {
  // Legacy clients could supply arbitrary additional_kwargs. A mark only
  // triggers the authorized read; it cannot assign a publication to a turn.
  const marked = props.messages.some(
    (m) => m.additional_kwargs?.shared_history,
  );
  return marked ? <VerifiedHistory {...props} /> : props.children;
}
function VerifiedHistory({ thread, messages, agent, children }: Props) {
  const access = useSharing(),
    w = useSnapshotCopy(),
    enabled = access.can("read");
  const proof = useQuery({
    queryKey: ["shared-conversations", access.scope, "history", thread],
    queryFn: ({ signal }) => threadSharedHistory(thread, signal),
    enabled,
    retry: false,
    gcTime: 0,
  });
  const seed = useMemo(
    () =>
      readSharedHistory(
        messages,
        enabled && !proof.isError ? proof.data : null,
      ),
    [messages, enabled, proof.isError, proof.data],
  );
  const publication = seed.copied.size ? proof.data?.publication_id : undefined;
  const detail = useSnapshotDetail(
    publication,
    enabled && access.can("read"),
    access.scope,
  );
  const row = !detail.isError && enabled ? detail.data : undefined;
  let line: ReactNode = null;
  if (row && seed.last) {
    const by = row.publisher ?? w.unknown,
      date = new Date(row.published_at).toLocaleDateString(w.locale);
    const text =
      "removed" in row
        ? w.retired(by, date)
        : w.copied(row.title, by, date, row.with?.name);
    const prior =
      "with" in row && row.with && row.with.id !== agent?.id
        ? row.with.name
        : undefined;
    line = (
      <p className="text-muted-foreground my-4 border-y py-2 text-sm">
        {text} <strong>{w.answers(agent?.name ?? w.product, prior)}</strong>
      </p>
    );
  }
  const frozen =
    enabled && (!proof.isSuccess || proof.isError)
      ? new Set(
          messages
            .filter((m) => m.additional_kwargs?.shared_history && m.id)
            .map((m) => m.id!),
        )
      : seed.copied;
  return (
    <HistoryContext.Provider value={{ frozen, last: seed.last, line }}>
      {children}
    </HistoryContext.Provider>
  );
}

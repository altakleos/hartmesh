"use client";

import { useCallback, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  InstanceApiError,
  isFullInstance,
  listInstanceConversations,
  listInstances,
} from "@/core/agent-instances/api";
import { INSPECT } from "@/core/agent-instances/presentation";
import { useI18n } from "@/core/i18n/hooks";
import {
  recentImportSources,
  type ImportDestination,
  type ImportSource,
} from "@/core/threads/conversation-file-import";

interface Page<T, C> {
  items: T[];
  next: C | null;
}
/** Do not interpret an empty result page as the end of a filtered listing. */
export function usePickerPage<T, C>(
  initial: C,
  load: (cursor: C, signal: AbortSignal) => Promise<Page<T, C>>,
) {
  const [page, setPage] = useState<Page<T, C>>({ items: [], next: initial });
  const [request, setRequest] = useState({ cursor: initial });
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(true);
  useEffect(() => {
    const owner = new AbortController();
    setBusy(true);
    setError(null);
    void load(request.cursor, owner.signal)
      .then((result) => {
        if (!owner.signal.aborted)
          setPage((prev) => ({
            items: [...prev.items, ...result.items],
            next: result.next,
          }));
      })
      .catch((failure: unknown) => {
        if (!owner.signal.aborted) setError(failure);
      })
      .finally(() => {
        if (!owner.signal.aborted) setBusy(false);
      });
    return () => owner.abort();
  }, [request, load]);
  return {
    page,
    error,
    busy,
    more: () => {
      if (page.next !== null) setRequest({ cursor: page.next });
    },
    retry: () => setRequest({ ...request }),
  };
}
export function PageFooter({
  busy,
  error,
  more,
  onMore,
  onRetry,
  empty,
  emptyMessage,
  denied,
}: {
  busy: boolean;
  error: unknown;
  more: boolean;
  onMore: () => void;
  onRetry: () => void;
  empty: boolean;
  emptyMessage: string;
  denied?: string;
}) {
  const { t } = useI18n();
  const restricted =
    error instanceof InstanceApiError && [403, 404].includes(error.status);
  const copy = t.inputBox.conversationFile;
  if (busy) return <p role="status">{t.common.loading}</p>;
  if (error)
    return (
      <div role="status">
        <p>{restricted ? (denied ?? copy.refused) : copy.loadFailed}</p>
        {!restricted && (
          <Button type="button" variant="outline" onClick={onRetry}>
            {copy.retry}
          </Button>
        )}
      </div>
    );
  return (
    <>
      {empty && <p role="status">{emptyMessage}</p>}
      {more && (
        <Button type="button" variant="outline" onClick={onMore}>
          {t.common.loadMore}
        </Button>
      )}
    </>
  );
}
type AgentSource = { id: string; name: string };
export type SourceBrowser = "recent" | "agents" | AgentSource;

async function loadAgents(offset: number, signal: AbortSignal) {
  const { instances } = await listInstances(undefined, offset, signal);
  return {
    items: instances.filter(
      (entry) =>
        isFullInstance(entry) &&
        (entry.permissions & INSPECT) !== 0 &&
        ["active", "suspended", "archived"].includes(entry.status),
    ),
    next: instances.length === 100 ? offset + 100 : null,
  };
}
function Agents({ select }: { select: (agent: AgentSource) => void }) {
  const { t } = useI18n();
  const { page, busy, error, more, retry } = usePickerPage(0, loadAgents);
  return (
    <>
      <ul>
        {!error &&
          page.items.map(({ id, name }) => (
            <li key={id}>
              <Button
                variant="ghost"
                type="button"
                onClick={() => select({ id, name })}
              >
                {name}
              </Button>
            </li>
          ))}
      </ul>
      <PageFooter
        busy={busy}
        error={error}
        empty={!page.items.length}
        emptyMessage={t.inputBox.conversationFile.noAgents}
        more={page.next !== null}
        onMore={more}
        onRetry={retry}
      />
    </>
  );
}
export function ConversationFileSources({
  account,
  destination,
  browse,
  setBrowse,
  agentSources,
  select,
}: {
  account: string;
  destination: ImportDestination;
  browse: SourceBrowser;
  setBrowse: (value: SourceBrowser) => void;
  agentSources: boolean;
  select: (source: ImportSource) => void;
}) {
  const { t } = useI18n();
  const copy = t.inputBox.conversationFile;
  const load = useCallback(
    async (offset: number, signal: AbortSignal) => {
      if (typeof browse !== "object")
        return recentImportSources(
          destination,
          account,
          offset,
          signal,
          agentSources,
        );
      const { conversations } = await listInstanceConversations(
        browse.id,
        offset,
        signal,
      );
      return {
        items: conversations
          .filter((row) => row.thread_id !== destination.threadId)
          .map((row) => ({
            thread_id: row.thread_id,
            title: row.title,
            instanceName: browse.name,
          })),
        next: conversations.length === 50 ? offset + 50 : null,
      };
    },
    [account, destination, browse, agentSources],
  );
  return (
    <>
      {browse === "recent" && agentSources && (
        <Button
          variant="outline"
          type="button"
          onClick={() => setBrowse("agents")}
        >
          {copy.browseAgents}
        </Button>
      )}
      {typeof browse === "object" && <p>{browse.name}</p>}
      {browse === "agents" ? (
        <Agents select={setBrowse} />
      ) : (
        <ConversationRows
          load={load}
          select={select}
          showAgentName={browse === "recent"}
        />
      )}
      {browse !== "recent" && (
        <Button
          variant="outline"
          type="button"
          onClick={() => setBrowse(browse === "agents" ? "recent" : "agents")}
        >
          {browse === "agents" ? copy.previous : copy.backToAgents}
        </Button>
      )}
    </>
  );
}
function ConversationRows({
  load,
  select,
  showAgentName,
}: {
  load: (
    offset: number,
    signal: AbortSignal,
  ) => Promise<Page<ImportSource, number>>;
  select: (source: ImportSource) => void;
  showAgentName: boolean;
}) {
  const { t } = useI18n();
  const { page, busy, error, more, retry } = usePickerPage(0, load);
  return (
    <>
      <ul>
        {!error &&
          page.items.map((row) => (
            <li key={row.thread_id}>
              <Button
                className="h-auto w-full justify-start text-left whitespace-normal"
                type="button"
                variant="ghost"
                onClick={() => select(row)}
              >
                {row.title ?? t.agentInstances.conversations.untitled}
                {showAgentName && row.instanceName && (
                  <span className="text-muted-foreground">
                    {row.instanceName}
                  </span>
                )}
              </Button>
            </li>
          ))}
      </ul>
      <PageFooter
        busy={busy}
        error={error}
        empty={!page.items.length}
        emptyMessage={t.inputBox.conversationFile.noSources}
        more={page.next !== null}
        onMore={more}
        onRetry={retry}
      />
    </>
  );
}

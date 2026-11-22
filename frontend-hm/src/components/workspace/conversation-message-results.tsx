"use client";

import {
  useInfiniteQuery,
  useQuery,
  useQueryClient,
  type InfiniteData,
} from "@tanstack/react-query";
import Link from "next/link";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from "react";
import type { z } from "zod";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { VirtualThreadList } from "@/components/workspace/thread-list-virtualizer";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import {
  messagePage,
  readMessageWindow,
  SearchReadError,
  searchWorkspace,
  useSearchReady,
  type MessageHit,
} from "@/core/threads/conversation-search";
import { pathOfThread } from "@/core/threads/utils";
import { formatTimeAgo } from "@/core/utils/datetime";

const POLL_MS = 2000;
const POLL_WINDOW_MS = 30_000;

function messageKey(hit: MessageHit) {
  return JSON.stringify([hit.thread_id, hit.message_id]);
}

function SpeakerTime({ role, at }: { role: MessageHit["role"]; at: string }) {
  const { t, locale } = useI18n();
  return (
    <span className="text-muted-foreground text-sm">
      {role === "user" ? t.chats.userRole : t.chats.assistantRole} ·{" "}
      <time dateTime={at}>{formatTimeAgo(at, locale)}</time>
    </span>
  );
}

function MessageWindow({ hit }: { hit: MessageHit }) {
  const { t } = useI18n();
  const { user } = useAuth();
  const target = useRef<HTMLLIElement>(null);
  const context = useQuery({
    queryKey: [
      "workspace-search",
      "around",
      user?.id,
      user?.system_role,
      hit.thread_id,
      hit.message_id,
      hit.seq,
    ],
    queryFn: ({ signal }) => readMessageWindow(hit, signal),
    retry: false,
    gcTime: 0,
  });
  useEffect(() => {
    if (context.isSuccess) target.current?.scrollIntoView({ block: "nearest" });
  }, [context.isSuccess]);
  const withdrawn =
    context.error instanceof SearchReadError &&
    [401, 403, 404].includes(context.error.status);
  return (
    <DialogContent className="flex max-h-[85dvh] flex-col">
      <DialogHeader>
        <DialogTitle>{t.chats.messageContext}</DialogTitle>
        <DialogDescription
          className={context.isSuccess ? "break-words" : "sr-only"}
        >
          {context.isSuccess ? hit.title : t.chats.messageContext}
        </DialogDescription>
      </DialogHeader>
      {context.isError ? (
        <div role="status">
          <p>
            {withdrawn ? t.chats.messageUnavailable : t.chats.contextFailed}
          </p>
          {!withdrawn && (
            <Button onClick={() => void context.refetch()} variant="outline">
              {t.chats.retrySearch}
            </Button>
          )}
        </div>
      ) : context.isPending ? (
        <p role="status">{t.chats.searching}</p>
      ) : (
        <>
          <ol className="min-h-0 space-y-3 overflow-auto">
            {context.data.map((m) => (
              <li
                key={m.message_id}
                ref={m.seq === hit.seq ? target : undefined}
                aria-current={m.seq === hit.seq ? true : undefined}
                className="aria-current:border-primary border-l-2 border-transparent px-3 py-1"
              >
                <SpeakerTime role={m.role} at={m.created_at} />
                <p className="break-words whitespace-pre-wrap">{m.text}</p>
              </li>
            ))}
          </ol>
          <Button asChild className="self-start">
            <Link
              href={pathOfThread(hit.thread_id, {
                agent_name: hit.agent_name ?? undefined,
              })}
            >
              {t.chats.openConversation}
            </Link>
          </Button>
        </>
      )}
    </DialogContent>
  );
}

function MessageRow({ hit }: { hit: MessageHit }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <button
          type="button"
          className="hover:bg-muted focus-visible:ring-ring flex w-full flex-col gap-1 border-b p-4 text-start focus-visible:ring-2"
        >
          <span className="font-medium">{hit.title}</span>
          <SpeakerTime role={hit.role} at={hit.updated_at} />
          <span className="break-words whitespace-pre-wrap">
            {hit.snippet.map((part, i) =>
              part.matched ? <mark key={i}>{part.text}</mark> : part.text,
            )}
          </span>
        </button>
      </DialogTrigger>
      {open && <MessageWindow hit={hit} />}
    </Dialog>
  );
}

export function ConversationMessageResults({
  query,
  onUnavailable,
  precedingSection,
}: {
  query: string;
  onUnavailable: () => void;
  precedingSection: RefObject<HTMLElement | null>;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const ready = useSearchReady();
  const client = useQueryClient();
  const key = useMemo(
    () => ["workspace-search", "messages", user?.id, user?.system_role, query],
    [user?.id, user?.system_role, query],
  );
  const [timedOut, setTimedOut] = useState(false);
  const started = useRef<number | null>(null);
  const search = useInfiniteQuery({
    queryKey: key,
    enabled: ready,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: !timedOut,
    refetchOnReconnect: !timedOut,
    initialPageParam: null as string | null,
    queryFn: async ({ pageParam, signal }) =>
      messagePage.parse(
        await searchWorkspace(query, "message", pageParam, signal),
      ),
    getNextPageParam: (page, _pages, _previous, used) =>
      page.cursor && !used.includes(page.cursor) ? page.cursor : undefined,
  });
  const pages = search.data?.pages;
  const last = pages?.at(-1);
  const unavailable =
    last?.messages_available === false ||
    (search.error instanceof SearchReadError &&
      [401, 403].includes(search.error.status));
  useEffect(() => {
    if (unavailable) onUnavailable();
  }, [unavailable, onUnavailable]);
  const indexing = last?.indexing === true && !search.isError && !unavailable;
  const { refetch } = search;
  const refreshFirst = useCallback(() => {
    if (client.isFetching({ queryKey: key, exact: true })) return;
    type Pages = InfiniteData<z.infer<typeof messagePage>, string | null>;
    if ((client.getQueryData<Pages>(key)?.pages.length ?? 0) > 1) {
      client.setQueryData<Pages>(
        key,
        (old) =>
          old && {
            pages: old.pages.slice(0, 1),
            pageParams: old.pageParams.slice(0, 1),
          },
      );
    }
    void refetch({ cancelRefetch: false });
  }, [client, key, refetch]);
  useEffect(() => {
    if (!indexing || timedOut) return;
    started.current ??= Date.now();
    const remaining = started.current + POLL_WINDOW_MS - Date.now();
    const deadline = started.current + POLL_WINDOW_MS;
    const interval = setInterval(() => {
      if (Date.now() < deadline) refreshFirst();
    }, POLL_MS);
    const stop = setTimeout(() => setTimedOut(true), Math.max(0, remaining));
    return () => {
      clearInterval(interval);
      clearTimeout(stop);
    };
  }, [indexing, timedOut, refreshFirst]);
  function retry() {
    started.current = null;
    setTimedOut(false);
    refreshFirst();
  }
  if (unavailable) return null;
  const hits = new Map<string, MessageHit>();
  for (const page of pages ?? [])
    for (const hit of page.items) hits.set(messageKey(hit), hit);
  let status;
  if (search.isPending) status = t.chats.searching;
  else if (indexing)
    status = timedOut ? t.chats.stillReading : t.chats.readingHistory;
  else if (!hits.size && !search.hasNextPage)
    status = t.chats.noMatchingMessages;
  return (
    <section aria-label={t.chats.messages}>
      <h2 className="px-4 pt-4 font-medium">{t.chats.messages}</h2>
      {search.isError ? (
        <div role="status" className="space-y-2 p-4">
          <p>{t.chats.messagesUnavailable}</p>
          <Button variant="outline" onClick={retry}>
            {t.chats.retrySearch}
          </Button>
        </div>
      ) : (
        <>
          <VirtualThreadList
            estimateSize={100}
            items={Array.from(hits.values())}
            itemKey={messageKey}
            precedingSection={precedingSection}
            scrollParentSelector='[data-slot="scroll-area-viewport"]'
            renderItem={(hit) => <MessageRow key={messageKey(hit)} hit={hit} />}
          />
          {status && (
            <p className="text-muted-foreground p-4" role="status">
              {status}
            </p>
          )}
          {indexing && timedOut && (
            <Button className="mx-4" variant="outline" onClick={retry}>
              {t.chats.retrySearch}
            </Button>
          )}
          {!indexing && search.hasNextPage && (
            <div className="flex justify-center p-4">
              <Button
                variant="outline"
                disabled={search.isFetching}
                onClick={() => void search.fetchNextPage()}
              >
                {search.isFetchingNextPage
                  ? t.chats.loadingMore
                  : t.chats.moreResults}
              </Button>
            </div>
          )}
        </>
      )}
    </section>
  );
}

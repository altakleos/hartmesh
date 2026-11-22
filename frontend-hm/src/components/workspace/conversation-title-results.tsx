"use client";

import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import {
  ThreadChannelBadge,
  ThreadChannelIcon,
} from "@/components/workspace/thread-channel-source";
import { VirtualThreadList } from "@/components/workspace/thread-list-virtualizer";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import {
  searchWorkspace,
  useSearchReady,
  SearchReadError,
} from "@/core/threads/conversation-search";
import type { AgentThread } from "@/core/threads/types";
import {
  channelSourceForProvider,
  channelSourceOfThread,
  pathOfThread,
  type ChannelThreadSource,
} from "@/core/threads/utils";
import { formatTimeAgo } from "@/core/utils/datetime";

const titlePage = z.object({
  items: z.array(
    z.object({
      type: z.literal("thread"),
      thread_id: z.string(),
      title: z.string(),
      agent_name: z.string().nullable(),
      channel_provider: z.string().nullish(),
      updated_at: z.string(),
    }),
  ),
  cursor: z.string().nullable(),
});

export function ConversationLink({
  href,
  title,
  updatedAt,
  source,
}: {
  href: string;
  title: string;
  updatedAt?: string;
  source: ChannelThreadSource | null;
}) {
  return (
    <Link href={href} className="flex flex-col gap-2 border-b p-4">
      <div className="flex min-w-0 items-center gap-2">
        <ThreadChannelIcon source={source} />
        <div className="min-w-0 flex-1 truncate">{title}</div>
        <ThreadChannelBadge source={source} className="hidden sm:inline-flex" />
      </div>
      {updatedAt && (
        <div className="text-muted-foreground text-sm">
          {formatTimeAgo(updatedAt)}
        </div>
      )}
    </Link>
  );
}

export function ConversationTitleResults({
  query,
  knownThreads,
}: {
  query: string;
  knownThreads: ReadonlyMap<string, AgentThread>;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  // The page keys this component by input. Retiring it cancels both this delay
  // and the query observer's request, including an outstanding next page.
  const ready = useSearchReady();
  const client = useQueryClient();
  const queryKey = [
    "workspace-search",
    "titles",
    user?.id,
    user?.system_role,
    query,
  ];
  const search = useInfiniteQuery({
    queryKey,
    enabled: ready,
    gcTime: 0,
    retry: false,
    initialPageParam: null as string | null,
    queryFn: async ({ pageParam, signal }) =>
      titlePage.parse(
        await searchWorkspace(query, "thread", pageParam, signal),
      ),
    getNextPageParam: (last, _pages, _previous, used) =>
      last.cursor && !used.includes(last.cursor) ? last.cursor : undefined,
  });
  const pageUnavailable =
    search.isFetchNextPageError &&
    ((search.error instanceof SearchReadError && search.error.status >= 500) ||
      search.error instanceof TypeError);
  if (search.isError && !pageUnavailable) {
    return (
      <div role="status" className="space-y-2 p-4">
        <p>{t.chats.searchFailed}</p>
        <Button
          variant="outline"
          onClick={() => void client.resetQueries({ queryKey, exact: true })}
        >
          {t.chats.retrySearch}
        </Button>
      </div>
    );
  }
  if (!ready || search.isPending)
    return (
      <p role="status" className="text-muted-foreground p-4">
        {t.chats.searching}
      </p>
    );
  const hits = [
    ...new Map(
      (search.data?.pages ?? []).flatMap((page) =>
        page.items.map((item) => [item.thread_id, item] as const),
      ),
    ).values(),
  ];
  return (
    <>
      <VirtualThreadList
        estimateSize={76}
        items={hits}
        scrollParentSelector='[data-slot="scroll-area-viewport"]'
        renderItem={(hit) => {
          // Older Gateways omit the badge field. Only their hits may use the
          // chat list as a fallback; an explicit null clears a previous badge.
          const known =
            hit.channel_provider === undefined
              ? knownThreads.get(hit.thread_id)
              : undefined;
          return (
            <ConversationLink
              key={hit.thread_id}
              href={pathOfThread(hit.thread_id, {
                agent_name: hit.agent_name ?? undefined,
              })}
              title={hit.title}
              updatedAt={hit.updated_at}
              source={
                known
                  ? channelSourceOfThread(known)
                  : channelSourceForProvider(hit.channel_provider)
              }
            />
          );
        }}
      />
      {!hits.length && !search.hasNextPage && (
        <p role="status" className="text-muted-foreground p-4">
          {t.chats.noMatchingTitle(query)}
        </p>
      )}
      {pageUnavailable && (
        <p role="status" className="text-muted-foreground px-4">
          {t.chats.moreResultsFailed}
        </p>
      )}
      {search.hasNextPage && (
        <div className="flex justify-center p-4">
          <Button
            variant="outline"
            disabled={search.isFetching}
            onClick={() => void search.fetchNextPage()}
          >
            {search.isFetchingNextPage
              ? t.chats.loadingMore
              : pageUnavailable
                ? t.chats.retrySearch
                : t.chats.moreResults}
          </Button>
        </div>
      )}
    </>
  );
}

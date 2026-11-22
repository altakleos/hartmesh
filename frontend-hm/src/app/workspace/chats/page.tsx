"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ConversationMessageResults } from "@/components/workspace/conversation-message-results";
import {
  ConversationLink,
  ConversationTitleResults,
} from "@/components/workspace/conversation-title-results";
import { VirtualThreadList } from "@/components/workspace/thread-list-virtualizer";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import { useAuth } from "@/core/auth/AuthProvider";
import { useDocumentTitle, useFeatures } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { isStaticWebsiteOnly } from "@/core/static-mode";
import { useInfiniteThreads } from "@/core/threads/hooks";
import { buildThreadListModel } from "@/core/threads/thread-list-model";
import {
  channelSourceOfThread,
  pathOfThread,
  titleOfThread,
} from "@/core/threads/utils";

export default function ChatsPage() {
  const { user } = useAuth();
  if (isStaticWebsiteOnly()) return <ChatList messageSearch={false} />;
  return <LiveChatList key={JSON.stringify([user?.id, user?.system_role])} />;
}

function LiveChatList() {
  const feature = useFeatures((data) => data.message_search === true);
  return <ChatList messageSearch={feature.data === true && !feature.isError} />;
}

function ChatList({ messageSearch }: { messageSearch: boolean }) {
  const { t } = useI18n();
  const {
    data: infiniteThreads,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteThreads();
  const threadListModel = useMemo(
    () => buildThreadListModel(infiniteThreads?.pages ?? []),
    [infiniteThreads?.pages],
  );
  const { threads } = threadListModel;
  const [search, setSearch] = useState("");
  const query = search.trim();
  const [unavailableQuery, setUnavailableQuery] = useState<string | null>(null);
  const retireMessageResults = useCallback(
    () => setUnavailableQuery(query),
    [query],
  );
  const showMessages = messageSearch && unavailableQuery !== query;
  const searchLabel = showMessages
    ? t.chats.searchConversations
    : t.chats.searchTitles;
  const isSearching = query.length > 0;
  const staticDemo = isStaticWebsiteOnly();

  useDocumentTitle(t.pages.chats, t.pages.appName);

  const listedThreads = useMemo(
    () =>
      staticDemo && query
        ? threads.filter((thread) =>
            titleOfThread(thread).toLowerCase().includes(query.toLowerCase()),
          )
        : threads,
    [threads, query, staticDemo],
  );

  const titleSection = useRef<HTMLElement | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const element = sentinelRef.current;
    if (!element || !hasNextPage || isSearching) {
      return;
    }
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting && hasNextPage && !isFetchingNextPage) {
          void fetchNextPage();
        }
      },
      { rootMargin: "200px 0px 200px 0px" },
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, [fetchNextPage, hasNextPage, isFetchingNextPage, isSearching]);

  return (
    <WorkspaceContainer>
      <WorkspaceHeader></WorkspaceHeader>
      <WorkspaceBody>
        <div className="flex size-full flex-col">
          <header className="flex shrink-0 items-center justify-center pt-8">
            <Input
              type="search"
              className="h-12 w-full max-w-(--container-width-md) text-xl"
              aria-label={searchLabel}
              placeholder={searchLabel}
              maxLength={256}
              autoFocus
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                setUnavailableQuery(null);
              }}
            />
          </header>
          <main className="min-h-0 flex-1">
            <ScrollArea className="size-full py-4">
              <div className="mx-auto flex size-full max-w-(--container-width-md) flex-col">
                {isSearching && !staticDemo ? (
                  <>
                    <section
                      ref={titleSection}
                      aria-label={
                        showMessages ? t.chats.conversations : undefined
                      }
                    >
                      {showMessages && (
                        <h2 className="px-4 pt-4 font-medium">
                          {t.chats.conversations}
                        </h2>
                      )}
                      <ConversationTitleResults
                        key={query}
                        query={query}
                        knownThreads={threadListModel.byId}
                      />
                    </section>
                    {showMessages && (
                      <ConversationMessageResults
                        key={query}
                        query={query}
                        onUnavailable={retireMessageResults}
                        precedingSection={titleSection}
                      />
                    )}
                  </>
                ) : (
                  <VirtualThreadList
                    estimateSize={76}
                    items={listedThreads}
                    scrollParentSelector='[data-slot="scroll-area-viewport"]'
                    renderItem={(thread) => (
                      <ConversationLink
                        key={thread.thread_id}
                        href={pathOfThread(thread)}
                        title={titleOfThread(thread)}
                        updatedAt={thread.updated_at}
                        source={channelSourceOfThread(thread)}
                      />
                    )}
                  />
                )}
                {hasNextPage && !isSearching && (
                  <div
                    ref={sentinelRef}
                    aria-hidden="true"
                    className="h-px w-full"
                    data-testid="chats-page-sentinel"
                  />
                )}
              </div>
            </ScrollArea>
          </main>
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

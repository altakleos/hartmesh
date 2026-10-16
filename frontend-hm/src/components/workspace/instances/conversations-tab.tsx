"use client";

import { useInfiniteQuery } from "@tanstack/react-query";
import Link from "next/link";

import { Button } from "@/components/ui/button";
import { usePersonName } from "@/components/workspace/person-name";
import * as api from "@/core/agent-instances/api";
import { hasCeiling, INSPECT } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";

export function ConversationsTab({
  instance,
}: {
  instance: api.AgentInstance;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const conversations = useInfiniteQuery({
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance.id,
      "conversations",
      instance.permissions,
    ],
    enabled: !!user && hasCeiling(user.permissions, "agents:read"),
    initialPageParam: 0,
    queryFn: ({ pageParam, signal }) =>
      api.listInstanceConversations(instance.id, pageParam, signal),
    getNextPageParam: (page, _pages, offset) =>
      page.conversations.length === 50 ? offset + 50 : undefined,
    retry: false,
  });
  const rows = !conversations.isError
    ? conversations.data?.pages.flatMap((page) => page.conversations)
    : undefined;
  return (
    <section className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold">{copy.tabs.conversations}</h2>
        <Button
          type="button"
          variant="outline"
          disabled={conversations.isFetching}
          onClick={() => void conversations.refetch()}
        >
          {copy.refresh}
        </Button>
      </div>
      {(instance.permissions & INSPECT) === 0 && (
        <p>{copy.conversations.ownOnly}</p>
      )}
      {conversations.isPending ? (
        <p role="status">{copy.loading}</p>
      ) : conversations.isError ? (
        <p role="alert">{copy.conversations.loadFailed}</p>
      ) : !rows?.length ? (
        <p>{copy.conversations.empty}</p>
      ) : (
        <ul className="space-y-2">
          {rows.map((row) => (
            <ConversationRow key={row.thread_id} row={row} />
          ))}
        </ul>
      )}
      {conversations.hasNextPage && !conversations.isError && (
        <Button
          type="button"
          variant="outline"
          disabled={conversations.isFetchingNextPage}
          onClick={() => void conversations.fetchNextPage()}
        >
          {copy.more}
        </Button>
      )}
    </section>
  );
}

function ConversationRow({ row }: { row: api.InstanceConversation }) {
  const { user } = useAuth();
  const { t, locale } = useI18n();
  const copy = t.agentInstances.conversations;
  const person = usePersonName(row.requester_id, true);
  const date = new Date(row.updated_at);
  return (
    <li className="space-y-1 rounded-lg border p-3">
      <Link
        className="font-medium underline"
        href={`/workspace/chats/${encodeURIComponent(row.thread_id)}`}
      >
        {row.title ?? copy.untitled}
      </Link>
      <p className="text-muted-foreground text-sm">
        {row.requester_id === user?.id ? copy.yours : copy.with(person ?? "…")}
      </p>
      <div className="flex gap-3 text-sm">
        {Number.isFinite(date.valueOf()) && (
          <time dateTime={row.updated_at}>
            {new Intl.DateTimeFormat(locale, {
              dateStyle: "medium",
              timeStyle: "short",
            }).format(date)}
          </time>
        )}
        {row.running && <span>{copy.running}</span>}
      </div>
    </li>
  );
}

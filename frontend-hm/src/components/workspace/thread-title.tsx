import type { BaseStream } from "@langchain/langgraph-sdk";

import { useDocumentTitle } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import type { AgentThreadState } from "@/core/threads";

import { useThreadChat } from "./chats";
import { FlipDisplay } from "./flip-display";

export type ThreadTitleProps = {
  className?: string;
  threadId: string;
  thread: BaseStream<AgentThreadState>;
  canonicalTitle?: string;
};

export function ThreadTitle({
  threadId,
  thread,
  canonicalTitle,
}: ThreadTitleProps) {
  const { t } = useI18n();
  const { isNewThread } = useThreadChat();
  const title = canonicalTitle?.length ? canonicalTitle : thread.values?.title;

  const page = thread.isThreadLoading
    ? "Loading..."
    : (title ?? (isNewThread ? t.pages.newChat : t.pages.untitled));
  useDocumentTitle(page, t.pages.appName);

  if (!title) {
    return null;
  }
  return <FlipDisplay uniqueKey={threadId}>{title}</FlipDisplay>;
}

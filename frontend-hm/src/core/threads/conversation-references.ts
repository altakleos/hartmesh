import { request } from "@/core/agent-instances/api";
import { getBackendBaseURL } from "@/core/config";

export interface ConversationReference {
  id: string;
  title: string | null;
}

/** Match a whole copied product URL. Never request an address from clipboard text. */
export function pastedConversationId(text: string, origin: string) {
  const prefix = origin + "/workspace/";
  if (!text.startsWith(prefix)) return null;
  const path = text.slice(prefix.length);
  const match = /^(?:agents\/[\w%-]+\/)?chats\/([A-Za-z0-9_-]{1,64})$/.exec(
    path,
  );
  if (match?.[0] !== path || match[1] === "new") return null;
  return match[1]!;
}
interface SourceThread {
  thread_id: string;
  owner_user_id?: string | null;
  agent_instance?: { id: string } | null;
  values?: { title?: unknown };
}
export function readableReference(
  thread: SourceThread,
  id: string,
  account: string,
  instance?: string,
): ConversationReference | null {
  if (thread.thread_id !== id) return null;
  if (
    instance
      ? thread.agent_instance?.id !== instance
      : thread.owner_user_id !== account
  )
    return null;
  return {
    id,
    title:
      typeof thread.values?.title === "string"
        ? thread.values.title.trim() || null
        : null,
  };
}
export async function resolveConversationReference(
  id: string,
  account: string,
  instance: string | undefined,
  signal: AbortSignal,
) {
  const thread = await request<SourceThread>(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(id)}`,
    { signal, headers: { "X-DeerFlow-Require-Thread-Binding": "true" } },
  );
  return readableReference(thread, id, account, instance);
}

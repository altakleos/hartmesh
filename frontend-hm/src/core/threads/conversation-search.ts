import { useEffect, useState } from "react";
import { z } from "zod";

import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

export class SearchReadError extends Error {
  constructor(readonly status: number) {
    super(`Search request failed (HTTP ${status})`);
  }
}

export function useSearchReady() {
  const [ready, setReady] = useState(false);
  useEffect(() => {
    const timer = setTimeout(() => setReady(true), 250);
    return () => clearTimeout(timer);
  }, []);
  return ready;
}

export async function searchWorkspace(
  query: string,
  type: "thread" | "message",
  cursor: string | null,
  signal: AbortSignal,
): Promise<unknown> {
  const response = await fetch(`${getBackendBaseURL()}/api/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query,
      types: [type],
      limit: 20,
      ...(cursor && { cursor }),
    }),
    signal,
  });
  if (!response.ok) throw new SearchReadError(response.status);
  return response.json();
}

const role = z.enum(["user", "assistant"]);
const messageHit = z.object({
  type: z.literal("message"),
  thread_id: z.string(),
  title: z.string(),
  agent_name: z.string().nullable(),
  message_id: z.string().min(1),
  seq: z.number().int().positive(),
  role,
  snippet: z.array(
    z.object({ text: z.string(), matched: z.boolean().optional() }),
  ),
  updated_at: z.string(),
});
export type MessageHit = z.infer<typeof messageHit>;
export const messagePage = z.object({
  items: z.array(messageHit),
  cursor: z.string().nullable(),
  indexing: z.boolean(),
  messages_available: z.boolean(),
});
const messageWindow = z.object({
  target_seq: z.number().int().positive(),
  messages: z.array(
    z.object({
      thread_id: z.string(),
      message_id: z.string(),
      seq: z.number().int().positive(),
      role,
      text: z.string(),
      created_at: z.string(),
    }),
  ),
});

export async function readMessageWindow(hit: MessageHit, signal: AbortSignal) {
  const query = new URLSearchParams({
    message_id: hit.message_id,
    seq: String(hit.seq),
  });
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(hit.thread_id)}/messages/around?${query.toString()}`,
    { signal },
  );
  if (!response.ok) throw new SearchReadError(response.status);
  const { target_seq, messages } = messageWindow.parse(await response.json());
  if (
    target_seq !== hit.seq ||
    !messages.every((m) => m.thread_id === hit.thread_id) ||
    !messages.some(
      (m) => m.message_id === hit.message_id && m.seq === target_seq,
    )
  ) {
    throw new SearchReadError(404);
  }
  return messages;
}

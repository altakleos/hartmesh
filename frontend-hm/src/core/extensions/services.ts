import { z } from "zod";

import { getAPIClient } from "@/core/api";
import { fetch } from "@/core/api/fetcher";
import { readBoundedArtifactBytes } from "@/core/artifacts/response";
import { getBackendBaseURL } from "@/core/config";
import { urlOfThreadExport } from "@/core/threads/export";
import { pathOfThread } from "@/core/threads/utils";

import type {
  ConversationActionContext,
  FrontendContribution,
  FrontendServices,
} from "./contracts";
import { awaitPluginOperation } from "./operations";

export type HostServices = Omit<FrontendServices, "callBackend">;

/** Namespace comes from the installed page snapshot, never from action input. */
export function bindFrontendServices(
  base: HostServices,
  entry: FrontendContribution,
  signal?: AbortSignal,
): FrontendServices {
  return {
    async conversationText(context) {
      signal?.throwIfAborted();
      const pending = base.conversationText({
        ...context,
        signal: signal ?? context.signal,
      });
      const result = signal
        ? await awaitPluginOperation(pending, signal)
        : await pending;
      signal?.throwIfAborted();
      return result;
    },
    ...(base.latestVisibleAnswer
      ? {
          async latestVisibleAnswer(context: ConversationActionContext) {
            signal?.throwIfAborted();
            const pending = base.latestVisibleAnswer!({
              ...context,
              signal: signal ?? context.signal,
            });
            const result = signal
              ? await awaitPluginOperation(pending, signal)
              : await pending;
            signal?.throwIfAborted();
            return result;
          },
        }
      : {}),
    showMessage(message) {
      signal?.throwIfAborted();
      base.showMessage(message);
    },
    async callBackend(action, payload) {
      signal?.throwIfAborted();
      if (!entry.backend_actions?.includes(action))
        throw new Error("Backend action not declared by this plugin");
      return withServiceDeadline(signal, async (requestSignal) => {
        const response = await awaitPluginOperation(
          fetch(
            `${getBackendBaseURL()}/api/plugins/${encodeURIComponent(entry.namespace)}/actions/${encodeURIComponent(action)}`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                ...(entry.viewer_id
                  ? { "X-Deerflow-Plugin-Viewer": entry.viewer_id }
                  : {}),
              },
              body: JSON.stringify(payload),
              signal: requestSignal,
            },
          ),
          requestSignal,
          (late) => {
            void late.body?.cancel().catch(() => undefined);
          },
        );
        if (!response.ok)
          throw new Error(`Plugin action unavailable (${response.status})`);
        return awaitPluginOperation(
          response.json() as Promise<unknown>,
          requestSignal,
        );
      });
    },
  };
}

export async function conversationText(context: ConversationActionContext) {
  const data = await visibleConversation(context);
  return data.messages.map((message) => message.content).join("\n\n");
}

async function visibleConversation(context: ConversationActionContext) {
  // HartMesh owns transcript visibility on the Gateway, including compacted
  // history. Do not recreate the upstream client export or send raw SDK state.
  return withServiceDeadline(context.signal, async (signal) => {
    const response = await awaitPluginOperation(
      fetch(urlOfThreadExport(context.thread.thread_id, "json"), {
        cache: "no-store",
        signal,
      }),
      signal,
      (late) => {
        void late.body?.cancel().catch(() => undefined);
      },
    );
    if (response.status === 404) return { messages: [] };
    if (!response.ok)
      throw new Error(`Plugin transcript unavailable (${response.status})`);
    const read = await readBoundedArtifactBytes(
      response,
      4 * 1024 * 1024,
      signal,
    );
    if (read.truncated)
      throw new Error("Plugin transcript exceeds its byte budget");
    return z
      .object({
        messages: z.array(
          z.object({
            id: z.string().optional(),
            type: z.string(),
            content: z.string(),
          }),
        ),
      })
      .parse(
        JSON.parse(
          new TextDecoder("utf-8", { fatal: true }).decode(read.bytes),
        ),
      );
  });
}

async function withServiceDeadline<T>(
  parent: AbortSignal | undefined,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  parent?.throwIfAborted();
  const controller = new AbortController();
  const abort = () => controller.abort(parent?.reason);
  parent?.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(
    () =>
      controller.abort(
        new DOMException("Plugin service timed out", "TimeoutError"),
      ),
    30_000,
  );
  try {
    return await awaitPluginOperation(
      operation(controller.signal),
      controller.signal,
    );
  } finally {
    clearTimeout(timer);
    parent?.removeEventListener("abort", abort);
  }
}

export async function latestVisibleAnswer(context: ConversationActionContext) {
  const { messages } = await visibleConversation(context);
  const answer = [...messages]
    .reverse()
    .find((message) => message.type === "ai" && !!message.id);
  return answer ? { id: answer.id!, text: answer.content } : null;
}

/** Resolve live routing metadata through the authenticated host API, including legacy bookmarks. */
export async function openConversation(
  threadId: string,
  navigate: (path: string) => void,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  const thread = await getAPIClient().threads.get(threadId, { signal });
  signal?.throwIfAborted();
  navigate(pathOfThread(thread));
}

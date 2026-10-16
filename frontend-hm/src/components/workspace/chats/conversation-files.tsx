"use client";

import { createContext, useContext } from "react";

import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import { getBackendBaseURL } from "@/core/config";
import type { AgentThread } from "@/core/threads/types";

export interface ConversationFilesState {
  locked: boolean;
  sentence: string | null;
  agent: NonNullable<AgentThread["agent_instance"]> | null;
  detail?: AgentInstanceDetail | null;
  providerName?: string | null;
  supportURL?: string | null;
  refusedNetworkRequests?: ReadonlySet<string>;
}
export const ConversationFilesContext = createContext<ConversationFilesState>({
  locked: false,
  sentence: null,
  agent: null,
});
export function useConversationFiles() {
  return useContext(ConversationFilesContext);
}
export function useLockedFilesSentence() {
  return useConversationFiles().sentence;
}

/** Recognize local artifact routes without probing any file. */
export function isConversationFileURL(href: string, threadId?: string) {
  if (href.startsWith("/mnt/")) return true;
  const origin =
    typeof window === "undefined"
      ? "http://hartmesh.invalid"
      : window.location.origin;
  try {
    const url = new URL(href, origin);
    const backend = new URL(getBackendBaseURL() || origin, origin);
    if (url.origin !== origin && url.origin !== backend.origin) return false;
    const roots = [""];
    if (url.origin === backend.origin)
      roots.push(backend.pathname.replace(/\/+$/, ""));
    return roots.some((root) => {
      if (!url.pathname.startsWith(`${root}/`)) return false;
      const path = url.pathname.slice(root.length);
      return threadId
        ? path.startsWith(
            `/api/threads/${encodeURIComponent(threadId)}/artifacts/`,
          )
        : /^\/api\/threads\/[^/]+\/artifacts\//.test(path);
    });
  } catch {
    return false;
  }
}

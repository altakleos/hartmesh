"use client";

import { useEffect, useRef } from "react";

import { prewarmThreadWorkspace } from "@/core/threads/api";

/**
 * Build a new thread's sandbox while the person is still typing.
 *
 * `useThreadChat` mints the thread id the moment `/new` opens, seconds before
 * the first message is sent. The sandbox the first turn needs depends on that
 * id and the signed-in user, not on the message, so the Gateway can start it
 * now and the turn finds it warm. One request per thread id: a re-render, or
 * a second component on the same page, must not ask twice.
 */
export function usePrewarmWorkspace({
  threadId,
  enabled,
}: {
  threadId: string | undefined;
  enabled: boolean;
}) {
  const requestedForRef = useRef<string | null>(null);
  useEffect(() => {
    if (!enabled || !threadId) {
      return;
    }
    if (requestedForRef.current === threadId) {
      return;
    }
    requestedForRef.current = threadId;
    void prewarmThreadWorkspace(threadId);
  }, [enabled, threadId]);
}

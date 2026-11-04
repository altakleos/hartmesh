/**
 * The run this tab streams for a thread, kept where the LangGraph SDK keeps
 * its own resumable-stream pointer (`sessionStorage["lg:stream:<thread>"]`).
 *
 * The SDK writes it for a submitted run, rejoins it on mount and removes it
 * when the stream settles; Stop cancels the run it names. A joined run gets no
 * pointer from the SDK, so callers that join one record it here.
 */
const reconnectRunKey = (threadId: string) => `lg:stream:${threadId}`;

export function readReconnectRun(
  threadId: string | null | undefined,
): string | null {
  if (typeof window === "undefined" || !threadId) return null;
  try {
    return window.sessionStorage.getItem(reconnectRunKey(threadId));
  } catch {
    return null;
  }
}

export function rememberReconnectRun(
  threadId: string | null | undefined,
  runId: string,
): void {
  if (typeof window === "undefined" || !threadId) return;
  try {
    window.sessionStorage.setItem(reconnectRunKey(threadId), runId);
  } catch {
    // Ignore storage access failures so gap recovery remains usable.
  }
}

export function clearReconnectRun(
  threadId: string | null | undefined,
  runId: string,
): void {
  if (typeof window === "undefined" || !threadId) return;
  try {
    const storage = window.sessionStorage;
    if (storage.getItem(reconnectRunKey(threadId)) === runId) {
      storage.removeItem(reconnectRunKey(threadId));
    }
  } catch {
    // Ignore storage access failures so reconnect cleanup never throws.
  }
}

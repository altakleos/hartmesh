import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import {
  readSpaceText,
  spaceFileURL,
  StorageRequestError,
} from "@/core/spaces/api";

/** Reads may race a direct file replacement. Only a 409 receives one retry. */
export async function retryFileRead<T>(
  read: () => Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  try {
    return await awaitAbortable(read(), signal);
  } catch (error) {
    signal.throwIfAborted();
    if (!(error instanceof StorageRequestError) || error.status !== 409)
      throw error;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await awaitAbortable(
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, 3000);
        }),
        signal,
      );
    } finally {
      clearTimeout(timer);
    }
    signal.throwIfAborted();
    return awaitAbortable(read(), signal);
  }
}
export function readAgentText(home: string, path: string, signal: AbortSignal) {
  return retryFileRead(() => readSpaceText(home, path, signal), signal);
}
export function downloadAgentFile(
  home: string,
  path: string,
  signal: AbortSignal,
) {
  return retryFileRead(async () => {
    const response = await awaitAbortable(
      fetch(spaceFileURL(home, path, true), { signal, cache: "no-store" }),
      signal,
      (late) => {
        void late.body?.cancel().catch(() => undefined);
      },
    );
    if (!response.ok) {
      void response.body?.cancel().catch(() => undefined);
      throw new StorageRequestError(response.status, "File read failed");
    }
    return awaitAbortable(response.blob(), signal);
  }, signal);
}

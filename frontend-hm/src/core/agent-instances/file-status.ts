import { UnauthorizedError } from "@/core/api/errors";

import type { SavedAgentFile } from "./agent-files";
import { InstanceApiError, instanceBaseURL, request } from "./api";

export type FileStatus =
  | { status: "pending" | "unknown" | "nothing-changed" | "unconfirmed" }
  | { status: "complete"; file: SavedAgentFile & { sandbox_path?: string } };
export type CheckState =
  | FileStatus["status"]
  | "checking"
  | "unavailable"
  | "denied";
export type FileLookup = {
  id: string;
  thread?: { id: string; filename: string };
};

export function unansweredWrite(error: unknown) {
  if (error instanceof InstanceApiError)
    return (
      (!error.reason && error.status >= 500) || error.reason === "storage-busy"
    );
  return !(error instanceof UnauthorizedError);
}

export function fileOutcome(error: unknown) {
  return error instanceof InstanceApiError
    ? error.extra.file_outcome
    : undefined;
}

async function readStatus(
  instance: string,
  lookup: FileLookup,
  signal: AbortSignal,
) {
  const base = instanceBaseURL(instance);
  if (lookup.thread) {
    const thread = lookup.thread;
    const query = new URLSearchParams({ filename: thread.filename });
    return request<FileStatus>(
      `${base}/conversations/${encodeURIComponent(thread.id)}/files/operations/${lookup.id}?${query}`,
      {
        signal,
        cache: "no-store",
      },
    );
  }
  return request<FileStatus>(`${base}/home/operations/${lookup.id}`, {
    signal,
    cache: "no-store",
  });
}

function delay(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    if (signal.aborted) finish();
    else signal.addEventListener("abort", finish, { once: true });
  });
}

export async function checkFileOperation(
  instance: string,
  lookup: FileLookup,
  signal: AbortSignal,
  automatic: boolean,
  update: (
    state: CheckState,
    result?: SavedAgentFile & { sandbox_path?: string },
  ) => void | Promise<void>,
) {
  for (const wait of automatic ? [0, 3000, 15000, 60000, 300000] : [0]) {
    // Let a retiring view cancel even its first read before issuing HTTP.
    await delay(wait, signal);
    if (signal.aborted) return;
    try {
      const outcome = await readStatus(instance, lookup, signal);
      if (signal.aborted) return;
      if (outcome.status === "complete") {
        if (!outcome.file || typeof outcome.file.path !== "string")
          throw new Error("Incomplete file receipt");
        await update("complete", outcome.file);
        return;
      }
      if (
        !["unknown", "pending", "nothing-changed", "unconfirmed"].includes(
          outcome.status,
        )
      )
        throw new Error("Unknown file outcome");
      await update(outcome.status);
      if (
        outcome.status === "nothing-changed" ||
        outcome.status === "unconfirmed"
      )
        return;
    } catch (error) {
      if (signal.aborted) return;
      if (
        error instanceof UnauthorizedError ||
        (error instanceof InstanceApiError &&
          [401, 403, 404].includes(error.status))
      ) {
        await update("denied");
        return;
      }
      await update("unavailable");
    }
  }
}

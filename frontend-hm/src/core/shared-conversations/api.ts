import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import type {
  ContinueResult,
  ContinueTarget,
  PublishResult,
  RetiredSnapshot,
  SharedHistory,
  Snapshot,
  SnapshotDraft,
  SnapshotSummary,
} from "./protocol";

const root = "/api/shared-conversations";
export const snapshotHref = (id: string) =>
  "/workspace/files/conversations/" + encodeURIComponent(id);
function entry(id: string) {
  return root + "/" + encodeURIComponent(id);
}

export class SnapshotError extends Error {
  constructor(
    public status: number,
    public detail: unknown,
  ) {
    super(
      typeof detail === "string"
        ? detail
        : `Shared conversation request failed (${status})`,
    );
  }
}
async function json<T>(
  path: string,
  signal: AbortSignal,
  method = "GET",
  body?: unknown,
): Promise<T> {
  const response = await fetch(getBackendBaseURL() + path, {
    method,
    cache: "no-store",
    signal,
    ...(body === undefined
      ? {}
      : {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }),
  });
  if (response.status === 204) return undefined as T;
  if (response.ok) return (await response.json()) as T;
  const payload: unknown = await response.json().catch(() => null);
  throw new SnapshotError(
    response.status,
    payload && typeof payload === "object" && "detail" in payload
      ? payload.detail
      : undefined,
  );
}
export const snapshotList = (signal: AbortSignal, before: string | null) =>
  json<{
    conversations: SnapshotSummary[];
    truncated: boolean;
    next_before: string | null;
  }>(
    before ? root + "?" + new URLSearchParams({ before }).toString() : root,
    signal,
  );
export const snapshotDraft = (thread: string, signal: AbortSignal) =>
  json<SnapshotDraft>(
    root + "/draft?" + new URLSearchParams({ thread_id: thread }).toString(),
    signal,
  );
export const publishSnapshot = (
  thread_id: string,
  snapshot: string,
  signal: AbortSignal,
  acknowledge?: { snapshot: string; audience: "company" },
) =>
  json<PublishResult>(root, signal, "POST", {
    thread_id,
    snapshot,
    ...(acknowledge ? { acknowledge } : {}),
  });
export const undoSnapshot = (
  id: string,
  publication_ids: string[],
  undo_receipt: string,
  signal: AbortSignal,
) =>
  json(entry(id) + "/undo", signal, "POST", { publication_ids, undo_receipt });
export const removeSnapshot = (id: string, signal: AbortSignal) =>
  json<{ retained: { path: string; publisher: string | null }[] }>(
    entry(id),
    signal,
    "DELETE",
  );
export const continueSnapshot = (
  id: string,
  request_id: string,
  signal: AbortSignal,
  answerer: ContinueTarget,
) =>
  json<ContinueResult>(entry(id) + "/continue", signal, "POST", {
    request_id,
    answerer,
  });
export const threadSharedHistory = (thread: string, signal: AbortSignal) =>
  json<SharedHistory | null>(
    "/api/threads/" + encodeURIComponent(thread) + "/shared-history",
    signal,
  );
export async function snapshotDetail(
  id: string,
  signal: AbortSignal,
): Promise<Snapshot | RetiredSnapshot> {
  try {
    return await json<Snapshot>(entry(id), signal);
  } catch (e) {
    const d = e instanceof SnapshotError && e.status === 410 ? e.detail : null;
    if (
      d &&
      typeof d === "object" &&
      "code" in d &&
      d.code === "snapshot_removed" &&
      "publisher" in d &&
      (d.publisher === null || typeof d.publisher === "string") &&
      "published_at" in d &&
      typeof d.published_at === "string"
    )
      return {
        removed: true,
        publisher: d.publisher,
        published_at: d.published_at,
      };
    throw e;
  }
}

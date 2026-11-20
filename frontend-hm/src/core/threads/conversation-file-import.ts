import { instanceBaseURL, request, write } from "@/core/agent-instances/api";
import { getBackendBaseURL } from "@/core/config";
import type { FileInMessage } from "@/core/messages/utils";
import { shouldShowInPrimaryThreadLists } from "@/core/sidecar/thread";

export interface ImportDestination {
  threadId: string;
  instance?: { id: string; name: string };
  newThread?: boolean;
}
export interface ImportSource {
  thread_id: string;
  title: string | null;
  instanceName?: string;
}
export interface PresentedFile {
  filename: string;
  run_id: string;
  path: string;
  size: number | null;
  modified: number | null;
  available: boolean;
}
interface PresentedPage {
  files: PresentedFile[];
  has_more: boolean;
  next_before_created_at: string | null;
  next_before_run_id: string | null;
}
export interface PresentedCursor {
  before_created_at: string;
  before_run_id: string;
}
export function nextPresentedCursor(
  page: PresentedPage,
): PresentedCursor | null {
  if (!page.has_more) return null;
  if (!page.next_before_created_at || !page.next_before_run_id)
    throw new Error("Incomplete file cursor");
  return {
    before_created_at: page.next_before_created_at,
    before_run_id: page.next_before_run_id,
  };
}
export interface ImportReceipt {
  filename: string;
  virtual_path: string;
  size: number;
  sha256: string;
}
export type SavedAttachment = Required<FileInMessage>;
export function importedAttachment(receipt: ImportReceipt): SavedAttachment {
  const { filename, virtual_path, size, sha256 } = receipt;
  if (
    typeof filename !== "string" ||
    !filename ||
    /[\\/\p{Cc}]/u.test(filename) ||
    [".", ".."].includes(filename) ||
    virtual_path !== `/mnt/user-data/uploads/${filename}` ||
    !Number.isSafeInteger(size) ||
    size < 0 ||
    typeof sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(sha256)
  )
    throw new Error("Invalid saved attachment");
  return { filename, path: virtual_path, size, status: "uploaded" };
}
export async function recentImportSources(
  destination: ImportDestination,
  account: string,
  offset: number,
  signal: AbortSignal,
  agentSources: boolean,
) {
  const limit = 25;
  const rows = await write<
    {
      thread_id: string;
      owner_user_id?: string | null;
      values?: { title?: string };
      metadata?: Record<string, unknown>;
      agent_instance?: { id: string; name: string } | null;
    }[]
  >(
    `${getBackendBaseURL()}/api/threads/search`,
    "POST",
    { limit, offset, archived: false },
    signal,
  );
  return {
    items: rows
      .filter(
        (row) =>
          row.thread_id !== destination.threadId &&
          shouldShowInPrimaryThreadLists(row) &&
          (row.agent_instance
            ? !destination.instance && agentSources
            : row.owner_user_id === account),
      )
      .map((row) => ({
        thread_id: row.thread_id,
        title: row.values?.title ?? null,
        instanceName: row.agent_instance?.name,
      })),
    next: rows.length === limit ? offset + limit : null,
  };
}
export async function presentedImportFiles(
  source: ImportSource,
  cursor: PresentedCursor | null,
  signal: AbortSignal,
) {
  const query = new URLSearchParams({ limit: "20" });
  if (cursor) {
    query.set("before_created_at", cursor.before_created_at);
    query.set("before_run_id", cursor.before_run_id);
  }
  const page = await request<PresentedPage>(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(source.thread_id)}/presented-files?${query}`,
    { signal },
  );
  return { items: page.files, next: nextPresentedCursor(page) };
}
export async function importConversationFile(
  destination: ImportDestination,
  source: ImportSource,
  file: PresentedFile,
  operation: string,
  signal: AbortSignal,
) {
  const thread = encodeURIComponent(destination.threadId);
  const home = destination.instance;
  if (home && source.instanceName)
    throw new Error("Unsupported file destination");
  const base = getBackendBaseURL();
  const url = home
    ? `${instanceBaseURL(home.id)}/conversations/${thread}/files/from-conversation?operation_id=${operation}`
    : `${base}/api/threads/${thread}/uploads/from-conversation`;
  const receipt = await write<ImportReceipt>(
    url,
    "POST",
    {
      source_thread_id: source.thread_id,
      source_run_id: file.run_id,
      path: file.path,
      ...(home
        ? { acknowledge_disclosure: true }
        : destination.newThread
          ? { create_destination: true }
          : {}),
    },
    signal,
  );
  return importedAttachment(receipt);
}

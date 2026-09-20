/**
 * The company's Shared area: what anyone published, readable by everyone.
 *
 * `GET /api/shared` is what the Files page's *Shared* tab lists;
 * `POST /api/shared/publish` puts one of the person's files, or one of a
 * conversation's uploads or outputs, there (the card's and the panel's
 * *Share with everyone*); `DELETE` takes one back, for its publisher or an
 * admin, which the listing says per file (`can_remove`).
 */

import { fetch } from "../api/fetcher";
import { getBackendBaseURL } from "../config";
import {
  encodeRelativePath,
  FileAreaRequestError,
  readErrorDetail,
} from "../file-areas";

/** Where every sandbox sees the Shared area. */
export const SHARED_VIRTUAL_PREFIX = "/mnt/user-data/shared";

export interface SharedFileInfo {
  /** Relative to the Shared root, e.g. `Reports/august.pdf`. */
  path: string;
  name: string;
  size: number;
  /** Seconds since the epoch. */
  modified: number;
  virtual_path: string;
  url: string;
  /** Who published it; a file an operator placed by hand has nobody. */
  published_by: string | null;
  /** ISO-8601, when there is a record. */
  published_at: string | null;
  from_thread_id: string | null;
  /** Whether the caller may take this one back: its publisher, or an admin. */
  can_remove: boolean;
}

export interface SharedFilesListResponse {
  files: SharedFileInfo[];
  count: number;
  /** The listing stopped at its ceiling; what it holds is a prefix. */
  truncated: boolean;
}

export interface PublishToSharedRequest {
  /**
   * The file, as the sandbox names it: one of the person's own files, or a
   * conversation's upload or output (then `thread_id` names the conversation).
   */
  path: string;
  thread_id?: string;
  /** A folder in Shared; the root when omitted. */
  folder?: string;
}

export class SharedRequestError extends FileAreaRequestError {
  constructor(status: number, message: string) {
    super(status, message, "SharedRequestError");
  }
}

/** Where the browser fetches one shared file. */
export function urlOfSharedFile(path: string, { download = false } = {}) {
  return `${getBackendBaseURL()}/api/shared/${encodeRelativePath(path)}${download ? "?download=true" : ""}`;
}

export async function listSharedFiles(): Promise<SharedFilesListResponse> {
  const response = await fetch(`${getBackendBaseURL()}/api/shared`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new SharedRequestError(
      response.status,
      await readErrorDetail(response, "Failed to list shared files"),
    );
  }
  return response.json() as Promise<SharedFilesListResponse>;
}

export async function removeSharedFile(path: string): Promise<void> {
  const response = await fetch(urlOfSharedFile(path), { method: "DELETE" });
  if (!response.ok) {
    throw new SharedRequestError(
      response.status,
      await readErrorDetail(response, "Failed to remove shared file"),
    );
  }
}

export async function publishToShared(
  request: PublishToSharedRequest,
): Promise<SharedFileInfo> {
  const response = await fetch(`${getBackendBaseURL()}/api/shared/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    throw new SharedRequestError(
      response.status,
      await readErrorDetail(response, "Failed to publish file"),
    );
  }
  return response.json() as Promise<SharedFileInfo>;
}

/**
 * Whether a file can be published from where it is: the person's own files,
 * and what a conversation was given or made. The workspace is scratch, and
 * Shared is already everyone's.
 */
export function canPublishToShared(filepath: string) {
  return (
    filepath.startsWith("/mnt/user-data/outputs/") ||
    filepath.startsWith("/mnt/user-data/uploads/") ||
    filepath.startsWith("/mnt/user-data/files/")
  );
}

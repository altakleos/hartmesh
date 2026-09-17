/**
 * The person's own files, kept across conversations.
 *
 * `GET /api/files` is what the Files page lists; `POST /api/threads/{id}/files`
 * keeps one of a conversation's files there (the card's *Save to my files*
 * and the panel's action); the rest is one file at a time.
 */

import { fetch } from "../api/fetcher";
import { getBackendBaseURL } from "../config";

/** Where the sandbox sees the person's files. */
export const MY_FILES_VIRTUAL_PREFIX = "/mnt/user-data/files";

export interface MyFileInfo {
  /** Relative to the person's files root, e.g. `Reports/august.pdf`. */
  path: string;
  name: string;
  size: number;
  /** Seconds since the epoch. */
  modified: number;
  virtual_path: string;
  url: string;
}

export interface MyFilesListResponse {
  files: MyFileInfo[];
  count: number;
  /** The listing stopped at its ceiling; what it holds is a prefix. */
  truncated: boolean;
}

export interface KeepInMyFilesRequest {
  /** The conversation's file, as the sandbox names it (uploads or outputs). */
  path: string;
  /** A folder in the person's files; the root when omitted. */
  folder?: string;
}

export class MyFilesRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "MyFilesRequestError";
    this.status = status;
  }
}

async function readErrorDetail(
  response: Response,
  fallback: string,
): Promise<string> {
  const data = (await response.json().catch(() => null)) as unknown;
  if (
    typeof data === "object" &&
    data !== null &&
    typeof (data as { detail?: unknown }).detail === "string"
  ) {
    return (data as { detail: string }).detail;
  }
  return fallback;
}

function encodeRelativePath(path: string) {
  return path.split("/").map(encodeURIComponent).join("/");
}

/** Where the browser fetches one of the person's files. */
export function urlOfMyFile(path: string, { download = false } = {}) {
  return `${getBackendBaseURL()}/api/files/${encodeRelativePath(path)}${download ? "?download=true" : ""}`;
}

export async function listMyFiles(): Promise<MyFilesListResponse> {
  const response = await fetch(`${getBackendBaseURL()}/api/files`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new MyFilesRequestError(
      response.status,
      await readErrorDetail(response, "Failed to list files"),
    );
  }
  return response.json() as Promise<MyFilesListResponse>;
}

export async function deleteMyFile(path: string): Promise<void> {
  const response = await fetch(urlOfMyFile(path), { method: "DELETE" });
  if (!response.ok) {
    throw new MyFilesRequestError(
      response.status,
      await readErrorDetail(response, "Failed to delete file"),
    );
  }
}

export async function keepInMyFiles(
  threadId: string,
  request: KeepInMyFilesRequest,
): Promise<MyFileInfo> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/files`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
  );
  if (!response.ok) {
    throw new MyFilesRequestError(
      response.status,
      await readErrorDetail(response, "Failed to keep file"),
    );
  }
  return response.json() as Promise<MyFileInfo>;
}

/**
 * Whether a conversation's file can be kept: what the person gave it and what
 * it made for them. The workspace is scratch, and the person's own files are
 * already theirs.
 */
export function canKeepInMyFiles(filepath: string) {
  return (
    filepath.startsWith("/mnt/user-data/outputs/") ||
    filepath.startsWith("/mnt/user-data/uploads/")
  );
}

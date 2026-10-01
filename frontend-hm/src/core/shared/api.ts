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
import {
  filingFolderFor,
  REPORTS_FOLDER,
  type Presented,
} from "../business-report/paths";
import { getBackendBaseURL } from "../config";
import {
  encodeRelativePath,
  FileAreaRequestError,
  readErrorDetail,
} from "../file-areas";
import { MY_FILES_VIRTUAL_PREFIX } from "../files/api";

/** Where every sandbox sees the Shared area. */
export const SHARED_VIRTUAL_PREFIX = "/mnt/user-data/shared";

/**
 * Where a file lands in Shared, or `undefined` for the root.
 *
 * One rule for every way of sharing — the report card, the artifact panel,
 * a row of *My files* — because the folder is a property of the file, not of
 * the button pressed: otherwise the same report reaches Shared twice, once
 * per route, and the company sees two copies of one thing.
 *
 * A report's download is filed with the reports, and so is one of the
 * person's own files that they keep in the same _Reports_ folder, which is
 * where this product puts a report it saves for them. Anything else sits at
 * the root: the rest of how someone keeps their own files is theirs, not the
 * company's.
 */
export function sharedFolderFor(
  path: string,
  presented: Presented,
): string | undefined {
  if (path.startsWith(`${MY_FILES_VIRTUAL_PREFIX}/`)) {
    // Only the product's own folder crosses over. The rest of how a person
    // keeps their files is theirs: a folder named for a customer or a deal
    // would otherwise become a company-wide folder on one click, and two
    // colleagues who file the same report differently would put two copies
    // in Shared, which is the thing this rule exists to prevent.
    const relative = path.slice(MY_FILES_VIRTUAL_PREFIX.length + 1);
    return relative.startsWith(`${REPORTS_FOLDER}/`) &&
      !relative.slice(REPORTS_FOLDER.length + 1).includes("/")
      ? REPORTS_FOLDER
      : undefined;
  }
  return filingFolderFor(path, presented);
}

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

/** What publishing did: the entry in Shared, and whether it was already there. */
export interface PublishOutcome {
  file: SharedFileInfo;
  /**
   * The same bytes were already in that folder of Shared, so nothing was
   * copied and the entry is the one that was there (the server answers
   * `200` instead of `201`). A second click, a second tab, a colleague's
   * identical file: all land once.
   */
  alreadyShared: boolean;
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
): Promise<PublishOutcome> {
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
  return {
    file: (await response.json()) as SharedFileInfo,
    alreadyShared: response.status === 200,
  };
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

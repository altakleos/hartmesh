import { fetch } from "../api/fetcher";
import { getBackendBaseURL } from "../config";

import type { BrowserRow } from "./browser";
import { FileAreaRequestError, readFileAreaFailure } from "./http";

export type WorkspaceList<T> = {
  files: T[];
  count: number;
  truncated: boolean;
  directories?: BrowserRow[];
  folderMissing?: boolean;
};

export class FolderRouteMissing extends FileAreaRequestError {}

export function retryWorkspaceFolder(failures: number, error: Error) {
  if (
    error instanceof FileAreaRequestError &&
    [401, 403, 404].includes(error.status)
  )
    return false;
  return failures < 3;
}

export async function readWorkspaceFolder<T>(
  area: "personal" | "shared",
  path: string,
  signal: AbortSignal,
): Promise<WorkspaceList<T>> {
  const url = `${getBackendBaseURL()}/api/file-browser/v1/${area}/folder?${new URLSearchParams({ path })}`;
  const response = await fetch(url, { cache: "no-store", signal });
  if (!response.ok) {
    const { detail, message, refusal } = await readFileAreaFailure(
      response,
      "Could not open this folder",
    );
    if (response.status === 404 && detail === "Not Found")
      throw new FolderRouteMissing(response.status, message);
    if (
      path &&
      response.status === 404 &&
      typeof detail === "object" &&
      detail !== null &&
      "code" in detail &&
      detail.code === "folder-unavailable"
    )
      return { files: [], count: 0, truncated: false, folderMissing: true };
    throw new FileAreaRequestError(
      response.status,
      message,
      "WorkspaceFolderError",
      refusal,
    );
  }
  const data = (await response.json()) as {
    files: ((T & { kind: "file" }) | (BrowserRow & { kind: "directory" }))[];
    truncated: boolean;
  };
  const files: T[] = [];
  const directories: BrowserRow[] = [];
  for (const row of data.files) {
    if (row.kind === "file") files.push(row);
    else if (row.kind === "directory") directories.push(row);
  }
  return {
    files,
    directories,
    count: data.files.length,
    truncated: data.truncated,
  };
}

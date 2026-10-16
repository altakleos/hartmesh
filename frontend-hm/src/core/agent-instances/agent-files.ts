import { UnauthorizedError } from "@/core/api/errors";
import { readCsrfCookie } from "@/core/api/fetcher";
import { buildLoginUrl } from "@/core/auth/types";

import { InstanceApiError, instanceBaseURL } from "./api";

export interface SavedAgentFile {
  path: string;
  size: number;
  sha256: string;
  deduplicated: boolean;
}

export interface TransferOptions {
  signal?: AbortSignal;
  onProgress?: (percentage: number) => void;
}

export type AgentFileAction = {
  operation_id: string;
  path: string;
} & (
  | { action: "mkdir" | "remove" }
  | { action: "rename"; destination: string }
);

/** A single write: only the caller can choose to repeat its operation id. */
function transfer<T>(
  method: string,
  url: string,
  body: Blob | FormData | string,
  bytes: number,
  options: TransferOptions,
  contentType?: string,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const { signal } = options;
    if (signal?.aborted) {
      reject(
        signal.reason instanceof Error
          ? signal.reason
          : new DOMException("Transfer cancelled", "AbortError"),
      );
      return;
    }
    const xhr = new XMLHttpRequest();
    let settled = false;
    function settle(error?: unknown, result?: T) {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", abort);
      xhr.onload = xhr.onerror = xhr.ontimeout = xhr.onabort = null;
      xhr.upload.onprogress = null;
      if (error !== undefined)
        reject(
          error instanceof Error
            ? error
            : new Error("File transfer failed", { cause: error }),
        );
      else resolve(result as T);
    }
    function abort() {
      settle(
        signal?.reason ?? new DOMException("Transfer cancelled", "AbortError"),
      );
      xhr.abort();
    }
    xhr.open(method, url);
    xhr.withCredentials = true;
    xhr.timeout = 45_000 + Math.ceil((bytes / (1024 * 1024)) * 2_000);
    const csrf = readCsrfCookie();
    if (csrf) xhr.setRequestHeader("X-CSRF-Token", csrf);
    if (contentType) xhr.setRequestHeader("Content-Type", contentType);
    xhr.upload.onprogress = (event) => {
      if (
        !settled &&
        !signal?.aborted &&
        event.lengthComputable &&
        event.total > 0
      ) {
        options.onProgress?.(
          Math.min(100, Math.max(0, (event.loaded / event.total) * 100)),
        );
      }
    };
    xhr.onload = () => {
      if (settled) return;
      if (signal?.aborted) {
        abort();
        return;
      }
      if (xhr.status === 401) {
        window.location.href = buildLoginUrl(
          `${window.location.pathname}${window.location.search}`,
        );
        settle(new UnauthorizedError());
        return;
      }
      let payload: unknown;
      try {
        payload = JSON.parse(xhr.responseText);
      } catch {
        if (xhr.status >= 200 && xhr.status < 300) {
          settle(new Error("Invalid file response"));
          return;
        }
      }
      if (xhr.status < 200 || xhr.status >= 300) {
        settle(InstanceApiError.fromResponse(xhr.status, payload));
      } else settle(undefined, payload as T);
    };
    xhr.onerror = () => settle(new TypeError("File transfer failed"));
    xhr.ontimeout = () =>
      settle(new DOMException("File transfer timed out", "TimeoutError"));
    xhr.onabort = () =>
      settle(new DOMException("Transfer cancelled", "AbortError"));
    signal?.addEventListener("abort", abort, { once: true });
    try {
      xhr.send(body);
    } catch (error) {
      settle(error);
    }
  });
}

export function uploadConversationFile(
  instanceId: string,
  threadId: string,
  file: File,
  operationId: string,
  options: TransferOptions = {},
) {
  const body = new FormData();
  body.append("file", file);
  const query = new URLSearchParams({ operation_id: operationId });
  return transfer<SavedAgentFile & { sandbox_path: string }>(
    "POST",
    `${instanceBaseURL(instanceId)}/conversations/${encodeURIComponent(threadId)}/files?${query}`,
    body,
    file.size,
    options,
  );
}

export function putAgentFile(
  instanceId: string,
  path: string,
  bytes: Blob,
  operationId: string,
  options: TransferOptions & { expectedSha256?: string } = {},
) {
  const query = new URLSearchParams({ path, operation_id: operationId });
  if (options.expectedSha256)
    query.set("expected_sha256", options.expectedSha256);
  return transfer<SavedAgentFile>(
    "PUT",
    `${instanceBaseURL(instanceId)}/home/content?${query}`,
    bytes,
    bytes.size,
    options,
  );
}

export function changeAgentFile(
  instanceId: string,
  action: AgentFileAction,
  options: TransferOptions = {},
) {
  const body = JSON.stringify(action);
  return transfer<{ complete: true; path: string }>(
    "POST",
    `${instanceBaseURL(instanceId)}/home/files`,
    body,
    new TextEncoder().encode(body).byteLength,
    options,
    "application/json",
  );
}

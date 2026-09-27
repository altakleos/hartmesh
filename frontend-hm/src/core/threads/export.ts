import { fetch } from "../api/fetcher";
import { getBackendBaseURL } from "../config";

/**
 * One conversation's transcript, downloaded from the Gateway.
 *
 * The Gateway writes it from the same messages this page draws: the
 * conversation's message feed, however far its checkpoint was compacted. It
 * carries the user-visible words only — no reasoning, tool calls, tool
 * results, hidden messages or injected `<system-reminder>` payloads
 * (bytedance/deer-flow#3107 BUG-006) — and
 * `contracts/visible_transcript_contract.json` holds the Gateway's rules to
 * the ones `core/messages/utils.ts` draws with.
 */

export type ThreadExportFormat = "markdown" | "json";

/** The conversation has nothing recorded yet, so there is no transcript. */
export class ThreadExportEmptyError extends Error {}

export function urlOfThreadExport(
  threadId: string,
  format: ThreadExportFormat,
): string {
  return `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/export?format=${format}`;
}

/** The download's name from `Content-Disposition`, preferring the UTF-8 `filename*`. */
export function filenameFromDisposition(disposition: string | null) {
  if (!disposition) return null;
  const encoded = /filename\*=UTF-8''([^;]+)/i.exec(disposition)?.[1];
  if (encoded) {
    try {
      return decodeURIComponent(encoded);
    } catch {
      // A malformed escape falls back to the plain name.
    }
  }
  return /filename="([^"]+)"/.exec(disposition)?.[1] ?? null;
}

function saveBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export async function exportThread(
  threadId: string,
  format: ThreadExportFormat,
) {
  const response = await fetch(urlOfThreadExport(threadId, format));
  if (response.status === 404) {
    throw new ThreadExportEmptyError();
  }
  if (!response.ok) {
    throw new Error(`Export failed with status ${response.status}`);
  }
  const filename =
    filenameFromDisposition(response.headers.get("Content-Disposition")) ??
    `conversation.${format === "markdown" ? "md" : "json"}`;
  saveBlob(await response.blob(), filename);
}

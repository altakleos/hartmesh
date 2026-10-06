import type { BaseStream } from "@langchain/langgraph-sdk/react";

import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import { isArtifactViewPath } from "@/core/artifact-views/contract";
import { isStaticWebsiteOnly } from "@/core/static-mode";

import type { AgentThreadState } from "../threads";

import { buildWriteFileDraftContent } from "./preview";
import { readBoundedArtifactBytes } from "./response";
import { urlOfArtifact } from "./utils";

function fnv1aHash(value: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

async function sha256OfText(content: string): Promise<string> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) {
    // crypto.subtle is only exposed in secure contexts (HTTPS or localhost).
    // On a non-secure origin such as http://<lan-ip>:<port> it is undefined, so
    // hashing would throw and break artifact preview + inline editing
    // (issue #4864). The Gateway returns the real SHA-256 via the ETag header,
    // so this fallback is only a last-resort fingerprint used for draft
    // reconciliation when that header is absent.
    return fnv1aHash(content);
  }
  const digest = await subtle.digest(
    "SHA-256",
    new TextEncoder().encode(content),
  );
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

export const ARTIFACT_PREVIEW_MAX_BYTES = 1024 * 1024;

function parseContentRange(value: string | null) {
  const match = value?.match(/^bytes (?:(\d+)-(\d+)|\*)\/(\d+)$/);
  if (!match) return undefined;
  return {
    start: match[1] === undefined ? undefined : Number(match[1]),
    end: match[2] === undefined ? undefined : Number(match[2]),
    total: Number(match[3]),
  };
}

export type ArtifactPresentationRequest = {
  namespace: string;
  id: string;
  sourceMaxBytes: number;
  previewMaxBytes: number;
  marker?: string;
};

type ArtifactLoadOptions = {
  filepath: string;
  threadId: string;
  isMock?: boolean;
  full?: boolean;
  previewMaxBytes?: number;
  signal?: AbortSignal;
  presentation?: ArtifactPresentationRequest;
};

type LoadedArtifactContent = {
  content: string;
  url: string;
  sha256: string | undefined;
  projected: boolean;
  truncated: boolean;
  previewBytes: number;
  totalBytes: number | undefined;
};

export async function loadArtifactContent(
  options: ArtifactLoadOptions,
): Promise<LoadedArtifactContent> {
  if (!isArtifactViewPath(options.filepath) && !options.presentation)
    return readArtifactContent(options);
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted) abort();
  else options.signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(
    () =>
      controller.abort(
        new DOMException("Artifact read timed out.", "TimeoutError"),
      ),
    20_000,
  );
  try {
    return await readArtifactContent({ ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", abort);
  }
}

async function readArtifactContent({
  filepath,
  threadId,
  isMock,
  full = false,
  previewMaxBytes = ARTIFACT_PREVIEW_MAX_BYTES,
  signal,
  presentation,
}: ArtifactLoadOptions): Promise<LoadedArtifactContent> {
  signal?.throwIfAborted();
  let enhancedFilepath = filepath;
  if (filepath.endsWith(".skill")) {
    enhancedFilepath = filepath + "/SKILL.md";
  }
  const url = urlOfArtifact({ filepath: enhancedFilepath, threadId, isMock });
  const nativeProjection = Boolean(
    presentation?.marker && !full && !isMock && !isStaticWebsiteOnly(),
  );
  const requestProjection = nativeProjection;
  const projectionMarker = presentation?.marker;
  const readBudget = presentation
    ? nativeProjection
      ? presentation.previewMaxBytes
      : presentation.sourceMaxBytes
    : previewMaxBytes;
  const projectionURL = nativeProjection
    ? `${url}?preview=${encodeURIComponent(`${presentation!.namespace}/${presentation!.id}`)}`
    : url;
  const pending = fetch(requestProjection ? projectionURL : url, {
    cache: "no-store",
    signal,
    headers: full ? undefined : { Range: `bytes=0-${readBudget - 1}` },
  });
  const response = signal
    ? await awaitAbortable(pending, signal, (late) => {
        void late.body?.cancel().catch(() => undefined);
      })
    : await pending;
  const loadSourcePreview = () =>
    loadArtifactContent({
      filepath,
      threadId,
      isMock,
      full,
      previewMaxBytes,
      signal,
      ...(presentation
        ? { presentation: { ...presentation, marker: undefined } }
        : {}),
    });
  if (requestProjection && [413, 415, 422, 501].includes(response.status)) {
    await response.body?.cancel();
    return loadSourcePreview();
  }
  if (
    requestProjection &&
    response.ok &&
    response.headers.get("X-Artifact-Projection") !== projectionMarker
  ) {
    // An old Gateway or proxy/CORS policy may omit the marker. Its body
    // cannot safely be treated as canonical source merely because it is JSON.
    await response.body?.cancel();
    return loadSourcePreview();
  }
  const contentRange = parseContentRange(response.headers.get("Content-Range"));
  if (response.status === 416 && contentRange?.total === 0) {
    return {
      content: "",
      url,
      truncated: false,
      previewBytes: 0,
      totalBytes: 0,
      projected: false,
      sha256:
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", // SHA-256 of empty content (keeps empty artifacts editable on non-secure origins)
    };
  }
  if (!response.ok) {
    throw new Error(`Failed to load artifact: ${response.status}`);
  }

  try {
    const read = full
      ? {
          bytes: new Uint8Array(await response.arrayBuffer()),
          truncated: false,
        }
      : await readBoundedArtifactBytes(response, readBudget, signal);
    signal?.throwIfAborted();
    const bytes = read.bytes;
    if ((isArtifactViewPath(filepath) || presentation) && !read.truncated) {
      if (
        response.status === 206 &&
        (contentRange?.start !== 0 ||
          contentRange.end !== bytes.byteLength - 1 ||
          !Number.isSafeInteger(contentRange.total) ||
          contentRange.total < bytes.byteLength ||
          (full && contentRange.total !== bytes.byteLength))
      ) {
        throw new Error("Incomplete artifact view response.");
      }
      const encoding = response.headers.get("Content-Encoding");
      const declared = response.headers.get("Content-Length");
      if (
        (!encoding || encoding.toLowerCase() === "identity") &&
        declared !== null &&
        /^\d+$/.test(declared) &&
        Number(declared) !== bytes.byteLength
      ) {
        throw new Error("Incomplete artifact view response.");
      }
    }
    const truncated =
      !full &&
      (read.truncated ||
        (response.status === 206 &&
          (contentRange?.end === undefined ||
            contentRange.total > contentRange.end + 1)));
    // Streaming decode intentionally holds an incomplete trailing UTF-8 code
    // point instead of fabricating U+FFFD at the range boundary.
    let content: string;
    try {
      content = new TextDecoder("utf-8", {
        fatal: isArtifactViewPath(filepath) || presentation !== undefined,
      }).decode(bytes, { stream: truncated });
    } catch (error) {
      if (nativeProjection) return loadSourcePreview();
      throw error;
    }
    const etag = response.headers.get("etag");
    const sourceRevision = etag
      ?.match(/^(?:W\/)?"([0-9a-fA-F]{64})"$/)?.[1]
      ?.toLowerCase();
    const projected =
      requestProjection &&
      response.headers.get("X-Artifact-Projection") === projectionMarker;
    // Never display or edit a partial projection as canonical source.
    if (
      projected &&
      (read.truncated ||
        bytes.byteLength >
          (nativeProjection ? readBudget : ARTIFACT_PREVIEW_MAX_BYTES) ||
        !sourceRevision)
    ) {
      return loadSourcePreview();
    }
    const sha256 =
      sourceRevision ?? (!truncated ? await sha256OfText(content) : undefined);
    signal?.throwIfAborted();
    const contentLengthHeader = response.headers.get(
      projected ? "X-Artifact-Source-Bytes" : "Content-Length",
    );
    const contentLength =
      contentLengthHeader === null ? undefined : Number(contentLengthHeader);
    if (
      nativeProjection &&
      projected &&
      (contentLengthHeader === null ||
        !/^\d+$/.test(contentLengthHeader) ||
        !Number.isSafeInteger(contentLength) ||
        contentLength! > presentation!.sourceMaxBytes)
    ) {
      return loadSourcePreview();
    }
    return {
      content,
      url,
      sha256,
      projected,
      truncated,
      previewBytes: bytes.byteLength,
      totalBytes:
        (projected ? undefined : contentRange?.total) ??
        (contentLength !== undefined && Number.isFinite(contentLength)
          ? contentLength
          : undefined),
    };
  } catch (error) {
    signal?.throwIfAborted();
    if (nativeProjection) return loadSourcePreview();
    throw error;
  }
}

export function loadArtifactContentFromToolCall({
  url: urlString,
  thread,
}: {
  url: string;
  thread: BaseStream<AgentThreadState>;
}) {
  const draftContent = buildWriteFileDraftContent({
    filepath: urlString,
    messages: thread.messages,
  });
  if (draftContent !== undefined) {
    return draftContent;
  }

  const url = new URL(urlString);
  const toolCallId = url.searchParams.get("tool_call_id");
  const messageId = url.searchParams.get("message_id");
  if (messageId && toolCallId) {
    const message = thread.messages.find((message) => message.id === messageId);
    if (message?.type === "ai" && message.tool_calls) {
      const toolCall = message.tool_calls.find(
        (toolCall) => toolCall.id === toolCallId,
      );
      if (toolCall) {
        return toolCall.args.content;
      }
    }
  }
}

/** Structured storage refusals are transport facts; copy uses the active locale. */
import {
  STORAGE_REASONS,
  type StorageFeature,
  type StorageReason,
} from "@/core/features/api";
import type { Translations } from "@/core/i18n/locales/types";

export interface StorageRefusal {
  status: number;
  code: "files-full" | "storage-busy" | "storage-unavailable";
  reason: string | null;
  detailCode: string | null;
  message: string;
}
export type FileArea = "files" | "shared" | "upload" | "chat";
export interface StorageCopyContext {
  area: FileArea;
  support: string;
  administrator?: boolean;
  filename?: string;
}

/** Personal assignment never closes the company area. */
export function fileAreaNotice(
  storage: StorageFeature,
  error: unknown,
  area: FileArea,
): StorageRefusal | null {
  if (storage.state === "unavailable")
    return {
      status: 503,
      code: "storage-unavailable",
      reason: storage.reason,
      detailCode: null,
      message: "",
    };
  const refusal = readStorageRefusal(error);
  if (refusal) return refusal;
  if (area !== "shared" && storage.state !== "off" && storage.own !== "ready") {
    return {
      status: 503,
      code: "storage-unavailable",
      reason:
        storage.own === "unverified"
          ? "space-unverified"
          : "space-not-assigned",
      detailCode: storage.own === "no-seat" ? "no-seat" : null,
      message: "",
    };
  }
  return null;
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Accept HTTP envelopes, SSE data and the SDK's bounded HTTP error message. */
export function readStorageRefusal(error: unknown): StorageRefusal | null {
  if (!record(error)) return null;
  if (record(error.refusal)) return readStorageRefusal(error.refusal);
  if (record(error.error)) return readStorageRefusal(error.error);
  let status = typeof error.status === "number" ? error.status : null;
  let data: Record<string, unknown> = error;
  if (record(error.detail)) data = error.detail;
  else if (
    typeof error.message === "string" &&
    error.message.length <= 16_384
  ) {
    const match = /^HTTP (\d{3}):\s*(\{[\s\S]*\})$/.exec(error.message);
    if (match) {
      try {
        const parsed: unknown = JSON.parse(match[2]!);
        if (record(parsed)) {
          status ??= Number(match[1]);
          data = record(parsed.detail) ? parsed.detail : parsed;
        }
      } catch {
        return null;
      }
    }
  }
  const code = data.code;
  if (
    !(
      (status === 507 && code === "files-full") ||
      (status === 409 && code === "storage-busy") ||
      (status === 503 && code === "storage-unavailable")
    )
  )
    return null;
  return {
    status,
    code,
    reason: typeof data.reason === "string" ? data.reason : null,
    detailCode:
      typeof (data.detailCode ?? data.detail_code) === "string"
        ? ((data.detailCode ?? data.detail_code) as string)
        : null,
    message: typeof data.message === "string" ? data.message : "",
  };
}

export const STORAGE_REFERENCES: Record<StorageReason, string> = {
  "disk-too-small": "disk-too-small",
  "storage-pool-missing": "storage-missing",
  "inventory-unreadable": "storage-unreadable",
  "host-unqualified": "server-not-ready",
  "admission-failed": "server-check-failed",
};

export function storageReasonSentence(
  reason: StorageReason | null,
  t: Translations,
  context: StorageCopyContext,
): string {
  if (!context.administrator || reason === null) return t.chat.filesUnavailable;
  const unavailable = t.agentInstances.unavailable;
  const sentences: Record<StorageReason, (provider: string) => string> = {
    "disk-too-small": unavailable.diskTooSmall,
    "storage-pool-missing": unavailable.storageMissing,
    "inventory-unreadable": unavailable.storageUnreadable,
    "host-unqualified": unavailable.serverNotReadyFiles,
    "admission-failed": unavailable.serverCheckFailedFiles,
  };
  return sentences[reason](context.support);
}

export function storageRefusalSentence(
  refusal: StorageRefusal | null,
  t: Translations,
  context: StorageCopyContext,
): string | null {
  if (!refusal) return null;
  if (refusal.code === "files-full") {
    if (context.area === "shared") return t.shared.full(context.support);
    if (context.area === "upload")
      return t.uploads.filesFull(context.filename ?? "", context.support);
    if (context.area === "chat") return t.chat.filesFull(context.support);
    return t.files.saveFull(context.support);
  }
  if (refusal.code === "storage-busy")
    return context.area === "shared" ? t.shared.busy : t.files.busy;
  if (refusal.reason === "space-not-assigned")
    return refusal.detailCode === "no-seat"
      ? t.chat.filesNoSeat(context.administrator === true, context.support)
      : t.chat.filesPreparing(context.support);
  if (refusal.reason === "space-unverified")
    return context.area === "shared"
      ? t.shared.unchecked
      : t.chat.myFilesUnchecked;
  if (STORAGE_REASONS.includes(refusal.reason as StorageReason))
    return storageReasonSentence(refusal.reason as StorageReason, t, context);
  return refusal.message || null;
}

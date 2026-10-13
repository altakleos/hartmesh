"use client";

import {
  ExecutionNotice,
  ProviderSupportLink,
} from "@/components/workspace/instances/execution-notice";
import { STORAGE_REASONS, type StorageReason } from "@/core/features/api";
import { useStorageCopy } from "@/core/file-areas/storage-copy";
import {
  storageRefusalSentence,
  type FileArea,
  type StorageRefusal,
} from "@/core/file-areas/storage-state";

/** Shared presentation for Files and ordinary chat; no storage geometry. */
export function FilesStateNotice({
  refusal,
  area = "files",
  filename,
}: {
  refusal: StorageRefusal;
  area?: FileArea;
  filename?: string;
}) {
  const copy = useStorageCopy(area);
  const sentence = storageRefusalSentence(refusal, copy.t, {
    area,
    support: copy.support,
    administrator: copy.administrator,
    filename,
  });
  if (
    refusal.code === "storage-unavailable" &&
    (refusal.reason === null ||
      refusal.reason === "space-unverified" ||
      STORAGE_REASONS.includes(refusal.reason as StorageReason))
  ) {
    return (
      <ExecutionNotice
        reason={refusal.reason as StorageReason | "space-unverified" | null}
        message={sentence ?? undefined}
        administrator={copy.administrator}
        provider={copy.providerName}
        supportURL={copy.supportURL}
      />
    );
  }
  return (
    <div className="space-y-2 text-sm" role="alert">
      <p>{sentence ?? copy.t.chat.filesUnavailable}</p>
      {copy.administrator && refusal.code !== "storage-busy" && (
        <ProviderSupportLink
          provider={copy.providerName}
          supportURL={copy.supportURL}
        />
      )}
    </div>
  );
}

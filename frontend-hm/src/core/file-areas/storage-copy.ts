"use client";

import { createElement } from "react";
import { toast } from "sonner";

import { FileRefusalHelp } from "@/components/workspace/instances/execution-notice";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding } from "@/core/features";
import { STORAGE_REASONS, type StorageReason } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";

import { messageForFileAreaError } from "./http";
import {
  readStorageRefusal,
  storageRefusalSentence,
  type FileArea,
} from "./storage-state";

/** Role and locale are presentation inputs; the server remains the authority. */
export function useStorageCopy(area: FileArea) {
  const { user } = useAuth();
  const { t } = useI18n();
  const { providerName, supportURL } = useBranding();
  const administrator = user?.system_role === "admin";
  const provider = providerName ?? t.agentInstances.providerFallback;
  const support = administrator
    ? provider
    : t.agentInstances.administratorSupport;
  const message = (error: unknown, fallback: string, filename?: string) =>
    storageRefusalSentence(readStorageRefusal(error), t, {
      area,
      support,
      administrator,
      filename,
    }) ??
    (area === "shared" ? messageForFileAreaError(error, fallback) : fallback);
  return {
    t,
    administrator,
    provider,
    providerName,
    supportURL,
    support,
    message,
    error(error: unknown, fallback: string, summary?: string) {
      const refusal = readStorageRefusal(error);
      const verbose =
        refusal?.code === "storage-unavailable" &&
        (refusal.reason === null ||
          refusal.reason === "space-unverified" ||
          STORAGE_REASONS.includes(refusal.reason as StorageReason));
      const help =
        verbose ||
        (administrator &&
          refusal &&
          refusal.code !== "storage-busy" &&
          supportURL);
      const text = message(error, fallback);
      if (!help && !(summary && refusal)) return toast.error(summary ?? text);
      return toast.error(summary ?? text, {
        description: createElement(
          "div",
          { className: "space-y-2" },
          summary && refusal ? createElement("p", null, text) : null,
          help
            ? createElement(FileRefusalHelp, {
                administrator,
                provider: providerName,
                supportURL,
                reason: verbose
                  ? (refusal.reason as
                      | StorageReason
                      | "space-unverified"
                      | null)
                  : undefined,
              })
            : null,
        ),
      });
    },
  };
}

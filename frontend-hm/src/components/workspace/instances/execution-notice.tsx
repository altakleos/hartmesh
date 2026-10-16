"use client";

import type { Custody } from "@/core/agent-instances/api";
import {
  STORAGE_REASONS,
  validateProviderSupportURL,
  type ExecutionReason,
  type StorageReason,
} from "@/core/features/api";
import {
  STORAGE_REFERENCES,
  storageReasonSentence,
} from "@/core/file-areas/storage-state";
import { useI18n } from "@/core/i18n/hooks";

export function ProviderSupportLink({
  provider,
  supportURL,
}: {
  provider?: string | null;
  supportURL?: string | null;
}) {
  const { t } = useI18n();
  const url = validateProviderSupportURL(supportURL);
  return url ? (
    <a
      className="text-sm underline"
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      referrerPolicy="no-referrer"
    >
      {provider
        ? t.workspace.providerSupport(provider)
        : t.workspace.contactSupport}
    </a>
  ) : null;
}

export function FileRefusalHelp({
  administrator,
  provider,
  supportURL,
  reason,
}: {
  administrator: boolean;
  provider?: string | null;
  supportURL?: string | null;
  reason?: StorageReason | "space-unverified" | null;
}) {
  const { t } = useI18n();
  if (reason === undefined)
    return administrator ? (
      <ProviderSupportLink provider={provider} supportURL={supportURL} />
    ) : null;
  if (!administrator) return <p>{t.agentInstances.unavailable.askAdmin}</p>;
  const reference =
    reason === "space-unverified"
      ? "files-unchecked"
      : reason === null
        ? null
        : STORAGE_REFERENCES[reason];
  return (
    <>
      <p>
        {t.agentInstances.unavailable.next(
          provider ?? t.agentInstances.providerFallback,
        )}
      </p>
      <ProviderSupportLink provider={provider} supportURL={supportURL} />
      {reference && (
        <p className="text-muted-foreground">
          {t.agentInstances.unavailable.reference(reference)}
        </p>
      )}
    </>
  );
}

function isStorageReason(
  reason: StorageReason | ExecutionReason | null,
): reason is StorageReason {
  return STORAGE_REASONS.some((value) => value === reason);
}

/** File and agent notices share support wording without exposing internal codes. */
export function ExecutionNotice({
  reason,
  message,
  administrator,
  provider,
  supportURL,
  area = "files",
  name: agentName,
  kind,
  storageReason,
  busy = false,
}: {
  reason: StorageReason | ExecutionReason | null;
  message?: string;
  administrator: boolean;
  provider?: string | null;
  supportURL?: string | null;
  area?: "files" | "agent";
  name?: string;
  kind?: Custody;
  storageReason?: StorageReason | null;
  busy?: boolean;
}) {
  const { t } = useI18n();
  const name = provider ?? t.agentInstances.providerFallback;
  const underlying =
    reason === "storage-unavailable" ? (storageReason ?? null) : reason;
  const unavailable = t.agentInstances.unavailable;
  let sentence: string;
  let reference: string | null = null;
  let agentsWait = false;
  let plain = false;
  if (area === "agent") {
    if (busy && agentName && reason === null) {
      sentence = t.agentInstances.filesBusy(agentName);
      plain = true;
    } else if (reason === "operator-hold") {
      sentence = unavailable.agentsPaused(name);
      plain = true;
    } else if (reason === "agents-none") {
      sentence =
        kind === "personal"
          ? t.agentInstances.assistants.notOffered
          : kind === "company"
            ? t.agentInstances.employees.notOffered
            : t.sidebar.agentsUnavailable;
      plain = true;
    } else if (reason === "host-replaced" && agentName) {
      sentence = unavailable.serverMoved(agentName, name);
      reference = "server-moved";
    } else if (reason === "space-unverified" && agentName) {
      sentence = unavailable.filesUnchecked(agentName);
      reference = "files-unchecked";
    } else if (isStorageReason(underlying)) {
      sentence =
        underlying === "host-unqualified"
          ? unavailable.serverNotReady
          : underlying === "admission-failed"
            ? unavailable.serverCheckFailed
            : storageReasonSentence(underlying, t, {
                area: "files",
                support: name,
                administrator,
              });
      reference = STORAGE_REFERENCES[underlying];
      agentsWait =
        underlying === "disk-too-small" ||
        underlying === "storage-pool-missing" ||
        underlying === "inventory-unreadable";
    } else {
      sentence = t.sidebar.agentsUnavailable;
      plain = true;
    }
  } else {
    const filesReason = isStorageReason(underlying) ? underlying : null;
    sentence =
      message ??
      (reason === "space-unverified"
        ? t.chat.myFilesUnchecked
        : storageReasonSentence(filesReason, t, {
            area: "files",
            support: name,
            administrator,
          }));
    reference =
      reason === "space-unverified"
        ? "files-unchecked"
        : filesReason
          ? STORAGE_REFERENCES[filesReason]
          : null;
  }
  return (
    <div className="space-y-2 text-sm" role="alert">
      <p>{sentence}</p>
      {agentsWait && <p>{unavailable.agentsWait}</p>}
      {!plain &&
        (administrator ? (
          <>
            <p>{t.agentInstances.unavailable.next(name)}</p>
            <ProviderSupportLink provider={provider} supportURL={supportURL} />
            {reference && (
              <p className="text-muted-foreground">
                {t.agentInstances.unavailable.reference(reference)}
              </p>
            )}
          </>
        ) : (
          <p>{t.agentInstances.unavailable.askAdmin}</p>
        ))}
    </div>
  );
}

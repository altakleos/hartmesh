"use client";

import {
  validateProviderSupportURL,
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

/** Stage-two file notices; agent execution reasons extend this in stage four. */
export function ExecutionNotice({
  reason,
  message,
  administrator,
  provider,
  supportURL,
}: {
  reason: StorageReason | "space-unverified" | null;
  message?: string;
  administrator: boolean;
  provider?: string | null;
  supportURL?: string | null;
}) {
  const { t } = useI18n();
  const name = provider ?? t.agentInstances.providerFallback;
  const sentence =
    message ??
    (reason === "space-unverified"
      ? t.chat.myFilesUnchecked
      : storageReasonSentence(reason, t, {
          area: "files",
          support: name,
          administrator,
        }));
  const reference =
    reason === "space-unverified"
      ? "files-unchecked"
      : reason === null
        ? null
        : STORAGE_REFERENCES[reason];
  return (
    <div className="space-y-2 text-sm" role="alert">
      <p>{sentence}</p>
      {administrator ? (
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
      )}
    </div>
  );
}

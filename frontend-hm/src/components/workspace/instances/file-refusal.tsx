"use client";

import { useSupervisorName } from "@/components/workspace/person-name";
import type { AgentInstance } from "@/core/agent-instances/api";
import { readRefusal } from "@/core/agent-instances/errors";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useStorage } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";
import { formatUploadSize } from "@/core/uploads/file-validation";

import { ExecutionNotice, ProviderSupportLink } from "./execution-notice";

export function AgentFileRefusal({
  instance,
  error,
  file,
  action = false,
  manager = true,
  place = "files",
}: {
  instance: Pick<AgentInstance, "name" | "custody"> & {
    supervisor?: AgentInstance["supervisor"];
  };
  error: unknown;
  file?: string;
  action?: boolean;
  manager?: boolean;
  place?: "files" | "conversation";
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  const supervisor = useSupervisorName(instance.supervisor?.subject_id ?? "");
  const copy =
    place === "conversation"
      ? { ...t.agentInstances.files, ...t.inputBox.agentFile }
      : t.agentInstances.files;
  const refusal = readRefusal(error);
  const administrator = user?.system_role === "admin";
  const support = administrator
    ? (branding.providerName ?? t.agentInstances.providerFallback)
    : t.agentInstances.administratorSupport;
  if (refusal?.reason === "execution-unavailable")
    return (
      <ExecutionNotice
        area="agent"
        kind={instance.custody}
        name={instance.name}
        reason={
          EXECUTION_REASONS.find(
            (reason) => reason === refusal.executionReason,
          ) ?? null
        }
        administrator={administrator}
        provider={branding.providerName}
        supportURL={branding.supportURL}
        storageReason={storage?.reason}
      />
    );
  let message: string;
  switch (refusal?.reason) {
    case "file-too-large": {
      const bytes = refusal.extra.max_bytes;
      message = copy.tooLarge(
        file ?? "",
        formatUploadSize(
          typeof bytes === "number" && bytes > 0 && Number.isFinite(bytes)
            ? bytes
            : 64 * 1024 * 1024,
        ),
      );
      break;
    }
    case "files-full":
      message = manager
        ? copy.full(instance.name, support)
        : copy.fullAsk(instance.name, supervisor, support);
      break;
    case "storage-busy":
      message = t.agentInstances.filesBusy(instance.name);
      break;
    case "file-write-failed":
      message = action ? copy.actionFailed : copy.refused(file ?? "");
      break;
    case "invalid-name":
      message = copy.invalidName;
      break;
    case "reserved-path":
      message = copy.reserved(instance.name);
      break;
    case "name-collision":
      message = copy.nameTaken;
      break;
    case "file-changed":
      message = copy.fileChanged;
      break;
    case "directory-not-empty":
      message = copy.folderNotEmpty;
      break;
    case "agent-not-active":
      message = copy.notActive(instance.name);
      break;
    case "not-found":
      message = copy.notFound;
      break;
    default:
      message = refusal
        ? t.agentInstances.operationError
        : error &&
            typeof error === "object" &&
            "status" in error &&
            (error.status === 403 || error.status === 404)
          ? copy.noAccess(instance.name)
          : action
            ? copy.actionFailed
            : copy.refused(file ?? "");
  }
  return (
    <div role="alert">
      <p>{message}</p>
      {administrator && refusal?.reason === "files-full" && (
        <ProviderSupportLink
          provider={branding.providerName}
          supportURL={branding.supportURL}
        />
      )}
    </div>
  );
}

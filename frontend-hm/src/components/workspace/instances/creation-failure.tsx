"use client";

import type { Custody, InstanceApiError } from "@/core/agent-instances/api";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useStorage } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";

import { ExecutionNotice, ProviderSupportLink } from "./execution-notice";

export interface CreationFailure {
  type: "unconfirmed" | "brief" | "refused";
  error?: InstanceApiError;
}

/** Setup dialogs and browser-remembered resumptions use the same refusal copy. */
export function CreationFailureNotice({
  failure,
  kind,
  name,
  ownName = "",
  unfinishedName,
  providerName,
  supportURL,
}: {
  failure: CreationFailure;
  kind: Custody;
  name: string;
  ownName?: string;
  unfinishedName?: string;
  providerName?: string | null;
  supportURL?: string | null;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const feature = useAgentInstances();
  const storage = useStorage();
  const copy = t.agentInstances;
  const error = failure.error;
  const administrator = user?.system_role === "admin";
  const provider = providerName ?? copy.providerFallback;
  const count = (key: string, fallback: number) =>
    typeof error?.extra[key] === "number" &&
    Number.isSafeInteger(error.extra[key]) &&
    error.extra[key] >= 0
      ? error.extra[key]
      : fallback;
  if (error?.reason === "execution-unavailable") {
    const reason =
      EXECUTION_REASONS.find((value) => value === error.executionReason) ??
      null;
    return (
      <ExecutionNotice
        area="agent"
        kind={kind}
        name={name}
        reason={reason}
        administrator={administrator}
        provider={providerName}
        supportURL={supportURL}
        storageReason={storage?.reason}
      />
    );
  }
  const perUser = count("per_user", feature.assistants?.perUser ?? 0);
  const max = count("allowed", feature.employees?.allowed ?? 0);
  const message =
    failure.type === "unconfirmed"
      ? copy.create.unconfirmed(name)
      : failure.type === "brief"
        ? copy.create.briefFailed
        : error?.reason === "ai-assistant-limit"
          ? perUser === 0
            ? copy.assistants.notOffered
            : unfinishedName && !ownName
              ? copy.assistants.limitUnfinished(unfinishedName)
              : copy.assistants.limitReached(
                  count("mine", feature.assistants?.mine ?? 0),
                  perUser,
                  ownName,
                )
          : error?.reason === "ai-assistant-none-free"
            ? copy.assistants.noneFree(administrator, provider)
            : error?.reason === "ai-employee-limit"
              ? max === 0
                ? copy.employees.notOffered
                : copy.employees.limitReached(max, provider)
              : kind === "company" && error?.status === 403
                ? copy.create.companyRefused
                : copy.create.failed(name);
  return (
    <div role="alert">
      <p>{message}</p>
      {administrator &&
        (error?.reason === "ai-assistant-none-free" ||
          error?.reason === "ai-employee-limit") && (
          <ProviderSupportLink
            provider={providerName}
            supportURL={supportURL}
          />
        )}
    </div>
  );
}

"use client";

import { useSupervisorName } from "@/components/workspace/person-name";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import { refusalSentence, type Refusal } from "@/core/agent-instances/errors";
import { INSPECT, MANAGE } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useStorage } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";
import type { AgentThread } from "@/core/threads/types";

import {
  ExecutionNotice,
  ProviderSupportLink,
} from "../instances/execution-notice";

/** Only protocol fields select product copy; diagnostic prose is never rendered. */
export function InstanceRunNotice({
  binding,
  detail,
  refusal,
}: {
  binding: NonNullable<AgentThread["agent_instance"]>;
  detail: AgentInstanceDetail | null;
  refusal: Refusal;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  const supervisor = useSupervisorName(detail?.supervisor.subject_id ?? "");
  const administrator = user?.system_role === "admin";
  const provider = branding.providerName ?? t.agentInstances.providerFallback;
  const support = administrator
    ? provider
    : t.agentInstances.administratorSupport;
  const key = refusalSentence(refusal, "conversation");
  const name = binding.name;
  if (key === "sandboxNetwork.managersOnly") return null;
  if (refusal.reason === "execution-unavailable")
    return (
      <ExecutionNotice
        area="agent"
        name={name}
        kind={binding.custody}
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
  let sentence: string;
  let help = false;
  switch (key) {
    case "agentInstances.busy":
      sentence = t.agentInstances.busy(name);
      break;
    case "agentInstances.conversationLimit":
      sentence = t.agentInstances.conversationLimit(name);
      break;
    case "agentInstances.restarting":
      sentence = t.agentInstances.restarting(name);
      break;
    case "agentInstances.startingElsewhere":
      sentence = t.agentInstances.startingElsewhere(name);
      break;
    case "runProgress.residentStartFailed":
      sentence = t.runProgress.residentStartFailed(name, support);
      help = true;
      break;
    case "agentInstances.fullStart": {
      const manager =
        !!detail &&
        (detail.permissions & (MANAGE | INSPECT)) === (MANAGE | INSPECT);
      sentence = manager
        ? t.agentInstances.fullStart(name, support)
        : t.agentInstances.fullStartAsk(name, supervisor, support);
      help = true;
      break;
    }
    case "agentInstances.filesBusy":
      sentence = t.agentInstances.filesBusy(name);
      break;
    case "agentInstances.interruptedByManager":
      sentence = t.agentInstances.interruptedByManager(name);
      break;
    case "agentInstances.interruptedByRecovery":
      sentence = t.agentInstances.interruptedByRecovery(provider, name);
      break;
    case "agentInstances.interrupted":
      sentence = t.agentInstances.interrupted(name);
      break;
    default:
      sentence = t.agentInstances.operationError;
  }
  return (
    <div role="alert" className="space-y-2 text-sm">
      <p>{sentence}</p>
      {help && administrator && (
        <ProviderSupportLink
          provider={branding.providerName}
          supportURL={branding.supportURL}
        />
      )}
    </div>
  );
}

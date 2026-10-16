import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  usePersonName,
  useSupervisorName,
} from "@/components/workspace/person-name";
import type { WorkCapacity, WorkRecord } from "@/core/agent-instances/api";
import type { Refusal } from "@/core/agent-instances/errors";
import type { AgentBlock } from "@/core/agent-instances/presentation";
import { workActivationBlock } from "@/core/agent-instances/work-presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useStorage } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";

import { ExecutionNotice, ProviderSupportLink } from "./execution-notice";
import type { WorkInstance } from "./work-attempt-box";

export interface WorkActivationProps {
  instance: WorkInstance;
  record: WorkRecord;
  allowed: boolean;
  policyEnabled: boolean;
  pending: boolean;
  unavailable?: AgentBlock | null;
  capacity?: WorkCapacity;
  disabled: boolean;
  canCreateConversation: boolean;
  activate: (newConversation: boolean) => void;
}
function CapacityLine({ entry }: { entry: WorkCapacity["running"][number] }) {
  const { t } = useI18n();
  const person = usePersonName(entry.requester_id ?? "", true);
  const at = entry.started_at ? new Date(entry.started_at) : null;
  return (
    <li>
      {entry.work_id
        ? t.agentWork.workLimit.which(
            entry.instance_name,
            entry.objective,
            at && Number.isFinite(at.getTime()) ? at.toLocaleString() : "…",
            person ?? "…",
          )
        : t.agentWork.workLimit.starting(entry.instance_name)}
    </li>
  );
}
export function WorkActivationRefusal({
  instance,
  refusal,
  action,
  capacity,
}: {
  instance: WorkInstance;
  refusal: Refusal;
  action: string;
  capacity?: WorkCapacity;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  const supervisor = useSupervisorName(instance.supervisor.subject_id);
  const copy = t.agentWork;
  const support =
    user?.system_role === "admin"
      ? (branding.providerName ?? t.agentInstances.providerFallback)
      : t.agentInstances.administratorSupport;
  const name = instance.name;
  let sentence: React.ReactNode = t.agentInstances.operationError;
  let help = false;
  switch (refusal.reason) {
    case "busy":
      sentence = copy.attempt.busy(name, action);
      break;
    case "run-limit":
      sentence = copy.attempt.runLimit(name, action);
      break;
    case "restarting":
      sentence = copy.attempt.restarting(name, action);
      break;
    case "files-full":
      sentence =
        (instance.permissions & 6) === 6
          ? copy.attempt.full(name, support)
          : copy.attempt.fullAsk(name, supervisor, support);
      help = true;
      break;
    case "storage-busy":
      sentence = t.agentInstances.filesBusy(name);
      break;
    case "execution-unavailable":
      sentence = (
        <ExecutionNotice
          area="agent"
          name={name}
          kind={instance.custody}
          reason={
            EXECUTION_REASONS.find(
              (value) => value === refusal.executionReason,
            ) ?? null
          }
          administrator={user?.system_role === "admin"}
          provider={branding.providerName}
          supportURL={branding.supportURL}
          storageReason={storage?.reason}
        />
      );
      break;
    case "work-limit": {
      const max =
        capacity?.max ??
        (typeof refusal.extra.max === "number" ? refusal.extra.max : 1);
      sentence = (
        <>
          <p>{max === 1 ? copy.workLimit.one : copy.workLimit.many(max)}</p>
          {capacity && (
            <ul>
              {capacity.running.map((entry, index) => (
                <CapacityLine
                  key={`${entry.instance_id}:${index}`}
                  entry={entry}
                />
              ))}
            </ul>
          )}
        </>
      );
    }
  }
  return (
    <div role="alert">
      {sentence}
      {help && user?.system_role === "admin" && (
        <ProviderSupportLink
          provider={branding.providerName}
          supportURL={branding.supportURL}
        />
      )}
    </div>
  );
}
export function WorkActivation({
  instance,
  record,
  allowed,
  policyEnabled,
  pending,
  unavailable,
  capacity,
  disabled,
  canCreateConversation,
  activate,
}: WorkActivationProps) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  const copy = t.agentWork;
  const [newConversation, setNewConversation] = useState(false);
  const [free, setFree] = useState(false);
  const full = !!capacity && capacity.in_use >= capacity.max;
  const previousFull = useRef(full);
  useEffect(() => {
    if (previousFull.current && !full) setFree(true);
    if (full) setFree(false);
    previousFull.current = full;
  }, [full]);
  const description = useId();
  const blocked = workActivationBlock({
    pending,
    status: instance.status,
    policy: policyEnabled,
    execution: record.execution_available && !unavailable,
    busy: instance.home?.state === "busy",
    reconcile: record.needs_mandate_reconciliation,
    capacity: capacity ?? null,
  });
  const active =
    !!record.attempt &&
    ["starting", "running", "stopping", "uncertain"].includes(
      record.attempt.status,
    );
  if (!allowed || active || !["open", "blocked"].includes(record.status))
    return null;
  const name = instance.name;
  let sentence: React.ReactNode = null;
  switch (blocked) {
    case "pending":
      sentence = t.agentInstances.pending(name);
      break;
    case "paused":
      sentence = t.agentInstances.pausedNotice(name);
      break;
    case "retired":
      sentence = t.agentInstances.retiredNotice(name);
      break;
    case "policy":
      sentence = copy.policyDisabled(name);
      break;
    case "execution":
      sentence =
        unavailable?.busy || instance.home?.state === "busy" ? (
          t.agentInstances.filesBusy(name)
        ) : (
          <ExecutionNotice
            area="agent"
            name={name}
            kind={instance.custody}
            reason={
              unavailable?.reason ??
              record.execution_reason ??
              instance.execution?.reason ??
              null
            }
            administrator={user?.system_role === "admin"}
            provider={branding.providerName}
            supportURL={branding.supportURL}
            storageReason={storage?.reason}
          />
        );
      break;
    case "reconcile":
      sentence = copy.reconcileNeeded(name);
      break;
    case "capacity":
      sentence =
        capacity!.max === 1
          ? copy.workLimit.one
          : copy.workLimit.many(capacity!.max);
      break;
  }
  return (
    <div className="space-y-2">
      {record.attempt && canCreateConversation && (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={newConversation}
            disabled={disabled}
            onChange={(event) => setNewConversation(event.target.checked)}
          />
          {copy.newConversation}
        </label>
      )}
      <Button
        disabled={disabled || !!blocked}
        aria-describedby={sentence ? description : undefined}
        onClick={() => {
          if (disabled || blocked) return;
          setFree(false);
          activate(newConversation);
        }}
      >
        {record.attempt ? copy.resume : copy.activate}
      </Button>
      {sentence && <div id={description}>{sentence}</div>}
      {blocked === "capacity" && capacity && (
        <ul>
          {capacity.running.map((entry, index) => (
            <CapacityLine
              key={`${entry.instance_id}:${entry.work_id ?? index}`}
              entry={entry}
            />
          ))}
        </ul>
      )}
      {!blocked && free && <p role="status">{copy.workLimit.free(name)}</p>}
    </div>
  );
}

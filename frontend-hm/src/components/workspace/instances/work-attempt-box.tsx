"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  usePersonName,
  useSupervisorName,
} from "@/components/workspace/person-name";
import {
  workHistory,
  type AgentInstance,
  type AgentInstanceDetail,
  type WorkRecord,
  type WorkStopCommand,
} from "@/core/agent-instances/api";
import {
  canManageInstance,
  hasCeiling,
} from "@/core/agent-instances/presentation";
import { stopWorkAttempt } from "@/core/agent-instances/stop-work";
import {
  workAttemptLabel,
  workStopCause,
} from "@/core/agent-instances/work-presentation";
import { readWorkRun } from "@/core/agent-instances/work-run";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import { pathOfThread } from "@/core/threads/utils";
import { useWorkspaceStartingLong } from "@/core/turn-progress/starting-long";

import { ProviderSupportLink } from "./execution-notice";

export type WorkInstance = AgentInstance &
  Partial<Pick<AgentInstanceDetail, "activity" | "home" | "execution">>;
export interface WorkAttemptBoxProps {
  instance: WorkInstance;
  record: WorkRecord;
  manager: boolean;
  collaborate: boolean;
  canStop: boolean;
  canResume: boolean;
  disabled: boolean;
  queryKey: unknown[];
  changed: () => void;
  activation?: React.ReactNode;
}
export function WorkAttemptBox(props: WorkAttemptBoxProps) {
  const { user } = useAuth();
  return (
    <AttemptBox
      key={JSON.stringify([
        user?.id,
        user?.system_role,
        user?.permissions,
        props.instance.id,
        props.instance.permissions,
        props.record.id,
        props.record.attempt?.id,
        props.canStop,
        props.manager,
        props.collaborate,
      ])}
      {...props}
    />
  );
}

export function WorkContainedNotice({
  reason,
  instance,
}: {
  reason: string;
  instance: Pick<AgentInstance, "name">;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const copy = t.agentWork.contained;
  const name = instance.name;
  const provider = branding.providerName ?? t.agentInstances.providerFallback;
  const support =
    user?.system_role === "admin"
      ? provider
      : t.agentInstances.administratorSupport;
  let sentence: string;
  switch (reason) {
    case "container-absent":
      sentence = copy.containerAbsent(name);
      break;
    case "run-orphaned":
      sentence = copy.runOrphaned(name);
      break;
    case "run-never-started":
      sentence = copy.runNeverStarted(name);
      break;
    case "start-failed":
      sentence = copy.startFailed(name, support);
      break;
    case "outcome-uncertain":
      sentence = copy.outcomeUncertain(name);
      break;
    case "idle":
      sentence = copy.idle(name);
      break;
    case "lifecycle":
      sentence = copy.lifecycle(name);
      break;
    case "storage-recovery":
      sentence = copy.storageRecovery(provider, name);
      break;
    case "operator":
      sentence = copy.operator(name, provider);
      break;
    case "host-replaced":
      sentence = copy.hostReplaced(name);
      break;
    default:
      sentence = copy.unknown(name);
  }
  return (
    <div>
      <p>{sentence}</p>
      {[
        "container-absent",
        "run-orphaned",
        "outcome-uncertain",
        "idle",
        "lifecycle",
        "operator",
      ].includes(reason) && <p>{copy.kept(name)}</p>}
      {reason === "start-failed" && user?.system_role === "admin" && (
        <ProviderSupportLink
          provider={branding.providerName}
          supportURL={branding.supportURL}
        />
      )}
    </div>
  );
}

function AttemptBox({
  instance,
  record,
  manager,
  collaborate,
  canStop,
  canResume,
  disabled,
  queryKey,
  changed,
  activation,
}: WorkAttemptBoxProps) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentWork;
  const branding = useBranding();
  const supervisor = useSupervisorName(instance.supervisor.subject_id);
  const support =
    user?.system_role === "admin"
      ? (branding.providerName ?? t.agentInstances.providerFallback)
      : t.agentInstances.administratorSupport;
  const capture = useSpaceActionSignal();
  const busyRef = useRef(false);
  const [busy, setBusy] = useState(false);
  const [confirmation, setConfirmation] = useState<WorkRecord | null>(null);
  const [retry, setRetry] = useState<{
    record: WorkRecord;
    command: WorkStopCommand;
  } | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [accepted, setAccepted] = useState(false);
  const attempt = record.attempt;
  const ended = useQuery({
    queryKey: [
      ...queryKey,
      "attempt-ending",
      record.id,
      attempt?.id,
      attempt?.thread_id,
      attempt?.run_id,
    ],
    queryFn: ({ signal }) =>
      readWorkRun(attempt!.thread_id!, attempt!.run_id!, signal),
    enabled:
      attempt?.status === "failed" &&
      !attempt.contained &&
      !!attempt.thread_id &&
      !!attempt.run_id &&
      hasCeiling(user?.permissions, "runs:read"),
    retry: false,
    gcTime: 0,
    staleTime: 0,
    refetchOnWindowFocus: false,
  });
  const history = useQuery({
    queryKey: [...queryKey, "stop-cause", record.id, attempt?.id],
    queryFn: ({ signal }) => workHistory(instance.id, record.id, 0, signal),
    enabled: attempt?.status === "cancelled",
    retry: false,
    gcTime: 0,
    staleTime: 0,
    refetchOnWindowFocus: false,
  });
  const cause = workStopCause(attempt?.id ?? "", history.data?.events ?? []);
  const actor = usePersonName("actor" in cause ? cause.actor : "", true);
  const label = workAttemptLabel(
    record,
    ended.isError ? null : ended.data?.stop_reason,
  );
  const long = useWorkspaceStartingLong(
    label === "starting" && attempt
      ? {
          runId: attempt.id,
          stage: "workspace_starting",
          atMs: Date.parse(record.updated_at),
          receivedAt: Number.isFinite(Date.parse(record.updated_at))
            ? Date.parse(record.updated_at)
            : undefined,
        }
      : null,
  );
  async function stop(value: WorkRecord, command?: WorkStopCommand) {
    if (busyRef.current || disabled || accepted) return;
    busyRef.current = true;
    setBusy(true);
    setNotice(null);
    const signal = capture();
    try {
      const result = await stopWorkAttempt(instance.id, value, {
        signal,
        retry: command,
      });
      if (signal.aborted) return;
      setAccepted(result.kind === "stopping");
      setRetry(
        result.retry ? { record: result.record, command: result.retry } : null,
      );
      setNotice(
        result.kind === "nothing"
          ? copy.stopNothing(instance.name)
          : result.kind === "failed"
            ? copy.stopFailed
            : copy.stopPending(instance.name),
      );
      setConfirmation(null);
      changed();
    } catch {
      if (!signal.aborted) setNotice(copy.stopFailed);
    } finally {
      busyRef.current = false;
      if (!signal.aborted) setBusy(false);
    }
  }
  if (!attempt || !label) return activation ?? null;
  const name = instance.name;
  const stopAllowed =
    canStop &&
    hasCeiling(user?.permissions, "runs:cancel") &&
    ((manager && canManageInstance(instance.permissions)) ||
      (collaborate &&
        (instance.permissions & 3) === 3 &&
        attempt.requester_id === user?.id));
  const filesFirst =
    attempt.contained &&
    ["outcome-uncertain", "storage-recovery", "host-replaced"].includes(
      attempt.contained.reason,
    );
  const files =
    label === "contained" ? (
      <Link href={`/workspace/instances?instance=${instance.id}&tab=files`}>
        {t.agentInstances.home}
      </Link>
    ) : null;
  let ending: React.ReactNode = null;
  if (!attempt.contained && label === "cancelled") {
    if (cause.kind === "cancel") ending = copy.stoppedByWithdraw(actor ?? "…");
    else if (canResume)
      ending =
        cause.kind === "stop"
          ? copy.stopped(actor ?? "…", new Date(cause.at).toLocaleString())
          : cause.kind === "edit"
            ? copy.stoppedByEdit(actor ?? "…")
            : cause.kind === "service"
              ? copy.stoppedByService(name)
              : copy.stoppedPlain;
  } else if (!attempt.contained && attempt.status === "failed" && canResume) {
    const reason = ended.isError ? null : ended.data?.stop_reason;
    const action = copy.resume;
    switch (reason) {
      case "busy":
        ending = copy.attempt.busy(name, action);
        break;
      case "run-limit":
        ending = copy.attempt.runLimit(name, action);
        break;
      case "restarting":
        ending = copy.attempt.restarting(name, action);
        break;
      case "starting-elsewhere":
        ending = copy.attempt.startingElsewhere(name, action);
        break;
      case "start-failed":
        ending = (
          <>
            <p>{copy.attempt.startFailed(name, action, support)}</p>
            {user?.system_role === "admin" && (
              <ProviderSupportLink
                provider={branding.providerName}
                supportURL={branding.supportURL}
              />
            )}
          </>
        );
        break;
      case "files-full":
        ending = (
          <>
            <p>
              {manager
                ? copy.attempt.full(name, support)
                : copy.attempt.fullAsk(name, supervisor, support)}
            </p>
            {user?.system_role === "admin" && (
              <ProviderSupportLink
                provider={branding.providerName}
                supportURL={branding.supportURL}
              />
            )}
          </>
        );
        break;
      case "storage-busy":
        ending = t.agentInstances.filesBusy(name);
        break;
      default:
        ending = (
          <>
            <p>{copy.attemptFailed(name, support)}</p>
            {user?.system_role === "admin" && (
              <ProviderSupportLink
                provider={branding.providerName}
                supportURL={branding.supportURL}
              />
            )}
          </>
        );
    }
  }
  return (
    <section
      aria-label={copy.execution(name)}
      className="space-y-2 rounded border p-3"
    >
      <h5>
        {copy.execution(name)}: {copy.attemptStatus[label]}
      </h5>
      {attempt.contained && (
        <WorkContainedNotice
          reason={attempt.contained.reason}
          instance={instance}
        />
      )}
      {label === "starting" && (
        <p>
          {long
            ? t.runProgress.residentStartingLong(name)
            : t.runProgress.residentStarting(name)}
        </p>
      )}
      {label === "stopping" && <p>{copy.stopPending(name)}</p>}
      {label === "uncertain" && (
        <p>
          {instance.activity?.running_runs
            ? copy.uncertainWaiting(name)
            : copy.uncertain(name)}
        </p>
      )}
      {ending && <div>{ending}</div>}
      {label === "noResult" &&
        record.status === "open" &&
        !record.human_input_request_id && (
          <p>{copy.endedWithoutReport(name)}</p>
        )}
      {filesFirst && files}
      {activation}
      {!filesFirst && files}
      {attempt.thread_id && (
        <Link href={pathOfThread({ thread_id: attempt.thread_id })}>
          {copy.openConversation}
        </Link>
      )}
      {attempt.candidate && !record.outcome && (
        <div>
          <p>
            {attempt.contained
              ? copy.contained.candidate(name)
              : copy.candidate}
          </p>
          <p>{attempt.candidate.statement}</p>
          <ul>
            {attempt.candidate.sources.map((source, index) => (
              <li key={index}>
                {source.kind === "space_file" ? (
                  <Link href={`/workspace/spaces?space=${source.space_id}`}>
                    {source.path}
                  </Link>
                ) : (
                  copy.unavailableSource
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
      {stopAllowed && ["starting", "running"].includes(attempt.status) && (
        <Button
          disabled={disabled || busy || !!retry || accepted}
          onClick={() => setConfirmation(record)}
        >
          {copy.stop}
        </Button>
      )}
      {notice &&
        (!accepted || ["starting", "running"].includes(attempt.status)) && (
          <p role="status">{notice}</p>
        )}
      {retry && (
        <Button
          disabled={disabled || busy}
          onClick={() => void stop(retry.record, retry.command)}
        >
          {copy.retry}
        </Button>
      )}
      <Dialog
        open={confirmation !== null}
        onOpenChange={(open) => {
          if (!open && !busy) setConfirmation(null);
        }}
      >
        <DialogContent>
          <DialogTitle>{copy.stop}</DialogTitle>
          <DialogDescription>{copy.stopConfirm(name)}</DialogDescription>
          <Button disabled={busy} onClick={() => setConfirmation(null)}>
            {copy.keepWorking}
          </Button>
          <Button
            disabled={disabled || busy}
            onClick={() => {
              if (confirmation) void stop(confirmation);
            }}
          >
            {copy.stopNow}
          </Button>
        </DialogContent>
      </Dialog>
    </section>
  );
}

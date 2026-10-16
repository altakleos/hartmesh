"use client";

import { useRouter } from "next/navigation";
import { useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  usePersonName,
  useSupervisorName,
} from "@/components/workspace/person-name";
import * as api from "@/core/agent-instances/api";
import {
  agentBlock,
  hasCeiling,
  USE,
  INSPECT,
  MANAGE,
  type AgentBlock,
} from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useBranding, useStorage } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { AgentAvatar } from "./agent-avatar";
import { ExecutionNotice } from "./execution-notice";

export function AgentAvailabilityNotice({
  instance,
  block,
}: {
  instance: api.AgentInstanceDetail;
  block: AgentBlock;
}) {
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  return (
    <ExecutionNotice
      area="agent"
      kind={instance.custody}
      name={instance.name}
      reason={block.reason}
      busy={block.busy}
      administrator={user?.system_role === "admin"}
      provider={branding.providerName}
      supportURL={branding.supportURL}
      storageReason={storage?.reason}
    />
  );
}

type HeaderProps = {
  instance: api.AgentInstanceDetail;
  lifecyclePending?: boolean;
  brief?: string;
  changed?: () => void;
};
export function AgentHeader(props: HeaderProps) {
  const { user } = useAuth();
  return (
    <HeaderContent
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.permissions}`}
      {...props}
    />
  );
}
function HeaderContent({
  instance,
  lifecyclePending = false,
  brief,
  changed,
}: HeaderProps) {
  const { user } = useAuth();
  const { t, locale } = useI18n();
  const copy = t.agentInstances;
  const feature = useAgentInstances();
  const router = useRouter();
  const owner = usePersonName(
    instance.custody === "personal" ? (instance.owner_id ?? "") : "",
    true,
  );
  const supervisor = usePersonName(instance.supervisor.subject_id, true);
  const help = useSupervisorName(instance.supervisor.subject_id);
  const block = agentBlock(instance, feature);
  const captureSignal = useSpaceActionSignal(
    `${instance.id}:${instance.permissions}`,
  );
  const busyRef = useRef(false);
  const creation = useRef<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<api.InstanceApiError | true | null>(null);
  const hintId = useId();
  const status = instance.status;
  let state: string;
  if (status === "provisioning") state = copy.state.unfinished;
  else if (status === "deleted") state = copy.state.deleted;
  else if (status === "archived") state = copy.state.retired;
  else if (lifecyclePending) state = copy.state.changing;
  else if (status === "suspended") state = copy.state.paused;
  else if (block) state = copy.state.unavailable;
  else if (instance.activity.running_runs > 0) state = copy.state.working;
  else {
    const time = instance.activity.last_active_at;
    const date = time ? new Date(time) : null;
    state = `${copy.state.idle}, ${date && Number.isFinite(date.valueOf()) ? copy.state.lastActive(new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(date)) : copy.state.neverUsed}`;
  }
  const canUse =
    (instance.permissions & USE) !== 0 &&
    hasCeiling(user?.permissions, "threads:write");
  const sentence = lifecyclePending
    ? copy.pending(instance.name)
    : status === "suspended"
      ? copy.pausedNotice(instance.name)
      : status === "archived"
        ? copy.retiredNotice(instance.name)
        : !canUse
          ? copy.conversations.noUse(help)
          : null;
  const disabled = busy || !!block || !!sentence || status !== "active";
  async function start() {
    if (busyRef.current || disabled) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    const signal = captureSignal();
    creation.current ??= api.operationID();
    try {
      const chat = await awaitAbortable(
        api.createInstanceConversation(instance.id, creation.current, signal),
        signal,
      );
      signal.throwIfAborted();
      router.push(`/workspace/chats/${encodeURIComponent(chat.thread_id)}`);
    } catch (failure) {
      if (signal.aborted) return;
      if (failure instanceof api.InstanceApiError) creation.current = null;
      setError(failure instanceof api.InstanceApiError ? failure : true);
      if (failure instanceof api.InstanceApiError) changed?.();
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  return (
    <header className="space-y-4 rounded-lg border p-4">
      <div className="flex items-start gap-4">
        <AgentAvatar instance={instance} />
        <div className="min-w-0 flex-1">
          <h1 className="text-2xl font-semibold break-words">
            {instance.name}
          </h1>
          <p>
            {instance.custody === "personal"
              ? copy.marker.aiAssistant
              : copy.marker.aiEmployee}
          </p>
          <p role="status">{state}</p>
          <p className="text-muted-foreground text-sm">
            {copy.header.line(
              instance.custody,
              instance.owner_id === user?.id,
              owner ?? "…",
              supervisor ?? "…",
            )}
          </p>
          <p className="text-sm">
            {copy.header.yourAccess}{" "}
            {[USE, INSPECT, MANAGE]
              .filter((bit) => (instance.permissions & bit) !== 0)
              .map((bit) =>
                bit === USE
                  ? copy.use
                  : bit === INSPECT
                    ? copy.inspect
                    : copy.manageAccess,
              )
              .join(", ")}
          </p>
        </div>
      </div>
      {status !== "deleted" && status !== "provisioning" && (
        <div className="space-y-2">
          <Button
            type="button"
            disabled={disabled}
            aria-describedby={block || sentence ? hintId : undefined}
            onClick={() => void start()}
          >
            {copy.start}
          </Button>
          {block ? (
            <div id={hintId}>
              <AgentAvailabilityNotice instance={instance} block={block} />
            </div>
          ) : (
            sentence && <p id={hintId}>{sentence}</p>
          )}
        </div>
      )}
      {(instance.permissions & INSPECT) !== 0 &&
        brief !== undefined &&
        status !== "deleted" && (
          <details>
            <summary>{copy.instructions}</summary>
            <p className="mt-2 whitespace-pre-wrap">{brief}</p>
          </details>
        )}
      {error &&
        (error !== true && error.reason === "execution-unavailable" ? (
          <AgentAvailabilityNotice
            instance={instance}
            block={{
              reason:
                EXECUTION_REASONS.find(
                  (reason) => reason === error.executionReason,
                ) ?? null,
              busy: false,
            }}
          />
        ) : (
          <p role="alert">{copy.operationError}</p>
        ))}
    </header>
  );
}

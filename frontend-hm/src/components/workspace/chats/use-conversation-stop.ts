import { useEffect, useRef, useState } from "react";

import {
  getWork,
  type AgentInstanceDetail,
  type WorkRecord,
  type WorkStopCommand,
} from "@/core/agent-instances/api";
import { conversationStopAuthority } from "@/core/agent-instances/conversation-presentation";
import {
  hasCeiling,
  canManageInstance,
  INSPECT,
  USE,
} from "@/core/agent-instances/presentation";
import { stopWorkAttempt } from "@/core/agent-instances/stop-work";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import type { AgentThread } from "@/core/threads/types";

export interface ConversationStopOptions {
  threadId: string;
  binding: AgentThread["agent_instance"];
  detail: AgentInstanceDetail | null;
  legacy: boolean;
  streaming: boolean;
  sentHere: boolean;
  stopStream: () => Promise<void>;
  changed: () => void;
}
interface Owner {
  key: string;
  active: boolean;
}
export function useConversationStop(options: ConversationStopOptions) {
  const { user } = useAuth();
  const { t } = useI18n();
  const { threadId, binding, detail, legacy, streaming, sentHere } = options;
  const authority = binding
    ? conversationStopAuthority({
        threadId,
        binding,
        detail,
        userId: user?.id,
        sentHere,
        canCancel: hasCeiling(user?.permissions, "runs:cancel"),
      })
    : { allowed: legacy };
  const key = JSON.stringify([
    user?.id,
    user?.system_role,
    user?.permissions,
    threadId,
    binding?.id,
    detail?.permissions,
    authority.allowed,
  ]);
  const lifetime = useRef<Owner>({ key, active: true });
  if (lifetime.current.key !== key) {
    lifetime.current.active = false;
    lifetime.current = { key, active: true };
  }
  const owner = lifetime.current;
  useEffect(() => {
    owner.active = true;
    return () => {
      owner.active = false;
    };
  }, [owner]);
  const signalForAction = useSpaceActionSignal(key);
  const current = useRef(options);
  current.current = options;
  const busyOwner = useRef<Owner | null>(null);
  const retry = useRef<{
    owner: Owner;
    record: WorkRecord;
    command: WorkStopCommand;
    runId?: string | null;
  } | null>(null);
  const [state, setState] = useState<{
    owner: Owner;
    busy: boolean;
    pending: boolean;
    message: string | null;
  } | null>(null);
  const own = state?.owner === owner ? state : null;
  useEffect(() => {
    if (!streaming)
      setState((value) =>
        value?.owner === owner && value.pending
          ? { ...value, pending: false, message: null }
          : value,
      );
  }, [owner, streaming]);
  const clear = () => {
    retry.current = null;
    setState(null);
  };
  const stop = async () => {
    if (
      !owner.active ||
      !authority.allowed ||
      !current.current.streaming ||
      busyOwner.current === owner ||
      own?.pending
    )
      return;
    const signal = signalForAction();
    const live = () =>
      owner.active && lifetime.current === owner && !signal.aborted;
    const activeRun = detail?.activity.running.find(
      (entry) => entry.thread_id === threadId,
    );
    const held = retry.current?.owner === owner ? retry.current : null;
    // A retry belongs to the original run even if refreshed activity is empty.
    // It must never fall through to ordinary cancellation for another attempt.
    if (held && activeRun && held.runId && held.runId !== activeRun.run_id)
      return;
    busyOwner.current = owner;
    setState({ owner, busy: true, pending: false, message: null });
    try {
      if (binding && (authority.workId || held)) {
        const record =
          held?.record ??
          (await getWork(binding.id, authority.workId!, signal));
        if (!live()) return;
        const attempt = record.attempt;
        if (
          !held &&
          (!attempt ||
            !["starting", "running"].includes(attempt.status) ||
            attempt.thread_id !== threadId ||
            (activeRun &&
              attempt.run_id &&
              attempt.run_id !== activeRun.run_id))
        ) {
          setState({
            owner,
            busy: false,
            pending: false,
            message: t.agentWork.stopNothing(binding.name),
          });
          options.changed();
          return;
        }
        if (
          !held &&
          !canManageInstance(detail!.permissions) &&
          (attempt?.requester_id !== user?.id ||
            (detail!.permissions & (USE | INSPECT)) !== (USE | INSPECT))
        ) {
          setState({
            owner,
            busy: false,
            pending: false,
            message: t.agentInstances.operationError,
          });
          options.changed();
          return;
        }
        const result = await stopWorkAttempt(binding.id, record, {
          signal,
          retry: held?.command,
        });
        if (!live()) return;
        retry.current = result.retry
          ? {
              owner,
              record: result.record,
              command: result.retry,
              runId: held?.runId ?? activeRun?.run_id,
            }
          : null;
        const pending = result.kind === "stopping" && current.current.streaming;
        setState({
          owner,
          busy: false,
          pending,
          message:
            result.kind === "nothing"
              ? t.agentWork.stopNothing(binding.name)
              : result.kind === "failed"
                ? t.agentWork.stopFailed
                : pending
                  ? t.agentWork.stopPending(binding.name)
                  : null,
        });
        options.changed();
      } else {
        await options.stopStream();
      }
    } catch {
      if (live())
        setState({
          owner,
          busy: false,
          pending: false,
          message:
            binding && authority.workId
              ? t.agentWork.stopFailed
              : t.agentInstances.operationError,
        });
    } finally {
      if (busyOwner.current === owner) busyOwner.current = null;
      if (live())
        setState((value) =>
          value?.owner === owner ? { ...value, busy: false } : value,
        );
    }
  };
  return {
    allowed: authority.allowed,
    busy: own?.busy ?? false,
    pending: own?.pending ?? false,
    message: own?.message ?? null,
    stop,
    clear,
  };
}

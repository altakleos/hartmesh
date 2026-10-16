"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { PeoplePicker } from "@/components/workspace/people-picker";
import * as api from "@/core/agent-instances/api";
import {
  clearCreation,
  rememberCreation,
  type RememberedCreation,
} from "@/core/agent-instances/unfinished";
import { createAgent, deleteAgent } from "@/core/agents/api";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import type { Person } from "@/core/people/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { BriefStep, type BriefDraft } from "./brief-step";
import {
  CreationFailureNotice,
  type CreationFailure,
} from "./creation-failure";

interface CreateProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  providerName?: string | null;
  supportURL?: string | null;
  ownName?: string;
  unfinishedName?: string;
}

export function CreateAssistant(props: CreateProps) {
  return <CreateAgent kind="personal" {...props} />;
}
export function CreateEmployee(props: CreateProps) {
  return <CreateAgent kind="company" {...props} />;
}

function CreateAgent(props: CreateProps & { kind: api.Custody }) {
  const { user } = useAuth();
  // Closing or changing authority retires every draft and in-flight callback.
  return props.open ? (
    <CreationDialog
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.kind}`}
      {...props}
    />
  ) : null;
}

function CreationDialog({
  kind,
  onOpenChange,
  providerName,
  supportURL,
  ownName = "",
  unfinishedName,
}: CreateProps & { kind: api.Custody }) {
  const { user } = useAuth();
  const { t } = useI18n();
  const feature = useAgentInstances();
  const captureSignal = useSpaceActionSignal();
  const queryClient = useQueryClient();
  const router = useRouter();
  const copy = t.agentInstances;
  const administrator = user?.system_role === "admin";
  const [draft, setDraft] = useState<BriefDraft>({
    name: "",
    brief: "",
    role: null,
    useCustom: false,
    customDefinition: null,
  });
  const [step, setStep] = useState(1);
  const [supervisor, setSupervisor] = useState<Person | null>(
    user ? { id: user.id, email: user.email, name: null, active: true } : null,
  );
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [slow, setSlow] = useState(false);
  const [failure, setFailure] = useState<CreationFailure>();
  const pending = useRef<RememberedCreation | null>(null);
  const definition = useRef<string | null>(null);
  const conversationID = useRef<string | null>(null);
  const name = pending.current?.body.name ?? draft.name.trim();
  const canWrite =
    user?.permissions == null ||
    user.permissions.some((permission) =>
      ["*", "agents:*", "agents:write"].includes(permission),
    );
  const canCreate =
    !!user &&
    canWrite &&
    (kind === "personal" || administrator) &&
    feature.known &&
    feature.enabled &&
    feature.execution === "ready";
  const valid =
    Array.from(draft.name.trim()).length > 0 &&
    Array.from(draft.name.trim()).length <= 128 &&
    (draft.useCustom
      ? !!draft.customDefinition
      : !!draft.brief.trim() && Array.from(draft.brief).length <= 16_000);

  useEffect(() => {
    if (!busy) {
      setSlow(false);
      return;
    }
    const timer = setTimeout(() => setSlow(true), 10_000);
    return () => clearTimeout(timer);
  }, [busy]);

  function refresh() {
    void queryClient.invalidateQueries({ queryKey: ["features"] });
    void queryClient.invalidateQueries({ queryKey: ["agent-instances"] });
  }
  async function discard(signal: AbortSignal) {
    signal.throwIfAborted();
    if (user) clearCreation(user.id);
    pending.current = null;
    const owned = definition.current;
    definition.current = null;
    if (owned) {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      // Definition cleanup is best effort, and never crosses an account boundary.
      await awaitAbortable(deleteAgent(owned, deadline), deadline).catch(
        () => undefined,
      );
    }
  }
  async function cancel() {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    const signal = captureSignal();
    try {
      await discard(signal);
      if (!signal.aborted) onOpenChange(false);
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  async function submit() {
    if (
      busyRef.current ||
      !canCreate ||
      (!pending.current && !valid) ||
      (kind === "company" && !supervisor)
    )
      return;
    busyRef.current = true;
    setBusy(true);
    setFailure(undefined);
    const signal = captureSignal();
    let phase: "brief" | "instance" | "conversation" = "brief";
    let created: api.AgentInstance | undefined;
    try {
      if (!pending.current) {
        let source = draft.customDefinition;
        if (!draft.useCustom) {
          const sourceName =
            `${draft.role?.id ?? "agent"}-${api.operationID().slice(0, 8)}`.toLowerCase();
          const deadline = AbortSignal.any([
            signal,
            AbortSignal.timeout(20_000),
          ]);
          const written = await awaitAbortable(
            createAgent(
              {
                name: sourceName,
                description:
                  draft.role?.description ??
                  Array.from(draft.brief.split(/\r?\n/u)[0] ?? "")
                    .slice(0, 200)
                    .join(""),
                soul: draft.brief,
                ...(draft.role ? { skills: draft.role.skills } : {}),
                work_policy: draft.role?.work_policy ?? {
                  enabled: true,
                  default_priority: "normal",
                  review_required: true,
                  allow_derived: false,
                  max_derived_per_activation: 3,
                  responsibilities: [],
                },
              },
              deadline,
            ),
            deadline,
          );
          signal.throwIfAborted();
          source = written.name;
          definition.current = written.name;
        }
        if (!source || !user) return;
        pending.current = {
          body: {
            creation_id: api.operationID(),
            name: draft.name.trim(),
            definition_name: source,
            custody: kind,
            ...(kind === "company" ? { supervisor_id: supervisor!.id } : {}),
          },
          definition_name: definition.current,
          copy_of: null,
        };
        rememberCreation(user.id, pending.current);
      }
      phase = "instance";
      const captured = pending.current;
      if (captured.copy_of !== null) return;
      created = await awaitAbortable(
        api.createInstance(captured.body, signal),
        signal,
      );
      signal.throwIfAborted();
      clearCreation(user.id);
      pending.current = null;
      definition.current = null;
      refresh();
      phase = "conversation";
      conversationID.current ??= api.operationID();
      const chat = await awaitAbortable(
        api.createInstanceConversation(
          created.id,
          conversationID.current,
          signal,
        ),
        signal,
      );
      signal.throwIfAborted();
      router.push(
        `/workspace/chats/${encodeURIComponent(chat.thread_id)}?welcome=1`,
      );
      onOpenChange(false);
    } catch (error) {
      if (signal.aborted) return;
      if (phase === "conversation" && created) {
        router.push(`/workspace/instances?instance=${created.id}&added=1`);
        onOpenChange(false);
      } else if (phase === "brief") setFailure({ type: "brief" });
      else if (error instanceof api.InstanceApiError) {
        await discard(signal);
        if (!signal.aborted) {
          setFailure({ type: "refused", error });
          refresh();
        }
      } else setFailure({ type: "unconfirmed" });
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }

  const error = failure?.error;
  const countRefusal =
    error?.reason === "ai-assistant-limit" ||
    error?.reason === "ai-assistant-none-free" ||
    error?.reason === "ai-employee-limit";
  const forbidden = kind === "company" && error?.status === 403;
  const executionRefusal = error?.reason === "execution-unavailable";
  const frozen = busy || failure?.type === "unconfirmed";

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) void cancel();
      }}
    >
      <DialogContent
        className="max-h-[90dvh] overflow-y-auto sm:max-w-2xl"
        aria-describedby={undefined}
        showCloseButton={!busy}
      >
        <DialogTitle>{copy.create.title(kind)}</DialogTitle>
        {kind === "company" && <p>{copy.create.step(step)}</p>}
        {step === 1 ? (
          <BriefStep
            kind={kind}
            value={draft}
            onChange={setDraft}
            disabled={frozen}
          />
        ) : (
          <section className="space-y-3">
            <h3>{copy.create.supervisorHeading(draft.name.trim())}</h3>
            <p>{copy.create.supervisorHelp(draft.name.trim())}</p>
            <PeoplePicker
              label={copy.supervisor}
              value={supervisor}
              onChange={setSupervisor}
              includeSelf
              disabled={frozen}
            />
          </section>
        )}
        {busy && (
          <div role="status">
            <p>{copy.create.settingUp(name)}</p>
            {slow && <p>{copy.create.slow}</p>}
          </div>
        )}
        {failure && (
          <CreationFailureNotice
            failure={failure}
            kind={kind}
            name={name}
            ownName={ownName}
            unfinishedName={unfinishedName}
            providerName={providerName}
            supportURL={supportURL}
          />
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={() => void cancel()}
          >
            {t.common.cancel}
          </Button>
          {step === 2 && !failure && (
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => setStep(1)}
            >
              {copy.create.back}
            </Button>
          )}
          {failure ? (
            !countRefusal &&
            !forbidden &&
            !executionRefusal && (
              <Button
                type="button"
                disabled={busy || !canCreate}
                onClick={() => void submit()}
              >
                {copy.retry}
              </Button>
            )
          ) : kind === "company" && step === 1 ? (
            <Button
              type="button"
              disabled={!valid || !canCreate}
              onClick={() => setStep(2)}
            >
              {copy.create.next}
            </Button>
          ) : (
            <Button
              type="button"
              disabled={
                busy ||
                !valid ||
                !canCreate ||
                (kind === "company" && !supervisor)
              }
              onClick={() => void submit()}
            >
              {kind === "personal" ? copy.assistants.add : copy.employees.add}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

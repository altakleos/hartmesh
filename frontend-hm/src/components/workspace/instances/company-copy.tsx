"use client";

import { useRouter } from "next/navigation";
import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { PeoplePicker } from "@/components/workspace/people-picker";
import * as api from "@/core/agent-instances/api";
import {
  clearCreation,
  readCreation,
  rememberCreation,
} from "@/core/agent-instances/unfinished";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useBranding, useStorage } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import type { Person } from "@/core/people/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import {
  CreationFailureNotice,
  type CreationFailure,
} from "./creation-failure";
import { ExecutionNotice } from "./execution-notice";
import { useLifecycleActions } from "./lifecycle-actions";

export function CompanyCopyAction({ disabled }: { disabled: boolean }) {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { user } = useAuth();
  const feature = useAgentInstances();
  const branding = useBranding();
  const storage = useStorage();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const [open, setOpen] = useState(false);
  const hint = useId();
  if (
    user?.system_role !== "admin" ||
    instance.custody !== "personal" ||
    !actions.canManage
  )
    return null;
  const held = feature.execution === "unavailable";
  const provider = branding.providerName ?? copy.providerFallback;
  const allowed = feature.employees.allowed;
  const inUse = feature.employees.inUse;
  const offered = allowed > 0 && !(held && feature.reason === "agents-none");
  const canStart =
    feature.known &&
    feature.enabled &&
    !feature.isLoading &&
    !held &&
    offered &&
    inUse < allowed;
  const message = feature.isLoading ? (
    copy.loading
  ) : !feature.known || !feature.enabled ? (
    t.sidebar.agentsUnavailable
  ) : !offered ? (
    copy.employees.notOffered
  ) : held ? (
    <ExecutionNotice
      area="agent"
      kind="company"
      name={instance.name}
      reason={feature.reason}
      administrator
      provider={branding.providerName}
      supportURL={branding.supportURL}
      storageReason={storage?.reason}
    />
  ) : inUse >= allowed ? (
    copy.employees.limitReached(allowed, provider)
  ) : null;
  return (
    <div className="space-y-2">
      <Button
        type="button"
        variant="outline"
        disabled={disabled || !canStart}
        aria-describedby={message ? hint : undefined}
        onClick={() => setOpen(true)}
      >
        {copy.makeEmployee}
      </Button>
      {message && <div id={hint}>{message}</div>}
      <CopyDialog
        open={open}
        onOpenChange={setOpen}
        disabled={disabled || !feature.known || !feature.enabled || held}
        canStart={canStart}
      />
    </div>
  );
}
function CopyDialog({
  open,
  onOpenChange,
  disabled,
  canStart,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  disabled: boolean;
  canStart: boolean;
}) {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const branding = useBranding();
  const router = useRouter();
  const id = useId();
  const captureSignal = useSpaceActionSignal();
  const [name, setName] = useState(instance.name);
  const [supervisor, setSupervisor] = useState<Person | null>(
    user ? { id: user.id, email: user.email, name: null, active: true } : null,
  );
  const pending = useRef<api.CompanyCopyRequest | null>(null);
  const [failure, setFailure] = useState<CreationFailure>();
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [slow, setSlow] = useState(false);
  useEffect(() => {
    if (!busy) return;
    const timer = setTimeout(() => setSlow(true), 10_000);
    return () => clearTimeout(timer);
  }, [busy]);
  const invalid = !name.trim() || Array.from(name.trim()).length > 128;
  async function submit() {
    if (
      !user ||
      disabled ||
      busyRef.current ||
      invalid ||
      !supervisor ||
      (!pending.current && !canStart)
    )
      return;
    const owner = captureSignal();
    busyRef.current = true;
    setBusy(true);
    setSlow(false);
    setFailure(undefined);
    pending.current ??= {
      creation_id: api.operationID(),
      generation: instance.generation,
      name: name.trim(),
      supervisor_id: supervisor.id,
    };
    const body = pending.current;
    rememberCreation(user.id, {
      body,
      definition_name: null,
      copy_of: instance.id,
    });
    try {
      const created = await awaitAbortable(
        api.createCompanyCopy(instance.id, body, owner),
        owner,
      );
      owner.throwIfAborted();
      if (readCreation(user.id)?.body.creation_id === body.creation_id)
        clearCreation(user.id);
      pending.current = null;
      void actions.refresh();
      router.push(
        `/workspace/instances?instance=${created.id}&employeeAdded=1`,
      );
      onOpenChange(false);
    } catch (error) {
      if (owner.aborted) return;
      if (error instanceof api.InstanceApiError) {
        if (readCreation(user.id)?.body.creation_id === body.creation_id)
          clearCreation(user.id);
        pending.current = null;
        setFailure({ type: "refused", error });
        void actions.refresh();
      } else setFailure({ type: "unconfirmed" });
    } finally {
      if (!owner.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  const frozen = disabled || busy || failure?.type === "unconfirmed";
  return (
    <Dialog
      open={open}
      onOpenChange={(value) => {
        if (!busy) onOpenChange(value);
      }}
    >
      <DialogContent showCloseButton={!busy}>
        <DialogTitle>{copy.makeEmployee}</DialogTitle>
        <DialogDescription>{copy.makeEmployeeNotice}</DialogDescription>
        <label htmlFor={id}>{copy.settings.employeeName}</label>
        <Input
          id={id}
          value={name}
          disabled={frozen}
          onChange={(event) => setName(event.target.value)}
        />
        {invalid && <p role="alert">{copy.create.nameInvalid}</p>}
        <PeoplePicker
          includeSelf
          label={copy.supervisor}
          value={supervisor}
          disabled={frozen}
          onChange={setSupervisor}
        />
        <p>{copy.create.supervisorHelp(name)}</p>
        {busy && <p role="status">{copy.create.settingUp(name)}</p>}
        {slow && busy && <p role="status">{copy.create.slow}</p>}
        {failure && (
          <CreationFailureNotice
            failure={failure}
            kind="company"
            name={name}
            providerName={branding.providerName}
            supportURL={branding.supportURL}
          />
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={() => onOpenChange(false)}
          >
            {t.common.cancel}
          </Button>
          <Button
            type="button"
            disabled={
              busy ||
              disabled ||
              invalid ||
              !supervisor ||
              (!pending.current && !canStart)
            }
            onClick={() => void submit()}
          >
            {failure?.type === "unconfirmed" ? copy.retry : copy.employees.add}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

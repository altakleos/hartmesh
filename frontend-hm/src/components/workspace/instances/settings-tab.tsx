"use client";

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
import * as api from "@/core/agent-instances/api";
import { agentBlock, INSPECT } from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { AgentAvailabilityNotice } from "./agent-header";
import { BriefSettings } from "./brief-settings";
import { CompanyCopyAction } from "./company-copy";
import { DeleteAgentDialog } from "./delete-agent-dialog";
import {
  LifecycleFailure,
  useLifecycleActions,
  useLifecycleModal,
} from "./lifecycle-actions";

export function SettingsTab() {
  const { instance } = useLifecycleActions();
  const { t } = useI18n();
  return !(instance.permissions & INSPECT) ? (
    <p>{t.agentInstances.access.manageNeedsInspect(instance.name)}</p>
  ) : (
    <SettingsControls />
  );
}
function SettingsControls() {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const feature = useAgentInstances();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const id = useId();
  const captureSignal = useSpaceActionSignal();
  const [name, setName] = useState(instance.name);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string>();
  const [renamed, setRenamed] = useState(false);
  const capturedRename = useRef<{ generation: number; name: string } | null>(
    null,
  );
  const [unconfirmed, setUnconfirmed] = useState(false);
  const [confirmation, setConfirmation] = useState<
    "suspend" | "archive" | null
  >(null);
  const [deleting, setDeleting] = useState(false);
  const block = agentBlock(instance, feature);
  const [briefBusy, setBriefBusy] = useState(false);
  const disabled =
    !actions.canManage || actions.pending || actions.busy || busy || briefBusy;
  const unavailable = disabled || !!block;
  useEffect(() => {
    if (!busyRef.current && !capturedRename.current) setName(instance.name);
  }, [instance.name]);
  async function rename() {
    if (disabled || busyRef.current) return;
    const value = name.trim();
    if (!capturedRename.current && (!value || Array.from(value).length > 128)) {
      setError(copy.create.nameInvalid);
      return;
    }
    capturedRename.current ??= { generation: instance.generation, name: value };
    const body = capturedRename.current;
    const owner = captureSignal();
    busyRef.current = true;
    setBusy(true);
    setError(undefined);
    setRenamed(false);
    try {
      await awaitAbortable(
        api.renameInstance(instance.id, body.generation, body.name, owner),
        owner,
      );
      owner.throwIfAborted();
      capturedRename.current = null;
      setUnconfirmed(false);
      setRenamed(true);
      void actions.refresh();
    } catch (failure) {
      if (owner.aborted) return;
      if (failure instanceof api.InstanceApiError) {
        capturedRename.current = null;
        setUnconfirmed(false);
        setError(copy.operationError);
      } else setUnconfirmed(true);
    } finally {
      if (!owner.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  return (
    <section className="space-y-5">
      <h2 className="text-lg font-medium">{copy.tabs.settings}</h2>
      {block && <AgentAvailabilityNotice instance={instance} block={block} />}
      <div className="space-y-2">
        <label htmlFor={`${id}-name`}>{copy.name}</label>
        <Input
          id={`${id}-name`}
          value={name}
          disabled={disabled || unconfirmed}
          onChange={(event) => {
            setName(event.target.value);
            setError(undefined);
            setRenamed(false);
          }}
        />
        {error && <p role="alert">{error}</p>}
        {unconfirmed && <p role="status">{copy.unconfirmed}</p>}
        <Button type="button" disabled={disabled} onClick={() => void rename()}>
          {unconfirmed ? copy.retry : copy.rename}
        </Button>
        {renamed && <p role="status">{copy.settings.renamed}</p>}
      </div>
      {instance.status === "active" && (
        <BriefSettings
          disabled={unavailable || unconfirmed}
          onBusy={setBriefBusy}
        />
      )}
      <div className="flex flex-wrap gap-2">
        {instance.status === "active" && (
          <Button
            type="button"
            variant="outline"
            disabled={unavailable || unconfirmed}
            onClick={() => setConfirmation("suspend")}
          >
            {copy.suspend}
          </Button>
        )}
        {instance.status === "suspended" && (
          <Button
            type="button"
            disabled={unavailable || unconfirmed}
            onClick={() => void actions.change({ action: "restore" })}
          >
            {copy.restore}
          </Button>
        )}
        {(instance.status === "active" || instance.status === "suspended") && (
          <Button
            type="button"
            variant="outline"
            disabled={unavailable || unconfirmed}
            onClick={() => setConfirmation("archive")}
          >
            {copy.archive}
          </Button>
        )}
        {instance.status === "archived" && (
          <Button
            type="button"
            disabled={unavailable || unconfirmed}
            onClick={() => void actions.change({ action: "restore" })}
          >
            {copy.bringBack}
          </Button>
        )}
        {instance.status !== "deleted" && (
          <Button
            type="button"
            variant="destructive"
            disabled={
              disabled ||
              unconfirmed ||
              (!!block && block.reason !== "space-unverified")
            }
            onClick={() => setDeleting(true)}
          >
            {copy.delete}
          </Button>
        )}
      </div>
      <CompanyCopyAction disabled={disabled || unconfirmed} />
      <p>{copy.settings.automatic(instance.name)}</p>
      <ActionConfirmation
        action={confirmation}
        close={() => setConfirmation(null)}
      />
      <DeleteAgentDialog open={deleting} onOpenChange={setDeleting} />
    </section>
  );
}
function ActionConfirmation({
  action,
  close,
}: {
  action: "suspend" | "archive" | null;
  close: () => void;
}) {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { t } = useI18n();
  const copy = t.agentInstances;
  useLifecycleModal(action !== null);
  const unknown =
    actions.receipt?.phase === "unconfirmed" &&
    actions.receipt.body.action === action;
  return (
    <Dialog
      open={action !== null}
      onOpenChange={(open) => {
        if (!open && !actions.busy) close();
      }}
    >
      <DialogContent showCloseButton={!actions.busy}>
        <DialogTitle>
          {action === "suspend" ? copy.suspend : copy.archive}
        </DialogTitle>
        <DialogDescription>
          {action === "suspend"
            ? copy.settings.pauseConfirm(instance.name)
            : copy.settings.retireConfirm(instance.name, instance.custody)}
        </DialogDescription>
        {instance.activity.running_runs > 0 && (
          <p>{copy.settings.runningNow(instance.name)}</p>
        )}
        {unknown && <p role="status">{copy.unconfirmed}</p>}
        {!!actions.failure && <LifecycleFailure error={actions.failure} />}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={actions.busy}
            onClick={close}
          >
            {t.common.cancel}
          </Button>
          <Button
            type="button"
            disabled={
              actions.busy ||
              !actions.canApply(action ?? undefined) ||
              (actions.pending && !unknown)
            }
            onClick={() => {
              if (unknown) {
                void actions.retry().then((outcome) => {
                  if (outcome === "complete" || outcome === "pending") close();
                });
                return;
              }
              if (action)
                void actions.change({ action }).then((outcome) => {
                  if (outcome === "complete" || outcome === "pending") close();
                });
            }}
          >
            {unknown
              ? copy.retry
              : action === "suspend"
                ? copy.suspend
                : copy.archive}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

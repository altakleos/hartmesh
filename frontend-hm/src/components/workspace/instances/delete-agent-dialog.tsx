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
import { useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

import {
  LifecycleFailure,
  useLifecycleActions,
  useLifecycleModal,
} from "./lifecycle-actions";

export function DeleteAgentDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { t } = useI18n();
  const copy = t.agentInstances;
  const branding = useBranding();
  const id = useId();
  const [confirmation, setConfirmation] = useState("");
  useLifecycleModal(open);
  const unknown =
    actions.receipt?.body.action === "delete" &&
    actions.receipt.phase === "unconfirmed";
  const unverified = instance.execution.reason === "space-unverified";
  return (
    <Dialog
      open={open}
      onOpenChange={(value) => {
        if (!actions.busy) onOpenChange(value);
      }}
    >
      <DialogContent showCloseButton={!actions.busy}>
        <DialogTitle>{copy.delete}</DialogTitle>
        <DialogDescription>
          {unverified
            ? copy.settings.deleteUnverified(
                instance.name,
                branding.providerName ?? copy.providerFallback,
              )
            : copy.settings.deleteConfirm(instance.name, instance.custody)}
        </DialogDescription>
        {instance.activity.running_runs > 0 && (
          <p>{copy.settings.runningNow(instance.name)}</p>
        )}
        <label htmlFor={id}>{copy.settings.typeName(instance.name)}</label>
        <Input
          id={id}
          value={confirmation}
          disabled={actions.busy || unknown}
          onChange={(event) => setConfirmation(event.target.value)}
        />
        {actions.busy && (
          <p role="status">{copy.settings.deleting(instance.name)}</p>
        )}
        {unknown && <p role="status">{copy.unconfirmed}</p>}
        {!!actions.failure && <LifecycleFailure error={actions.failure} />}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={actions.busy}
            onClick={() => onOpenChange(false)}
          >
            {t.common.cancel}
          </Button>
          <Button
            type="button"
            variant="destructive"
            disabled={
              actions.busy ||
              !actions.canApply("delete") ||
              (actions.pending && !unknown) ||
              confirmation !== instance.name
            }
            onClick={() => {
              if (unknown) {
                void actions.retry();
                return;
              }
              void actions.change({ action: "delete" }).then((outcome) => {
                if (outcome === "pending") onOpenChange(false);
              });
            }}
          >
            {unknown ? copy.retry : copy.delete}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
/** Page-owned so retrying from another tab can still finish deletion. */
export function DeletedAgentNavigation() {
  const actions = useLifecycleActions();
  const router = useRouter();
  const navigated = useRef<string | undefined>(undefined);
  const applied = actions.lastApplied;
  useEffect(() => {
    if (
      applied?.body.action !== "delete" ||
      typeof applied.result.space_released !== "boolean" ||
      navigated.current === applied.body.operation_id
    )
      return;
    navigated.current = applied.body.operation_id;
    const agent = applied.result.instance;
    const query = new URLSearchParams({ deleted: agent.name });
    if (!applied.result.space_released && !applied.unverified)
      query.set("clearing", "1");
    router.push(
      `/workspace/${agent.custody === "personal" ? "assistants" : "employees"}?${query}`,
    );
  }, [applied, router]);
  return null;
}

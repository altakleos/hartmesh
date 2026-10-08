"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import {
  acceptSpaceCurrentState,
  changeSpaceLifecycle,
  retireSpaceAttachments,
  type StorageSpace,
} from "@/core/spaces/api";
import { useSpaceRecovery } from "@/core/spaces/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

export function SpaceRecoveryPanel({
  space,
  refresh,
}: {
  space: StorageSpace;
  refresh: () => void;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirmation, setConfirmation] = useState<{
    action: "restore" | "delete" | "accept";
    id?: string;
  } | null>(null);
  const [acknowledged, setAcknowledged] = useState(false);
  const recovery = useSpaceRecovery(space.id, open);
  const captureSignal = useSpaceActionSignal();
  const lifetime = useFileActionLifetime();

  function confirm(action: "restore" | "delete" | "accept", id?: string) {
    setConfirmation({ action, id });
    setAcknowledged(false);
    setError(null);
  }

  async function run(
    action: "backup" | "restore" | "archive" | "delete" | "fence" | "accept",
    id?: string,
  ) {
    const signal = captureSignal();
    setBusy(true);
    setError(null);
    try {
      if (action === "accept" && id)
        await acceptSpaceCurrentState(space.id, space.generation, id, signal);
      else if (action === "fence")
        await retireSpaceAttachments(
          space.id,
          space.generation,
          recovery.data?.attachments.map((item) => item.id) ?? [],
          signal,
        );
      else if (action !== "accept")
        await changeSpaceLifecycle(
          space.id,
          space.generation,
          action,
          id,
          signal,
        );
      if (signal.aborted || !lifetime.active) return;
      setConfirmation(null);
      await recovery.refetch();
      if (!signal.aborted && lifetime.active) refresh();
    } catch (failure) {
      if (!signal.aborted && lifetime.active)
        setError(
          failure instanceof Error
            ? failure.message
            : t.storageSpaces.operationError,
        );
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  return (
    <section className="rounded border p-4">
      <Button variant="outline" onClick={() => setOpen(!open)}>
        {t.storageSpaces.recovery}
      </Button>
      {open && (
        <div className="mt-4 flex flex-col gap-3">
          <p className="text-muted-foreground text-sm">
            {t.storageSpaces.recoveryNotice}
          </p>
          {recovery.isPending && <p role="status">{t.common.loading}</p>}
          {recovery.error && <p role="alert">{t.storageSpaces.loadError}</p>}
          {error && <p role="alert">{error}</p>}
          {recovery.data && !recovery.error && (
            <>
              <div className="flex flex-wrap gap-2">
                <Button
                  disabled={busy || space.storage_state === "recovery-pending"}
                  onClick={() => void run("backup")}
                >
                  {t.storageSpaces.backup}
                </Button>
                {space.status === "active" && (
                  <Button
                    variant="outline"
                    disabled={
                      busy || space.storage_state === "recovery-pending"
                    }
                    onClick={() => void run("archive")}
                  >
                    {t.storageSpaces.archive}
                  </Button>
                )}
                <Button
                  variant="destructive"
                  disabled={busy || space.storage_state === "recovery-pending"}
                  onClick={() => confirm("delete")}
                >
                  {t.storageSpaces.deleteSpace}
                </Button>
              </div>
              {recovery.data.attachments.length > 0 && (
                <div>
                  <p>
                    {t.storageSpaces.attachedEnvironments}:{" "}
                    {recovery.data.attachments.length}
                  </p>
                  <Button
                    variant="outline"
                    disabled={busy || !recovery.data.can_fence}
                    onClick={() => void run("fence")}
                  >
                    {t.storageSpaces.retireEnvironments}
                  </Button>
                  {!recovery.data.can_fence && (
                    <p>{t.storageSpaces.providerUnavailable}</p>
                  )}
                </div>
              )}
              <ul className="space-y-2">
                {recovery.data.backups.map((backup) => (
                  <li
                    key={backup.id}
                    className="flex flex-wrap items-center gap-2"
                  >
                    <span>
                      {t.storageSpaces.backup} {backup.id.slice(0, 8)} ·{" "}
                      {backup.size_bytes.toLocaleString()} B ·{" "}
                      {t.storageSpaces.consistentBackup}
                    </span>
                    <Button
                      variant="outline"
                      disabled={
                        busy || space.storage_state === "recovery-pending"
                      }
                      onClick={() => confirm("restore", backup.id)}
                    >
                      {t.storageSpaces.restore}
                    </Button>
                  </li>
                ))}
              </ul>
              {recovery.data.operations
                .filter((op) => op.phase === "pending")
                .map((op) => (
                  <div key={op.operation_id}>
                    <p>
                      {t.storageSpaces.pendingOperation}: {op.request.action} ·{" "}
                      {op.operation_id.slice(0, 8)}
                    </p>
                    <Button
                      variant="outline"
                      disabled={busy}
                      onClick={() => confirm("accept", op.operation_id)}
                    >
                      {t.storageSpaces.acceptCurrentState}
                    </Button>
                  </div>
                ))}
            </>
          )}
        </div>
      )}
      <Dialog
        open={confirmation !== null}
        onOpenChange={(value) => {
          if (!value && !busy) setConfirmation(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {confirmation?.action === "restore"
                ? t.storageSpaces.restore
                : confirmation?.action === "delete"
                  ? t.storageSpaces.deleteSpace
                  : t.storageSpaces.acceptCurrentState}
            </DialogTitle>
            <DialogDescription>
              {confirmation?.action === "restore"
                ? t.storageSpaces.restoreNotice
                : confirmation?.action === "delete"
                  ? t.storageSpaces.deleteNotice
                  : t.storageSpaces.acceptStateNotice}
            </DialogDescription>
          </DialogHeader>
          <label className="flex gap-2">
            <input
              type="checkbox"
              checked={acknowledged}
              onChange={(e) => setAcknowledged(e.target.checked)}
            />
            {t.storageSpaces.confirmLifecycle}
          </label>
          {error && <p role="alert">{error}</p>}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={busy}
              onClick={() => setConfirmation(null)}
            >
              {t.common.cancel}
            </Button>
            <Button
              disabled={busy || !acknowledged}
              onClick={() => {
                if (confirmation)
                  void run(confirmation.action, confirmation.id);
              }}
            >
              {t.storageSpaces.confirmAction}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}

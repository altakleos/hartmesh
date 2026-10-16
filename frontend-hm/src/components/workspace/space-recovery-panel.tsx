"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { getInstance, type AgentInstance } from "@/core/agent-instances/api";
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

interface RecoveryConfirmation {
  action: "backup" | "restore" | "delete" | "accept";
  id?: string;
  running?: boolean;
}

export function SpaceRecoveryPanel({
  space,
  refresh,
  agent,
  findAgent,
  discoveryPending = false,
}: {
  space: StorageSpace;
  refresh: () => void;
  agent?: AgentInstance;
  findAgent?: () => Promise<AgentInstance | undefined>;
  discoveryPending?: boolean;
}) {
  const { t } = useI18n();
  const scope = `${space.id}:${space.generation}`;
  const [open, setOpen] = useState(false);
  const [busyScope, setBusyScope] = useState<string | null>(null);
  const busy = busyScope === scope;
  function setBusy(value: boolean) {
    setBusyScope((current) =>
      value ? scope : current === scope ? null : current,
    );
  }
  const [error, setError] = useState<string | null>(null);
  const [storedConfirmation, storeConfirmation] = useState<
    (RecoveryConfirmation & { scope: string }) | null
  >(null);
  function setConfirmation(value: RecoveryConfirmation | null) {
    storeConfirmation(value ? { ...value, scope } : null);
  }
  const [acknowledged, setAcknowledged] = useState(false);
  const recovery = useSpaceRecovery(space.id, open);
  const captureSignal = useSpaceActionSignal(`${space.id}:${space.generation}`);
  const lifetime = useFileActionLifetime();
  const [discovered, setDiscovered] = useState<AgentInstance>();
  const [ownedRefusalSpace, setOwnedRefusalSpace] = useState<string | null>(
    null,
  );
  const ownedRefusal = ownedRefusalSpace === space.id;
  const owner = agent ?? discovered;
  const owned = !!owner || ownedRefusal;
  const confirmation =
    storedConfirmation?.scope === scope &&
    !(owned && storedConfirmation.action === "delete")
      ? storedConfirmation
      : null;
  const recoveryNote = owner
    ? t.storageSpaces.agentFilesRecoveryNote(owner.name)
    : ownedRefusal
      ? t.storageSpaces.agentFilesRecoveryUnknown
      : t.storageSpaces.recoveryNotice;

  useEffect(() => {
    if (!open || !owner) return;
    const signal = captureSignal();
    void getInstance(owner.id, signal).catch(() => undefined);
  }, [open, owner, captureSignal]);

  useEffect(() => {
    if (owned && storedConfirmation?.action === "delete") {
      storeConfirmation(null);
      setAcknowledged(false);
    }
  }, [owned, storedConfirmation?.action]);

  async function runningNow(signal: AbortSignal) {
    if (!owner) return ownedRefusal;
    try {
      const current = await getInstance(owner.id, signal);
      return current.activity.running_runs > 0;
    } catch (failure) {
      if (
        typeof failure === "object" &&
        failure !== null &&
        "status" in failure &&
        failure.status === 404
      )
        return false;
      throw failure;
    }
  }

  async function prepare(action: "backup" | "restore" | "accept", id?: string) {
    const signal = captureSignal();
    setBusy(true);
    setError(null);
    try {
      const running = await runningNow(signal);
      if (signal.aborted || !lifetime.active) return;
      if (action === "backup" && !running) await run(action, id);
      else {
        setConfirmation({ action, id, running });
        setAcknowledged(false);
      }
    } catch {
      if (!signal.aborted && lifetime.active)
        setError(t.storageSpaces.loadError);
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  async function performConfirmation() {
    if (!confirmation) return;
    const selected = confirmation;
    const signal = captureSignal();
    setBusy(true);
    try {
      if (selected.action !== "delete") {
        const running = await runningNow(signal);
        if (signal.aborted || !lifetime.active) return;
        if (running && !selected.running) {
          setConfirmation({ ...selected, running: true });
          setAcknowledged(false);
          return;
        }
      }
      if (!signal.aborted && lifetime.active)
        await run(selected.action, selected.id);
    } catch {
      if (!signal.aborted && lifetime.active)
        setError(t.storageSpaces.loadError);
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  async function rediscover(signal: AbortSignal) {
    const current = await findAgent?.();
    if (
      !signal.aborted &&
      lifetime.active &&
      current?.home_id === space.id &&
      current.status !== "deleted" &&
      current.permissions > 0
    )
      setDiscovered(current);
  }

  function confirm(action: "restore" | "delete" | "accept", id?: string) {
    setConfirmation({ action, id });
    setAcknowledged(false);
    setError(null);
  }

  async function run(
    action: "backup" | "restore" | "archive" | "delete" | "fence" | "accept",
    id?: string,
  ) {
    if (
      owned &&
      (action === "archive" || action === "delete" || action === "fence")
    ) {
      setConfirmation(null);
      return;
    }
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
      if (signal.aborted || !lifetime.active) return;
      if (
        typeof failure === "object" &&
        failure !== null &&
        "reason" in failure &&
        failure.reason === "owned-space"
      ) {
        setOwnedRefusalSpace(space.id);
        setConfirmation(null);
        setError(null);
        try {
          await rediscover(signal);
        } catch {
          /* Keep generic guidance and retired controls. */
        }
      } else
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
          <p className="text-muted-foreground text-sm">{recoveryNote}</p>
          {owned && (
            <div role="status" className="space-y-2">
              <p>
                {owner
                  ? t.storageSpaces.agentFilesOf(owner.name)
                  : t.storageSpaces.agentFilesUnknown}
              </p>
              {owner ? (
                <p className="flex gap-4">
                  <Link
                    href={`/workspace/instances?instance=${owner.id}&tab=access`}
                  >
                    {t.agentInstances.tabs.access}
                  </Link>
                  <Link
                    href={`/workspace/instances?instance=${owner.id}&tab=settings`}
                  >
                    {t.agentInstances.tabs.settings}
                  </Link>
                </p>
              ) : (
                <Button
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    const signal = captureSignal();
                    void rediscover(signal).catch(() => undefined);
                  }}
                >
                  {t.agentInstances.refresh}
                </Button>
              )}
            </div>
          )}
          {recovery.isPending && <p role="status">{t.common.loading}</p>}
          {recovery.error && <p role="alert">{t.storageSpaces.loadError}</p>}
          {error && <p role="alert">{error}</p>}
          {discoveryPending && <p role="status">{t.common.loading}</p>}
          {recovery.data && !recovery.error && !discoveryPending && (
            <>
              <div className="flex flex-wrap gap-2">
                <Button
                  disabled={busy || space.storage_state === "recovery-pending"}
                  onClick={() => void prepare("backup")}
                >
                  {t.storageSpaces.backup}
                </Button>
                {!owned && space.status === "active" && (
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
                {!owned && (
                  <Button
                    variant="destructive"
                    disabled={
                      busy || space.storage_state === "recovery-pending"
                    }
                    onClick={() => confirm("delete")}
                  >
                    {t.storageSpaces.deleteSpace}
                  </Button>
                )}
              </div>
              {recovery.data.attachments.length > 0 && (
                <div>
                  <p>
                    {t.storageSpaces.attachedEnvironments}:{" "}
                    {recovery.data.attachments.length}
                  </p>
                  {!owned && (
                    <Button
                      variant="outline"
                      disabled={busy || !recovery.data.can_fence}
                      onClick={() => void run("fence")}
                    >
                      {t.storageSpaces.retireEnvironments}
                    </Button>
                  )}
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
                      onClick={() => void prepare("restore", backup.id)}
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
                      onClick={() => void prepare("accept", op.operation_id)}
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
              {confirmation?.action === "backup"
                ? t.storageSpaces.backup
                : confirmation?.action === "restore"
                  ? t.storageSpaces.restore
                  : confirmation?.action === "delete"
                    ? t.storageSpaces.deleteSpace
                    : t.storageSpaces.acceptCurrentState}
            </DialogTitle>
            <DialogDescription>
              {confirmation?.action === "backup"
                ? recoveryNote
                : confirmation?.action === "restore"
                  ? t.storageSpaces.restoreNotice
                  : confirmation?.action === "delete"
                    ? t.storageSpaces.deleteNotice
                    : t.storageSpaces.acceptStateNotice}
            </DialogDescription>
          </DialogHeader>
          {confirmation?.running && (
            <p>
              {owner
                ? t.agentInstances.settings.runningNow(owner.name)
                : t.storageSpaces.agentFilesRecoveryUnknown}
            </p>
          )}
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
                void performConfirmation();
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

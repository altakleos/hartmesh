"use client";

import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  getInstance,
  type AgentInstanceDetail,
} from "@/core/agent-instances/api";
import {
  describeRecoveryChange,
  recoveryAccessLost,
  recoveryOutcome,
  sameChange,
  workspaceReady,
  type FileChange,
  type RecoveryResult,
} from "@/core/agent-instances/file-recovery";
import {
  canManageInstance,
  hasCeiling,
} from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import {
  acceptSpaceCurrentState,
  getSpaceRecovery,
  getSpaceRecoveryOperation,
  type SpaceRecoveryOperation,
  type StorageSpace,
} from "@/core/spaces/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

const minute = 60_000;
function ReaderNotice({
  name,
  supervisor,
  reading,
}: {
  name: string;
  supervisor: string;
  reading: number;
}) {
  const { t } = useI18n();
  const [first] = useState(reading);
  return reading - first >= minute ? (
    <p role="status">
      {t.agentInstances.files.recovery.reader(name, supervisor)}
    </p>
  ) : null;
}

export function FileRecoveryNotice({
  instance,
  space,
  reading,
  supervisor,
  refreshSpace,
}: {
  instance: AgentInstanceDetail;
  space?: StorageSpace;
  reading: number;
  supervisor: string;
  refreshSpace: () => Promise<StorageSpace | undefined>;
}) {
  const { user } = useAuth();
  if (
    !user ||
    !space ||
    !instance.home_generation ||
    space.id !== instance.home_id ||
    space.generation !== instance.home_generation ||
    instance.status !== "active" ||
    space.status !== "active" ||
    !(space.permissions & 1)
  )
    return null;
  if (
    !canManageInstance(instance.permissions) ||
    !hasCeiling(user.permissions, "agents:write") ||
    !(space.permissions & 8)
  )
    return reading && space.storage_state === "recovery-pending" ? (
      <ReaderNotice
        name={instance.name}
        reading={reading}
        supervisor={supervisor}
      />
    ) : null;
  return (
    <ManagerNotice
      key={space.generation}
      instance={instance}
      space={space}
      reading={reading}
      refreshSpace={refreshSpace}
    />
  );
}

function ManagerNotice({
  instance,
  space,
  reading,
  refreshSpace,
}: {
  instance: AgentInstanceDetail;
  space: StorageSpace;
  reading: number;
  refreshSpace: () => Promise<StorageSpace | undefined>;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const branding = useBranding();
  const copy = t.agentInstances.files.recovery;
  const support =
    user?.system_role === "admin"
      ? (branding.providerName ?? t.agentInstances.providerFallback)
      : t.agentInstances.administratorSupport;
  const pending = !!reading && space.storage_state === "recovery-pending";
  const [lostAccess, setLostAccess] = useState(false);
  const listing = useQuery({
    queryKey: [
      "home-file-recovery",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance.id,
      instance.generation,
      instance.permissions,
      space.id,
      space.generation,
      space.permissions,
    ],
    enabled: pending && !lostAccess,
    queryFn: ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return awaitAbortable(getSpaceRecovery(space.id, deadline), deadline);
    },
    retry: false,
    gcTime: 0,
    staleTime: 0,
    refetchInterval: minute,
  });
  const capture = useSpaceActionSignal();
  const inFlight = useRef(false);
  const [checking, setChecking] = useState(false);
  const [selection, setSelection] = useState<SpaceRecoveryOperation>();
  const [acknowledged, setAcknowledged] = useState(false);
  const [result, setResult] = useState<RecoveryResult>();
  const rows =
    pending && listing.isSuccess && !listing.isError
      ? (listing.data.operations ?? []).filter(
          (row) =>
            row.request &&
            row.phase === "pending" &&
            row.generation === space.generation &&
            /^[0-9a-f]{32}$/.test(row.operation_id) &&
            Date.now() - Date.parse(row.created_at ?? "") >= 5 * minute,
        )
      : [];
  const changes = rows.flatMap((row) => {
    const change = describeRecoveryChange(row, copy);
    return change ? [change] : [];
  });
  const selected =
    selection && changes.find((change) => sameChange(change.row, selection));
  useEffect(() => {
    if (selection && !selected) {
      setSelection(undefined);
      setAcknowledged(false);
    }
  }, [selection, selected]);
  const lifecycle = rows.some(
    (row) =>
      row.request.action === "backup" || row.request.action === "restore",
  );

  async function check(change: FileChange, submit: boolean) {
    if (inFlight.current || (submit && !acknowledged)) return;
    const owner = capture();
    if (owner.aborted) return;
    inFlight.current = true;
    setChecking(true);
    setSelection(undefined);
    setAcknowledged(false);
    setResult(undefined);
    const { operation_id, generation } = change.row;
    try {
      if (submit) {
        const signal = AbortSignal.any([owner, AbortSignal.timeout(30_000)]);
        try {
          await awaitAbortable(
            acceptSpaceCurrentState(
              space.id,
              generation,
              operation_id,
              signal,
              { fileChangeOnly: true },
            ),
            signal,
          );
        } catch {
          // An unsuccessful response can arrive after the durable result was recorded.
        }
      }
      if (owner.aborted) return;
      const signal = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
      const [record, home, employee] = await Promise.allSettled([
        awaitAbortable(
          getSpaceRecoveryOperation(space.id, generation, operation_id, signal),
          signal,
        ),
        awaitAbortable(refreshSpace(), signal),
        awaitAbortable(getInstance(instance.id, signal), signal),
      ]);
      if (owner.aborted) return;
      if (
        (record.status === "rejected" && recoveryAccessLost(record.reason)) ||
        (home.status === "rejected" && recoveryAccessLost(home.reason, true)) ||
        (employee.status === "rejected" &&
          recoveryAccessLost(employee.reason, true)) ||
        (home.status === "fulfilled" &&
          home.value &&
          !(home.value.permissions & 8)) ||
        (employee.status === "fulfilled" &&
          !canManageInstance(employee.value.permissions))
      ) {
        setLostAccess(true);
        return;
      }
      const current =
        employee.status === "fulfilled" ? employee.value : undefined;
      const available = home.status === "fulfilled" ? home.value : undefined;
      const ready =
        !!available &&
        available.id === space.id &&
        available.generation === generation &&
        available.status === "active" &&
        available.storage_state === "available" &&
        current?.id === instance.id &&
        current.generation === instance.generation &&
        current.status === "active" &&
        current.home_id === space.id &&
        current.home_generation === generation &&
        workspaceReady(current);
      setResult({ change, status: recoveryOutcome(record, change.row), ready });
      if (available?.storage_state === "recovery-pending")
        void listing.refetch();
    } finally {
      if (!owner.aborted) {
        inFlight.current = false;
        setChecking(false);
      }
    }
  }

  if (lostAccess || recoveryAccessLost(listing.error, true)) return null;
  return (
    <>
      {lifecycle && (
        <p role="status">{copy.lifecycle(instance.name, support)}</p>
      )}
      {changes.length > 0 && (
        <section className="space-y-2 rounded-md border p-3">
          <p>{copy.pending(instance.name)}</p>
          {changes.map((change) => (
            <div key={change.row.operation_id} className="space-y-1">
              <p className="break-words">{change.description}</p>
              <Button
                type="button"
                variant="outline"
                disabled={checking}
                onClick={() => {
                  setSelection(change.row);
                  setAcknowledged(false);
                }}
              >
                {copy.accept}
              </Button>
            </div>
          ))}
        </section>
      )}
      <Dialog
        open={!!selected}
        onOpenChange={(open) => {
          if (!open) {
            setSelection(undefined);
            setAcknowledged(false);
          }
        }}
      >
        {selected && (
          <DialogContent>
            <DialogTitle>{copy.accept}</DialogTitle>
            <DialogDescription>{copy.confirm(instance.name)}</DialogDescription>
            <p className="break-words">{selected.description}</p>
            <p className="break-words">
              {copy.inspect(selected.folder, selected.file)}
            </p>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={acknowledged}
                onChange={(e) => setAcknowledged(e.target.checked)}
              />
              {copy.acknowledge}
            </label>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setSelection(undefined)}
              >
                {t.common.cancel}
              </Button>
              <Button
                type="button"
                disabled={!acknowledged || checking}
                onClick={() => void check(selected, true)}
              >
                {copy.accept}
              </Button>
            </DialogFooter>
          </DialogContent>
        )}
      </Dialog>
      {checking && <p role="status">{t.agentInstances.loading}</p>}
      {result && (
        <section className="space-y-1" role="status">
          <p className="break-words">
            {result.status === "accepted"
              ? copy.accepted(result.change.folder, result.change.file)
              : copy[result.status]}
          </p>
          {result.status === "accepted" && (
            <p>
              {result.ready &&
              reading &&
              space.storage_state === "available" &&
              workspaceReady(instance)
                ? copy.ready(instance.name)
                : copy.unconfirmed(instance.name)}
            </p>
          )}
          <Button
            type="button"
            variant="outline"
            onClick={() => void check(result.change, false)}
          >
            {t.agentInstances.files.checkAgain}
          </Button>
        </section>
      )}
    </>
  );
}

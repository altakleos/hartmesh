"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { usePersonNames } from "@/components/workspace/person-name";
import * as api from "@/core/agent-instances/api";
import {
  agentBlock,
  hasCeiling,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { EXECUTION_REASONS } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { AgentAvailabilityNotice } from "./agent-header";

type ChangeInput = Omit<api.LifecycleChange, "operation_id" | "generation">;
type Outcome =
  | "complete"
  | "pending"
  | "unconfirmed"
  | "refused"
  | "interrupted";
type Phase = "sending" | "pending" | "unconfirmed" | "interruption" | "waiting";
type Receipt = {
  body: api.LifecycleChange;
  phase: Phase;
  running?: api.AgentInstanceDetail["activity"]["running"];
};
type Result = Awaited<ReturnType<typeof api.changeInstanceLifecycle>>;
interface Actions {
  instance: api.AgentInstanceDetail;
  operations: api.LifecycleOperation[];
  busy: boolean;
  pending: boolean;
  receipt: Receipt | null;
  failure: unknown;
  abandonFailure: unknown;
  lastApplied: {
    body: api.LifecycleChange;
    result: Result;
    unverified: boolean;
  } | null;
  change: (input: ChangeInput) => Promise<Outcome>;
  retry: () => Promise<Outcome>;
  wait: () => void;
  now: () => void;
  cancel: () => void;
  abandon: (body: {
    generation: number;
    operation_id: string;
  }) => Promise<boolean>;
  canApply: (action?: api.LifecycleChange["action"]) => boolean;
  modal: boolean;
  registerModal: () => () => void;
  canManage: boolean;
  refresh: () => Promise<void>;
}
const Context = createContext<Actions | null>(null);
type Props = {
  instance: api.AgentInstanceDetail;
  operations: api.LifecycleOperation[];
  refresh: () => void | Promise<void>;
  observationEpoch?: number;
  onDeletePendingChange?: (pending: boolean) => void;
  children: ReactNode;
};
export function LifecycleActionsProvider(props: Props) {
  const { user } = useAuth();
  return (
    <Coordinator
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.permissions}`}
      {...props}
    />
  );
}
export function useLifecycleActions() {
  const context = useContext(Context);
  if (!context)
    throw new Error("Lifecycle actions require their page provider");
  return context;
}

function Coordinator({
  instance,
  operations,
  refresh,
  children,
  observationEpoch,
  onDeletePendingChange,
}: Props) {
  const { user } = useAuth();
  const captureSignal = useSpaceActionSignal();
  const feature = useAgentInstances();
  const [modalCount, setModalCount] = useState(0);
  const registerModal = useCallback(() => {
    setModalCount((count) => count + 1);
    return () => setModalCount((count) => count - 1);
  }, []);
  const [authorityRetired, setAuthorityRetired] = useState(false);
  const canManage =
    !authorityRetired &&
    !!user &&
    (instance.permissions & (INSPECT | MANAGE)) === (INSPECT | MANAGE) &&
    hasCeiling(user?.permissions, "agents:write");
  const [receipt, setReceipt] = useState<Receipt | null>(null);
  const captured = useRef<Receipt | null>(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [failure, setFailure] = useState<unknown>();
  const [abandonFailure, setAbandonFailure] = useState<unknown>();
  const deleteContexts = useRef(new Map<string, boolean>());
  const [lastApplied, setLastApplied] = useState<Actions["lastApplied"]>(null);
  const current = useRef(instance);
  current.current = instance;
  const serverPending = operations.find((operation) => !operation.complete);
  const admission = useRef({ feature, canManage, observationEpoch });
  admission.current = { feature, canManage, observationEpoch };
  const observed = useRef<{
    epoch?: number;
    baseline: api.AgentInstanceDetail;
    detail: api.AgentInstanceDetail;
  } | null>(null);
  const completionReads = useRef(new Set<string>());
  const deletePending = receipt?.body.action === "delete";
  useEffect(() => {
    onDeletePendingChange?.(deletePending);
    return () => onDeletePendingChange?.(false);
  }, [deletePending, onDeletePendingChange]);
  const canApply = useCallback(
    (
      action?: api.LifecycleChange["action"],
      detail = observed.current?.epoch === admission.current.observationEpoch &&
      observed.current?.baseline === current.current
        ? observed.current.detail
        : current.current,
    ) => {
      if (
        !admission.current.canManage ||
        (detail.permissions & (INSPECT | MANAGE)) !== (INSPECT | MANAGE)
      )
        return false;
      const block = agentBlock(detail, admission.current.feature);
      return (
        !block || (block.reason === "space-unverified" && action === "delete")
      );
    },
    [],
  );
  const remember = useCallback((value: Receipt | null) => {
    captured.current = value;
    setReceipt(value);
  }, []);
  useEffect(() => {
    const held = captured.current;
    if (!held || !["pending", "unconfirmed"].includes(held.phase)) return;
    const terminal = operations.find(
      (operation) =>
        operation.operation_id === held.body.operation_id &&
        operation.generation === held.body.generation &&
        operation.complete,
    );
    if (!terminal) return;
    if (held.body.action === "delete" && !terminal.abandoned) {
      if (
        !completionReads.current.has(held.body.operation_id) &&
        canApply("delete")
      ) {
        completionReads.current.add(held.body.operation_id);
        void executeCurrent.current(held.body);
      }
      return;
    }
    remember(null);
    setFailure(undefined);
    if (!terminal.abandoned)
      setLastApplied({
        body: held.body,
        unverified: deleteContexts.current.get(held.body.operation_id) === true,
        result: {
          instance: current.current,
          complete: true,
          operation_id: terminal.operation_id,
        },
      });
  }, [
    operations,
    receipt?.body.operation_id,
    receipt?.phase,
    remember,
    canApply,
  ]);
  const refreshCurrent = useRef(refresh);
  refreshCurrent.current = refresh;
  const safeRefresh = useCallback(async () => {
    try {
      await refreshCurrent.current();
    } catch {
      /* Confirmed actions stay confirmed when a refresh fails. */
    }
  }, []);
  async function execute(
    body: api.LifecycleChange,
    waiting = false,
    emptyRetry = true,
  ): Promise<Outcome> {
    if (busyRef.current || !canApply(body.action)) return "refused";
    if (
      body.action === "delete" &&
      !deleteContexts.current.has(body.operation_id)
    )
      deleteContexts.current.set(
        body.operation_id,
        current.current.execution.reason === "space-unverified",
      );
    const owner = captureSignal();
    busyRef.current = true;
    setBusy(true);
    setFailure(undefined);
    remember({ body, phase: "sending" });
    try {
      const result = await awaitAbortable(
        api.changeInstanceLifecycle(instance.id, body, owner),
        owner,
      );
      owner.throwIfAborted();
      if (result.complete) {
        remember(null);
        setLastApplied({
          body,
          result,
          unverified: deleteContexts.current.get(body.operation_id) === true,
        });
      } else remember({ body, phase: "pending" });
      await safeRefresh();
      owner.throwIfAborted();
      return result.complete ? "complete" : "pending";
    } catch (error) {
      if (owner.aborted) return "refused";
      if (
        error instanceof api.InstanceApiError &&
        error.reason === "work-in-flight"
      ) {
        const runIDs = Array.isArray(error.extra.run_ids)
          ? error.extra.run_ids.filter(
              (id): id is string => typeof id === "string",
            )
          : [];
        let latest: api.AgentInstanceDetail | undefined;
        try {
          latest = await awaitAbortable(
            api.getInstance(instance.id, owner),
            owner,
          );
        } catch {
          if (owner.aborted) return "refused";
        }
        if (owner.aborted) return "refused";
        if (
          latest &&
          (latest.permissions & (INSPECT | MANAGE)) !== (INSPECT | MANAGE)
        ) {
          setAuthorityRetired(true);
          remember(null);
          setFailure(error);
          void safeRefresh();
          return "refused";
        }
        if (latest) {
          observed.current = {
            epoch: admission.current.observationEpoch,
            baseline: current.current,
            detail: latest,
          };
          if (!canApply(body.action, latest)) {
            const block = agentBlock(latest, admission.current.feature);
            remember(null);
            setFailure(
              new api.InstanceApiError(
                409,
                "",
                "execution-unavailable",
                block?.reason ?? undefined,
              ),
            );
            void safeRefresh();
            return "refused";
          }
        }
        if (!runIDs.length && !waiting && emptyRetry) {
          busyRef.current = false;
          return await execute(body, false, false);
        }
        remember({
          body,
          phase: waiting ? "waiting" : "interruption",
          running:
            latest?.activity.running.filter((run) =>
              runIDs.includes(run.run_id),
            ) ?? [],
        });
        return "interrupted";
      }
      if (error instanceof api.InstanceApiError) {
        remember(null);
        setFailure(error);
        await safeRefresh();
        return "refused";
      }
      remember({ body, phase: "unconfirmed" });
      return "unconfirmed";
    } finally {
      if (!owner.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  function retry() {
    const body = captured.current?.body ?? serverPending?.retry;
    return body ? execute(body) : Promise.resolve<Outcome>("refused");
  }
  function now() {
    const body = captured.current?.body;
    if (!body) return;
    const force = { ...body };
    delete force.only_if_idle;
    void execute(force);
  }
  function wait() {
    const request = captured.current;
    if (request) remember({ ...request, phase: "waiting" });
  }
  function cancel() {
    if (!busyRef.current) {
      remember(null);
      setFailure(undefined);
    }
  }
  const executeCurrent = useRef(execute);
  executeCurrent.current = execute;
  useEffect(() => {
    if (receipt?.phase !== "waiting" || !canApply(receipt.body.action)) return;
    const controller = new AbortController();
    const owner = AbortSignal.any([controller.signal, captureSignal()]);
    const timer = setTimeout(() => {
      void (async () => {
        try {
          const latest = await awaitAbortable(
            api.getInstance(instance.id, owner),
            owner,
          );
          owner.throwIfAborted();
          if (
            (latest.permissions & (INSPECT | MANAGE)) !==
            (INSPECT | MANAGE)
          ) {
            setAuthorityRetired(true);
            remember(null);
            void safeRefresh();
            return;
          }
          if (!canApply(receipt.body.action, latest)) {
            remember({ ...receipt });
            void safeRefresh();
            return;
          }
          if (latest.activity.running_runs === 0)
            void executeCurrent.current(
              { ...receipt.body, only_if_idle: true },
              true,
            );
          else if (captured.current?.phase === "waiting")
            remember({ ...receipt });
        } catch {
          if (!owner.aborted && captured.current?.phase === "waiting")
            remember({ ...receipt });
        }
      })();
    }, 5000);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
    // Every iteration creates one owned timer; only the waiting receipt advances it.
  }, [
    receipt,
    captureSignal,
    instance.id,
    remember,
    feature.known,
    feature.enabled,
    feature.execution,
    feature.reason,
    instance.execution.execution,
    instance.execution.reason,
    instance.home?.state,
    canApply,
    safeRefresh,
  ]);
  async function abandon(body: { generation: number; operation_id: string }) {
    if (busyRef.current || !canApply("delete")) return false;
    const owner = captureSignal();
    busyRef.current = true;
    setBusy(true);
    setAbandonFailure(undefined);
    try {
      await awaitAbortable(
        api.abandonInstanceLifecycle(instance.id, body, owner),
        owner,
      );
      owner.throwIfAborted();
      remember(null);
      await safeRefresh();
      return true;
    } catch (error) {
      if (!owner.aborted) {
        setAbandonFailure(error);
        await safeRefresh();
      }
      return false;
    } finally {
      if (!owner.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  return (
    <Context.Provider
      value={{
        instance,
        operations,
        busy,
        pending: !!(serverPending ?? receipt),
        receipt,
        failure,
        abandonFailure,
        lastApplied,
        change: (input) =>
          captured.current || serverPending || busyRef.current
            ? Promise.resolve("refused")
            : execute({
                ...input,
                generation: current.current.generation,
                operation_id: api.operationID(),
              }),
        retry,
        wait,
        now,
        cancel,
        abandon,
        canManage,
        refresh: safeRefresh,
        canApply,
        modal: modalCount > 0,
        registerModal,
      }}
    >
      {children}
    </Context.Provider>
  );
}

/** A tab's own confirmation displays its outcome inside that dialog. */
export function useLifecycleModal(open: boolean) {
  const { registerModal } = useLifecycleActions();
  useEffect(() => (open ? registerModal() : undefined), [open, registerModal]);
}

export function LifecycleFailure({ error }: { error: unknown }) {
  const { instance } = useLifecycleActions();
  const { t } = useI18n();
  return error instanceof api.InstanceApiError &&
    error.reason === "execution-unavailable" ? (
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
    <p role="alert">{t.agentInstances.operationError}</p>
  );
}

export function LifecycleState() {
  const actions = useLifecycleActions();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const [givingUp, setGivingUp] = useState<{
    generation: number;
    operation_id: string;
  } | null>(null);
  const server = actions.operations.find((operation) => !operation.complete);
  const unknown = actions.receipt?.phase === "unconfirmed";
  const prompt = actions.receipt?.phase === "interruption";
  const waiting = actions.receipt?.phase === "waiting";
  const abandoned = actions.operations[0]?.abandoned;
  return (
    <div className="space-y-3">
      {!actions.modal &&
        (!!server || unknown || actions.receipt?.phase === "pending") && (
          <div role="status" className="space-y-2">
            <p>
              {unknown ? copy.unconfirmed : copy.pending(actions.instance.name)}
            </p>
            {actions.canManage && (
              <div className="flex gap-2">
                <Button
                  type="button"
                  disabled={
                    actions.busy ||
                    !actions.canApply(
                      actions.receipt?.body.action ?? server?.action,
                    ) ||
                    (!actions.receipt && !server?.retry)
                  }
                  onClick={() => void actions.retry()}
                >
                  {copy.retry}
                </Button>
                {server && (
                  <Button
                    type="button"
                    variant="outline"
                    disabled={actions.busy || !actions.canApply("delete")}
                    onClick={() =>
                      setGivingUp({
                        generation: actions.instance.generation,
                        operation_id: server.operation_id,
                      })
                    }
                  >
                    {copy.abandon}
                  </Button>
                )}
              </div>
            )}
          </div>
        )}
      {waiting && (
        <div role="status">
          <p>{copy.interrupt.pending(actions.instance.name)}</p>
          <Button type="button" variant="outline" onClick={actions.cancel}>
            {t.common.cancel}
          </Button>
        </div>
      )}
      {abandoned && <p role="status">{copy.abandoned}</p>}
      {!actions.modal && !!actions.failure && (
        <LifecycleFailure error={actions.failure} />
      )}
      <InterruptionDialog open={prompt} />
      <Dialog
        open={givingUp !== null}
        onOpenChange={(open) => {
          if (!open && !actions.busy) setGivingUp(null);
        }}
      >
        <DialogContent showCloseButton={!actions.busy}>
          <DialogTitle>{copy.abandon}</DialogTitle>
          <DialogDescription>
            {copy.settings.abandonConfirm(actions.instance.name)}
          </DialogDescription>
          {!!actions.abandonFailure &&
            (actions.abandonFailure instanceof api.InstanceApiError &&
            actions.abandonFailure.reason === "execution-unavailable" ? (
              <LifecycleFailure error={actions.abandonFailure} />
            ) : (
              <p role="alert">
                {actions.abandonFailure instanceof api.InstanceApiError &&
                actions.abandonFailure.status === 409
                  ? copy.settings.abandonRefused
                  : actions.abandonFailure instanceof api.InstanceApiError
                    ? copy.operationError
                    : copy.unconfirmed}
              </p>
            ))}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={actions.busy}
              onClick={() => setGivingUp(null)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={actions.busy || !actions.canApply("delete")}
              onClick={() => {
                if (!givingUp) return;
                void actions.abandon(givingUp).then((done) => {
                  if (done) setGivingUp(null);
                });
              }}
            >
              {actions.abandonFailure ? copy.retry : copy.abandon}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
function InterruptionDialog({ open }: { open: boolean }) {
  const actions = useLifecycleActions();
  const { t } = useI18n();
  const copy = t.agentInstances.interrupt;
  const name = actions.instance.name;
  const kind = actions.receipt?.body.action;
  const change =
    kind === "grant"
      ? copy.grant
      : kind === "supervise"
        ? copy.supervise
        : copy.adopt;
  const running = actions.receipt?.running ?? [];
  return (
    <Dialog
      open={open}
      onOpenChange={(value) => {
        if (!value && !actions.busy) actions.cancel();
      }}
    >
      <DialogContent aria-describedby={undefined}>
        <DialogTitle>{copy.title(name)}</DialogTitle>
        {running.length ? (
          <InterruptionBody change={change} name={name} running={running} />
        ) : (
          <p>{copy.bodyUnknown(change, name)}</p>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={actions.busy}
            onClick={actions.cancel}
          >
            {t.common.cancel}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={
              actions.busy || !actions.canApply(actions.receipt?.body.action)
            }
            onClick={actions.now}
          >
            {copy.now}
          </Button>
          <Button
            type="button"
            disabled={
              actions.busy || !actions.canApply(actions.receipt?.body.action)
            }
            onClick={actions.wait}
          >
            {copy.wait(name)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
function InterruptionBody({
  change,
  name,
  running,
}: {
  change: string;
  name: string;
  running: api.AgentInstanceDetail["activity"]["running"];
}) {
  const { t } = useI18n();
  const people = usePersonNames(
    running.filter((run) => !run.work_id).map((run) => run.requester_id),
    true,
  );
  const entries = running.map((run) =>
    run.work_id
      ? t.agentInstances.interrupt.work(run.objective ?? "…")
      : t.agentInstances.interrupt.reply(people.get(run.requester_id) ?? "…"),
  );
  return (
    <p>{t.agentInstances.interrupt.body(change, name, entries.join(", "))}</p>
  );
}

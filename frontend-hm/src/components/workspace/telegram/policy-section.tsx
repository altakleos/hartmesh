"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useId, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import type { AgentInstance } from "@/core/agent-instances/api";
import {
  canManageInstance,
  hasCeiling,
} from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useTelegram } from "@/core/features/hooks";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import {
  getTelegramPolicy,
  saveTelegramPolicy,
  TelegramRefusal,
  type TelegramPolicy,
} from "@/core/telegram-bots/api";
import {
  telegramBotsQueryPrefix,
  useRefreshTelegramBots,
} from "@/core/telegram-bots/hooks";

/** Prepared independently of page activation. The service still checks current Manage. */
export function TelegramPolicySection({
  instance,
}: {
  instance: AgentInstance;
}) {
  const { user } = useAuth();
  const { known } = useTelegram();
  if (
    !known ||
    !user ||
    instance.custody !== "company" ||
    instance.status === "deleted" ||
    !canManageInstance(instance.permissions) ||
    !hasCeiling(user.permissions, "agents:read")
  )
    return null;
  const scope = JSON.stringify([
    user.id,
    user.system_role,
    user.permissions,
    instance.id,
    instance.generation,
    instance.permissions,
    instance.status,
  ]);
  return (
    <PolicyControls
      key={scope}
      instance={instance}
      scope={scope}
      writable={hasCeiling(user.permissions, "agents:write")}
    />
  );
}

function PolicyControls({
  instance,
  scope,
  writable,
}: {
  instance: AgentInstance;
  scope: string;
  writable: boolean;
}) {
  const { t } = useI18n();
  const copy = t.telegramBots.policy;
  const name = { name: instance.name };
  const queryClient = useQueryClient();
  const refresh = useRefreshTelegramBots();
  const lifetime = useFileActionLifetime();
  const capture = useSpaceActionSignal(scope);
  const queryKey = [...telegramBotsQueryPrefix, "policy", scope];
  const policy = useQuery({
    queryKey,
    queryFn: ({ signal }) =>
      getTelegramPolicy(instance.id, AbortSignal.any([signal, capture()])),
    retry: false,
  });
  const [choice, setChoice] = useState<TelegramPolicy | null>(null);
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const saving = useRef(false);
  const id = useId();
  const data = !policy.isError ? policy.data : undefined;
  const selected = choice ?? data?.policy;
  const disabled = !writable || busy || !data;
  async function save(value: TelegramPolicy) {
    if (disabled || saving.current) return;
    saving.current = true;
    setBusy(true);
    const signal = capture();
    try {
      const saved = await saveTelegramPolicy(instance.id, value, signal);
      if (signal.aborted) return;
      queryClient.setQueryData(queryKey, saved);
      setChoice(null);
      setConfirm(false);
      lifetime.toasts.add(toast.success(copy.saved[value](name)));
      void refresh();
    } catch (error) {
      if (signal.aborted) return;
      setConfirm(false);
      setChoice(null);
      lifetime.toasts.add(toast.error(copy.failed));
      if (
        error instanceof TelegramRefusal &&
        error.reason === "manage-required"
      )
        void queryClient.invalidateQueries({ queryKey: ["agent-instances"] });
      await policy.refetch();
    } finally {
      saving.current = false;
      if (!signal.aborted) setBusy(false);
    }
  }
  return (
    <section className="space-y-4" aria-label={copy.heading}>
      <h3 className="font-medium">{copy.heading}</h3>
      {policy.isPending ? (
        <p role="status">{t.agentInstances.loading}</p>
      ) : !data ? (
        <div role="alert">
          <p>{copy.failed}</p>
          <Button onClick={() => void policy.refetch()}>
            {t.agentInstances.refresh}
          </Button>
        </div>
      ) : (
        <>
          <p>
            {copy.count({
              ...name,
              connected: data.connected,
              people: data.people,
            })}
          </p>
          <fieldset className="space-y-3" disabled={disabled}>
            <legend className="mb-3 font-medium">{copy.label(name)}</legend>
            {(["allowed", "protected", "off"] as const).map((value) => (
              <div key={value}>
                <label className="flex items-center gap-2">
                  <input
                    type="radio"
                    name={id}
                    value={value}
                    checked={selected === value}
                    disabled={disabled}
                    aria-describedby={`${id}-${value}`}
                    onChange={() => setChoice(value)}
                  />
                  {copy[value].title}
                </label>
                <p
                  id={`${id}-${value}`}
                  className="text-muted-foreground mt-1 text-sm"
                >
                  {copy[value].help(name)}
                </p>
              </div>
            ))}
          </fieldset>
          <Button
            disabled={disabled || selected === data.policy}
            onClick={() =>
              selected === "off"
                ? setConfirm(true)
                : selected && void save(selected)
            }
          >
            {copy.save}
          </Button>
          <Dialog
            open={confirm}
            onOpenChange={(open) => {
              if (!busy) setConfirm(open);
            }}
          >
            <DialogContent>
              <DialogTitle>{copy.offConfirm.title(name)}</DialogTitle>
              <DialogDescription>
                {data.connected === 0
                  ? copy.offConfirm.none
                  : copy.offConfirm.some({ count: data.connected })}
              </DialogDescription>
              <DialogFooter>
                <Button
                  variant="outline"
                  disabled={busy}
                  onClick={() => setConfirm(false)}
                >
                  {t.common.cancel}
                </Button>
                <Button disabled={disabled} onClick={() => void save("off")}>
                  {copy.offConfirm.button}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </>
      )}
    </section>
  );
}

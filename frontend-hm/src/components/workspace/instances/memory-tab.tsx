"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import * as api from "@/core/agent-instances/api";
import {
  hasCeiling,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

type Props = { instance: api.AgentInstance; lifecyclePending?: boolean };
export function MemoryTab(props: Props) {
  const { user } = useAuth();
  return (
    <MemoryContent
      key={`${user?.id}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.generation}:${props.instance.permissions}`}
      {...props}
    />
  );
}
function MemoryContent({ instance, lifecyclePending = false }: Props) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const client = useQueryClient();
  const id = useId();
  const captureSignal = useSpaceActionSignal();
  const canRead =
    !!user &&
    (instance.permissions & INSPECT) !== 0 &&
    hasCeiling(user.permissions, "memory:read") &&
    instance.status !== "deleted";
  const writable =
    canRead &&
    (instance.permissions & MANAGE) !== 0 &&
    hasCeiling(user?.permissions, "memory:write") &&
    instance.status === "active" &&
    !lifecyclePending;
  const key = [
    "agent-instances",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance.id,
    "memory",
    instance.generation,
  ];
  const memory = useQuery({
    queryKey: key,
    queryFn: ({ signal }) => api.getInstanceMemory(instance.id, signal),
    enabled: canRead,
    retry: false,
    staleTime: 0,
  });
  const [fact, setFact] = useState("");
  const [text, setText] = useState("");
  const [confirmation, setConfirmation] = useState<
    { action: "clear" } | { action: "replace"; document: unknown }
  >();
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string>();
  async function run(work: (signal: AbortSignal) => Promise<unknown>) {
    if (!writable || busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setError(undefined);
    const signal = captureSignal();
    try {
      await awaitAbortable(work(signal), signal);
      signal.throwIfAborted();
      setConfirmation(undefined);
      setFact("");
      await client.invalidateQueries({ queryKey: key });
    } catch {
      if (!signal.aborted) setError(copy.operationError);
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  function replace() {
    setError(undefined);
    if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) {
      setError(copy.memoryTooLarge);
      return;
    }
    try {
      const document: unknown = JSON.parse(text);
      setConfirmation({ action: "replace", document });
    } catch {
      setError(copy.memoryInvalid);
    }
  }
  if (!canRead) return null;
  const data = !memory.isError ? memory.data : undefined;
  const markdown = t.settings.memory.markdown;
  return (
    <section className="space-y-4">
      <h2 className="text-lg font-semibold">{copy.memory}</h2>
      <p>{copy.memoryNotice(instance.name)}</p>
      {memory.isPending && <p role="status">{copy.loading}</p>}
      {memory.isError && (
        <div role="alert">
          <p>{copy.memoryUnavailable(instance.name)}</p>
          <Button
            type="button"
            variant="outline"
            onClick={() => void memory.refetch()}
          >
            {copy.refresh}
          </Button>
        </div>
      )}
      {data && (
        <>
          <dl className="grid gap-3 sm:grid-cols-2">
            {[
              [markdown.work, data.user.workContext.summary],
              [markdown.personal, data.user.personalContext.summary],
              [markdown.topOfMind, data.user.topOfMind.summary],
              [markdown.recentMonths, data.history.recentMonths.summary],
              [markdown.earlierContext, data.history.earlierContext.summary],
              [
                markdown.longTermBackground,
                data.history.longTermBackground.summary,
              ],
            ]
              .filter(([, summary]) => summary)
              .map(([label, summary]) => (
                <div key={label}>
                  <dt className="font-medium">{label}</dt>
                  <dd className="whitespace-pre-wrap">{summary}</dd>
                </div>
              ))}
          </dl>
          <ul className="space-y-2">
            {data.facts.map((value) => (
              <li
                key={value.id}
                className="flex items-start justify-between gap-3 rounded border p-2"
              >
                <p className="whitespace-pre-wrap">{value.content}</p>
                {writable && (
                  <Button
                    type="button"
                    variant="outline"
                    disabled={busy}
                    onClick={() =>
                      void run((signal) =>
                        api.deleteInstanceFact(instance.id, value.id, signal),
                      )
                    }
                  >
                    {copy.removeFact}
                  </Button>
                )}
              </li>
            ))}
          </ul>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              const url = URL.createObjectURL(
                new Blob([JSON.stringify(data, null, 2)], {
                  type: "application/json",
                }),
              );
              const anchor = document.createElement("a");
              anchor.href = url;
              anchor.download = "instance-memory.json";
              anchor.click();
              URL.revokeObjectURL(url);
            }}
          >
            {copy.exportMemory}
          </Button>
          {writable && (
            <div className="space-y-3">
              <label htmlFor={`${id}-fact`}>{copy.fact}</label>
              <Input
                id={`${id}-fact`}
                value={fact}
                disabled={busy}
                onChange={(event) => setFact(event.target.value)}
              />
              <Button
                type="button"
                disabled={busy || !fact.trim()}
                onClick={() =>
                  void run((signal) =>
                    api.createInstanceFact(instance.id, fact, signal),
                  )
                }
              >
                {copy.addFact}
              </Button>
              <div>
                <Button
                  type="button"
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    setError(undefined);
                    setConfirmation({ action: "clear" });
                  }}
                >
                  {copy.clearMemory}
                </Button>
              </div>
              <label htmlFor={`${id}-import`}>{copy.importMemory}</label>
              <Textarea
                id={`${id}-import`}
                className="min-h-24"
                value={text}
                disabled={busy}
                onChange={(event) => setText(event.target.value)}
              />
              <Button
                type="button"
                variant="outline"
                disabled={busy || !text.trim()}
                onClick={replace}
              >
                {copy.replaceMemory}
              </Button>
            </div>
          )}
        </>
      )}
      {error && !confirmation && <p role="alert">{error}</p>}
      <Dialog
        open={!!confirmation}
        onOpenChange={(open) => {
          if (!open && !busy) setConfirmation(undefined);
        }}
      >
        <DialogContent showCloseButton={!busy}>
          <DialogTitle>
            {confirmation?.action === "clear"
              ? copy.clearMemory
              : copy.replaceMemory}
          </DialogTitle>
          <DialogDescription>
            {copy.confirmMemory(instance.name)}
          </DialogDescription>
          {error && <p role="alert">{error}</p>}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => setConfirmation(undefined)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={busy}
              onClick={() => {
                if (confirmation)
                  void run((signal) =>
                    confirmation.action === "clear"
                      ? api.clearInstanceMemory(instance.id, signal)
                      : api.importInstanceMemory(
                          instance.id,
                          confirmation.document,
                          signal,
                        ),
                  );
              }}
            >
              {confirmation?.action === "clear"
                ? copy.clearMemory
                : copy.replaceMemory}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}

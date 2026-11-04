"use client";

import { useQueryClient } from "@tanstack/react-query";
import { Clock3Icon, RotateCcwIcon, XIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { useI18n } from "@/core/i18n/hooks";
import {
  changeRunInput,
  runInputsKey,
  type RunInput,
} from "@/core/threads/run-inputs";

/**
 * Queue/Steer inputs that have not reached their run yet. Each row can be
 * removed while it is still waiting, or sent again once the run is over.
 * The Gateway decides every status; a change only asks and then re-reads.
 */
export function ThreadRunInputs({
  account,
  threadId,
  isRunning,
  inputs,
}: {
  /** The signed-in account; a switch retires any change still in flight. */
  account?: string | null;
  threadId?: string;
  isRunning: boolean;
  inputs: readonly RunInput[];
}) {
  const { t } = useI18n();
  const client = useQueryClient();
  const [busy, setBusy] = useState<string | null>(null);
  const action = useRef<AbortController | null>(null);
  useEffect(() => {
    setBusy(null);
    return () => action.current?.abort();
  }, [account, threadId]);
  const items = inputs.filter(
    (input) => input.status !== "applied" && input.status !== "cancelled",
  );

  async function change(input: RunInput, operation: "cancel" | "retry") {
    if (!threadId || busy) return;
    const controller = new AbortController();
    action.current = controller;
    setBusy(input.id);
    try {
      await changeRunInput(threadId, input, operation, controller.signal);
      if (!controller.signal.aborted) {
        await client.invalidateQueries({ queryKey: runInputsKey(threadId) });
        await client.invalidateQueries({ queryKey: ["thread", threadId] });
      }
    } catch {
      if (!controller.signal.aborted)
        toast.error(t.inputBox.runInputUnavailable);
    } finally {
      if (!controller.signal.aborted) setBusy(null);
    }
  }

  if (!threadId || !items.length) return null;
  return (
    <div
      className="bg-background mb-2 max-h-36 space-y-1 overflow-y-auto rounded-lg border px-3 py-2"
      data-testid="run-inputs"
      aria-live="polite"
    >
      {items.map((input) => (
        <div
          key={`${input.target_run_id}:${input.id}`}
          className="flex min-w-0 items-center gap-2 text-sm"
          data-testid="run-input-item"
          data-input-status={input.status}
        >
          <Clock3Icon
            className="text-muted-foreground size-3.5 shrink-0"
            aria-hidden
          />
          <span className="text-muted-foreground shrink-0 text-xs">
            {input.status === "not_applied"
              ? t.inputBox.runInputUnapplied
              : input.status === "dispatching"
                ? t.inputBox.runInputDispatching
                : input.mode === "queue"
                  ? t.inputBox.runInputQueued
                  : t.inputBox.runInputSteering}
          </span>
          <span className="min-w-0 flex-1 truncate" title={input.text}>
            {input.text}
          </span>
          {!isRunning && (
            <Button
              size="icon-sm"
              variant="ghost"
              aria-label={t.inputBox.runInputRetry}
              title={t.inputBox.runInputRetry}
              disabled={busy !== null}
              onClick={() => void change(input, "retry")}
            >
              <RotateCcwIcon className="size-3.5" />
            </Button>
          )}
          {input.status !== "dispatching" && input.status !== "applying" && (
            <Button
              size="icon-sm"
              variant="ghost"
              aria-label={t.inputBox.runInputCancel}
              title={t.inputBox.runInputCancel}
              disabled={busy !== null}
              onClick={() => void change(input, "cancel")}
            >
              <XIcon className="size-3.5" />
            </Button>
          )}
        </div>
      ))}
    </div>
  );
}

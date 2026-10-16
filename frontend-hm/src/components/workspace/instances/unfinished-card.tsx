"use client";

import { useRouter } from "next/navigation";
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import * as api from "@/core/agent-instances/api";
import {
  clearCreation,
  matchesCreation,
  readCreation,
} from "@/core/agent-instances/unfinished";
import { deleteAgent } from "@/core/agents/api";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import {
  CreationFailureNotice,
  type CreationFailure,
} from "./creation-failure";

export function UnfinishedCard({
  entry,
  executable,
  notice,
  refresh,
  providerName,
  supportURL,
}: {
  entry: Pick<api.UnfinishedCreation, "id" | "name" | "custody">;
  executable: boolean;
  notice?: React.ReactNode;
  refresh: () => void;
  providerName?: string | null;
  supportURL?: string | null;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const router = useRouter();
  const captureSignal = useSpaceActionSignal(entry.id);
  const busyRef = useRef(false);
  const [busy, setBusy] = useState(false);
  const [removed, setRemoved] = useState(false);
  const [message, setMessage] = useState<string>();
  const [failure, setFailure] = useState<CreationFailure>();
  const remembered = user ? readCreation(user.id) : null;
  const request =
    remembered && matchesCreation(remembered, entry) ? remembered : null;

  async function act(remove: boolean) {
    if (busyRef.current || !user || (!remove && (!request || !executable)))
      return;
    busyRef.current = true;
    setBusy(true);
    setMessage(undefined);
    setFailure(undefined);
    const signal = captureSignal();
    try {
      if (remove) {
        let existed = true;
        try {
          await awaitAbortable(api.removeUnfinished(entry.id, signal), signal);
        } catch (error) {
          if (error instanceof api.InstanceApiError && error.status === 404)
            existed = false;
          else throw error;
        }
        signal.throwIfAborted();
        if (request) {
          clearCreation(user.id);
          if (existed && request.definition_name) {
            const deadline = AbortSignal.any([
              signal,
              AbortSignal.timeout(20_000),
            ]);
            await awaitAbortable(
              deleteAgent(request.definition_name, deadline),
              deadline,
            ).catch(() => undefined);
            signal.throwIfAborted();
          }
        }
        setRemoved(true);
        refresh();
      } else if (request) {
        const created = await awaitAbortable(
          request.copy_of === null
            ? api.createInstance(request.body, signal)
            : api.createCompanyCopy(request.copy_of, request.body, signal),
          signal,
        );
        signal.throwIfAborted();
        clearCreation(user.id);
        try {
          const chat = await awaitAbortable(
            api.createInstanceConversation(
              created.id,
              api.operationID(),
              signal,
            ),
            signal,
          );
          signal.throwIfAborted();
          refresh();
          router.push(
            `/workspace/chats/${encodeURIComponent(chat.thread_id)}?welcome=1`,
          );
        } catch {
          if (!signal.aborted) {
            refresh();
            router.push(`/workspace/instances?instance=${created.id}&added=1`);
          }
        }
      }
    } catch (error) {
      if (signal.aborted) return;
      if (
        remove &&
        error instanceof api.InstanceApiError &&
        error.reason === "not-removable"
      ) {
        setMessage(copy.unfinishedCompleted(entry.name));
        refresh();
      } else if (!remove && request) {
        if (error instanceof api.InstanceApiError) {
          clearCreation(user.id);
          if (request.definition_name) {
            const deadline = AbortSignal.any([
              signal,
              AbortSignal.timeout(20_000),
            ]);
            await awaitAbortable(
              deleteAgent(request.definition_name, deadline),
              deadline,
            ).catch(() => undefined);
          }
          if (!signal.aborted) {
            setFailure({ type: "refused", error });
            refresh();
          }
        } else setFailure({ type: "unconfirmed" });
      } else setMessage(copy.operationError);
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  if (removed) return null;
  return (
    <article className="space-y-3 rounded-lg border p-4">
      <h3 className="font-medium">{entry.name}</h3>
      <p>{copy.unfinished(entry.name)}</p>
      {!request && (
        <p className="text-muted-foreground text-sm">
          {copy.unfinishedElsewhere(entry.name)}
        </p>
      )}
      <div className="flex gap-2">
        {request && (
          <Button
            type="button"
            disabled={busy || !executable}
            onClick={() => void act(false)}
          >
            {copy.finishSetup}
          </Button>
        )}
        <Button
          type="button"
          variant="outline"
          disabled={busy}
          onClick={() => void act(true)}
        >
          {copy.remove}
        </Button>
      </div>
      {request && !executable && notice}
      {message && <p role="status">{message}</p>}
      {failure && (
        <CreationFailureNotice
          failure={failure}
          kind={entry.custody}
          name={entry.name}
          unfinishedName={entry.name}
          providerName={providerName}
          supportURL={supportURL}
        />
      )}
    </article>
  );
}

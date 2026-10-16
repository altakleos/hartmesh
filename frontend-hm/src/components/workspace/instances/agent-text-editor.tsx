"use client";
import { useCallback, useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { putAgentFile } from "@/core/agent-instances/agent-files";
import * as api from "@/core/agent-instances/api";
import { readAgentText } from "@/core/agent-instances/file-reads";
import { awaitAbortable } from "@/core/api/abort";
import { useI18n } from "@/core/i18n/hooks";
import { StorageRequestError, type SpaceFile } from "@/core/spaces/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { AgentFileRefusal } from "./file-refusal";

export function AgentTextEditor({
  instance,
  home,
  file,
  editable,
  close,
  changed,
}: {
  instance: api.AgentInstance;
  home: string;
  file: SpaceFile;
  editable: boolean;
  close: () => void;
  changed: () => void;
}) {
  const { t } = useI18n();
  const copy = t.storageSpaces;
  const id = useId();
  const captureSignal = useSpaceActionSignal();
  const [text, setText] = useState("");
  const [revision, setRevision] = useState<string>();
  const [error, setError] = useState<unknown>();
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const pending = useRef<
    | {
        operation: string;
        text: string;
        revision: string;
      }
    | undefined
  >(undefined);
  const reader = useRef<AbortController | undefined>(undefined);
  const dirty = useRef(false);
  const [reading, setReading] = useState(false);
  const load = useCallback(
    async (keepDraft: boolean) => {
      reader.current?.abort();
      const controller = new AbortController();
      reader.current = controller;
      const owner = AbortSignal.any([controller.signal, captureSignal()]);
      const signal = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
      setReading(true);
      setError(undefined);
      pending.current = undefined;
      try {
        const value = await readAgentText(home, file.path, signal);
        if (owner.aborted) return;
        if (!keepDraft || !dirty.current) {
          setText(value.text);
          dirty.current = false;
        }
        setRevision(value.sha256);
      } catch (failure) {
        if (!owner.aborted) setError(failure);
      } finally {
        if (!owner.aborted) setReading(false);
      }
    },
    [home, file.path, captureSignal],
  );
  useEffect(() => {
    void load(false);
    return () => reader.current?.abort();
  }, [load]);
  async function save() {
    if (!editable || !revision || reading || busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setError(undefined);
    const signal = captureSignal();
    pending.current ??= { operation: api.operationID(), text, revision };
    const captured = pending.current;
    try {
      const result = await awaitAbortable(
        putAgentFile(
          instance.id,
          file.path,
          new Blob([captured.text], { type: "text/plain;charset=utf-8" }),
          captured.operation,
          { signal, expectedSha256: captured.revision },
        ),
        signal,
      );
      signal.throwIfAborted();
      setRevision(result.sha256);
      pending.current = undefined;
      changed();
      close();
    } catch (failure) {
      if (!signal.aborted) {
        setError(failure);
        if (
          failure instanceof api.InstanceApiError &&
          failure.reason !== "file-write-failed"
        )
          pending.current = undefined;
      }
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) close();
      }}
    >
      <DialogContent className="sm:max-w-3xl" showCloseButton={!busy}>
        <DialogTitle>{file.name}</DialogTitle>
        <DialogDescription>
          {editable ? copy.editorNotice : copy.utf8Notice}
        </DialogDescription>
        {revision !== undefined ? (
          <>
            <label htmlFor={id}>{copy.textContent}</label>
            <Textarea
              id={id}
              className="min-h-80 font-mono"
              value={text}
              readOnly={!editable}
              disabled={busy || reading}
              onChange={(event) => {
                dirty.current = true;
                pending.current = undefined;
                setText(event.target.value);
              }}
            />
          </>
        ) : (
          !error && <p role="status">{t.common.loading}</p>
        )}
        <p className="text-muted-foreground text-sm">{copy.utf8Notice}</p>
        {error instanceof StorageRequestError && error.status === 415 ? (
          <p role="status">{copy.noEditor}</p>
        ) : (
          !!error && (
            <AgentFileRefusal
              instance={instance}
              error={error}
              file={file.name}
              action
            />
          )
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={busy || reading}
            onClick={() => void load(true)}
          >
            {t.agentInstances.refresh}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={close}
          >
            {t.common.cancel}
          </Button>
          {editable && (
            <Button
              type="button"
              disabled={busy || reading || revision === undefined}
              onClick={() => void save()}
            >
              {t.common.save}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

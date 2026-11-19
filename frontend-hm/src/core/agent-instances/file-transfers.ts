import { useEffect, useRef, useState } from "react";

import { awaitAbortable } from "@/core/api/abort";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import {
  putAgentFile,
  uploadConversationFile,
  type SavedAgentFile,
} from "./agent-files";
import { InstanceApiError, operationID } from "./api";
import {
  checkFileOperation,
  fileOutcome,
  unansweredWrite,
  type CheckState,
  type FileLookup,
} from "./file-status";

export const MAX_AGENT_FILE_BYTES = 64 * 1024 * 1024;
export interface AgentTransfer {
  id: string;
  file: File;
  folder: string;
  status: "uploading" | "failed" | "saved";
  percentage: number;
  retryable: boolean;
  error?: unknown;
  result?: SavedAgentFile & { sandbox_path?: string };
  controller?: AbortController;
  lookup?: FileLookup;
  check?: CheckState;
  checkController?: AbortController;
}

/** Both file tabs and conversation attachments own their exact write identity. */
export function useAgentFileTransfers({
  instanceId,
  threadId,
  folder,
  scope = `${instanceId}:${threadId ?? "files"}`,
  enabled,
  changed,
}: {
  instanceId: string;
  threadId?: string;
  folder: string;
  scope?: string;
  enabled: boolean;
  changed: () => void;
}) {
  const captureSignal = useSpaceActionSignal(scope);
  const requests = useRef(new Map<string, AgentTransfer>());
  const [rows, setRows] = useState<AgentTransfer[]>([]);
  const owner = useRef(scope);
  const currentRows = owner.current === scope ? rows : [];
  useEffect(() => {
    if (owner.current === scope) return;
    owner.current = scope;
    for (const row of requests.current.values()) {
      row.controller?.abort();
      row.checkController?.abort();
    }
    requests.current.clear();
    setRows([]);
  }, [scope]);
  function publish(row?: AgentTransfer) {
    if (row) requests.current.set(row.id, row);
    setRows([...requests.current.values()]);
  }
  function remove(id: string) {
    const row = requests.current.get(id);
    requests.current.delete(id);
    row?.controller?.abort();
    row?.checkController?.abort();
    publish();
  }
  async function check(id: string, automatic: boolean) {
    const row = requests.current.get(id);
    if (!row?.lookup) return;
    row.checkController?.abort();
    const controller = new AbortController();
    const signal = AbortSignal.any([captureSignal(), controller.signal]);
    publish({
      ...row,
      check: "checking",
      checkController: controller,
      retryable: false,
    });
    await checkFileOperation(
      instanceId,
      row.lookup,
      signal,
      automatic,
      (state, result) => {
        const current = requests.current.get(id);
        if (signal.aborted || current?.checkController !== controller) return;
        if (state === "complete" && result) {
          publish({
            ...current,
            status: "saved",
            result,
            check: undefined,
            percentage: 100,
            checkController: undefined,
          });
          changed();
        } else if (state === "nothing-changed") {
          publish({
            ...current,
            status: "failed",
            retryable: true,
            lookup: undefined,
            check: undefined,
            checkController: undefined,
            error: new InstanceApiError(
              503,
              "Write refused",
              "file-write-failed",
              undefined,
              { file_outcome: state },
            ),
          });
        } else {
          publish({
            ...current,
            status: "failed",
            check: state,
            retryable: false,
          });
        }
      },
    );
  }
  async function send(id: string) {
    const row = requests.current.get(id);
    if (
      !enabled ||
      !row ||
      row.controller ||
      row.check ||
      row.status === "saved"
    )
      return;
    const controller = new AbortController();
    const owner = captureSignal();
    const signal = AbortSignal.any([owner, controller.signal]);
    try {
      const key =
        fileOutcome(row.error) === "nothing-changed" ? operationID() : id;
      const lookup =
        row.lookup ??
        (threadId
          ? {
              id: key,
              thread: { id: threadId, filename: row.file.name },
            }
          : { id: key });
      signal.throwIfAborted();
      publish({
        ...row,
        lookup,
        status: "uploading",
        percentage: 0,
        error: undefined,
        controller,
      });
      const options = {
        signal,
        onProgress: (percentage: number) => {
          const current = requests.current.get(id);
          if (!signal.aborted && current?.controller === controller)
            publish({ ...current, percentage });
        },
      };
      const result = await awaitAbortable(
        threadId
          ? uploadConversationFile(
              instanceId,
              threadId,
              row.file,
              lookup.id,
              options,
            )
          : putAgentFile(
              instanceId,
              `${row.folder ? `${row.folder}/` : ""}${row.file.name}`,
              row.file,
              lookup.id,
              options,
            ),
        signal,
      );
      if (signal.aborted || requests.current.get(id)?.controller !== controller)
        return;
      publish({
        ...row,
        status: "saved",
        percentage: 100,
        result,
        controller: undefined,
      });
      changed();
    } catch (error) {
      if (signal.aborted || requests.current.get(id)?.controller !== controller)
        return;
      const current = requests.current.get(id)!;
      const outcome = fileOutcome(error);
      const unanswered = unansweredWrite(error) && !!current.lookup;
      publish({
        ...current,
        status: "failed",
        error,
        lookup: outcome === "nothing-changed" ? undefined : current.lookup,
        check: unanswered
          ? "checking"
          : outcome === "unconfirmed"
            ? "unconfirmed"
            : undefined,
        retryable:
          !unanswered &&
          outcome !== "unconfirmed" &&
          error instanceof InstanceApiError &&
          error.reason === "file-write-failed",
        controller: undefined,
      });
      if (unanswered) void check(id, true);
      if (error instanceof InstanceApiError && error.reason === "storage-busy")
        changed();
    }
  }
  function add(files: readonly File[]) {
    const added: string[] = [];
    if (!enabled) return added;
    for (const file of files) {
      const id = operationID();
      added.push(id);
      const tooLarge = file.size > MAX_AGENT_FILE_BYTES;
      const invalid =
        !file.name.trim() ||
        /[\\/\p{Cc}]/u.test(file.name) ||
        file.name === "." ||
        file.name === "..";
      const error = tooLarge
        ? new InstanceApiError(
            413,
            "File too large",
            "file-too-large",
            undefined,
            { max_bytes: MAX_AGENT_FILE_BYTES },
          )
        : invalid
          ? new InstanceApiError(400, "Invalid file name", "invalid-name")
          : undefined;
      const row: AgentTransfer = {
        id,
        file,
        folder,
        status: "failed",
        percentage: 0,
        retryable: !error,
        ...(error ? { error } : {}),
      };
      publish(row);
      if (!error) void send(id);
    }
    return added;
  }
  const observeInterrupted = useRef(check);
  observeInterrupted.current = check;
  useEffect(() => {
    if (enabled) return;
    for (const row of requests.current.values())
      if (row.controller) {
        row.controller.abort();
        requests.current.set(row.id, {
          ...row,
          status: "failed",
          controller: undefined,
          error: new DOMException("Transfer interrupted", "AbortError"),
          check: row.lookup ? "checking" : undefined,
          retryable: !row.lookup,
        });
        if (row.lookup) void observeInterrupted.current(row.id, true);
      }
    setRows([...requests.current.values()]);
  }, [enabled]);
  return {
    rows: currentRows,
    add,
    retry: (id: string) => {
      const row = requests.current.get(id);
      if (row?.check && !["unconfirmed", "denied"].includes(row.check))
        void check(id, false);
      else if (row?.retryable) void send(id);
    },
    remove,
    hasUnfinished: currentRows.some((row) => row.status !== "saved"),
  };
}

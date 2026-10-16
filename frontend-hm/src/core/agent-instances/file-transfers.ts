import { useEffect, useRef, useState } from "react";

import { awaitAbortable } from "@/core/api/abort";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import {
  putAgentFile,
  uploadConversationFile,
  type SavedAgentFile,
} from "./agent-files";
import { InstanceApiError, operationID } from "./api";

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
}

/** Both file tabs and conversation attachments own their exact write identity. */
export function useAgentFileTransfers({
  instanceId,
  threadId,
  folder,
  enabled,
  changed,
}: {
  instanceId: string;
  threadId?: string;
  folder: string;
  enabled: boolean;
  changed: () => void;
}) {
  const captureSignal = useSpaceActionSignal(
    `${instanceId}:${threadId ?? "files"}`,
  );
  const requests = useRef(new Map<string, AgentTransfer>());
  const [rows, setRows] = useState<AgentTransfer[]>([]);
  const scope = `${instanceId}:${threadId ?? "files"}`;
  const owner = useRef(scope);
  const currentRows = owner.current === scope ? rows : [];
  useEffect(() => {
    if (owner.current === scope) return;
    owner.current = scope;
    for (const row of requests.current.values()) row.controller?.abort();
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
    publish();
  }
  async function send(id: string) {
    const row = requests.current.get(id);
    if (!enabled || !row || row.controller || row.status === "saved") return;
    const controller = new AbortController();
    const owner = captureSignal();
    const signal = AbortSignal.any([owner, controller.signal]);
    publish({
      ...row,
      status: "uploading",
      percentage: 0,
      error: undefined,
      controller,
    });
    try {
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
          ? uploadConversationFile(instanceId, threadId, row.file, id, options)
          : putAgentFile(
              instanceId,
              `${row.folder ? `${row.folder}/` : ""}${row.file.name}`,
              row.file,
              id,
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
      publish({
        ...row,
        status: "failed",
        error,
        retryable:
          !(error instanceof InstanceApiError) ||
          error.reason === "file-write-failed",
        controller: undefined,
      });
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
          retryable: true,
        });
      }
    setRows([...requests.current.values()]);
  }, [enabled]);
  return {
    rows: currentRows,
    add,
    retry: (id: string) => void send(id),
    remove,
    hasUnfinished: currentRows.some((row) => row.status !== "saved"),
  };
}

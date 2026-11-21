import { useEffect, useMemo, useRef, useSyncExternalStore } from "react";

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
  file: Pick<File, "name" | "size">;
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

export function isFinalTransfer(row: AgentTransfer) {
  return (
    row.status === "saved" ||
    (row.status === "failed" &&
      !row.retryable &&
      (!row.check || row.check === "denied" || row.check === "unconfirmed"))
  );
}

/** A single write and receipt owner. Only its mounting lifetime decides when to retire it. */
export function createFileTransfers({
  instanceId,
  threadId,
  captureSignal,
  enabled,
  changed,
  confirmAccess,
}: {
  instanceId: string;
  threadId?: string;
  captureSignal: () => AbortSignal;
  enabled: () => boolean;
  changed: () => void;
  confirmAccess?: () => Promise<boolean>;
}) {
  const requests = new Map<string, AgentTransfer>();
  const payloads = new Map<string, File>();
  const listeners = new Set<() => void>();
  let rows: AgentTransfer[] = [];
  function publish(row?: AgentTransfer) {
    if (row) {
      if (isFinalTransfer(row)) payloads.delete(row.id);
      requests.set(row.id, row);
    }
    rows = [...requests.values()];
    listeners.forEach((listener) => listener());
  }
  function clear() {
    for (const row of requests.values()) {
      row.controller?.abort();
      row.checkController?.abort();
    }
    payloads.clear();
    requests.clear();
    publish();
  }
  function remove(id: string) {
    const row = requests.get(id);
    requests.delete(id);
    payloads.delete(id);
    row?.controller?.abort();
    row?.checkController?.abort();
    publish();
  }
  async function check(id: string, automatic: boolean) {
    const row = requests.get(id);
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
      async (state, result) => {
        if (
          state === "complete" &&
          result &&
          confirmAccess &&
          !(await confirmAccess())
        ) {
          if (
            !signal.aborted &&
            requests.get(id)?.checkController === controller
          )
            publish({
              ...requests.get(id)!,
              check: "unavailable",
              retryable: false,
              checkController: undefined,
            });
          return;
        }
        const current = requests.get(id);
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
    const row = requests.get(id);
    const payload = payloads.get(id);
    if (
      !payload ||
      !enabled() ||
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
          const current = requests.get(id);
          if (!signal.aborted && current?.controller === controller)
            publish({ ...current, percentage });
        },
      };
      const result = await awaitAbortable(
        threadId
          ? uploadConversationFile(
              instanceId,
              threadId,
              payload,
              lookup.id,
              options,
            )
          : putAgentFile(
              instanceId,
              `${row.folder ? `${row.folder}/` : ""}${row.file.name}`,
              payload,
              lookup.id,
              options,
            ),
        signal,
      );
      if (confirmAccess && !(await confirmAccess())) {
        if (!signal.aborted && requests.get(id)?.controller === controller)
          publish({
            ...requests.get(id)!,
            status: "failed",
            check: "unavailable",
            retryable: false,
            controller: undefined,
          });
        return;
      }
      if (signal.aborted || requests.get(id)?.controller !== controller) return;
      publish({
        ...row,
        status: "saved",
        percentage: 100,
        result,
        controller: undefined,
      });
      changed();
    } catch (error) {
      if (signal.aborted || requests.get(id)?.controller !== controller) return;
      const current = requests.get(id)!;
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
  function add(files: readonly File[], folder: string) {
    const added: string[] = [];
    if (!enabled()) return added;
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
        file: { name: file.name, size: file.size },
        folder,
        status: "failed",
        percentage: 0,
        retryable: !error,
        ...(error ? { error } : {}),
      };
      if (!error) payloads.set(id, file);
      publish(row);
      if (!error) void send(id);
    }
    return added;
  }
  function interrupt() {
    for (const row of requests.values())
      if (row.controller) {
        row.controller.abort();
        requests.set(row.id, {
          ...row,
          status: "failed",
          controller: undefined,
          error: new DOMException("Transfer interrupted", "AbortError"),
          check: row.lookup ? "checking" : undefined,
          retryable: !row.lookup,
        });
        if (row.lookup) void check(row.id, true);
      }
    publish();
  }
  return {
    snapshot: () => rows,
    subscribe(this: void, listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    clear,
    interrupt,
    add,
    retry: (id: string) => {
      const row = requests.get(id);
      if (row?.check && !["unconfirmed", "denied"].includes(row.check))
        void check(id, false);
      else if (row?.retryable) void send(id);
    },
    remove,
  };
}

/** A conversation owns its attachments until its grant, thread or component retires. */
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
  const config = useRef({ enabled, changed, scope });
  config.current = { enabled, changed, scope };
  const queue = useMemo(
    () =>
      createFileTransfers({
        instanceId,
        threadId,
        captureSignal,
        enabled: () => config.current.scope === scope && config.current.enabled,
        changed: () => {
          if (config.current.scope === scope) config.current.changed();
        },
      }),
    [instanceId, threadId, captureSignal, scope],
  );
  const rows = useSyncExternalStore(
    queue.subscribe,
    queue.snapshot,
    queue.snapshot,
  );
  useEffect(() => () => queue.clear(), [queue]);
  useEffect(() => {
    if (!enabled) queue.interrupt();
  }, [queue, enabled]);
  return {
    rows,
    add: (files: readonly File[]) => queue.add(files, folder),
    retry: queue.retry,
    remove: queue.remove,
    hasUnfinished: rows.some((row) => row.status !== "saved"),
  };
}

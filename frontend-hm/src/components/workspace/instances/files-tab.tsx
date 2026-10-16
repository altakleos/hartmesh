"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useSupervisorName } from "@/components/workspace/person-name";
import {
  changeAgentFile,
  type AgentFileAction,
} from "@/core/agent-instances/agent-files";
import * as api from "@/core/agent-instances/api";
import { downloadAgentFile } from "@/core/agent-instances/file-reads";
import { useAgentFileTransfers } from "@/core/agent-instances/file-transfers";
import {
  agentBlock,
  hasCeiling,
  INSPECT,
  USE,
} from "@/core/agent-instances/presentation";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import {
  getSpace,
  listSpaceFiles,
  StorageRequestError,
  type SpaceFile,
} from "@/core/spaces/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import { formatUploadSize } from "@/core/uploads/file-validation";

import { AgentTransferRow } from "./agent-file-transfer";
import { AgentAvailabilityNotice } from "./agent-header";
import { AgentTextEditor } from "./agent-text-editor";
import { AgentFileRefusal } from "./file-refusal";

type Props = {
  instance: api.AgentInstanceDetail;
  changed?: () => void;
  initialFolder?: string;
};
const isBusy = (error: unknown) =>
  error instanceof StorageRequestError && error.reason === "storage-busy";
const lostAccess = (error: unknown) =>
  error instanceof StorageRequestError &&
  (error.status === 403 || error.status === 404);
const validName = (name: string) =>
  !!name.trim() && name !== "." && name !== ".." && !/[\\/\p{Cc}]/u.test(name);

export function FilesTab(props: Props) {
  const { user } = useAuth();
  const { t } = useI18n();
  const feature = useAgentInstances();
  const supervisor = useSupervisorName(props.instance.supervisor.subject_id);
  if ((props.instance.permissions & INSPECT) === 0)
    return (
      <section className="space-y-2">
        <p>
          {t.agentInstances.files.needsInspect(props.instance.name, supervisor)}
        </p>
        {(props.instance.permissions & USE) !== 0 && (
          <p>{t.agentInstances.files.readOnlyHint(props.instance.name)}</p>
        )}
      </section>
    );
  if (agentBlock(props.instance, feature)?.busy)
    return (
      <p role="status">{t.agentInstances.filesBusy(props.instance.name)}</p>
    );
  if (!props.instance.home_id)
    return (
      <p role="alert">
        {t.agentInstances.files.listFailed(props.instance.name)}
      </p>
    );
  return (
    <FileBrowser
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.generation}:${props.instance.permissions}:${props.instance.status}:${props.instance.home_id}:${props.initialFolder}`}
      {...props}
      home={props.instance.home_id}
    />
  );
}

function FileBrowser({
  instance,
  changed,
  home,
  initialFolder,
}: {
  instance: api.AgentInstanceDetail;
  changed?: () => void;
  home: string;
  initialFolder?: string;
}) {
  const { user } = useAuth();
  const { t, locale } = useI18n();
  const copy = t.agentInstances.files;
  const feature = useAgentInstances();
  const client = useQueryClient();
  const [path, setPath] = useState(
    initialFolder === "uploads" || initialFolder === "outputs"
      ? initialFolder
      : "",
  );
  const input = useRef<HTMLInputElement>(null);
  const inputId = useId();
  const key = [
    "agent-files",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance.id,
    instance.generation,
    instance.permissions,
  ];
  const space = useQuery({
    queryKey: [...key, "space"],
    queryFn: ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return awaitAbortable(getSpace(home, deadline), deadline);
    },
    retry: false,
    staleTime: 0,
  });
  const files = useQuery({
    queryKey: [...key, "files", path],
    queryFn: ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return awaitAbortable(listSpaceFiles(home, path, deadline), deadline);
    },
    enabled: space.isSuccess && !space.isError,
    retry: false,
    staleTime: 0,
    refetchInterval: (query) => (isBusy(query.state.error) ? 3000 : false),
  });
  const block = agentBlock(instance, feature);
  const writable =
    !space.isError &&
    !!space.data &&
    (space.data.permissions & 2) !== 0 &&
    hasCeiling(user?.permissions, "agents:write") &&
    instance.status === "active" &&
    !block;
  const [writeBusy, setWriteBusy] = useState(false);
  useEffect(() => {
    if (files.isSuccess && !files.isFetching) setWriteBusy(false);
  }, [files.dataUpdatedAt, files.isSuccess, files.isFetching]);
  function refresh() {
    void client.invalidateQueries({ queryKey: key });
    changed?.();
  }
  const transfers = useAgentFileTransfers({
    instanceId: instance.id,
    folder: path,
    enabled: writable,
    changed: refresh,
  });
  const failedBusy = transfers.rows.some(
    (row) =>
      row.error instanceof api.InstanceApiError &&
      row.error.reason === "storage-busy",
  );
  const refetchFiles = files.refetch;
  useEffect(() => {
    if (failedBusy) {
      setWriteBusy(true);
      void refetchFiles();
    }
  }, [failedBusy, refetchFiles]);
  const top =
    files.data?.files.filter((file) => file.name !== ".agent-skill-packages") ??
    [];
  const rootFolders =
    path === "" &&
    top.length === 2 &&
    top.every(
      (file) =>
        file.kind === "directory" && ["uploads", "outputs"].includes(file.name),
    );
  const empty = useQuery({
    queryKey: [...key, "empty-root", files.dataUpdatedAt],
    enabled: rootFolders && !files.isError,
    queryFn: async ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return (
        await Promise.all(
          ["uploads", "outputs"].map((folder) =>
            awaitAbortable(listSpaceFiles(home, folder, deadline), deadline),
          ),
        )
      ).every((folder) => !folder.files.length && !folder.truncated);
    },
    retry: false,
  });
  const [editor, setEditor] = useState<SpaceFile>();
  const [action, setAction] = useState<
    { kind: "mkdir" } | { kind: "rename" | "remove"; file: SpaceFile }
  >();
  const [name, setName] = useState("");
  const [error, setError] = useState<unknown>();
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const request = useRef<AgentFileAction | undefined>(undefined);
  const captureSignal = useSpaceActionSignal();
  const [downloadError, setDownloadError] = useState<unknown>();
  const [downloading, setDownloading] = useState<string>();
  const sorted = [...top].sort((a, b) => {
    const priority = (file: SpaceFile) =>
      path === ""
        ? file.name === "uploads"
          ? -2
          : file.name === "outputs"
            ? -1
            : 0
        : 0;
    return priority(a) - priority(b);
  });
  function openAction(value: NonNullable<typeof action>) {
    request.current = undefined;
    setName(value.kind === "rename" ? value.file.name : "");
    setError(undefined);
    setAction(value);
  }
  async function execute() {
    if (!action || busyRef.current || !writable) return;
    if (action.kind !== "remove" && !validName(name)) {
      setError(new api.InstanceApiError(400, "Invalid name", "invalid-name"));
      return;
    }
    busyRef.current = true;
    setBusy(true);
    setError(undefined);
    const signal = captureSignal();
    request.current ??= {
      operation_id: api.operationID(),
      ...(action.kind === "mkdir"
        ? { action: "mkdir", path: `${path ? `${path}/` : ""}${name}` }
        : action.kind === "remove"
          ? { action: "remove", path: action.file.path }
          : {
              action: "rename",
              path: action.file.path,
              destination: `${path ? `${path}/` : ""}${name}`,
            }),
    };
    try {
      await awaitAbortable(
        changeAgentFile(instance.id, request.current, { signal }),
        signal,
      );
      signal.throwIfAborted();
      request.current = undefined;
      setAction(undefined);
      refresh();
    } catch (failure) {
      if (!signal.aborted) {
        setError(failure);
        if (
          failure instanceof api.InstanceApiError &&
          failure.reason !== "file-write-failed"
        )
          request.current = undefined;
        if (
          failure instanceof api.InstanceApiError &&
          failure.reason === "storage-busy"
        ) {
          setWriteBusy(true);
          void files.refetch();
          changed?.();
        }
      }
    } finally {
      if (!signal.aborted) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  }
  async function download(file: SpaceFile) {
    if (downloading) return;
    const owner = captureSignal();
    const signal = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
    setDownloading(file.path);
    setDownloadError(undefined);
    try {
      const blob = await downloadAgentFile(home, file.path, signal);
      signal.throwIfAborted();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = file.name;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (failure) {
      if (!owner.aborted) setDownloadError(failure);
    } finally {
      if (!owner.aborted) setDownloading(undefined);
    }
  }
  if (
    isBusy(space.error) ||
    isBusy(files.error) ||
    (writeBusy && !files.isError)
  )
    return <p role="status">{t.agentInstances.filesBusy(instance.name)}</p>;
  if (lostAccess(space.error) || lostAccess(files.error))
    return <p role="alert">{copy.accessNotSaved(instance.name)}</p>;
  if (space.isError || files.isError)
    return (
      <section className="space-y-2">
        <p role="alert">{copy.listFailed(instance.name)}</p>
        <Button type="button" variant="outline" onClick={refresh}>
          {t.agentInstances.refresh}
        </Button>
      </section>
    );
  if (space.isPending || files.isPending)
    return <p role="status">{t.agentInstances.loading}</p>;
  const rowTransfers = transfers.rows.filter((row) => row.folder === path);
  return (
    <section
      className="space-y-4"
      onDragOver={(event) => {
        if (writable) {
          event.preventDefault();
          event.dataTransfer.dropEffect = "copy";
        }
      }}
      onDrop={(event) => {
        event.preventDefault();
        if (writable) transfers.add(Array.from(event.dataTransfer.files));
      }}
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <nav
          aria-label={copy.root(instance.name)}
          className="flex flex-wrap gap-2"
        >
          <Button type="button" variant="link" onClick={() => setPath("")}>
            {copy.root(instance.name)}
          </Button>
          {path
            .split("/")
            .filter(Boolean)
            .map((part, index, parts) => (
              <Button
                key={index}
                type="button"
                variant="link"
                onClick={() => setPath(parts.slice(0, index + 1).join("/"))}
              >
                {part}
              </Button>
            ))}
        </nav>
        <Button
          type="button"
          variant="outline"
          disabled={files.isFetching}
          onClick={refresh}
        >
          {t.agentInstances.refresh}
        </Button>
      </div>
      {instance.activity.running_runs > 0 && (
        <p>{copy.listLive(instance.name)}</p>
      )}
      {block ? (
        <AgentAvailabilityNotice instance={instance} block={block} />
      ) : instance.status !== "active" ? (
        <p>{copy.notActive(instance.name)}</p>
      ) : writable ? (
        <div className="space-y-3">
          <div className="flex gap-2">
            <Button type="button" onClick={() => input.current?.click()}>
              {copy.add}
            </Button>
            <Button
              type="button"
              variant="outline"
              onClick={() => openAction({ kind: "mkdir" })}
            >
              {copy.newFolder}
            </Button>
          </div>
          <div className="rounded-lg border border-dashed p-4">
            <p>{copy.drop(instance.name)}</p>
            <Button
              type="button"
              variant="link"
              onClick={() => input.current?.click()}
            >
              {copy.choose}
            </Button>
            <input
              ref={input}
              id={inputId}
              className="sr-only"
              type="file"
              multiple
              aria-label={copy.choose}
              onChange={(event) => {
                transfers.add(Array.from(event.target.files ?? []));
                event.target.value = "";
              }}
            />
          </div>
        </div>
      ) : (
        (instance.permissions & USE) !== 0 && (
          <p>{copy.readOnlyHint(instance.name)}</p>
        )
      )}
      {rowTransfers
        .filter(
          (row) =>
            row.status !== "saved" ||
            !sorted.some((file) => file.path === row.result?.path),
        )
        .map((row) => (
          <AgentTransferRow
            key={row.id}
            instance={instance}
            row={row}
            retry={() => transfers.retry(row.id)}
            remove={() => transfers.remove(row.id)}
            disabled={!writable}
          />
        ))}
      {!sorted.length && <p>{t.storageSpaces.empty}</p>}
      {!!sorted.length && (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr>
                <th className="p-2">{copy.name}</th>
                <th className="p-2">{copy.size}</th>
                <th className="p-2">{copy.modified}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {sorted.map((file) => {
                const accessible =
                  file.accessible &&
                  (file.kind === "file" || file.kind === "directory");
                const saved = rowTransfers.find(
                  (row) =>
                    row.status === "saved" && row.result?.path === file.path,
                );
                return (
                  <tr key={file.path} className="border-t">
                    <td className="p-2">
                      {accessible ? (
                        <Button
                          type="button"
                          variant="link"
                          className="px-0"
                          onClick={() =>
                            file.kind === "directory"
                              ? setPath(file.path)
                              : setEditor(file)
                          }
                        >
                          {file.name}
                        </Button>
                      ) : (
                        <span>{file.name}</span>
                      )}
                      {!accessible && <p>{t.storageSpaces.unsupportedEntry}</p>}
                      {path === "" && file.name === "uploads" && (
                        <p className="text-muted-foreground">
                          {copy.uploadsHint(instance.name)}
                        </p>
                      )}
                      {path === "" && file.name === "outputs" && (
                        <p className="text-muted-foreground">
                          {copy.outputsHint(instance.name)}
                        </p>
                      )}
                      {path.split("/")[0] === "outputs" &&
                        file.kind === "file" && (
                          <p>
                            {copy.producedBy(
                              instance.name,
                              new Intl.DateTimeFormat(locale, {
                                dateStyle: "medium",
                                timeStyle: "short",
                              }).format(new Date(file.modified * 1000)),
                            )}
                          </p>
                        )}
                      {saved && (
                        <p role="status">
                          {saved.file.name === file.name
                            ? copy.saved(instance.name)
                            : copy.savedAs(file.name)}
                        </p>
                      )}
                    </td>
                    <td className="p-2">
                      {file.kind === "file" ? formatUploadSize(file.size) : "—"}
                    </td>
                    <td className="p-2">
                      <time
                        dateTime={new Date(file.modified * 1000).toISOString()}
                      >
                        {new Intl.DateTimeFormat(locale, {
                          dateStyle: "medium",
                          timeStyle: "short",
                        }).format(new Date(file.modified * 1000))}
                      </time>
                    </td>
                    <td className="p-2">
                      {accessible && (
                        <div className="flex gap-2">
                          {file.kind === "file" && (
                            <Button
                              type="button"
                              variant="outline"
                              disabled={!!downloading}
                              onClick={() => void download(file)}
                            >
                              {t.common.download}
                            </Button>
                          )}
                          {writable && (
                            <>
                              <Button
                                type="button"
                                variant="outline"
                                onClick={() =>
                                  openAction({ kind: "rename", file })
                                }
                              >
                                {t.common.rename}
                              </Button>
                              <Button
                                type="button"
                                variant="outline"
                                onClick={() =>
                                  openAction({ kind: "remove", file })
                                }
                              >
                                {t.common.delete}
                              </Button>
                            </>
                          )}
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {rootFolders && empty.data === true && (
        <p>{writable ? copy.emptyRoot(instance.name) : copy.emptyRootReader}</p>
      )}
      {files.data.truncated && <p>{t.storageSpaces.truncated}</p>}
      {!!downloadError && (
        <AgentFileRefusal instance={instance} error={downloadError} action />
      )}
      <Dialog
        open={!!action}
        onOpenChange={(open) => {
          if (!open && !busy) setAction(undefined);
        }}
      >
        <DialogContent showCloseButton={!busy} aria-describedby={undefined}>
          <DialogTitle>
            {action?.kind === "mkdir"
              ? copy.newFolder
              : action?.kind === "rename"
                ? t.common.rename
                : copy.deleteConfirm(action?.file.name ?? "")}
          </DialogTitle>
          {action?.kind !== "remove" && (
            <>
              <label htmlFor={`${inputId}-name`}>
                {action?.kind === "mkdir" ? copy.folderName : copy.newName}
              </label>
              <Input
                id={`${inputId}-name`}
                value={name}
                disabled={busy}
                onChange={(event) => {
                  request.current = undefined;
                  setName(event.target.value);
                }}
              />
            </>
          )}
          {!!error && (
            <AgentFileRefusal instance={instance} error={error} action />
          )}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => setAction(undefined)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={busy || (action?.kind !== "remove" && !name.trim())}
              onClick={() => void execute()}
            >
              {action?.kind === "mkdir"
                ? t.common.create
                : action?.kind === "rename"
                  ? t.common.rename
                  : t.common.delete}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {editor && (
        <AgentTextEditor
          key={editor.path}
          instance={instance}
          home={home}
          file={editor}
          editable={writable}
          close={() => setEditor(undefined)}
          changed={refresh}
        />
      )}
    </section>
  );
}

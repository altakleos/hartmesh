"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { BrowserView } from "@/components/workspace/file-browser/browser-view";
import { useBrowserCopy } from "@/components/workspace/file-browser/copy";
import { BrowserLocations } from "@/components/workspace/file-browser/locations";
import { useSupervisorName } from "@/components/workspace/person-name";
import {
  changeAgentFile,
  type AgentFileAction,
} from "@/core/agent-instances/agent-files";
import * as api from "@/core/agent-instances/api";
import {
  downloadAgentFile,
  downloadAgentURL,
} from "@/core/agent-instances/file-reads";
import {
  fileOutcome,
  unansweredWrite,
} from "@/core/agent-instances/file-status";
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
import { browserFolder, browserHref } from "@/core/file-areas/browser";
import { useRetireBrowserHistory } from "@/core/file-areas/browser-history";
import {
  useBrowserProtocol,
  readAgentFolder,
  agentBrowserURL,
} from "@/core/file-areas/browser-protocol";
import { useBrowserRenderer } from "@/core/file-areas/browser-renderer";
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
import {
  FileOperationNotice,
  PendingFileChange,
} from "./file-operation-notice";
import { FileRecoveryNotice } from "./file-recovery-notice";
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
  const [observed, setObserved] = useState<{
    home: string;
    generation: number;
  }>();
  const { user } = useAuth();
  const { t } = useI18n();
  const supervisor = useSupervisorName(props.instance.supervisor.subject_id);
  const canInspect =
    !!user &&
    hasCeiling(user.permissions, "agents:read") &&
    (props.instance.permissions & INSPECT) !== 0;
  useRetireBrowserHistory(
    "agent:" + props.instance.id,
    !canInspect || !props.instance.home_id,
  );
  if (!canInspect)
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
  if (!props.instance.home_id)
    return (
      <p role="alert">
        {t.agentInstances.files.listFailed(props.instance.name)}
      </p>
    );
  return (
    <FileBrowser
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.generation}:${props.instance.permissions}:${props.instance.status}:${props.instance.home_id}:${Math.max(props.instance.home_generation ?? 0, observed?.home === props.instance.home_id ? observed.generation : 0)}:${props.initialFolder}`}
      {...props}
      home={props.instance.home_id}
      supervisor={supervisor}
      onGeneration={setObserved}
    />
  );
}

function FileBrowser({
  instance,
  changed,
  home,
  initialFolder,
  onGeneration,
  supervisor,
}: {
  instance: api.AgentInstanceDetail;
  changed?: () => void;
  home: string;
  initialFolder?: string;
  supervisor: string;
  onGeneration: (value: { home: string; generation: number }) => void;
}) {
  const { user } = useAuth();
  const router = useRouter(),
    renderer = useBrowserRenderer(),
    words = useBrowserCopy(),
    protocol = useBrowserProtocol();
  const requestedNative = renderer.native;
  const qualified =
    requestedNative &&
    protocol.supports("agent", "folder") &&
    protocol.supports("agent", "content");
  const qualifiedBefore = useRef(false);
  if (qualified) qualifiedBefore.current = true;
  const closing = requestedNative && qualifiedBefore.current && !qualified;
  const waiting =
    requestedNative && (!protocol.settled || protocol.failed || closing);
  const checked = browserFolder(initialFolder ?? "");
  const [drag, setDrag] = useState(false);
  const { t, locale } = useI18n();
  const copy = t.agentInstances.files;
  const feature = useAgentInstances();
  const client = useQueryClient();
  const [legacyPath, setLegacyPath] = useState(
    initialFolder === "uploads" || initialFolder === "outputs"
      ? initialFolder
      : "",
  );
  const path = qualified ? (checked ?? "") : legacyPath;
  function setPath(next: string) {
    if (qualified)
      router.push(
        browserHref("agent", next, {
          instance: instance.id,
          view: renderer.choice,
        }),
        { scroll: false },
      );
    else setLegacyPath(next);
  }
  const input = useRef<HTMLInputElement>(null);
  const inputId = useId();
  const key = [
    qualified ? "file-browser" : "agent-files",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance.id,
    instance.generation,
    instance.permissions,
    home,
    instance.home_generation,
  ];
  const space = useQuery({
    queryKey: [...key, "space"],
    queryFn: ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return awaitAbortable(getSpace(home, deadline), deadline);
    },
    retry: false,
    staleTime: 0,
    refetchInterval: 60_000,
  });
  const files = useQuery({
    queryKey: [...key, "files", path],
    queryFn: ({ signal }) => {
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
      return awaitAbortable(
        qualified
          ? readAgentFolder(instance.id, path, deadline)
          : listSpaceFiles(home, path, deadline),
        deadline,
      );
    },
    enabled:
      !waiting &&
      space.isSuccess &&
      !space.isError &&
      (!qualified || checked !== null),
    retry: false,
    staleTime: 0,
    refetchInterval: (query) => (isBusy(query.state.error) ? 3000 : false),
  });
  useRetireBrowserHistory(
    "agent:" + instance.id,
    lostAccess(space.error) ||
      lostAccess(files.error) ||
      (space.data !== undefined && !(space.data.permissions & 1)),
  );
  const refreshDiscovery = protocol.refresh;
  const missingRoute =
    qualified &&
    files.error instanceof StorageRequestError &&
    files.error.status === 404;
  useEffect(() => {
    if (missingRoute) void refreshDiscovery();
  }, [missingRoute, refreshDiscovery]);
  const generation = space.data?.generation;
  useEffect(() => {
    if (generation !== undefined) onGeneration({ home, generation });
  }, [onGeneration, home, generation]);
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
            awaitAbortable(
              qualified
                ? readAgentFolder(instance.id, folder, deadline)
                : listSpaceFiles(home, folder, deadline),
              deadline,
            ),
          ),
        )
      ).every((folder) => !folder.files.length && !folder.truncated);
    },
    retry: false,
  });
  const [editor, setEditor] = useState<SpaceFile>();
  const [checks, setChecks] = useState<{ id: string; file: string }[]>([]);
  const [uncertain, setUncertain] = useState<string>();
  function observe(id: string, file: string) {
    setChecks((previous) => [
      ...previous.filter((item) => item.id !== id),
      { id, file },
    ]);
  }
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [error, setError] = useState<unknown>();
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const request = useRef<AgentFileAction | undefined>(undefined);
  const captureSignal = useSpaceActionSignal(
    JSON.stringify([
      home,
      generation,
      space.data?.permissions,
      lostAccess(space.error) || lostAccess(files.error),
    ]),
  );
  const [downloadError, setDownloadError] = useState<{
    path: string;
    error: unknown;
  }>();
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
  function startFolder() {
    request.current = undefined;
    setName("");
    setError(undefined);
    setCreating(true);
  }
  async function execute() {
    if (!creating || busyRef.current || !writable) return;
    if (!validName(name)) {
      setError(new api.InstanceApiError(400, "Invalid name", "invalid-name"));
      return;
    }
    busyRef.current = true;
    setBusy(true);
    setError(undefined);
    const signal = captureSignal();
    request.current ??= {
      operation_id: api.operationID(),
      action: "mkdir",
      path: `${path ? `${path}/` : ""}${name}`,
    };
    try {
      await awaitAbortable(
        changeAgentFile(instance.id, request.current, { signal }),
        signal,
      );
      signal.throwIfAborted();
      request.current = undefined;
      setCreating(false);
      refresh();
    } catch (failure) {
      if (!signal.aborted) {
        if (request.current && unansweredWrite(failure)) {
          observe(request.current.operation_id, request.current.path);
          request.current = undefined;
          setCreating(false);
          return;
        }
        if (fileOutcome(failure) === "unconfirmed") {
          setUncertain(request.current?.path);
          request.current = undefined;
          setCreating(false);
          return;
        }
        setError(failure);
        if (
          failure instanceof api.InstanceApiError &&
          (failure.reason !== "file-write-failed" ||
            fileOutcome(failure) === "nothing-changed")
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
  async function download(file: Pick<SpaceFile, "path" | "name">) {
    if (downloading) return;
    const owner = captureSignal();
    const signal = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
    setDownloading(file.path);
    setDownloadError(undefined);
    try {
      const blob = qualified
        ? await downloadAgentURL(
            agentBrowserURL(instance.id, "content", file.path, true),
            signal,
          )
        : await downloadAgentFile(home, file.path, signal);
      signal.throwIfAborted();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = file.name;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (failure) {
      if (!owner.aborted) setDownloadError({ path: file.path, error: failure });
    } finally {
      if (!owner.aborted) setDownloading(undefined);
    }
  }
  if (waiting)
    return (
      <p role="status">
        {protocol.failed || closing
          ? copy.listFailed(instance.name)
          : t.common.loading}
      </p>
    );
  if (qualified && checked === null)
    return (
      <>
        <p role="alert">{words.missing}</p>
        <Link href={browserHref("agent", "", { instance: instance.id })}>
          {instance.name}
        </Link>
      </>
    );
  if (
    qualified &&
    files.error instanceof StorageRequestError &&
    files.error.status === 404
  )
    return (
      <>
        <p role="alert">{words.missing}</p>
        <Link href={browserHref("agent", "", { instance: instance.id })}>
          {instance.name}
        </Link>
        <BrowserLocations area="agent" instance={instance.id} />
      </>
    );
  if (lostAccess(space.error) || lostAccess(files.error))
    return (
      <>
        <p role="alert">{copy.accessNotSaved(instance.name)}</p>
        {qualified && <BrowserLocations area="agent" instance={instance.id} />}
      </>
    );
  const notices = (
    <>
      <FileRecoveryNotice
        instance={instance}
        space={space.data}
        reading={space.isSuccess ? space.dataUpdatedAt : 0}
        supervisor={supervisor}
        refreshSpace={async () =>
          (await space.refetch({ throwOnError: true })).data
        }
      />
      {checks.map(({ id, file }) => (
        <PendingFileChange
          key={id}
          instance={instance.id}
          operation={id}
          file={file}
          changed={refresh}
          dismiss={() =>
            setChecks((previous) => previous.filter((item) => item.id !== id))
          }
        />
      ))}
      {uncertain && (
        <FileOperationNotice
          state="unconfirmed"
          file={uncertain}
          check={() => undefined}
        />
      )}
      {editor && (
        <AgentTextEditor
          key={editor.path}
          instance={instance}
          home={home}
          file={editor}
          versioned={qualified}
          editable={writable}
          close={() => setEditor(undefined)}
          changed={refresh}
          observe={observe}
        />
      )}
    </>
  );
  const preserved = (content: React.ReactNode) => (
    <section className="space-y-4">
      {notices}
      {transfers.rows
        .filter((row) => row.check)
        .map((row) => (
          <AgentTransferRow
            key={row.id}
            instance={instance}
            row={row}
            retry={() => transfers.retry(row.id)}
            remove={() => transfers.remove(row.id)}
            disabled
          />
        ))}
      {content}
    </section>
  );
  if (
    block?.busy ||
    isBusy(space.error) ||
    isBusy(files.error) ||
    (writeBusy && !files.isError)
  )
    return preserved(
      <p role="status">{t.agentInstances.filesBusy(instance.name)}</p>,
    );
  if (space.isError || files.isError)
    return preserved(
      <section className="space-y-2">
        <p role="alert">{copy.listFailed(instance.name)}</p>
        <Button type="button" variant="outline" onClick={refresh}>
          {t.agentInstances.refresh}
        </Button>
      </section>,
    );
  if (space.isPending || files.isPending)
    return preserved(<p role="status">{t.agentInstances.loading}</p>);
  const rowTransfers = transfers.rows.filter((row) => row.folder === path);
  const fileTools = (
    <>
      {instance.activity.running_runs > 0 && (
        <p>{copy.listLive(instance.name)}</p>
      )}
      {block ? (
        <AgentAvailabilityNotice instance={instance} block={block} />
      ) : instance.status !== "active" ? (
        <p>{copy.notActive(instance.name)}</p>
      ) : writable ? (
        <div className="space-y-3">
          {qualified && (
            <p className="text-sm [overflow-wrap:anywhere]">
              {words.drop([instance.name, path].filter(Boolean).join("/"))}
            </p>
          )}
          <div className="flex gap-2">
            <Button type="button" onClick={() => input.current?.click()}>
              {qualified ? words.upload : copy.add}
            </Button>
            <Button type="button" variant="outline" onClick={startFolder}>
              {copy.newFolder}
            </Button>
          </div>
          <div
            className={qualified ? "" : "rounded-lg border border-dashed p-4"}
          >
            {qualified ? (
              drag && (
                <p role="status">{words.drop(instance.name + "/" + path)}</p>
              )
            ) : (
              <>
                <p>{copy.drop(instance.name)}</p>
                <Button
                  type="button"
                  variant="link"
                  onClick={() => input.current?.click()}
                >
                  {copy.choose}
                </Button>
              </>
            )}
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
    </>
  );
  return (
    <section
      className="space-y-4"
      onDragOver={(event) => {
        if (writable && event.dataTransfer.types.includes("Files")) {
          if (qualified) setDrag(true);
          event.preventDefault();
          event.dataTransfer.dropEffect = "copy";
        }
      }}
      onDragLeave={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null))
          setDrag(false);
      }}
      onDrop={(event) => {
        setDrag(false);
        if (!event.dataTransfer.types.includes("Files")) return;
        event.preventDefault();
        if (writable) transfers.add(Array.from(event.dataTransfer.files));
      }}
    >
      {notices}
      {!qualified && (
        <>
          {protocol.supports("agent", "folder") && (
            <Link
              className="text-sm underline"
              href={browserHref("agent", path, {
                instance: instance.id,
                view: "browser",
              })}
            >
              {words.native}
            </Link>
          )}
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
        </>
      )}
      {!qualified && fileTools}
      {qualified ? (
        <BrowserView
          area="agent"
          title={instance.name}
          audience={words.agentAudience}
          tools={fileTools}
          folder={path}
          instance={instance.id}
          scope={JSON.stringify([
            ...key,
            space.data?.generation,
            space.data?.permissions,
          ])}
          rows={sorted}
          truncated={files.data.truncated}
          navigate={setPath}
          hrefFor={(row) =>
            row.kind === "file" && !protocol.supports("agent", "text")
              ? agentBrowserURL(instance.id, "content", row.path)
              : ""
          }
          open={(row) => {
            const file = sorted.find((f) => f.path === row.path);
            if (file?.accessible) setEditor(file);
          }}
          describe={(row) =>
            row.kind === "directory" && !path && row.name === "uploads" ? (
              <p>{copy.uploadsHint(instance.name)}</p>
            ) : row.kind === "directory" && !path && row.name === "outputs" ? (
              <p>{copy.outputsHint(instance.name)}</p>
            ) : row.kind === "file" && path.split("/")[0] === "outputs" ? (
              <p>
                {copy.producedBy(
                  instance.name,
                  new Date(row.modified * 1000).toLocaleString(locale),
                )}
              </p>
            ) : null
          }
          actions={(row) =>
            row.kind === "file" ? (
              <>
                <Button
                  variant="outline"
                  disabled={!!downloading}
                  onClick={() => void download(row)}
                >
                  {t.common.download}
                </Button>
                {downloadError?.path === row.path && (
                  <p role="alert">
                    {lostAccess(downloadError.error)
                      ? words.downloadDenied(row.name)
                      : words.downloadFailed(row.name)}
                  </p>
                )}
              </>
            ) : null
          }
        />
      ) : (
        <>
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
                        row.status === "saved" &&
                        row.result?.path === file.path,
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
                          {!accessible && (
                            <p>{t.storageSpaces.unsupportedEntry}</p>
                          )}
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
                          {file.kind === "file"
                            ? formatUploadSize(file.size)
                            : "—"}
                        </td>
                        <td className="p-2">
                          <time
                            dateTime={new Date(
                              file.modified * 1000,
                            ).toISOString()}
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
            <p>
              {writable ? copy.emptyRoot(instance.name) : copy.emptyRootReader}
            </p>
          )}
          {files.data.truncated && <p>{t.storageSpaces.truncated}</p>}
        </>
      )}
      {!qualified && !!downloadError && (
        <AgentFileRefusal
          instance={instance}
          error={downloadError.error}
          action
        />
      )}
      <Dialog
        open={creating}
        onOpenChange={(open) => {
          if (!open && !busy) setCreating(false);
        }}
      >
        <DialogContent showCloseButton={!busy} aria-describedby={undefined}>
          <DialogTitle>{copy.newFolder}</DialogTitle>
          <label htmlFor={`${inputId}-name`}>{copy.folderName}</label>
          <Input
            id={`${inputId}-name`}
            value={name}
            disabled={busy}
            onChange={(event) => {
              request.current = undefined;
              setName(event.target.value);
            }}
          />
          {!!error && (
            <AgentFileRefusal instance={instance} error={error} action />
          )}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => setCreating(false)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={busy || !name.trim()}
              onClick={() => void execute()}
            >
              {t.common.create}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}

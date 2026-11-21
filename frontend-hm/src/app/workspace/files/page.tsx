"use client";

import { DownloadIcon, FolderIcon, Trash2Icon, UsersIcon } from "lucide-react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatArtifactBytes } from "@/components/workspace/artifacts/artifact-file-preview";
import { NativeDownloadLink } from "@/components/workspace/artifacts/native-download-link";
import { FilesStateNotice } from "@/components/workspace/chats/files-state-notice";
import { BrowserView } from "@/components/workspace/file-browser/browser-view";
import { useBrowserCopy } from "@/components/workspace/file-browser/copy";
import { BrowserLocations } from "@/components/workspace/file-browser/locations";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import {
  installedFileCollection,
  isFileFilingReady,
} from "@/core/extensions/filing";
import { useFrontendExtensions } from "@/core/extensions/hooks";
import { useDocumentTitle, useStorage } from "@/core/features";
import { STORAGE_REASONS, type StorageReason } from "@/core/features/api";
import { browserFolder, browserHref } from "@/core/file-areas/browser";
import { useRetireBrowserHistory } from "@/core/file-areas/browser-history";
import { useBrowserProtocol } from "@/core/file-areas/browser-protocol";
import { useBrowserRenderer } from "@/core/file-areas/browser-renderer";
import {
  selectLoadedFiles,
  type FileListOrder,
} from "@/core/file-areas/selection";
import { useStorageCopy } from "@/core/file-areas/storage-copy";
import {
  fileAreaNotice,
  readStorageRefusal,
  type StorageRefusal,
} from "@/core/file-areas/storage-state";
import { FolderRouteMissing } from "@/core/file-areas/workspace-folder";
import { MY_FILES_VIRTUAL_PREFIX, urlOfMyFile, useMyFiles } from "@/core/files";
import { useI18n } from "@/core/i18n/hooks";
import {
  urlOfSharedFile,
  useRemoveSharedFile,
  useShareWithEveryone,
  useSharedFiles,
  type SharedFileInfo,
  selectSharedFile,
  type PublicationSelection,
} from "@/core/shared";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import { formatTimeAgo } from "@/core/utils/datetime";

type FilesTab = "mine" | "shared";
const EMPTY_FILES: readonly never[] = [];
type StorageObservation = ReturnType<typeof useStorage>;

function useDeploymentRefusal(
  error: unknown,
  report: (refusal: StorageRefusal) => void,
) {
  useEffect(() => {
    const refusal = readStorageRefusal(error);
    if (
      refusal?.code === "storage-unavailable" &&
      STORAGE_REASONS.includes(refusal.reason as StorageReason)
    )
      report(refusal);
  }, [error, report]);
}

// Keep the search input mounted across a change of listing only after a fresh
// read admitted this location. A refusal retires the previous admission.
function useFolderAdmission(
  hasData: boolean,
  error: unknown,
  refreshProtocol: () => unknown,
) {
  const [admitted, setAdmitted] = useState(false);
  useEffect(() => {
    if (error) {
      setAdmitted(false);
      if (error instanceof FolderRouteMissing) void refreshProtocol();
    } else if (hasData) setAdmitted(true);
  }, [hasData, error, refreshProtocol]);
  return admitted;
}

function readDenied(error: unknown) {
  return (
    !!error &&
    typeof error === "object" &&
    "status" in error &&
    (error.status === 401 || error.status === 403)
  );
}

function useBusyRetry(error: unknown, refetch: () => unknown) {
  useEffect(() => {
    if (readStorageRefusal(error)?.code !== "storage-busy") return;
    const timer = setInterval(() => {
      void refetch();
    }, 5_000);
    return () => clearInterval(timer);
  }, [error, refetch]);
}

function useStorageRecovery(
  error: unknown,
  storage: StorageObservation,
  area: "files" | "shared",
  refetch: () => unknown,
) {
  const revision = useRef(storage.dataUpdatedAt);
  const retried = useRef(false);
  useEffect(() => {
    if (!error) retried.current = false;
    if (revision.current === storage.dataUpdatedAt) return;
    revision.current = storage.dataUpdatedAt;
    if (
      !retried.current &&
      readStorageRefusal(error)?.code === "storage-unavailable" &&
      storage.known &&
      storage.state !== "unavailable" &&
      (area === "shared" || storage.state === "off" || storage.own === "ready")
    ) {
      retried.current = true;
      void refetch();
    }
  }, [
    error,
    storage.known,
    storage.state,
    storage.own,
    storage.dataUpdatedAt,
    area,
    refetch,
  ]);
}

/** The folder a file sits in, or nothing for the root. */
function folderOf(file: { path: string }) {
  const separator = file.path.lastIndexOf("/");
  return separator === -1 ? "" : file.path.slice(0, separator);
}

/** The tab a link asked for (`?tab=shared`), else the person's own files. */
function tabOf(value: string | null): FilesTab {
  return value === "shared" ? "shared" : "mine";
}

export default function FilesPage() {
  const { t } = useI18n();
  const router = useRouter();
  const searchParams = useSearchParams();
  const discovered = useStorage();
  const { user } = useAuth();
  const identity = `${user?.id ?? ""}:${user?.system_role ?? ""}`;
  const latest = useRef({ discovered, identity });
  latest.current = { discovered, identity };
  const [failure, setFailure] = useState<{
    refusal: StorageRefusal;
    observedAt: number;
    identity: string;
  } | null>(null);
  const reportFailure = useCallback((refusal: StorageRefusal) => {
    const { discovered, identity } = latest.current;
    if (!STORAGE_REASONS.includes(refusal.reason as StorageReason)) return;
    setFailure({
      refusal,
      observedAt: discovered.dataUpdatedAt ?? 0,
      identity,
    });
    void discovered.refetch?.();
  }, []);
  const storage: StorageObservation =
    failure?.identity === identity &&
    (discovered.dataUpdatedAt ?? 0) <= failure.observedAt
      ? {
          ...discovered,
          state: "unavailable",
          reason: failure.refusal.reason as StorageReason,
        }
      : discovered;
  const browserCopy = useBrowserCopy();
  const browserProtocol = useBrowserProtocol(),
    renderer = useBrowserRenderer();
  const legacy =
    !renderer.native ||
    !browserProtocol.supports(
      searchParams?.get("tab") === "shared" ? "shared" : "personal",
      "select",
    );
  const folder = browserFolder(searchParams?.get("folder") ?? null);
  const denied = !user || !hasCeiling(user.permissions, "threads:read");
  useRetireBrowserHistory("personal", denied);
  useRetireBrowserHistory("shared", denied);
  const tab = tabOf(searchParams?.get("tab") ?? null);
  // The tab is also written back to the URL, so a person who copies the link
  // sends their colleague to the tab they were looking at.
  const setTab = (next: FilesTab) => {
    router.push(
      browserHref(next === "shared" ? "shared" : "personal", "", {
        view: renderer.choice,
      }),
    );
  };
  const headingRef = useRef<HTMLHeadingElement>(null);

  useDocumentTitle(
    tab === "shared" ? t.shared.title : t.files.title,
    t.pages.appName,
  );

  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div
          className={
            legacy
              ? "mx-auto flex w-full max-w-(--container-width-md) flex-col gap-4 p-6"
              : "mx-auto w-full max-w-7xl min-w-0 p-3 sm:p-5"
          }
        >
          {legacy && (
            <>
              <div>
                <h1
                  className="text-2xl font-semibold focus:outline-none"
                  ref={headingRef}
                  tabIndex={-1}
                >
                  {t.files.pageTitle}
                </h1>
                <p className="text-muted-foreground mt-1 text-sm">
                  {tab === "shared"
                    ? t.shared.description
                    : t.files.description}
                </p>
              </div>

              <Tabs
                onValueChange={(value) => setTab(value as FilesTab)}
                value={tab}
              >
                <TabsList variant="line">
                  <TabsTrigger data-testid="files-tab-mine" value="mine">
                    {t.files.title}
                  </TabsTrigger>
                  <TabsTrigger data-testid="files-tab-shared" value="shared">
                    {t.shared.title}
                  </TabsTrigger>
                </TabsList>
              </Tabs>

              {browserProtocol.supports(
                tab === "shared" ? "shared" : "personal",
                "select",
              ) && (
                <Link
                  className="text-sm underline"
                  href={browserHref(
                    tab === "shared" ? "shared" : "personal",
                    folder ?? "",
                    { view: "browser" },
                  )}
                >
                  {browserCopy.native}
                </Link>
              )}
            </>
          )}
          {!user || !hasCeiling(user.permissions, "threads:read") ? (
            <p role="alert">{t.files.loadFailed}</p>
          ) : folder === null ? (
            <>
              <p role="alert">{browserCopy.missing}</p>
              <BrowserLocations
                area={tab === "shared" ? "shared" : "personal"}
              />
            </>
          ) : !browserProtocol.settled ? (
            <p role="status">{t.common.loading}</p>
          ) : tab === "shared" ? (
            <SharedFiles
              key={
                identity +
                JSON.stringify(user?.permissions) +
                tab +
                folder +
                legacy
              }
              native={!legacy}
              directFolder={
                !legacy && browserProtocol.supports("shared", "folder")
              }
              refreshProtocol={browserProtocol.refresh}
              folder={folder}
              storage={storage}
              onGlobalFailure={reportFailure}
              onChanged={() => headingRef.current?.focus()}
            />
          ) : (
            <MyFiles
              key={
                identity +
                JSON.stringify(user?.permissions) +
                tab +
                folder +
                legacy
              }
              native={!legacy}
              directFolder={
                !legacy && browserProtocol.supports("personal", "folder")
              }
              refreshProtocol={browserProtocol.refresh}
              folder={folder}
              storage={storage}
              onGlobalFailure={reportFailure}
            />
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

function FileListControls({
  query,
  onQuery,
  order,
  onOrder,
  found,
  loaded,
}: {
  query: string;
  onQuery: (value: string) => void;
  order: FileListOrder;
  onOrder: (value: FileListOrder) => void;
  found: number;
  loaded: number;
}) {
  const { t } = useI18n();
  const searchId = useId();
  const sortId = useId();
  const input = useRef<HTMLInputElement>(null);
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2">
        <label className="sr-only" htmlFor={searchId}>
          {t.files.filterLabel}
        </label>
        <Input
          className="min-w-40 flex-1"
          id={searchId}
          onChange={(event) => onQuery(event.target.value)}
          placeholder={t.files.filterPlaceholder}
          ref={input}
          type="search"
          value={query}
        />
        <Button
          disabled={!query}
          onClick={() => {
            onQuery("");
            input.current?.focus();
          }}
          variant="outline"
        >
          {t.files.clearFilter}
        </Button>
        <label className="sr-only" htmlFor={sortId}>
          {t.files.sortLabel}
        </label>
        <select
          className="border-input bg-background h-9 rounded-md border px-3 text-sm"
          id={sortId}
          onChange={(event) => onOrder(event.target.value as FileListOrder)}
          value={order}
        >
          <option value="name">{t.files.sortName}</option>
          <option value="newest">{t.files.sortNewest}</option>
        </select>
      </div>
      <p className="text-muted-foreground text-xs" role="status">
        {t.files.foundCount(found, loaded)}
      </p>
    </div>
  );
}

function MyFiles({
  native,
  folder,
  directFolder,
  refreshProtocol,
  storage,
  onGlobalFailure,
}: {
  native: boolean;
  directFolder: boolean;
  refreshProtocol: () => unknown;
  folder: string;
  storage: StorageObservation;
  onGlobalFailure: (refusal: StorageRefusal) => void;
}) {
  const pluginQuery = useFrontendExtensions();
  const { t, locale } = useI18n();
  const { user } = useAuth();
  const words = useBrowserCopy();
  const [searching, setSearching] = useState(false);
  const { data, error, isPending, refetch } = useMyFiles(
    directFolder && !searching ? folder : undefined,
  );
  const admitted = useFolderAdmission(!!data, error, refreshProtocol);
  useDeploymentRefusal(error, onGlobalFailure);
  useRetireBrowserHistory("personal", readDenied(error));
  useBusyRetry(error, refetch);
  useStorageRecovery(error, storage, "files", refetch);

  // The empty state tells people they can share one of their own files; this
  // is where they do it. No conversation is involved, so no thread.
  const everyone = useShareWithEveryone();

  const loadedFiles = data?.files ?? EMPTY_FILES;
  const [query, setQuery] = useState("");
  const [order, setOrder] = useState<FileListOrder>("name");
  const files = useMemo(
    () =>
      native ? [] : selectLoadedFiles(loadedFiles, { query, order, locale }),
    [native, loadedFiles, query, order, locale],
  );

  const rows = useMemo(
    () => [
      ...(data?.directories ?? []),
      ...loadedFiles.map((file) => ({ ...file, kind: "file" as const })),
    ],
    [data?.directories, loadedFiles],
  );
  const notice = fileAreaNotice(storage, error, "files");
  if (notice)
    return (
      <div
        className="space-y-3 rounded-lg border p-4"
        data-testid="my-files-storage-notice"
      >
        {native && <BrowserLocations area="personal" />}
        <FilesStateNotice refusal={notice} />
        {
          <Button size="sm" variant="outline" onClick={() => void refetch()}>
            {t.files.retry}
          </Button>
        }
      </div>
    );

  if (native && !error && (data || (directFolder && admitted))) {
    const byPath = new Map(
      loadedFiles.map((file) => [file.path, file] as const),
    );
    return (
      <BrowserView
        area="personal"
        title={words.personal}
        audience={words.myAudience}
        folder={folder}
        scope={JSON.stringify([
          user?.id,
          user?.system_role,
          user?.permissions,
          "personal",
        ])}
        rows={rows}
        truncated={data?.truncated ?? false}
        searchMode={directFolder ? searching : undefined}
        onSearchMode={directFolder ? setSearching : undefined}
        loading={isPending}
        folderKnown={
          directFolder
            ? !data?.folderMissing
            : !folder ||
              !!data?.truncated ||
              rows.some((r) => r.path.startsWith(folder + "/"))
        }
        open={() => undefined}
        hrefFor={(row) => (row.kind === "file" ? urlOfMyFile(row.path) : "")}
        empty={t.files.emptyHint}
        actions={(row) => {
          const file = byPath.get(row.path);
          if (!file) return null;
          return (
            <>
              <Button asChild variant="outline">
                <NativeDownloadLink
                  area="files"
                  onGlobalFailure={onGlobalFailure}
                  href={urlOfMyFile(file.path, { download: true })}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {t.files.download}
                </NativeDownloadLink>
              </Button>
              {user &&
                hasCeiling(user.permissions, "threads:write") &&
                isFileFilingReady(pluginQuery) && (
                  <>
                    <p className="text-sm">{words.disclose}</p>
                    <Button
                      disabled={everyone.isPending}
                      onClick={() => {
                        const path = MY_FILES_VIRTUAL_PREFIX + "/" + file.path;
                        void everyone.share(
                          [path],
                          installedFileCollection(pluginQuery.data ?? [], {
                            filepath: path,
                            destination: "shared",
                            presented: [],
                          }),
                        );
                      }}
                    >
                      {t.shared.shareWithEveryone}
                    </Button>
                  </>
                )}
            </>
          );
        }}
      />
    );
  }
  if (native)
    return (
      <>
        <BrowserLocations area="personal" />
        {error && (
          <p role="alert">
            {readDenied(error) ? words.denied : t.files.loadFailed}
          </p>
        )}
        {error instanceof FolderRouteMissing && (
          <Link href={browserHref("personal", folder, { view: "classic" })}>
            {words.classic}
          </Link>
        )}
        {error ? (
          <Button onClick={() => void refetch()} variant="outline">
            {t.files.retry}
          </Button>
        ) : (
          <p role="status">{t.common.loading}</p>
        )}
      </>
    );
  return (
    <>
      {!pluginQuery.isPending && !isFileFilingReady(pluginQuery) && (
        <p role="status" className="text-muted-foreground text-sm">
          {t.extensions.fileDestinationsUnavailable}
        </p>
      )}
      {!error && !isPending && loadedFiles.length > 0 && (
        <FileListControls
          query={query}
          onQuery={setQuery}
          order={order}
          onOrder={setOrder}
          found={files.length}
          loaded={loadedFiles.length}
        />
      )}
      {error ? (
        <div
          className="flex items-center justify-between gap-3 rounded-lg border p-4"
          data-testid="my-files-load-error"
        >
          <span className="text-destructive text-sm">{t.files.loadFailed}</span>
          <Button size="sm" variant="outline" onClick={() => refetch()}>
            {t.files.retry}
          </Button>
        </div>
      ) : isPending ? (
        <div className="text-muted-foreground text-sm">{t.common.loading}</div>
      ) : loadedFiles.length === 0 ? (
        <Empty className="border" data-testid="my-files-empty">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <FolderIcon />
            </EmptyMedia>
            <EmptyTitle>{t.files.empty}</EmptyTitle>
            <EmptyDescription>{t.files.emptyHint}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : files.length === 0 ? (
        <Empty className="border" data-testid="my-files-no-matches">
          <EmptyHeader>
            <EmptyTitle>{t.files.noMatches}</EmptyTitle>
            <EmptyDescription>{t.files.noMatchesHint}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        // Focusable so a keyboard-only reader can scroll a wide table at all.
        <div
          aria-label={t.files.title}
          className="focus-visible:ring-ring overflow-x-auto rounded-lg border focus-visible:ring-2 focus-visible:outline-none"
          role="region"
          tabIndex={0}
        >
          <table className="w-full border-collapse text-sm">
            <caption className="sr-only">{t.files.title}</caption>
            <thead>
              <tr className="bg-muted/50 text-muted-foreground text-left text-xs">
                <th className="px-3 py-2 font-medium" scope="col">
                  {t.files.name}
                </th>
                <th
                  className="hidden px-3 py-2 font-medium sm:table-cell"
                  scope="col"
                >
                  {t.files.folder}
                </th>
                <th className="px-3 py-2 text-right font-medium" scope="col">
                  {t.files.size}
                </th>
                <th
                  className="hidden px-3 py-2 font-medium whitespace-nowrap sm:table-cell"
                  scope="col"
                >
                  {t.files.modified}
                </th>
                <th className="px-3 py-2" scope="col">
                  <span className="sr-only">{t.files.actions}</span>
                </th>
              </tr>
            </thead>
            <tbody data-testid="my-files-list">
              {files.map((file) => (
                <tr
                  className="border-border/60 border-t"
                  data-testid={`my-file-${file.path}`}
                  key={file.path}
                >
                  <th
                    className="max-w-[40vw] truncate px-3 py-2 font-normal"
                    scope="row"
                  >
                    <a
                      className="hover:underline"
                      href={urlOfMyFile(file.path)}
                      rel="noopener noreferrer"
                      target="_blank"
                      title={file.path}
                    >
                      {file.name}
                    </a>
                    {folderOf(file) && (
                      <span
                        className="text-muted-foreground block truncate text-xs sm:hidden"
                        title={folderOf(file)}
                      >
                        {folderOf(file)}
                      </span>
                    )}
                  </th>
                  <td className="text-muted-foreground hidden max-w-[25vw] truncate px-3 py-2 sm:table-cell">
                    {folderOf(file) || "—"}
                  </td>
                  <td className="px-3 py-2 text-right whitespace-nowrap tabular-nums">
                    {formatArtifactBytes(file.size)}
                  </td>
                  <td
                    className="text-muted-foreground hidden px-3 py-2 whitespace-nowrap sm:table-cell"
                    title={new Date(file.modified * 1000).toLocaleString()}
                  >
                    {formatTimeAgo(file.modified * 1000, locale)}
                  </td>
                  <td className="px-2 py-1 text-right whitespace-nowrap">
                    <Button asChild size="icon-sm" variant="ghost">
                      <NativeDownloadLink
                        aria-label={`${t.files.download} ${file.name}`}
                        area="files"
                        onGlobalFailure={onGlobalFailure}
                        href={urlOfMyFile(file.path, { download: true })}
                        rel="noopener noreferrer"
                        target="_blank"
                      >
                        <DownloadIcon className="size-4" />
                      </NativeDownloadLink>
                    </Button>
                    <Button
                      aria-label={`${t.shared.shareWithEveryone} ${file.name}`}
                      disabled={
                        !everyone.available ||
                        everyone.isPending ||
                        !isFileFilingReady(pluginQuery)
                      }
                      onClick={() => {
                        const path = `${MY_FILES_VIRTUAL_PREFIX}/${file.path}`;
                        // No conversation is involved, so the file itself is
                        // all there is to go on.
                        void everyone.share(
                          [path],
                          installedFileCollection(pluginQuery.data ?? [], {
                            filepath: path,
                            destination: "shared",
                            presented: [],
                          }),
                        );
                      }}
                      size="icon-sm"
                      title={t.shared.description}
                      variant="ghost"
                    >
                      <UsersIcon className="size-4" />
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {data?.truncated && (
        <p className="text-muted-foreground text-xs">
          {t.files.truncated(loadedFiles.length)}
        </p>
      )}
    </>
  );
}

function SharedFiles({
  native,
  folder,
  directFolder,
  refreshProtocol,
  onChanged,
  storage,
  onGlobalFailure,
}: {
  native: boolean;
  directFolder: boolean;
  refreshProtocol: () => unknown;
  folder: string;
  onChanged: () => void;
  storage: StorageObservation;
  onGlobalFailure: (refusal: StorageRefusal) => void;
}) {
  const { t, locale } = useI18n();
  const { user } = useAuth();
  const words = useBrowserCopy();
  const [searching, setSearching] = useState(false);
  const { data, error, isPending, refetch } = useSharedFiles(
    directFolder && !searching ? folder : undefined,
  );
  const admitted = useFolderAdmission(!!data, error, refreshProtocol);
  useDeploymentRefusal(error, onGlobalFailure);
  useRetireBrowserHistory("shared", readDenied(error));
  const copy = useStorageCopy("shared");
  useBusyRetry(error, refetch);
  useStorageRecovery(error, storage, "shared", refetch);
  const removeFile = useRemoveSharedFile();
  const [pendingRemove, setPendingRemove] = useState<
    (PublicationSelection & { operationId: string }) | null
  >(null);
  const captureSelection = useSpaceActionSignal(String(removeFile.enabled));
  const eligible = (file: SharedFileInfo) =>
    removeFile.enabled &&
    file.can_remove &&
    ((typeof file.publication_id === "string" &&
      file.publication_id.length > 0) ||
      (file.publication_id === null && user?.system_role === "admin"));
  const pick = async (file: SharedFileInfo) => {
    if (!eligible(file)) return;
    const signal = captureSelection();
    try {
      const selection = await selectSharedFile(file.path, signal);
      if (signal.aborted) return;
      if (
        selection.path !== file.path ||
        !selection.can_remove ||
        !selection.revision ||
        selection.publication_id !== file.publication_id
      ) {
        void refetch();
        return;
      }
      setPendingRemove({
        ...selection,
        operationId: crypto.randomUUID().replaceAll("-", ""),
      });
    } catch (failure) {
      if (!signal.aborted) {
        removeFile.refused(failure);
        toast.error(copy.message(failure, t.shared.removeFailed));
      }
    }
  };
  const loadedFiles = data?.files ?? EMPTY_FILES;
  const [query, setQuery] = useState("");
  const [order, setOrder] = useState<FileListOrder>("name");
  const files = useMemo(
    () =>
      native ? [] : selectLoadedFiles(loadedFiles, { query, order, locale }),
    [native, loadedFiles, query, order, locale],
  );

  const rows = useMemo(
    () => [
      ...(data?.directories ?? []),
      ...loadedFiles.map((file) => ({ ...file, kind: "file" as const })),
    ],
    [data?.directories, loadedFiles],
  );
  const notice = fileAreaNotice(storage, error, "shared");
  if (notice)
    return (
      <div
        className="space-y-3 rounded-lg border p-4"
        data-testid="shared-files-storage-notice"
      >
        {native && <BrowserLocations area="shared" />}
        <FilesStateNotice refusal={notice} area="shared" />
        {
          <Button size="sm" variant="outline" onClick={() => void refetch()}>
            {t.files.retry}
          </Button>
        }
      </div>
    );

  const confirmation = (
    <Dialog
      onOpenChange={(open) => {
        if (!open) setPendingRemove(null);
      }}
      open={removeFile.enabled && pendingRemove !== null}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t.shared.removeTitle}</DialogTitle>
          <DialogDescription>
            {pendingRemove ? t.shared.removeConfirm(pendingRemove.name) : ""}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button
            disabled={removeFile.isPending}
            onClick={() => setPendingRemove(null)}
            variant="outline"
          >
            {t.common.cancel}
          </Button>
          <Button
            disabled={removeFile.isPending}
            onClick={() => {
              if (!pendingRemove || !removeFile.enabled) return;
              const { name } = pendingRemove;
              removeFile.mutate(
                {
                  selection: pendingRemove,
                  operationId: pendingRemove.operationId,
                },
                {
                  onSuccess: () => {
                    setPendingRemove(null);
                    toast.success(t.shared.removed(name));
                    onChanged();
                  },
                  onError: (failure) => {
                    if (
                      failure &&
                      typeof failure === "object" &&
                      "status" in failure &&
                      [403, 404, 409].includes(Number(failure.status))
                    )
                      setPendingRemove(null);
                    console.error("Remove from Shared failed:", failure);
                    toast.error(copy.message(failure, t.shared.removeFailed));
                  },
                },
              );
            }}
            variant="destructive"
          >
            {removeFile.isPending ? t.shared.removing : t.shared.remove}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
  if (native && !error && (data || (directFolder && admitted))) {
    const byPath = new Map(
      loadedFiles.map((file) => [file.path, file] as const),
    );
    return (
      <>
        <BrowserView
          area="shared"
          title={words.shared}
          audience={words.companyAudience}
          folder={folder}
          scope={JSON.stringify([
            user?.id,
            user?.system_role,
            user?.permissions,
            "shared",
          ])}
          rows={rows}
          truncated={data?.truncated ?? false}
          searchMode={directFolder ? searching : undefined}
          onSearchMode={directFolder ? setSearching : undefined}
          loading={isPending}
          folderKnown={
            directFolder
              ? !data?.folderMissing
              : !folder ||
                !!data?.truncated ||
                rows.some((r) => r.path.startsWith(folder + "/"))
          }
          open={() => undefined}
          hrefFor={(row) =>
            row.kind === "file" ? urlOfSharedFile(row.path) : ""
          }
          empty={t.shared.emptyHint}
          describe={(row) => {
            const file = byPath.get(row.path);
            return (
              file && (
                <p>
                  {t.shared.publisher}:{" "}
                  {file.published_by ?? t.shared.publishedByNobody}
                </p>
              )
            );
          }}
          actions={(row) => {
            const file = byPath.get(row.path);
            if (!file) return null;
            return (
              <>
                <Button asChild variant="outline">
                  <NativeDownloadLink
                    area="shared"
                    onGlobalFailure={onGlobalFailure}
                    href={urlOfSharedFile(file.path, { download: true })}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {t.files.download}
                  </NativeDownloadLink>
                </Button>

                {eligible(file) && (
                  <Button variant="destructive" onClick={() => void pick(file)}>
                    {t.shared.remove}
                  </Button>
                )}
              </>
            );
          }}
        />
        {confirmation}
      </>
    );
  }
  if (native)
    return (
      <>
        <BrowserLocations area="shared" />
        {error && (
          <p role="alert">
            {readDenied(error) ? words.denied : t.files.loadFailed}
          </p>
        )}
        {error instanceof FolderRouteMissing && (
          <Link href={browserHref("shared", folder, { view: "classic" })}>
            {words.classic}
          </Link>
        )}
        {error ? (
          <Button onClick={() => void refetch()} variant="outline">
            {t.files.retry}
          </Button>
        ) : (
          <p role="status">{t.common.loading}</p>
        )}
      </>
    );
  return (
    <>
      {!error && !isPending && loadedFiles.length > 0 && (
        <FileListControls
          query={query}
          onQuery={setQuery}
          order={order}
          onOrder={setOrder}
          found={files.length}
          loaded={loadedFiles.length}
        />
      )}
      {error ? (
        <div
          className="flex items-center justify-between gap-3 rounded-lg border p-4"
          data-testid="shared-files-load-error"
        >
          <span className="text-destructive text-sm">
            {t.shared.loadFailed}
          </span>
          <Button size="sm" variant="outline" onClick={() => refetch()}>
            {t.files.retry}
          </Button>
        </div>
      ) : isPending ? (
        <div className="text-muted-foreground text-sm">{t.common.loading}</div>
      ) : loadedFiles.length === 0 ? (
        <Empty className="border" data-testid="shared-files-empty">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <UsersIcon />
            </EmptyMedia>
            <EmptyTitle>{t.shared.empty}</EmptyTitle>
            <EmptyDescription>{t.shared.emptyHint}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : files.length === 0 ? (
        <Empty className="border" data-testid="shared-files-no-matches">
          <EmptyHeader>
            <EmptyTitle>{t.files.noMatches}</EmptyTitle>
            <EmptyDescription>{t.files.noMatchesHint}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div
          aria-label={t.shared.title}
          className="focus-visible:ring-ring overflow-x-auto rounded-lg border focus-visible:ring-2 focus-visible:outline-none"
          role="region"
          tabIndex={0}
        >
          <table className="w-full border-collapse text-sm">
            <caption className="sr-only">{t.shared.title}</caption>
            <thead>
              <tr className="bg-muted/50 text-muted-foreground text-left text-xs">
                <th className="px-3 py-2 font-medium" scope="col">
                  {t.files.name}
                </th>
                <th
                  className="hidden px-3 py-2 font-medium sm:table-cell"
                  scope="col"
                >
                  {t.files.folder}
                </th>
                <th className="px-3 py-2 text-right font-medium" scope="col">
                  {t.files.size}
                </th>
                <th className="px-3 py-2 font-medium" scope="col">
                  {t.shared.publisher}
                </th>
                <th
                  className="hidden px-3 py-2 font-medium whitespace-nowrap sm:table-cell"
                  scope="col"
                >
                  {t.shared.published}
                </th>
                <th className="px-3 py-2" scope="col">
                  <span className="sr-only">{t.files.actions}</span>
                </th>
              </tr>
            </thead>
            <tbody data-testid="shared-files-list">
              {files.map((file) => {
                const when = file.published_at
                  ? new Date(file.published_at).getTime()
                  : file.modified * 1000;
                return (
                  <tr
                    className="border-border/60 border-t"
                    data-testid={`shared-file-${file.path}`}
                    key={file.path}
                  >
                    <th
                      className="max-w-[40vw] truncate px-3 py-2 font-normal"
                      scope="row"
                    >
                      <a
                        className="hover:underline"
                        href={urlOfSharedFile(file.path)}
                        rel="noopener noreferrer"
                        target="_blank"
                        title={file.path}
                      >
                        {file.name}
                      </a>
                      {folderOf(file) && (
                        <span
                          className="text-muted-foreground block truncate text-xs sm:hidden"
                          title={folderOf(file)}
                        >
                          {folderOf(file)}
                        </span>
                      )}
                    </th>
                    <td className="text-muted-foreground hidden max-w-[25vw] truncate px-3 py-2 sm:table-cell">
                      {folderOf(file) || "—"}
                    </td>
                    <td className="px-3 py-2 text-right whitespace-nowrap tabular-nums">
                      {formatArtifactBytes(file.size)}
                    </td>
                    <td className="text-muted-foreground max-w-[25vw] truncate px-3 py-2">
                      {file.published_by ?? (
                        <span className="italic">
                          {t.shared.publishedByNobody}
                        </span>
                      )}
                    </td>
                    <td
                      className="text-muted-foreground hidden px-3 py-2 whitespace-nowrap sm:table-cell"
                      title={new Date(when).toLocaleString()}
                    >
                      {formatTimeAgo(when, locale)}
                    </td>
                    <td className="px-2 py-1 text-right whitespace-nowrap">
                      <Button asChild size="icon-sm" variant="ghost">
                        <NativeDownloadLink
                          aria-label={`${t.files.download} ${file.name}`}
                          area="shared"
                          onGlobalFailure={onGlobalFailure}
                          href={urlOfSharedFile(file.path, { download: true })}
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          <DownloadIcon className="size-4" />
                        </NativeDownloadLink>
                      </Button>
                      {eligible(file) && (
                        <Button
                          aria-label={`${t.shared.remove} ${file.name}`}
                          onClick={() => void pick(file)}
                          size="icon-sm"
                          variant="ghost"
                        >
                          <Trash2Icon className="size-4" />
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {data?.truncated && (
        <p className="text-muted-foreground text-xs">
          {t.shared.truncated(loadedFiles.length)}
        </p>
      )}

      {confirmation}
    </>
  );
}

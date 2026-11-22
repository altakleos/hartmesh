"use client";

import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { NativeDownloadLink } from "@/components/workspace/artifacts/native-download-link";
import { FilesStateNotice } from "@/components/workspace/chats/files-state-notice";
import { BrowserView } from "@/components/workspace/file-browser/browser-view";
import { useBrowserCopy } from "@/components/workspace/file-browser/copy";
import { BrowserLocations } from "@/components/workspace/file-browser/locations";
import { ConversationGroup } from "@/components/workspace/shared-conversations/group";
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
import { browserFolder } from "@/core/file-areas/browser";
import { useRetireBrowserHistory } from "@/core/file-areas/browser-history";
import { useBrowserProtocol } from "@/core/file-areas/browser-protocol";
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
import { useSharing, useSnapshotList } from "@/core/shared-conversations/hooks";
import { ordinarySharedRows } from "@/core/shared-conversations/protocol";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

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

/** The tab a link asked for (`?tab=shared`), else the person's own files. */
function tabOf(value: string | null): FilesTab {
  return value === "shared" ? "shared" : "mine";
}

export default function FilesPage() {
  const { t } = useI18n();
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
  const browserProtocol = useBrowserProtocol();
  const folder = browserFolder(searchParams?.get("folder") ?? null);
  const denied = !user || !hasCeiling(user.permissions, "threads:read");
  useRetireBrowserHistory("personal", denied);
  useRetireBrowserHistory("shared", denied);
  const tab = tabOf(searchParams?.get("tab") ?? null);
  useDocumentTitle(
    tab === "shared" ? t.shared.title : t.files.title,
    t.pages.appName,
  );

  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto w-full max-w-7xl min-w-0 p-3 sm:p-5">
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
              key={identity + JSON.stringify(user?.permissions) + tab + folder}
              directFolder={browserProtocol.supports("shared", "folder")}
              refreshProtocol={browserProtocol.refresh}
              folder={folder}
              storage={storage}
              onGlobalFailure={reportFailure}
            />
          ) : (
            <MyFiles
              key={identity + JSON.stringify(user?.permissions) + tab + folder}
              directFolder={browserProtocol.supports("personal", "folder")}
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

function MyFiles({
  folder,
  directFolder,
  refreshProtocol,
  storage,
  onGlobalFailure,
}: {
  directFolder: boolean;
  refreshProtocol: () => unknown;
  folder: string;
  storage: StorageObservation;
  onGlobalFailure: (refusal: StorageRefusal) => void;
}) {
  const pluginQuery = useFrontendExtensions();
  const { t } = useI18n();
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
        <BrowserLocations area="personal" />
        <FilesStateNotice refusal={notice} />
        {
          <Button size="sm" variant="outline" onClick={() => void refetch()}>
            {t.files.retry}
          </Button>
        }
      </div>
    );

  if (!error && (data || (directFolder && admitted))) {
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
  return (
    <>
      <BrowserLocations area="personal" />
      {error && (
        <p role="alert">
          {readDenied(error) ? words.denied : t.files.loadFailed}
        </p>
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
}

function SharedFiles({
  folder,
  directFolder,
  refreshProtocol,
  storage,
  onGlobalFailure,
}: {
  directFolder: boolean;
  refreshProtocol: () => unknown;
  folder: string;
  storage: StorageObservation;
  onGlobalFailure: (refusal: StorageRefusal) => void;
}) {
  const { t } = useI18n();
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
  const sharing = useSharing();
  const canReadSnapshots =
    sharing.can("read") && !!data && !error && storage.state !== "unavailable";
  const snapshots = useSnapshotList(canReadSnapshots, sharing.scope);
  const fold = canReadSnapshots && !snapshots.isError && !!snapshots.data;
  const loadedFiles = useMemo(
    () => ordinarySharedRows(data?.files ?? EMPTY_FILES, fold),
    [data?.files, fold],
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
        <BrowserLocations area="shared" />
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
  if (!error && (data || (directFolder && admitted))) {
    const byPath = new Map(
      loadedFiles.map((file) => [file.path, file] as const),
    );
    return (
      <>
        <BrowserView
          relatedResults={(query) =>
            canReadSnapshots &&
            (!folder || query.trim() !== "") && (
              <ConversationGroup listing={snapshots} filter={query} />
            )
          }
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
  return (
    <>
      <BrowserLocations area="shared" />
      {error && (
        <p role="alert">
          {readDenied(error) ? words.denied : t.shared.loadFailed}
        </p>
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
}

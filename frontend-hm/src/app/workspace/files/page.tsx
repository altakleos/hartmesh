"use client";

import { DownloadIcon, FolderIcon, Trash2Icon } from "lucide-react";
import { useEffect, useState } from "react";
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
import { formatArtifactBytes } from "@/components/workspace/artifacts/artifact-file-preview";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import {
  urlOfMyFile,
  useDeleteMyFile,
  useMyFiles,
  type MyFileInfo,
} from "@/core/files";
import { useI18n } from "@/core/i18n/hooks";
import { formatTimeAgo } from "@/core/utils/datetime";

/** The folder a file sits in, or nothing for the root. */
function folderOf(file: MyFileInfo) {
  const separator = file.path.lastIndexOf("/");
  return separator === -1 ? "" : file.path.slice(0, separator);
}

export default function FilesPage() {
  const { t, locale } = useI18n();
  const { data, error, isPending, refetch } = useMyFiles();
  const deleteFile = useDeleteMyFile();
  const [pendingDelete, setPendingDelete] = useState<MyFileInfo | null>(null);

  useEffect(() => {
    document.title = `${t.files.title} - ${t.pages.appName}`;
  }, [t.files.title, t.pages.appName]);

  const files = data?.files ?? [];

  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto flex w-full max-w-(--container-width-md) flex-col gap-4 p-6">
          <div>
            <h1 className="text-2xl font-semibold">{t.files.title}</h1>
            <p className="text-muted-foreground mt-1 text-sm">
              {t.files.description}
            </p>
          </div>

          {error ? (
            <div
              className="flex items-center justify-between gap-3 rounded-lg border p-4"
              data-testid="my-files-load-error"
            >
              <span className="text-destructive text-sm">
                {t.files.loadFailed}: {error.message}
              </span>
              <Button size="sm" variant="outline" onClick={() => refetch()}>
                {t.files.retry}
              </Button>
            </div>
          ) : isPending ? (
            <div className="text-muted-foreground text-sm">
              {t.common.loading}
            </div>
          ) : files.length === 0 ? (
            <Empty className="border" data-testid="my-files-empty">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <FolderIcon />
                </EmptyMedia>
                <EmptyTitle>{t.files.empty}</EmptyTitle>
                <EmptyDescription>{t.files.emptyHint}</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <div className="overflow-x-auto rounded-lg border">
              <table className="w-full border-collapse text-sm">
                <caption className="sr-only">{t.files.title}</caption>
                <thead>
                  <tr className="bg-muted/50 text-muted-foreground text-left text-xs">
                    <th className="px-3 py-2 font-medium" scope="col">
                      {t.files.name}
                    </th>
                    <th className="px-3 py-2 font-medium" scope="col">
                      {t.files.folder}
                    </th>
                    <th
                      className="px-3 py-2 text-right font-medium"
                      scope="col"
                    >
                      {t.files.size}
                    </th>
                    <th
                      className="px-3 py-2 font-medium whitespace-nowrap"
                      scope="col"
                    >
                      {t.files.modified}
                    </th>
                    <th className="px-3 py-2" scope="col">
                      <span className="sr-only">{t.common.more}</span>
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
                          title={file.name}
                        >
                          {file.name}
                        </a>
                      </th>
                      <td className="text-muted-foreground max-w-[25vw] truncate px-3 py-2">
                        {folderOf(file) || "—"}
                      </td>
                      <td className="px-3 py-2 text-right whitespace-nowrap tabular-nums">
                        {formatArtifactBytes(file.size)}
                      </td>
                      <td className="text-muted-foreground px-3 py-2 whitespace-nowrap">
                        {formatTimeAgo(file.modified * 1000, locale)}
                      </td>
                      <td className="px-2 py-1 text-right whitespace-nowrap">
                        <Button asChild size="icon-sm" variant="ghost">
                          <a
                            aria-label={`${t.files.download} ${file.name}`}
                            href={urlOfMyFile(file.path, { download: true })}
                            rel="noopener noreferrer"
                            target="_blank"
                          >
                            <DownloadIcon className="size-4" />
                          </a>
                        </Button>
                        <Button
                          aria-label={`${t.files.delete} ${file.name}`}
                          onClick={() => setPendingDelete(file)}
                          size="icon-sm"
                          variant="ghost"
                        >
                          <Trash2Icon className="size-4" />
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
              {t.files.truncated(files.length)}
            </p>
          )}
        </div>
      </WorkspaceBody>

      <Dialog
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null);
        }}
        open={pendingDelete !== null}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t.files.delete}</DialogTitle>
            <DialogDescription>
              {pendingDelete ? t.files.deleteConfirm(pendingDelete.name) : ""}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              disabled={deleteFile.isPending}
              onClick={() => setPendingDelete(null)}
              variant="outline"
            >
              {t.common.cancel}
            </Button>
            <Button
              disabled={deleteFile.isPending}
              onClick={() => {
                if (!pendingDelete) return;
                deleteFile.mutate(pendingDelete.path, {
                  onSuccess: () => {
                    setPendingDelete(null);
                    toast.success(t.files.deleted);
                  },
                  onError: (failure) => {
                    toast.error(
                      failure instanceof Error
                        ? failure.message
                        : t.files.deleteFailed,
                    );
                  },
                });
              }}
              variant="destructive"
            >
              {deleteFile.isPending ? t.common.loading : t.files.delete}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </WorkspaceContainer>
  );
}

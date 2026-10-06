"use client";

import {
  DownloadIcon,
  FolderPlusIcon,
  LoaderIcon,
  UsersIcon,
} from "lucide-react";
import { useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  eligibleViewExports,
  viewCollection,
  type ArtifactViewDocument,
} from "@/core/artifact-views/contract";
import { useLiveArtifactExports } from "@/core/artifact-views/exports";
import { urlOfArtifact } from "@/core/artifacts/utils";
import { useSaveToMyFiles } from "@/core/files";
import { useI18n } from "@/core/i18n/hooks";
import { useShareWithEveryone } from "@/core/shared";
import { isStaticWebsiteOnly } from "@/core/static-mode";

type Props = {
  view: Pick<ArtifactViewDocument, "exports" | "destination">;
  filepath: string;
  threadId: string;
  revision?: string;
  viewerId?: string;
  artifacts: readonly string[];
  presentedKnown: boolean;
  runSettled: boolean;
  isMock?: boolean;
};

/** File actions accept explicit canonical references, independent of a renderer. */
export function ArtifactFileControls(props: Props) {
  const identity = JSON.stringify([
    props.viewerId ?? "",
    props.threadId,
    props.filepath,
    props.revision ?? "",
    Boolean(props.isMock),
  ]);
  return <FileControlsSession key={identity} {...props} />;
}

function FileControlsSession({
  view,
  filepath,
  threadId,
  revision = "",
  artifacts,
  presentedKnown,
  runSettled,
  isMock,
}: Props) {
  const { t } = useI18n();
  const eligible = useMemo(
    () =>
      presentedKnown ? eligibleViewExports(view, filepath, artifacts) : [],
    [view, filepath, artifacts, presentedKnown],
  );
  const live = useLiveArtifactExports({
    eligible,
    filepath,
    threadId,
    revision,
    isMock,
    runSettled,
  });
  const [selection, setSelection] = useState<ReadonlySet<string> | null>(null);
  const selected = live.files.filter(
    (file) => selection === null || selection.has(file.path),
  );
  const destination = viewCollection(view);
  const myFiles = useSaveToMyFiles(threadId);
  const shared = useShareWithEveryone(threadId);
  const mutable = !isMock && !isStaticWebsiteOnly();
  const pending = myFiles.isPending || shared.isPending;

  return (
    <>
      {view.exports.length > 0 && (
        <section
          className="mt-6 space-y-3 border-t pt-4"
          aria-label={t.artifactViews.exports}
        >
          <h3 className="font-semibold">{t.artifactViews.exports}</h3>
          <p className="text-muted-foreground text-sm">
            {t.artifactViews.exportsHint}
          </p>
          {destination.collection && (
            <p className="text-sm break-words">
              {t.artifactViews.collection(destination.collection)}
            </p>
          )}
          {destination.ignored && (
            <p role="note" className="text-muted-foreground text-sm">
              {t.artifactViews.ignoredCollection}
            </p>
          )}
          {(!presentedKnown || (!live.settled && live.checking)) && (
            <p role="status" className="text-muted-foreground text-sm">
              {t.artifactViews.checkingFiles}
            </p>
          )}
          {live.uncertain && (
            <div
              className="flex flex-wrap items-center gap-2 text-sm"
              role="status"
            >
              <span>{t.artifactViews.filesUncertain}</span>
              <Button
                variant="outline"
                size="sm"
                onClick={live.retry}
                disabled={live.checking}
              >
                {t.artifactViews.retry}
              </Button>
            </div>
          )}
          {presentedKnown &&
            live.settled &&
            !live.uncertain &&
            live.files.length === 0 && (
              <p className="text-muted-foreground text-sm">
                {t.artifactViews.noAvailableFiles}
              </p>
            )}
          <div className="space-y-2">
            {live.files.map((file) => (
              <div
                key={file.path}
                className="flex min-w-0 items-center justify-between gap-3"
              >
                <label className="flex min-w-0 items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={selection === null || selection.has(file.path)}
                    disabled={pending}
                    onChange={(event) => {
                      setSelection((current) => {
                        const next = new Set(
                          current ?? live.files.map((item) => item.path),
                        );
                        if (event.target.checked) next.add(file.path);
                        else next.delete(file.path);
                        return next;
                      });
                    }}
                  />
                  <span className="min-w-0 break-words">{file.label}</span>
                </label>
                <Button variant="outline" size="sm" asChild>
                  <a
                    href={urlOfArtifact({
                      filepath: file.path,
                      threadId,
                      download: true,
                      isMock,
                    })}
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label={`${t.common.download} ${file.label}`}
                  >
                    <DownloadIcon className="size-4" aria-hidden />
                    {t.common.download}
                  </a>
                </Button>
              </div>
            ))}
          </div>
          {mutable && (
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={pending || selected.length === 0}
                onClick={() =>
                  void myFiles.save(
                    selected.map((item) => item.path),
                    destination.collection,
                  )
                }
              >
                {myFiles.isPending ? (
                  <LoaderIcon className="size-4 animate-spin" />
                ) : (
                  <FolderPlusIcon className="size-4" />
                )}
                {t.files.saveToMyFiles}
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={pending || selected.length === 0}
                onClick={() =>
                  void shared.share(
                    selected.map((item) => item.path),
                    destination.collection,
                  )
                }
              >
                {shared.isPending ? (
                  <LoaderIcon className="size-4 animate-spin" />
                ) : (
                  <UsersIcon className="size-4" />
                )}
                {t.shared.shareWithEveryone}
              </Button>
            </div>
          )}
        </section>
      )}
    </>
  );
}

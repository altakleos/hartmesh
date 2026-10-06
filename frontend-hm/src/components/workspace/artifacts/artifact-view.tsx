"use client";

import {
  DownloadIcon,
  FolderPlusIcon,
  LoaderIcon,
  UsersIcon,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  type ArtifactViewBlock,
  type ArtifactViewDocument,
  eligibleViewExports,
  resolveViewReference,
  viewCollection,
} from "@/core/artifact-views/contract";
import { useLiveArtifactExports } from "@/core/artifact-views/exports";
import { ArtifactImageSession } from "@/core/artifact-views/images";
import { urlOfArtifact } from "@/core/artifacts/utils";
import { useSaveToMyFiles } from "@/core/files";
import { useI18n } from "@/core/i18n/hooks";
import { useShareWithEveryone } from "@/core/shared";
import { isStaticWebsiteOnly } from "@/core/static-mode";
import { cn } from "@/lib/utils";

type ArtifactViewProps = {
  view: ArtifactViewDocument;
  filepath: string;
  threadId: string;
  revision?: string;
  artifacts: readonly string[];
  presentedKnown: boolean;
  runSettled: boolean;
  isMock?: boolean;
  viewerId?: string;
};

export function ArtifactView(props: ArtifactViewProps) {
  const identity = JSON.stringify([
    props.viewerId ?? "",
    props.threadId,
    props.filepath,
    props.revision ?? "",
    Boolean(props.isMock),
  ]);
  return <ArtifactViewSession key={identity} {...props} />;
}

function ArtifactViewSession({
  view,
  filepath,
  threadId,
  revision = "",
  artifacts,
  presentedKnown,
  runSettled,
  isMock,
  viewerId = "",
}: ArtifactViewProps) {
  const { t } = useI18n();
  const [session, setSession] = useState<ArtifactImageSession | null>(null);
  // Effect-owned sessions survive StrictMode's effect replay correctly.
  useEffect(() => {
    const current = new ArtifactImageSession();
    setSession(current);
    return () => current.dispose();
  }, [filepath, threadId, revision, viewerId, isMock]);
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
    <article
      className="text-foreground size-full min-w-0 overflow-y-auto p-4"
      data-testid="artifact-view"
    >
      <header
        className="mb-5 min-w-0 border-b-2 pb-3"
        style={{ borderBottomColor: view.accent }}
      >
        <h2 className="text-xl font-semibold break-words whitespace-pre-wrap">
          {view.title}
        </h2>
        {view.subtitle && (
          <p className="text-muted-foreground mt-1 text-sm break-words whitespace-pre-wrap">
            {view.subtitle}
          </p>
        )}
      </header>
      <div className="space-y-5">
        {view.blocks.map((block, index) => (
          <ViewBlock
            key={index}
            block={block}
            session={session}
            filepath={filepath}
            threadId={threadId}
            revision={revision}
            isMock={isMock}
          />
        ))}
      </div>
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
    </article>
  );
}

function ViewBlock({
  block,
  session,
  filepath,
  threadId,
  revision,
  isMock,
}: {
  block: ArtifactViewBlock;
  session: ArtifactImageSession | null;
  filepath: string;
  threadId: string;
  revision: string;
  isMock?: boolean;
}) {
  const { t } = useI18n();
  const heading = block.heading && (
    <h3 className="mb-2 font-semibold break-words whitespace-pre-wrap">
      {block.heading}
    </h3>
  );
  switch (block.type) {
    case "facts":
      return (
        <section>
          {heading}
          <dl className="grid grid-cols-[repeat(auto-fit,minmax(min(100%,10rem),1fr))] gap-3">
            {block.items.map((item, index) => (
              <div
                key={index}
                className="border-border min-w-0 rounded-lg border p-3"
              >
                <dt className="text-muted-foreground text-sm break-words">
                  {item.label}
                </dt>
                <dd className="mt-1 text-2xl font-semibold break-words tabular-nums">
                  {item.value}
                </dd>
                {item.detail && (
                  <dd className="text-muted-foreground mt-1 text-sm break-words whitespace-pre-wrap">
                    {item.detail}
                  </dd>
                )}
              </div>
            ))}
          </dl>
        </section>
      );
    case "text":
      return (
        <section>
          {heading}
          <div className="space-y-2">
            {block.paragraphs.map((paragraph, index) => (
              <p
                key={index}
                className="text-sm break-words whitespace-pre-wrap"
              >
                {paragraph}
              </p>
            ))}
          </div>
        </section>
      );
    case "list": {
      const Tag = block.ordered ? "ol" : "ul";
      return (
        <section>
          {heading}
          <Tag
            className={cn(
              "space-y-1 pl-6 text-sm",
              block.ordered ? "list-decimal" : "list-disc",
            )}
          >
            {block.items.map((item, index) => (
              <li key={index} className="break-words whitespace-pre-wrap">
                {item}
              </li>
            ))}
          </Tag>
        </section>
      );
    }
    case "table":
      return (
        <section>
          {heading}
          <div
            className="border-border max-w-full overflow-x-auto rounded-md border"
            tabIndex={0}
            role="region"
            aria-label={block.heading ?? t.artifactViews.table}
          >
            <table className="min-w-full border-collapse text-sm">
              <caption className="sr-only">
                {block.heading ?? t.artifactViews.table}
              </caption>
              <thead className="bg-muted/50">
                <tr>
                  {block.columns.map((column, index) => (
                    <th
                      key={index}
                      scope="col"
                      className={cn(
                        "min-w-[7rem] px-3 py-2 align-top font-medium break-words",
                        column.align === "end" ? "text-right" : "text-left",
                      )}
                    >
                      {column.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {block.rows.map((row, rowIndex) => (
                  <tr key={rowIndex} className="border-border border-t">
                    {row.map((cell, columnIndex) => (
                      <td
                        key={columnIndex}
                        className={cn(
                          "max-w-[20rem] px-3 py-2 align-top break-words whitespace-pre-wrap",
                          block.columns[columnIndex]?.align === "end"
                            ? "text-right tabular-nums"
                            : "text-left",
                        )}
                      >
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
              {block.footer && (
                <tfoot className="border-border bg-muted/30 border-t font-medium">
                  <tr>
                    {block.footer.map((cell, index) => (
                      <td
                        key={index}
                        className={cn(
                          "px-3 py-2 break-words whitespace-pre-wrap",
                          block.columns[index]?.align === "end"
                            ? "text-right tabular-nums"
                            : "text-left",
                        )}
                      >
                        {cell}
                      </td>
                    ))}
                  </tr>
                </tfoot>
              )}
            </table>
          </div>
        </section>
      );
    case "image":
      return (
        <section>
          {heading}
          <ViewImage
            block={block}
            session={session}
            filepath={filepath}
            threadId={threadId}
            revision={revision}
            isMock={isMock}
          />
        </section>
      );
    case "notice":
      return (
        <aside
          role="note"
          className={cn(
            "rounded-md border-l-4 p-3 text-sm",
            {
              neutral: "border-border bg-muted/40",
              positive: "border-emerald-500 bg-emerald-500/5",
              warning: "border-amber-500 bg-amber-500/5",
              negative: "border-red-500 bg-red-500/5",
            }[block.tone],
          )}
        >
          {heading}
          <p className="break-words whitespace-pre-wrap">{block.text}</p>
          {block.attribution && (
            <p className="text-muted-foreground mt-2 text-xs break-words whitespace-pre-wrap">
              {block.attribution}
            </p>
          )}
        </aside>
      );
  }
}

function ViewImage({
  block,
  session,
  filepath,
  threadId,
  revision,
  isMock,
}: {
  block: Extract<ArtifactViewBlock, { type: "image" }>;
  session: ArtifactImageSession | null;
  filepath: string;
  threadId: string;
  revision: string;
  isMock?: boolean;
}) {
  const { t } = useI18n();
  const [url, setURL] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);
  const resource = resolveViewReference(filepath, block.path);
  useEffect(() => {
    setURL(null);
    setFailed(false);
    if (!session || !resource) return;
    const controller = new AbortController();
    let active = true;
    let loaded: string | undefined;
    void session
      .load({
        filepath: resource,
        threadId,
        revision,
        isMock,
        signal: controller.signal,
      })
      .then((next) => {
        if (!active) {
          session.releaseURL(next);
          return;
        }
        loaded = next;
        setURL(next);
      })
      .catch(() => {
        if (active) setFailed(true);
      });
    return () => {
      active = false;
      controller.abort();
      if (loaded) session.releaseURL(loaded);
    };
  }, [session, resource, threadId, revision, isMock]);
  return (
    <figure className="min-w-0">
      {failed || !resource ? (
        <p
          className="bg-muted/40 text-muted-foreground rounded-md p-4 text-sm"
          role="status"
        >
          {t.artifactViews.imageUnavailable}
        </p>
      ) : url ? (
        // An authenticated, bounded and decoded raster Blob cannot use Next's remote optimizer.
        <img
          src={url}
          alt={block.alt}
          className="mx-auto h-auto max-h-[28rem] max-w-full rounded-md object-contain"
          onError={() => {
            session?.releaseURL(url);
            setURL(null);
            setFailed(true);
          }}
        />
      ) : (
        <div
          className="bg-muted/30 flex min-h-16 items-center justify-center rounded-md"
          role="status"
          aria-label={block.alt}
        >
          <LoaderIcon
            className="text-muted-foreground size-5 animate-spin"
            aria-hidden
          />
        </div>
      )}
      {block.caption && (
        <figcaption className="text-muted-foreground mt-2 text-sm break-words whitespace-pre-wrap">
          {block.caption}
        </figcaption>
      )}
    </figure>
  );
}

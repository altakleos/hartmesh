"use client";

import { ChevronRight, Ellipsis, File, Folder, FolderOpen } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  Fragment,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { formatArtifactBytes } from "@/components/workspace/artifacts/artifact-file-preview";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import {
  boundedBrowserRows,
  browserHref,
  parentFolder,
  type BrowserArea,
  type BrowserRow,
} from "@/core/file-areas/browser";
import { useBrowserHistory } from "@/core/file-areas/browser-history";
import { useI18n } from "@/core/i18n/hooks";

import { useBrowserCopy } from "./copy";
import { BrowserLocations } from "./locations";

import styles from "./browser.module.css";

type Props = {
  area: BrowserArea;
  title: string;
  audience: string;
  folder: string;
  scope: string;
  instance?: string;
  rows: readonly BrowserRow[];
  truncated: boolean;
  open: (row: BrowserRow) => void;
  hrefFor?: (row: BrowserRow) => string;
  actions: (row: BrowserRow) => ReactNode;
  describe?: (row: BrowserRow) => ReactNode;
  rowStatus?: (row: BrowserRow) => ReactNode;
  tools?: ReactNode;
  relatedResults?: (query: string) => ReactNode;
  empty?: ReactNode;
  navigate?: (path: string) => void;
  folderKnown?: boolean;
  searchMode?: boolean;
  onSearchMode?: (searching: boolean) => void;
  loading?: boolean;
};
export function BrowserView({
  area,
  title,
  audience,
  folder,
  scope,
  instance,
  rows,
  truncated,
  open,
  hrefFor,
  actions,
  describe,
  rowStatus,
  tools,
  relatedResults,
  empty,
  navigate,
  folderKnown = true,
  searchMode,
  onSearchMode,
  loading = false,
}: Props) {
  const { t, locale } = useI18n(),
    copy = useBrowserCopy(),
    router = useRouter(),
    { user } = useAuth();
  const { view, save, container, heading } = useBrowserHistory(
    area === "agent" ? "agent:" + instance : area,
    scope,
    folder,
    !loading,
    searchMode,
  );
  const searching = Boolean(view.query.trim());
  useLayoutEffect(() => {
    onSearchMode?.(searching);
  }, [onSearchMode, searching]);
  const waiting =
    loading || (searchMode !== undefined && searchMode !== searching);
  const [selected, setSelected] = useState<string | null>(null),
    [trail, setTrail] = useState(false);
  const trigger = useRef<HTMLButtonElement | null>(null),
    trailButton = useRef<HTMLButtonElement | null>(null);
  const result = useMemo(
    () =>
      boundedBrowserRows(rows, {
        folder,
        query: view.query,
        order: view.order,
        locale,
        virtual: area !== "agent",
      }),
    [rows, folder, view.query, view.order, locale, area],
  );
  const crumbBox = useRef<HTMLDivElement>(null);
  const crumbMeasure = useRef<HTMLSpanElement>(null);
  const [trailFits, setTrailFits] = useState(false);
  useLayoutEffect(() => {
    const box = crumbBox.current,
      measure = crumbMeasure.current;
    if (!box || !measure) return;
    const check = () => setTrailFits(measure.offsetWidth <= box.clientWidth);
    check();
    const observer = new ResizeObserver(check);
    observer.observe(box);
    observer.observe(measure);
    return () => observer.disconnect();
  }, [folder]);
  const priorFolder = useRef({ folder, known: false });
  useLayoutEffect(() => {
    if (waiting) return;
    const prior = priorFolder.current;
    priorFolder.current = { folder, known: folderKnown };
    if (
      area === "agent" ||
      !folder ||
      folderKnown ||
      !prior.known ||
      prior.folder !== folder
    )
      return;
    let ancestor = parentFolder(folder);
    while (
      searchMode === undefined &&
      ancestor &&
      !rows.some((row) => row.path.startsWith(ancestor + "/"))
    )
      ancestor = parentFolder(ancestor);
    router.replace(browserHref(area, ancestor), {
      scroll: false,
    });
  }, [area, folder, folderKnown, rows, router, waiting, searchMode]);
  const detail = waiting ? undefined : result.find((r) => r.path === selected);
  const step = (path: string) => {
    setSelected(null);
    setTrail(false);
    if (navigate) navigate(path);
    else router.push(browserHref(area, path, { instance }), { scroll: false });
  };
  const editing = view.query.trim() !== "",
    parts = folder.split("/").filter(Boolean);
  const label =
    area === "agent"
      ? copy.folderFilter
      : truncated || searchMode !== undefined
        ? copy.limited
        : copy.search(title);
  const when = (r: BrowserRow) =>
    r.published_at && Number.isFinite(Date.parse(r.published_at))
      ? new Date(r.published_at)
      : new Date(r.modified * 1000);
  const focus = (id: string) => save({ control: id }, false);
  const entries = waiting ? [] : result.slice(0, view.count);
  return (
    <section aria-label={title} className={styles.browser}>
      <button
        type="button"
        className="sr-only focus:not-sr-only"
        onClick={() => heading.current?.focus()}
      >
        {copy.skip}
      </button>
      <div className={styles.layout}>
        <aside className={styles.rail} aria-label={copy.locations}>
          <h2 className="mb-2 font-medium">{copy.locations}</h2>
          {user &&
            hasCeiling(user.permissions, "threads:read") &&
            (["personal", "shared"] as const).map((a) => (
              <Link
                key={a}
                className={styles.location}
                aria-current={a === area ? "location" : undefined}
                href={browserHref(a, "")}
              >
                {copy[a]}
              </Link>
            ))}
          {area === "agent" && (
            <span aria-current="location" className={styles.location}>
              {title}
            </span>
          )}
          <BrowserLocations area={area} instance={instance} />
        </aside>
        <div
          className={styles.pane}
          ref={container}
          onScroll={(e) => save({ scroll: e.currentTarget.scrollTop }, false)}
        >
          <div className={styles.trail}>
            <button
              type="button"
              className={styles.root}
              onClick={() => step("")}
            >
              {title}
            </button>
            {parts.length > 0 && (
              <div className={styles.crumbBox} ref={crumbBox}>
                <span aria-hidden="true" className={styles.measureClip}>
                  <span ref={crumbMeasure} className={styles.measure}>
                    {parts.map((part, i) => (
                      <span key={i}>{part}</span>
                    ))}
                  </span>
                </span>
                {trailFits ? (
                  parts.map((part, i) => (
                    <Fragment key={i}>
                      <ChevronRight size={16} aria-hidden />
                      <button
                        className={styles.segment}
                        type="button"
                        onClick={() => step(parts.slice(0, i + 1).join("/"))}
                      >
                        {part}
                      </button>
                    </Fragment>
                  ))
                ) : (
                  <>
                    <ChevronRight size={16} aria-hidden />
                    <button
                      type="button"
                      className={styles.crumb}
                      aria-label={copy.path}
                      ref={trailButton}
                      onClick={() => setTrail(true)}
                    >
                      {parts.at(-1)}
                    </button>
                  </>
                )}
              </div>
            )}
            <span className={styles.mobileLocations}>
              <BrowserLocations area={area} instance={instance} />
            </span>
          </div>
          <h1
            className="mt-3 text-xl font-semibold [overflow-wrap:anywhere]"
            tabIndex={-1}
            ref={heading}
          >
            {parts.at(-1) ?? title}
          </h1>
          <p className="text-muted-foreground text-sm">{audience}</p>
          {tools}
          <div className={styles.toolbar}>
            <Input
              aria-label={label}
              type="search"
              placeholder={label}
              value={view.query}
              onChange={(e) => save({ query: e.target.value, count: 100 })}
            />
            <label className="flex items-center gap-2 text-sm">
              {t.files.sortLabel}
              <select
                className="bg-background min-h-11 rounded-md border px-2"
                value={view.order}
                onChange={(e) =>
                  save({
                    order: e.target.value === "newest" ? "newest" : "name",
                    count: 100,
                  })
                }
              >
                <option value="name">{t.files.sortName}</option>
                <option value="newest">{t.files.sortNewest}</option>
              </select>
            </label>
          </div>
          {!waiting && relatedResults?.(view.query)}
          {!waiting && truncated && (
            <p className="text-muted-foreground text-sm">
              {searchMode === false && rows.length === 0
                ? copy.folderScanLimit
                : copy.partial(rows.length)}
            </p>
          )}
          {waiting && <p role="status">{t.common.loading}</p>}
          {!waiting && editing && (
            <p role="status">{copy.matches(result.length)}</p>
          )}
          {!folderKnown && (
            <p role="status">
              {copy.missing}{" "}
              <Button variant="link" onClick={() => step("")}>
                {title}
              </Button>
            </p>
          )}
          {!waiting &&
            !(searchMode === false && truncated) &&
            entries.length === 0 &&
            folderKnown && (
              <p className="mt-4 text-sm">
                {editing ? (
                  <>
                    {copy.noMatch}{" "}
                    <span>
                      {view.query} · {title}
                    </span>{" "}
                    <Button
                      variant="link"
                      onClick={() => save({ query: "", count: 100 })}
                    >
                      {t.files.clearFilter}
                    </Button>
                  </>
                ) : (
                  (empty ?? copy.empty)
                )}
              </p>
            )}
          <ul aria-label={title} className="mt-3 divide-y">
            {entries.map((row) => {
              const folderRow = row.kind === "directory",
                disabled =
                  row.accessible === false ||
                  (!folderRow && row.kind !== "file"),
                href = hrefFor?.(row);
              const id = "open:" + row.path;
              const extension = /\.([a-z0-9]{1,5})$/i.exec(row.name)?.[1];
              const visibleName =
                area === "agent" &&
                !folder &&
                row.kind === "directory" &&
                (row.name === "uploads" || row.name === "outputs")
                  ? copy[row.name]
                  : row.name;
              const contents = (
                <>
                  {folderRow ? (
                    <Folder className="size-4 shrink-0" aria-hidden />
                  ) : (
                    <span className={styles.glyph} aria-hidden="true">
                      {extension?.toUpperCase() ?? <File className="size-4" />}
                    </span>
                  )}
                  <span>{visibleName}</span>
                </>
              );
              return (
                <li key={row.path} className={styles.row}>
                  <div className={styles.identity}>
                    {disabled ? (
                      <span className={styles.open}>{contents}</span>
                    ) : href && !folderRow ? (
                      <a
                        className={styles.open}
                        data-file-control={id}
                        href={href}
                        target="_blank"
                        rel="noopener noreferrer"
                        onFocus={() => focus(id)}
                      >
                        {contents}
                      </a>
                    ) : (
                      <button
                        className={styles.open}
                        data-file-control={id}
                        onFocus={() => focus(id)}
                        onClick={() => (folderRow ? step(row.path) : open(row))}
                      >
                        {contents}
                      </button>
                    )}
                    {area === "agent" && !folder && folderRow && (
                      <small>{describe?.(row)}</small>
                    )}
                    {rowStatus?.(row)}
                    {disabled && (
                      <small>{t.storageSpaces.unsupportedEntry}</small>
                    )}
                    {editing && area !== "agent" && (
                      <button
                        className={styles.parent}
                        data-file-control={"parent:" + row.path}
                        onFocus={() => focus("parent:" + row.path)}
                        onClick={() => step(parentFolder(row.path))}
                        aria-label={copy.parent(
                          parentFolder(row.path) || title,
                        )}
                      >
                        <FolderOpen className="size-3" aria-hidden />
                        {parentFolder(row.path) || title}
                      </button>
                    )}
                  </div>
                  <div className={styles.meta}>
                    {!folderRow && <span>{formatArtifactBytes(row.size)}</span>}
                    {row.modified > 0 && (
                      <time dateTime={when(row).toISOString()}>
                        {when(row).toLocaleDateString(locale)}
                      </time>
                    )}
                  </div>
                  <button
                    className={styles.more}
                    type="button"
                    data-file-control={"actions:" + row.path}
                    aria-label={copy.actions(row.name)}
                    onFocus={() => focus("actions:" + row.path)}
                    onClick={(e) => {
                      trigger.current = e.currentTarget;
                      setSelected(row.path);
                    }}
                  >
                    <Ellipsis aria-hidden className="size-5" />
                  </button>
                </li>
              );
            })}
          </ul>
          {!waiting && entries.length < result.length && (
            <Button
              variant="outline"
              className="min-h-11"
              onClick={() => save({ count: view.count + 100 })}
            >
              {copy.more}
            </Button>
          )}
        </div>
      </div>
      <Dialog
        open={!!detail}
        onOpenChange={(v) => {
          if (!v) setSelected(null);
        }}
      >
        <DialogContent
          className="max-h-[85dvh] overflow-y-auto [overflow-wrap:anywhere]"
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            (trigger.current?.isConnected
              ? trigger.current
              : heading.current
            )?.focus();
          }}
        >
          <DialogTitle>{detail?.name}</DialogTitle>
          <DialogDescription>{copy.details}</DialogDescription>
          {detail && (
            <>
              <dl className="grid min-w-0 gap-2 text-sm">
                <div>
                  <dt>{copy.path}</dt>
                  <dd>{detail.path}</dd>
                </div>
                <div>
                  <dt>{copy.audience}</dt>
                  <dd>{audience}</dd>
                </div>
                {detail.kind === "file" && (
                  <div>
                    <dt>{t.files.size}</dt>
                    <dd>{formatArtifactBytes(detail.size)}</dd>
                  </div>
                )}
                {detail.modified > 0 && (
                  <div>
                    <dt>
                      {detail.published_at ? copy.published : t.files.modified}
                    </dt>
                    <dd>{when(detail).toLocaleString(locale)}</dd>
                  </div>
                )}
              </dl>
              {describe?.(detail)}
              {detail.accessible !== false && actions(detail)}
            </>
          )}
        </DialogContent>
      </Dialog>
      <Dialog open={trail} onOpenChange={setTrail}>
        <DialogContent
          className="max-h-[80dvh] overflow-y-auto"
          onCloseAutoFocus={(e) => {
            e.preventDefault();
            trailButton.current?.focus();
          }}
          aria-describedby={undefined}
        >
          <DialogTitle>{copy.path}</DialogTitle>
          <button className={styles.open} onClick={() => step("")}>
            {title}
          </button>
          {parts.map((part, i) => (
            <button
              className={styles.open}
              key={i}
              onClick={() => step(parts.slice(0, i + 1).join("/"))}
            >
              {part}
            </button>
          ))}
        </DialogContent>
      </Dialog>
    </section>
  );
}

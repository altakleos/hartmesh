"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useId, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { SpaceRecoveryPanel } from "@/components/workspace/space-recovery-panel";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import { useAuth } from "@/core/auth/AuthProvider";
import { useDocumentTitle, useStorageSpacesEnabled } from "@/core/features";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import {
  copySpaceFile,
  createSpace,
  mutateSpaceFile,
  readSpaceText,
  spaceFileURL,
  writeSpaceFile,
  type StorageSpace,
  type SpaceFile,
} from "@/core/spaces/api";
import { useSpace, useSpaceFiles, useSpaces } from "@/core/spaces/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

const WRITE = 2;
const EXPORT = 16;

export default function SpacesPage() {
  const { t } = useI18n();
  const { user } = useAuth();
  const capability = useStorageSpacesEnabled();
  const search = useSearchParams();
  const router = useRouter();
  const raw = search.get("space");
  const id = raw && /^[0-9a-f]{32}$/.test(raw) ? raw : null;
  const path = search.get("path") ?? "";
  const spaces = useSpaces(capability.enabled);
  const space = useSpace(id, capability.enabled);
  const lifetime = useFileActionLifetime();
  const captureSignal = useSpaceActionSignal();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [custody, setCustody] = useState<"personal" | "company">("personal");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const formId = useId();
  useDocumentTitle(t.storageSpaces.title, t.pages.appName);

  function navigate(spaceId: string, folder = "") {
    router.replace(
      `/workspace/spaces?${new URLSearchParams({ space: spaceId, ...(folder ? { path: folder } : {}) })}`,
    );
  }

  async function provision() {
    const signal = captureSignal();
    setBusy(true);
    setError(null);
    try {
      const resource = await createSpace(name, custody, signal);
      if (signal.aborted || !lifetime.active) return;
      setCreating(false);
      await spaces.refetch();
      if (!signal.aborted && lifetime.active) navigate(resource.id);
    } catch (failure) {
      if (!signal.aborted && lifetime.active)
        setError(
          failure instanceof Error
            ? failure.message
            : t.storageSpaces.operationError,
        );
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto flex w-full max-w-5xl flex-col gap-4 p-6">
          <h1 className="text-2xl font-semibold">{t.storageSpaces.title}</h1>
          <p className="text-muted-foreground">{t.storageSpaces.description}</p>
          {capability.isLoading ? (
            <p role="status">{t.common.loading}</p>
          ) : !capability.enabled ? (
            <div role="status">
              <h2>{t.storageSpaces.unavailable}</h2>
              <p>{t.storageSpaces.unavailableHint}</p>
            </div>
          ) : (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <label htmlFor={`${formId}-space`}>
                  {t.storageSpaces.title}
                </label>
                <select
                  id={`${formId}-space`}
                  className="bg-background rounded border p-2"
                  value={id ?? ""}
                  onChange={(event) => navigate(event.target.value)}
                >
                  <option value="">{t.storageSpaces.noSpaces}</option>
                  {space.data &&
                    !spaces.data?.spaces.some(
                      (item) => item.id === space.data.id,
                    ) && (
                      <option value={space.data.id}>{space.data.name}</option>
                    )}
                  {spaces.data?.spaces.map((item) => (
                    <option key={item.id} value={item.id}>
                      {item.name}
                    </option>
                  ))}
                </select>
                <Button
                  onClick={() => {
                    setCreating(true);
                    setError(null);
                  }}
                >
                  {t.storageSpaces.newSpace}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => {
                    void spaces.refetch();
                    void space.refetch();
                  }}
                >
                  {t.storageSpaces.reload}
                </Button>
              </div>
              {(spaces.error ?? space.error) && (
                <p role="alert">{t.storageSpaces.loadError}</p>
              )}
              {space.isPending && id ? (
                <p role="status">{t.common.loading}</p>
              ) : (
                space.data &&
                !space.error && (
                  <>
                    {(space.data.permissions & 8) !== 0 &&
                      space.data.mode === "native" && (
                        <SpaceRecoveryPanel
                          key={`${user?.id}:${space.data.id}`}
                          space={space.data}
                          refresh={() => {
                            void space.refetch();
                            void spaces.refetch();
                          }}
                        />
                      )}
                    {space.data.storage_state === "recovery-pending" ? (
                      <p role="status">{t.storageSpaces.recoveryPending}</p>
                    ) : (
                      <SpaceBrowser
                        key={`${user?.id}:${space.data.id}:${path}`}
                        space={space.data}
                        spaces={spaces.data?.spaces ?? []}
                        path={path}
                        navigate={(folder) => navigate(space.data.id, folder)}
                        refresh={() => {
                          void space.refetch();
                        }}
                      />
                    )}
                  </>
                )
              )}
            </>
          )}
        </div>
        <Dialog
          open={creating && capability.enabled}
          onOpenChange={(open) => {
            if (!busy) setCreating(open);
          }}
        >
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t.storageSpaces.newSpace}</DialogTitle>
              <DialogDescription>
                {t.storageSpaces.description}
              </DialogDescription>
            </DialogHeader>
            <label htmlFor={`${formId}-name`}>
              {t.storageSpaces.spaceName}
            </label>
            <Input
              id={`${formId}-name`}
              maxLength={128}
              value={name}
              onChange={(event) => setName(event.target.value)}
              disabled={busy}
            />
            <label htmlFor={`${formId}-custody`}>
              {t.storageSpaces.custody}
            </label>
            <select
              id={`${formId}-custody`}
              value={custody}
              onChange={(event) =>
                setCustody(event.target.value as "personal" | "company")
              }
              disabled={busy}
            >
              <option value="personal">{t.storageSpaces.personal}</option>
              <option value="company">{t.storageSpaces.company}</option>
            </select>
            {error && <p role="alert">{error}</p>}
            <DialogFooter>
              <Button
                variant="outline"
                disabled={busy}
                onClick={() => setCreating(false)}
              >
                {t.common.cancel}
              </Button>
              <Button
                disabled={busy || !name.trim()}
                onClick={() => void provision()}
              >
                {t.common.create}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

type Action = {
  kind: "mkdir" | "create" | "rename" | "remove" | "copy";
  file?: SpaceFile;
} | null;

function SpaceBrowser({
  space,
  spaces,
  path,
  navigate,
  refresh,
}: {
  space: StorageSpace;
  spaces: StorageSpace[];
  path: string;
  navigate: (path: string) => void;
  refresh: () => void;
}) {
  const { t } = useI18n();
  const files = useSpaceFiles(space.id, path, true);
  const lifetime = useFileActionLifetime();
  const captureSignal = useSpaceActionSignal();
  const writable =
    space.status === "active" &&
    space.mode === "native" &&
    (space.permissions & WRITE) !== 0;
  const exportable = (space.permissions & EXPORT) !== 0;
  const [action, setAction] = useState<Action>(null);
  const [editor, setEditor] = useState<SpaceFile | null>(null);
  const [name, setName] = useState("");
  const [text, setText] = useState("");
  const [source, setSource] = useState("");
  const [sourcePath, setSourcePath] = useState("");
  const [acknowledge, setAcknowledge] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const formId = useId();
  const relative = (leaf: string) => (path ? `${path}/${leaf}` : leaf);
  const sources = spaces.filter(
    (item) => item.id !== space.id && (item.permissions & EXPORT) !== 0,
  );

  function choose(next: Action) {
    setAction(next);
    setError(null);
    setName(next?.file?.name ?? "");
    setText("");
    setAcknowledge(false);
  }

  async function run(work: (signal: AbortSignal) => Promise<unknown>) {
    const signal = captureSignal();
    setBusy(true);
    setError(null);
    try {
      await work(signal);
      if (signal.aborted || !lifetime.active) return;
      setAction(null);
      await files.refetch();
      if (!signal.aborted && lifetime.active) refresh();
    } catch (failure) {
      if (!signal.aborted && lifetime.active)
        setError(
          failure instanceof Error
            ? failure.message
            : t.storageSpaces.operationError,
        );
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  async function execute() {
    if (!action || !writable) return;
    const kind = action.kind;
    if (kind === "create")
      await run((signal) =>
        writeSpaceFile(space.id, relative(name), text, {
          generation: space.generation,
          create: true,
          signal,
        }),
      );
    else if (kind === "copy") {
      const origin = sources.find((item) => item.id === source);
      if (origin)
        await run((signal) =>
          copySpaceFile(
            space,
            origin,
            sourcePath,
            relative(name),
            acknowledge,
            signal,
          ),
        );
    } else
      await run((signal) =>
        mutateSpaceFile(
          space.id,
          space.generation,
          kind,
          action.file?.path ?? relative(name),
          action.kind === "rename" ? relative(name) : undefined,
          signal,
        ),
      );
  }

  return (
    <section className="space-y-4" aria-label={space.name}>
      <div className="flex flex-wrap items-center gap-2">
        <h2 className="text-lg font-semibold">{space.name}</h2>
        {!writable && <span>{t.storageSpaces.readOnly}</span>}
        <span className="text-muted-foreground text-sm">
          {space.custody?.kind === "company"
            ? t.storageSpaces.company
            : t.storageSpaces.personal}
        </span>
      </div>
      {space.quota && (
        <p className="text-muted-foreground text-sm">
          {t.storageSpaces.quota}:{" "}
          {space.quota.available_bytes.toLocaleString()} /{" "}
          {space.quota.max_bytes.toLocaleString()} bytes;{" "}
          {space.quota.available_inodes.toLocaleString()} /{" "}
          {space.quota.max_inodes.toLocaleString()} inodes
        </p>
      )}
      <div className="flex flex-wrap items-center gap-2">
        <Button variant="outline" onClick={() => navigate("")}>
          {t.storageSpaces.openRoot}
        </Button>
        {path && (
          <Button
            variant="outline"
            onClick={() => navigate(path.split("/").slice(0, -1).join("/"))}
          >
            {t.storageSpaces.back}
          </Button>
        )}
        <span className="break-all">{path || "/"}</span>
      </div>
      {writable && (
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => choose({ kind: "mkdir" })}>
            {t.storageSpaces.newFolder}
          </Button>
          <Button onClick={() => choose({ kind: "create" })}>
            {t.storageSpaces.newFile}
          </Button>
          <Button variant="outline" onClick={() => choose({ kind: "copy" })}>
            {t.storageSpaces.copy}
          </Button>
          <label className="border-input cursor-pointer rounded border px-3 py-2 text-sm">
            {t.storageSpaces.upload}
            <input
              aria-label={t.storageSpaces.upload}
              className="sr-only"
              type="file"
              disabled={busy}
              onChange={(event) => {
                const file = event.target.files?.[0];
                event.target.value = "";
                if (!file) return;
                if (file.size > 64 * 1024 * 1024) {
                  setError(t.storageSpaces.limitText);
                  return;
                }
                void run((signal) =>
                  writeSpaceFile(space.id, relative(file.name), file, {
                    generation: space.generation,
                    create: true,
                    signal,
                  }),
                );
              }}
            />
          </label>
        </div>
      )}
      <p className="text-muted-foreground text-sm">
        {t.storageSpaces.limitText}
      </p>
      {error && !action && <p role="alert">{error}</p>}
      {files.error ? (
        <p role="alert">{t.storageSpaces.loadError}</p>
      ) : files.isPending ? (
        <p role="status">{t.common.loading}</p>
      ) : (
        <>
          {files.data?.files.length === 0 && <p>{t.storageSpaces.empty}</p>}
          <ul className="divide-y rounded border">
            {files.data?.files.map((file) => (
              <li
                key={file.path}
                className="flex flex-wrap items-center gap-3 p-3"
              >
                {file.kind === "directory" ||
                (file.kind === "symlink" &&
                  file.target_kind === "directory") ? (
                  <Button variant="ghost" onClick={() => navigate(file.path)}>
                    {file.name}/
                  </Button>
                ) : (
                  <span className="min-w-0 flex-1 break-all">
                    {file.name}
                    {file.kind === "symlink" ? " ↗" : ""}
                  </span>
                )}
                {!file.accessible ? (
                  <span className="text-muted-foreground text-sm">
                    {t.storageSpaces.unsupportedEntry}
                  </span>
                ) : (
                  file.kind !== "directory" &&
                  file.target_kind !== "directory" && (
                    <>
                      {exportable && (
                        <a
                          className="text-sm underline"
                          aria-label={`${t.common.download} ${file.name}`}
                          href={spaceFileURL(space.id, file.path, true)}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          {t.common.download}
                        </a>
                      )}
                      <Button
                        variant="outline"
                        disabled={busy}
                        aria-label={`${writable && file.kind === "file" ? t.common.edit : t.common.preview} ${file.name}`}
                        onClick={() => setEditor(file)}
                      >
                        {writable && file.kind === "file"
                          ? t.common.edit
                          : t.common.preview}
                      </Button>
                    </>
                  )
                )}
                {writable && (
                  <>
                    <Button
                      variant="ghost"
                      disabled={busy}
                      aria-label={`${t.common.rename} ${file.name}`}
                      onClick={() => choose({ kind: "rename", file })}
                    >
                      {t.common.rename}
                    </Button>
                    <Button
                      variant="ghost"
                      disabled={busy}
                      aria-label={`${t.common.delete} ${file.name}`}
                      onClick={() => choose({ kind: "remove", file })}
                    >
                      {t.common.delete}
                    </Button>
                  </>
                )}
              </li>
            ))}
          </ul>
          {files.data?.truncated && (
            <p role="status">{t.storageSpaces.truncated}</p>
          )}
        </>
      )}
      <Dialog
        open={action !== null}
        onOpenChange={(open) => {
          if (!open && !busy) setAction(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {action?.kind === "remove"
                ? t.storageSpaces.confirmRemove
                : action?.kind === "copy"
                  ? t.storageSpaces.copy
                  : action?.kind === "mkdir"
                    ? t.storageSpaces.newFolder
                    : action?.kind === "rename"
                      ? t.common.rename
                      : t.storageSpaces.newFile}
            </DialogTitle>
            <DialogDescription>
              {action?.kind === "remove"
                ? t.storageSpaces.removeHint
                : action?.kind === "copy"
                  ? t.storageSpaces.disclosure
                  : t.storageSpaces.description}
            </DialogDescription>
          </DialogHeader>
          {action?.kind !== "remove" && (
            <>
              <label htmlFor={`${formId}-name`}>
                {t.storageSpaces.newName}
              </label>
              <Input
                id={`${formId}-name`}
                value={name}
                onChange={(event) => setName(event.target.value)}
                disabled={busy}
              />
            </>
          )}
          {action?.kind === "create" && (
            <>
              <label htmlFor={`${formId}-text`}>
                {t.storageSpaces.textContent}
              </label>
              <textarea
                id={`${formId}-text`}
                className="min-h-40 rounded border p-2 font-mono"
                value={text}
                onChange={(event) => setText(event.target.value)}
                disabled={busy}
              />
            </>
          )}
          {action?.kind === "copy" && (
            <>
              <label htmlFor={`${formId}-source`}>
                {t.storageSpaces.copySource}
              </label>
              <select
                id={`${formId}-source`}
                value={source}
                onChange={(event) => setSource(event.target.value)}
                disabled={busy}
              >
                <option value="">{t.storageSpaces.copySource}</option>
                {sources.map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.name}
                  </option>
                ))}
              </select>
              <label htmlFor={`${formId}-source-path`}>
                {t.storageSpaces.sourcePath}
              </label>
              <Input
                id={`${formId}-source-path`}
                value={sourcePath}
                onChange={(event) => setSourcePath(event.target.value)}
                disabled={busy}
              />
              <label className="flex gap-2">
                <input
                  type="checkbox"
                  checked={acknowledge}
                  onChange={(event) => setAcknowledge(event.target.checked)}
                  disabled={busy}
                />
                {t.storageSpaces.disclosureLabel}
              </label>
            </>
          )}
          {error && (
            <div role="alert">
              <p>{error}</p>
              <p>{t.storageSpaces.operationError}</p>
            </div>
          )}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={busy}
              onClick={() => setAction(null)}
            >
              {t.common.cancel}
            </Button>
            <Button
              disabled={
                busy ||
                (action?.kind !== "remove" && !name.trim()) ||
                (action?.kind === "copy" && (!source || !sourcePath))
              }
              onClick={() => void execute()}
            >
              {action?.kind === "remove" ? t.common.delete : t.common.save}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {editor && (
        <TextEditor
          key={editor.path}
          space={space}
          file={editor}
          editable={writable && editor.kind === "file"}
          close={() => setEditor(null)}
          changed={() => {
            void files.refetch();
            refresh();
          }}
        />
      )}
    </section>
  );
}

function TextEditor({
  space,
  file,
  close,
  changed,
  editable,
}: {
  space: StorageSpace;
  file: SpaceFile;
  close: () => void;
  changed: () => void;
  editable: boolean;
}) {
  const { t } = useI18n();
  const lifetime = useFileActionLifetime();
  const captureSignal = useSpaceActionSignal();
  const [text, setText] = useState("");
  const [revision, setRevision] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [generation] = useState(space.generation);
  const inputId = useId();

  useEffect(() => {
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, lifetime.signal]);
    void readSpaceText(space.id, file.path, signal)
      .then((value) => {
        if (!signal.aborted) {
          setText(value.text);
          setRevision(value.sha256);
        }
      })
      .catch((failure: unknown) => {
        if (!signal.aborted)
          setError(
            failure instanceof Error
              ? failure.message
              : t.storageSpaces.noEditor,
          );
      });
    return () => controller.abort();
  }, [space.id, file.path, lifetime, t.storageSpaces.noEditor]);

  async function save() {
    const signal = captureSignal();
    if (!revision || !editable) return;
    setBusy(true);
    setError(null);
    try {
      const result = await writeSpaceFile(space.id, file.path, text, {
        generation,
        expectedSha256: revision,
        signal,
      });
      if (signal.aborted || !lifetime.active) return;
      setRevision(result.sha256);
      changed();
      close();
    } catch (failure) {
      if (!signal.aborted && lifetime.active)
        setError(
          failure instanceof Error
            ? failure.message
            : t.storageSpaces.operationError,
        );
    } finally {
      if (!signal.aborted && lifetime.active) setBusy(false);
    }
  }

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) close();
      }}
    >
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>{file.name}</DialogTitle>
          <DialogDescription>
            {editable
              ? t.storageSpaces.editorNotice
              : t.storageSpaces.utf8Notice}
          </DialogDescription>
        </DialogHeader>
        {revision ? (
          <>
            <label htmlFor={inputId}>{t.storageSpaces.textContent}</label>
            <textarea
              id={inputId}
              className="min-h-80 rounded border p-3 font-mono text-sm"
              value={text}
              onChange={(event) => setText(event.target.value)}
              disabled={busy}
              readOnly={!editable}
            />
          </>
        ) : (
          !error && <p role="status">{t.common.loading}</p>
        )}
        <p className="text-muted-foreground text-sm">
          {t.storageSpaces.utf8Notice}
        </p>
        {error && (
          <div role="alert">
            <p>{error}</p>
            {editable && <p>{t.storageSpaces.operationError}</p>}
          </div>
        )}
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={close}>
            {t.common.cancel}
          </Button>
          {editable && (
            <Button
              disabled={busy || revision === null}
              onClick={() => void save()}
            >
              {t.common.save}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

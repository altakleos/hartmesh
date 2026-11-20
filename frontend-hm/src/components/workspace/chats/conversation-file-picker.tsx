"use client";

import { useCallback, useLayoutEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { FileOperationNotice } from "@/components/workspace/instances/file-operation-notice";
import { InstanceApiError, operationID } from "@/core/agent-instances/api";
import {
  checkFileOperation,
  fileOutcome,
  type CheckState,
  unansweredWrite,
} from "@/core/agent-instances/file-status";
import { useAgentInstances } from "@/core/features";
import {
  readStorageRefusal,
  type StorageRefusal,
} from "@/core/file-areas/storage-state";
import { useI18n } from "@/core/i18n/hooks";
import {
  importedAttachment,
  importConversationFile,
  presentedImportFiles,
  type PresentedCursor,
  type PresentedFile,
  type ImportDestination,
  type ImportSource,
  type SavedAttachment,
} from "@/core/threads/conversation-file-import";
import { formatUploadSize } from "@/core/uploads";


import {
  ConversationFileSources,
  PageFooter,
  usePickerPage,
  type SourceBrowser,
} from "./conversation-file-sources";
import { FilesStateNotice } from "./files-state-notice";

function Files({
  source,
  select,
}: {
  source: ImportSource;
  select: (file: PresentedFile) => void;
}) {
  const { locale, t } = useI18n();
  const copy = t.inputBox.conversationFile;
  const load = useCallback(
    (cursor: PresentedCursor | null, signal: AbortSignal) =>
      presentedImportFiles(source, cursor, signal),
    [source],
  );
  const { page, busy, error, more, retry } = usePickerPage<
    PresentedFile,
    PresentedCursor | null
  >(null, load);
  return (
    <>
      <ul>
        {!error &&
          page.items.map((file) => (
            <li key={JSON.stringify([file.run_id, file.path])}>
              <Button
                type="button"
                variant="ghost"
                className="h-auto w-full flex-col items-start text-left whitespace-normal"
                disabled={!file.available}
                onClick={() => select(file)}
              >
                <span className="break-all">{file.filename}</span>
                <span className="text-muted-foreground text-xs">
                  {!file.available
                    ? copy.unavailable
                    : [
                        file.size !== null ? formatUploadSize(file.size) : null,
                        file.modified !== null
                          ? copy.changed(
                              new Date(file.modified * 1000).toLocaleString(
                                locale,
                              ),
                            )
                          : null,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                </span>
              </Button>
            </li>
          ))}
      </ul>
      <PageFooter
        busy={busy}
        error={error}
        empty={!page.items.length}
        emptyMessage={copy.noFiles}
        denied={
          source.instanceName
            ? copy.needsInspect(source.instanceName)
            : undefined
        }
        more={page.next !== null}
        onMore={more}
        onRetry={retry}
      />
    </>
  );
}
type Outcome =
  | "confirm"
  | "saving"
  | "unknown"
  | "failed"
  | "refused"
  | "large"
  | "busy";
interface PickerProps {
  account: string;
  agentReadAllowed: boolean;
  destination: ImportDestination;
  close: () => void;
  onSaved: (file: SavedAttachment, sourceTitle?: string) => void;
  maxBytes?: number;
  openRequest?: number;
}
export function ConversationFilePicker(props: PickerProps) {
  return !props.destination.instance && props.agentReadAllowed ? (
    <PersonalFilePicker {...props} />
  ) : (
    <FilePicker {...props} agentSources={false} />
  );
}
function PersonalFilePicker(props: PickerProps) {
  const feature = useAgentInstances();
  const agentSources = feature.known && feature.enabled;
  return <FilePicker {...props} agentSources={agentSources} />;
}
function FilePicker({
  account,
  destination,
  close,
  onSaved,
  maxBytes,
  openRequest = 0,
  agentSources,
}: PickerProps & { agentSources: boolean }) {
  const [browse, setBrowse] = useState<SourceBrowser>("recent");
  const { t } = useI18n();
  const copy = t.inputBox.conversationFile;
  const [source, setSource] = useState<ImportSource | null>(null);
  const [choice, setChoice] = useState<{
    file: PresentedFile;
    id: string;
  } | null>(null);
  const [refusal, setRefusal] = useState<StorageRefusal | null>(null);
  const [lookup, setLookup] = useState<CheckState | null>(null);
  const [collapsedAt, setCollapsedAt] = useState<number | null>(null);
  const [outcome, setOutcome] = useState<Outcome>("confirm");
  const owner = useRef<AbortController | null>(null);
  useLayoutEffect(() => () => owner.current?.abort(), []);

  const lostAgentSource =
    !agentSources && (browse !== "recent" || !!source?.instanceName);
  useLayoutEffect(() => {
    if (lostAgentSource) {
      owner.current?.abort();
      owner.current = null;
      setBrowse("recent");
      setSource(null);
      setChoice(null);
      setRefusal(null);
      setOutcome("confirm");
    }
  }, [lostAgentSource]);

  const saved = (file: SavedAttachment) => {
    if (owner.current?.signal.aborted || !source) return;
    onSaved(
      file,
      destination.instance
        ? undefined
        : (source.title ?? t.agentInstances.conversations.untitled),
    );
    close();
  };
  async function check(file: PresentedFile, id: string, signal: AbortSignal) {
    if (!destination.instance) return;
    setOutcome("failed");
    setLookup("checking");
    await checkFileOperation(
      destination.instance.id,
      { id, thread: { id: destination.threadId, filename: file.filename } },
      signal,
      false,
      (state, result) => {
        if (signal.aborted) return;
        if (state === "complete" && result?.sandbox_path) {
          const filename = result.sandbox_path.split("/").at(-1) ?? "";
          saved(
            importedAttachment({
              ...result,
              filename,
              virtual_path: result.sandbox_path,
            }),
          );
        } else setLookup(state === "complete" ? "unavailable" : state);
      },
    );
  }
  async function copyChoice(file: PresentedFile, id: string) {
    if (!source || owner.current) return;
    const controller = new AbortController();
    owner.current = controller;
    setLookup(null);
    setOutcome("saving");
    try {
      const receipt = await importConversationFile(
        destination,
        source,
        file,
        id,
        controller.signal,
      );
      if (!controller.signal.aborted) saved(receipt);
    } catch (error) {
      if (controller.signal.aborted) return;
      if (!destination.instance && error instanceof InstanceApiError) {
        const denied = readStorageRefusal({
          ...error.extra,
          reason: error.reason,
          status: error.status,
        });
        if (denied) {
          setRefusal(denied);
          setOutcome("failed");
          return;
        }
      }
      const reported = fileOutcome(error);
      if (
        destination.instance &&
        (reported === "unconfirmed" || reported === "nothing-changed")
      ) {
        setOutcome("failed");
        setLookup(reported);
      } else if (destination.instance && unansweredWrite(error))
        await check(file, id, controller.signal);
      else if (error instanceof InstanceApiError)
        setOutcome(
          error.status === 413
            ? "large"
            : [403, 404].includes(error.status)
              ? "refused"
              : error.status === 503
                ? "busy"
                : "failed",
        );
      else setOutcome("unknown");
    } finally {
      if (owner.current === controller) owner.current = null;
    }
  }
  const select = (file: PresentedFile) => {
    const id = operationID();
    setChoice({ file, id });
    if (maxBytes !== undefined && file.size !== null && file.size > maxBytes) {
      setOutcome("large");
      return;
    }
    setOutcome("confirm");
    if (!destination.instance) void copyChoice(file, id);
  };
  const agent = destination.instance;
  const cancel = () => {
    if (
      agent &&
      choice &&
      (outcome === "saving" ||
        (lookup &&
          ["checking", "pending", "unknown", "unavailable"].includes(lookup)))
    ) {
      setCollapsedAt(openRequest);
      return;
    }
    owner.current?.abort();
    close();
  };
  const checkAgain = () => {
    if (!choice || owner.current) return;
    const controller = new AbortController();
    owner.current = controller;
    void check(choice.file, choice.id, controller.signal).finally(() => {
      if (owner.current === controller) owner.current = null;
    });
  };
  if (collapsedAt === openRequest && choice)
    return (
      <div
        className="flex flex-wrap items-center gap-1"
        role="group"
        aria-label={choice.file.filename}
      >
        <Button
          variant="outline"
          type="button"
          onClick={() => setCollapsedAt(null)}
        >
          {copy.check}: {choice.file.filename}
        </Button>
        <Button
          variant="ghost"
          type="button"
          onClick={() => {
            owner.current?.abort();
            close();
          }}
        >
          {t.agentInstances.remove}
        </Button>
      </div>
    );
  const outcomeText = {
    saving: copy.copying,
    unknown: agent ? copy.uncertain : copy.unconfirmedOwn,
    refused: copy.refused,
    large: copy.tooLarge,
    busy: agent ? t.agentInstances.filesBusy(agent.name) : copy.failed,
    failed: copy.failed,
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) cancel();
      }}
    >
      <DialogContent className="max-h-[85dvh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {choice && agent ? copy.giveTitle(agent.name) : copy.menu}
          </DialogTitle>
          <DialogDescription>
            {choice && agent
              ? copy.disclosure(choice.file.filename, agent.name)
              : source
                ? (source.title ?? t.agentInstances.conversations.untitled)
                : copy.sources}
          </DialogDescription>
        </DialogHeader>
        {choice ? (
          <div className="space-y-3">
            {lookup && agent ? (
              <FileOperationNotice
                state={lookup}
                file={choice.file.filename}
                conversation={agent.name}
                check={checkAgain}
              />
            ) : refusal ? (
              <FilesStateNotice
                area="upload"
                refusal={refusal}
                filename={choice.file.filename}
              />
            ) : (
              outcome !== "confirm" && (
                <p role="status">{outcomeText[outcome]}</p>
              )
            )}
            <div className="flex flex-wrap gap-2">
              {outcome === "confirm" && agent && (
                <Button
                  type="button"
                  onClick={() => void copyChoice(choice.file, choice.id)}
                >
                  {copy.give}
                </Button>
              )}
              {lookup === "nothing-changed" && (
                <Button
                  type="button"
                  onClick={() => {
                    const id = operationID();
                    setChoice({ ...choice, id });
                    void copyChoice(choice.file, id);
                  }}
                >
                  {copy.retry}
                </Button>
              )}
              <Button type="button" variant="outline" onClick={cancel}>
                {outcome === "confirm" ? t.common.cancel : t.common.close}
              </Button>
            </div>
          </div>
        ) : source ? (
          <>
            <Files key={source.thread_id} source={source} select={select} />
            <Button
              variant="outline"
              type="button"
              onClick={() => setSource(null)}
            >
              {copy.previous}
            </Button>
          </>
        ) : (
          <ConversationFileSources
            key={
              typeof browse === "string"
                ? browse + String(agentSources)
                : browse.id
            }
            browse={browse}
            setBrowse={setBrowse}
            agentSources={agentSources}
            account={account}
            destination={destination}
            select={setSource}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

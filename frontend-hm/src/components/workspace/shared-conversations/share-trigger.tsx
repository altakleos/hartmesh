"use client";

import { useQuery } from "@tanstack/react-query";
import { Share2Icon } from "lucide-react";
import { useRouter } from "next/navigation";
import { useId, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogTrigger,
} from "@/components/ui/dialog";
import { canManageInstance, MANAGE } from "@/core/agent-instances/presentation";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import {
  publishSnapshot,
  snapshotDraft,
  snapshotHref,
  undoSnapshot,
} from "@/core/shared-conversations/api";
import {
  useSharing,
  useRefreshShared,
} from "@/core/shared-conversations/hooks";
import { reviewedAgentAudience } from "@/core/shared-conversations/protocol";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { useSnapshotCopy } from "./copy";

interface ShareAgent {
  id: string;
  name: string;
  supervisor: string;
  permissions: number | null;
}
export function ShareConversationTrigger({
  thread,
  company,
  canExport,
  agent,
}: {
  agent?: ShareAgent;
  thread: string;
  company: string | null;
  canExport: boolean;
}) {
  const access = useSharing();
  const w = useSnapshotCopy();
  const reasonId = useId();
  if (agent && !access.agentPublishing) return null;
  if (agent?.permissions != null && !canManageInstance(agent.permissions)) {
    const reason =
      agent.permissions & MANAGE
        ? w.needInspect(agent.supervisor)
        : w.needManager(agent.name, agent.supervisor);
    return (
      <span className="group relative inline-flex">
        <Button
          size="icon"
          variant="ghost"
          type="button"
          aria-disabled="true"
          aria-label={w.shareConversation}
          aria-describedby={reasonId}
          title={reason}
        >
          <Share2Icon />
        </Button>
        <span
          id={reasonId}
          className="bg-popover text-popover-foreground absolute end-0 top-full z-50 hidden w-60 rounded border p-2 text-sm shadow group-focus-within:block group-hover:block"
        >
          {reason}
        </span>
      </span>
    );
  }
  if (!access.can("publish") || agent?.permissions === null) return null;
  return (
    <ShareDialog
      key={JSON.stringify([
        access.scope,
        thread,
        agent?.id,
        agent?.permissions,
      ])}
      agent={agent}
      thread={thread}
      company={company}
      canExport={canExport}
      scope={access.scope}
      canUndo={access.canUndo}
    />
  );
}
function ShareDialog({
  thread,
  company,
  canExport,
  scope,
  canUndo,
  agent,
}: {
  agent?: ShareAgent;
  thread: string;
  company: string | null;
  canExport: boolean;
  scope: string;
  canUndo: boolean;
}) {
  const w = useSnapshotCopy(),
    router = useRouter(),
    lifetime = useFileActionLifetime(),
    capture = useSpaceActionSignal(thread),
    refresh = useRefreshShared();
  const [open, setOpen] = useState(false),
    [pending, setPending] = useState(false),
    [failure, setFailure] = useState<string | null>(null);
  const draft = useQuery({
    queryKey: ["shared-conversations", scope, "draft", thread, agent?.id],
    queryFn: ({ signal }) => snapshotDraft(thread, signal),
    enabled: open,
    retry: false,
    gcTime: 0,
  });
  const data = draft.data;
  const audience = agent ? reviewedAgentAudience(data, agent.id) : null;
  const [ack, setAck] = useState<string | null>(null);
  const reviewKey =
    audience && data
      ? JSON.stringify([data.snapshot, data.with, audience, company])
      : null;
  const confirmed = !agent || (reviewKey !== null && ack === reviewKey);
  async function publish() {
    if (!data || pending || draft.isFetching || draft.isError || !confirmed)
      return;
    const signal = capture();
    setPending(true);
    setFailure(null);
    try {
      const result = await publishSnapshot(
        thread,
        data.snapshot,
        signal,
        agent ? { snapshot: data.snapshot, audience: "company" } : undefined,
      );
      if (signal.aborted) return;
      setOpen(false);
      void refresh();
      const receipt = result.undo_receipt;
      const undo = async () => {
        if (!receipt || !lifetime.active) return;
        try {
          await undoSnapshot(
            result.publication_id,
            result.created_publication_ids,
            receipt,
            lifetime.signal,
          );
          if (lifetime.active) lifetime.toasts.add(toast.success(w.removed));
        } catch (e) {
          if (lifetime.active) lifetime.toasts.add(toast.error(w.error(e)));
        } finally {
          if (lifetime.active) void refresh();
        }
      };
      lifetime.toasts.add(
        toast.success(result.already_shared ? w.already : w.shared(company), {
          action:
            canUndo && !!receipt && result.created_publication_ids.length
              ? { label: w.undo, onClick: () => void undo() }
              : {
                  label: w.open,
                  onClick: () => {
                    if (lifetime.active)
                      router.push(snapshotHref(result.publication_id));
                  },
                },
        }),
      );
    } catch (e) {
      if (!signal.aborted) {
        setAck(null);
        setFailure(w.error(e));
        void draft.refetch();
        void refresh();
      }
    } finally {
      if (!signal.aborted) setPending(false);
    }
  }
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!pending) {
          setFailure(null);
          setAck(null);
          setOpen(next);
        }
      }}
    >
      <DialogTrigger asChild>
        <Button
          size="icon"
          variant="ghost"
          aria-label={w.shareConversation}
          title={w.shareConversation}
        >
          <Share2Icon />
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{w.share}</DialogTitle>
          <DialogDescription>
            {agent ? w.after(company) : w.audience(company)}
          </DialogDescription>
        </DialogHeader>
        <p>{w.freeze}</p>
        {canExport && <p>{w.outside}</p>}
        {draft.isFetching ? (
          <p role="status">{w.loading}</p>
        ) : draft.isError ? (
          <p role="alert">
            {w.error(draft.error)}{" "}
            <Button variant="outline" onClick={() => void draft.refetch()}>
              {w.retry}
            </Button>
          </p>
        ) : (
          data &&
          (agent && !audience ? (
            <p role="alert">{w.unavailable}</p>
          ) : (
            <div className="max-h-60 overflow-auto">
              <h3 className="font-medium">{data.title || w.untitled}</h3>
              {agent && audience && (
                <div className="space-y-2">
                  <p>
                    {w.currentAudience(
                      data.with!.name,
                      audience.starter,
                      audience.starter_can_read,
                      audience.inspect_count,
                      audience.chat_count,
                    )}
                  </p>
                  {audience.authors.length > 0 && (
                    <p>{w.authors(audience.authors)}</p>
                  )}
                  <p>{w.currentFiles(data.with!.name)}</p>
                </div>
              )}
              <p>{w.count(data.message_count)}</p>
              <ul>
                {data.files.map((f) => (
                  <li key={f.path}>
                    {f.path} ·{" "}
                    {new Intl.NumberFormat(w.locale, {
                      style: "unit",
                      unit: "byte",
                    }).format(f.size)}
                    {f.also_named?.length ? (
                      <span className="block text-sm">
                        {w.alias(f.also_named)}
                      </span>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          ))
        )}
        {agent &&
          audience &&
          !data?.already_shared &&
          !draft.isFetching &&
          !draft.isError && (
            <label className="flex items-start gap-2">
              <input
                type="checkbox"
                checked={confirmed}
                onChange={(e) => setAck(e.target.checked ? reviewKey : null)}
              />
              {w.acknowledge(company)}
            </label>
          )}
        {failure && <p role="alert">{failure}</p>}
        <DialogFooter>
          <Button
            disabled={pending}
            variant="outline"
            onClick={() => setOpen(false)}
          >
            {w.cancel}
          </Button>
          {(!agent || audience) &&
          data?.already_shared &&
          data.publication_id &&
          !draft.isFetching &&
          !draft.isError ? (
            <Button
              onClick={() => router.push(snapshotHref(data.publication_id!))}
            >
              {w.already} {w.open}
            </Button>
          ) : (
            <Button
              disabled={
                pending ||
                draft.isFetching ||
                draft.isError ||
                !data ||
                !confirmed
              }
              onClick={() => void publish()}
            >
              {w.confirm}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

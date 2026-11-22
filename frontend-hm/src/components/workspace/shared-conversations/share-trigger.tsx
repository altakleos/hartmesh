"use client";

import { useQuery } from "@tanstack/react-query";
import { Share2Icon } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState } from "react";
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
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { useSnapshotCopy } from "./copy";

export function ShareConversationTrigger({
  thread,
  company,
}: {
  thread: string;
  company: string | null;
}) {
  const access = useSharing();
  if (!access.can("publish")) return null;
  return (
    <ShareDialog
      key={JSON.stringify([access.scope, thread])}
      thread={thread}
      company={company}
      scope={access.scope}
      canUndo={access.canUndo}
    />
  );
}
function ShareDialog({
  thread,
  company,
  scope,
  canUndo,
}: {
  thread: string;
  company: string | null;
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
    queryKey: ["shared-conversations", scope, "draft", thread],
    queryFn: ({ signal }) => snapshotDraft(thread, signal),
    enabled: open,
    retry: false,
    gcTime: 0,
  });
  const data = draft.data;
  async function publish() {
    if (!data || pending || draft.isFetching || draft.isError) return;
    const signal = capture();
    setPending(true);
    setFailure(null);
    try {
      const result = await publishSnapshot(thread, data.snapshot, signal);
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
          <DialogDescription>{w.audience(company)}</DialogDescription>
        </DialogHeader>
        <p>{w.freeze}</p>
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
          data && (
            <div className="max-h-60 overflow-auto">
              <h3 className="font-medium">{data.title || w.untitled}</h3>
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
          )
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
          {data?.already_shared &&
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
              disabled={pending || draft.isFetching || draft.isError || !data}
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

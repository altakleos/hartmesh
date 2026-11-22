"use client";
import { useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState, useRef } from "react";
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
import { MarkdownContent } from "@/components/workspace/messages/markdown-content";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { urlOfSharedFile } from "@/core/shared/api";
import {
  continueSnapshot,
  SnapshotError,
  removeSnapshot,
} from "@/core/shared-conversations/api";
import { saveContinuedDraft } from "@/core/shared-conversations/handoff";
import {
  useSharing,
  useSnapshotDetail,
  useRefreshShared,
} from "@/core/shared-conversations/hooks";
import {
  continueTarget,
  sameTarget,
  validAnswerer,
  type Answerer,
  type ContinueTarget,
  type Snapshot,
  type SharedMember,
} from "@/core/shared-conversations/protocol";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import { getSessionComposerDraftStorage } from "@/core/threads/composer-draft";

import { useSnapshotCopy } from "./copy";

const listHref = "/workspace/files?tab=shared";
function Member({ item, name }: { item: SharedMember; name?: string }) {
  const w = useSnapshotCopy();
  return (
    <li className="flex flex-wrap items-center gap-3 py-2">
      <span className="break-all">{name ?? item.path}</span>
      {item.also_named?.length ? (
        <span className="text-sm">{w.alias(item.also_named)}</span>
      ) : null}
      {item.available && item.shared_path && item.publication_id ? (
        <>
          <a
            className="underline"
            href={urlOfSharedFile(item.shared_path)}
            target="_blank"
            rel="noopener noreferrer"
          >
            {w.open}
          </a>
          <a
            className="underline"
            href={urlOfSharedFile(item.shared_path, { download: true })}
            target="_blank"
            rel="noopener noreferrer"
          >
            {w.download}
          </a>
        </>
      ) : (
        <span>{w.missing}</span>
      )}
    </li>
  );
}
export function SnapshotReader({ id }: { id: string }) {
  const w = useSnapshotCopy(),
    access = useSharing(),
    detail = useSnapshotDetail(id, access.can("read"), access.scope);
  const row = !detail.isError && access.can("read") ? detail.data : undefined;
  return (
    <main className="mx-auto w-full max-w-3xl overflow-auto p-4 sm:p-8">
      <Link href={listHref} className="underline">
        {w.group}
      </Link>
      {row && "removed" in row ? (
        <p role="status" className="my-4">
          {w.retired(
            row.publisher ?? w.unknown,
            new Date(row.published_at).toLocaleDateString(w.locale),
          )}
        </p>
      ) : row && access.account ? (
        <ActiveSnapshot
          key={JSON.stringify([access.scope, id])}
          row={row}
          account={access.account}
          canContinue={access.can("continue")}
          refresh={() => void detail.refetch()}
        />
      ) : (
        <p role="status">
          {detail.isFetching ? w.loading : w.unavailable}{" "}
          {detail.isError && (
            <Button onClick={() => void detail.refetch()} variant="outline">
              {w.retry}
            </Button>
          )}
        </p>
      )}
    </main>
  );
}
function ActiveSnapshot({
  row,
  account,
  canContinue,
  refresh,
}: {
  row: Snapshot;
  account: string;
  canContinue: boolean;
  refresh: () => void;
}) {
  const w = useSnapshotCopy(),
    client = useQueryClient(),
    router = useRouter(),
    lifetime = useFileActionLifetime(),
    capture = useSpaceActionSignal(row.publication_id),
    refreshLists = useRefreshShared();
  const [pending, setPending] = useState(false),
    [removing, setRemoving] = useState(false),
    [failure, setFailure] = useState<string | null>(null);
  const requestRef = useRef<{ id: string; target: ContinueTarget } | null>(
    null,
  );
  const [changed, setChanged] = useState<{
    at: Snapshot;
    value: Answerer | null;
  } | null>(null);
  const choice = changed?.at === row ? changed.value : row.answerer;
  const answerer =
    validAnswerer(choice) &&
    (choice.kind === "assistant" || choice.id === row.with?.id)
      ? choice
      : null;
  const keepCopies = row.with
    ? w.copies + " " + w.agentCopies(row.with.name)
    : w.copies;
  const onContinue = async () => {
    if (!answerer || pending) return;
    const target = continueTarget(answerer);
    const signal = capture();
    setPending(true);
    setFailure(null);
    const store = getSessionComposerDraftStorage(),
      key = JSON.stringify([
        "shared-conversation-request",
        account,
        row.publication_id,
      ]);
    try {
      let attempt = requestRef.current;
      if (!attempt) {
        try {
          const raw = store?.getItem(key);
          if (raw && /^[a-f0-9-]{36}$/.test(raw))
            attempt = { id: raw, target: { kind: "assistant" } };
          else if (raw) {
            const v = JSON.parse(raw) as { id: string; target: ContinueTarget };
            if (
              /^[a-f0-9-]{36}$/.test(v.id) &&
              v.target &&
              (v.target.kind === "assistant" ||
                (v.target.kind === "agent" && typeof v.target.id === "string"))
            )
              attempt = v;
          }
        } catch {
          /* use this visit's request */
        }
      }
      if (!attempt || !sameTarget(attempt.target, target))
        attempt = { id: crypto.randomUUID(), target };
      requestRef.current = attempt;
      try {
        store?.setItem(key, JSON.stringify(attempt));
      } catch {
        /* retain in memory */
      }
      const result = await continueSnapshot(
        row.publication_id,
        attempt.id,
        signal,
        target,
      );
      if (signal.aborted) return;
      if (
        !validAnswerer(result.answerer) ||
        !sameTarget(result.answerer, target) ||
        !result.thread_id
      )
        throw new Error("Conversation response changed");
      saveContinuedDraft(
        client,
        account,
        row.publication_id,
        attempt.id,
        row.title,
        result,
      );
      try {
        store?.removeItem(key);
      } catch {
        /* the same request remains replayable */
      }
      requestRef.current = null;
      router.push("/workspace/chats/" + encodeURIComponent(result.thread_id));
    } catch (e) {
      if (!signal.aborted) {
        if (
          e instanceof SnapshotError &&
          e.status === 409 &&
          e.detail &&
          typeof e.detail === "object" &&
          "code" in e.detail &&
          e.detail.code === "answerer_changed"
        ) {
          setChanged({
            at: row,
            value:
              "answerer" in e.detail && validAnswerer(e.detail.answerer)
                ? e.detail.answerer
                : null,
          });
          requestRef.current = null;
          try {
            store?.removeItem(key);
          } catch {
            /* the next confirmed choice has its own request */
          }
          setFailure(w.changedAnswerer);
        } else setFailure(w.error(e));
        refresh();
      }
    } finally {
      if (!signal.aborted) setPending(false);
    }
  };
  const remove = async () => {
    const signal = capture();
    setPending(true);
    setFailure(null);
    try {
      const result = await removeSnapshot(row.publication_id, signal);
      if (signal.aborted) return;
      const retained = result.retained
        .map((f) => f.path + (f.publisher ? " (" + f.publisher + ")" : ""))
        .join(", ");
      lifetime.toasts.add(
        toast.success(
          [w.removed, keepCopies, retained ? w.retained + ": " + retained : ""]
            .filter(Boolean)
            .join(" "),
        ),
      );
      router.push(listHref);
    } catch (e) {
      if (!signal.aborted) setFailure(w.error(e));
    } finally {
      if (!signal.aborted) {
        setRemoving(false);
        setPending(false);
        void refreshLists();
        refresh();
      }
    }
  };
  return (
    <article className="space-y-6 py-5">
      <header>
        <h1 className="text-2xl font-semibold break-words">
          {row.title || w.untitled}
        </h1>
        {row.with && <p>{w.with(row.with.name)}</p>}
        <p>
          {w.by(
            row.publisher ?? w.unknown,
            new Date(row.published_at).toLocaleDateString(w.locale),
          )}
        </p>
      </header>
      <ol className="space-y-4">
        {row.messages.map((m, i) => (
          <li key={i} className="rounded-md border p-3">
            <h2 className="text-muted-foreground text-sm font-semibold">
              {m.type === "human" ? w.user : (row.with?.name ?? w.product)}
            </h2>
            <MarkdownContent content={m.content} isLoading={false} />
          </li>
        ))}
      </ol>
      <section>
        <h2 className="font-medium">{w.files}</h2>
        <ul>
          <Member name={w.transcript} item={row.transcript} />
          {row.files.map((f) => (
            <Member key={f.path} item={f} />
          ))}
        </ul>
      </section>
      {canContinue && answerer && (
        <div>
          <Button disabled={pending} onClick={() => void onContinue()}>
            {answerer.kind === "agent"
              ? w.continueWith(answerer.name)
              : w.continue}
          </Button>
          {answerer.kind === "assistant" && row.with && (
            <p className="mt-2 text-sm">
              {w.reason(row.with.name, answerer.reason) ??
                w.fallback(row.with.name)}
            </p>
          )}
          {answerer.kind === "agent" &&
            row.files.some((file) => file.available) && (
              <p className="mt-2 text-sm">
                {w.intoHome(
                  answerer.name,
                  row.files.filter((file) => file.available).length,
                )}
              </p>
            )}
        </div>
      )}
      {row.can_remove && (
        <Dialog
          open={removing}
          onOpenChange={(v) => {
            if (!pending) setRemoving(v);
          }}
        >
          <DialogTrigger asChild>
            <Button variant="outline" disabled={pending}>
              {w.remove}
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{w.removeTitle}</DialogTitle>
              <DialogDescription>{keepCopies}</DialogDescription>
            </DialogHeader>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={pending}
                onClick={() => setRemoving(false)}
              >
                {w.cancel}
              </Button>
              <Button
                variant="destructive"
                disabled={pending}
                onClick={() => void remove()}
              >
                {w.remove}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      )}
      {failure && <p role="alert">{failure}</p>}
    </article>
  );
}

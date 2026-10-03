"use client";

import { useParams, usePathname, useRouter } from "next/navigation";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { resetThreadChatAfterDelete } from "@/components/workspace/chats/use-thread-chat";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import { useDeleteThread } from "@/core/threads/hooks";
import type { AgentThread } from "@/core/threads/types";
import { pathOfThread, titleOfThread } from "@/core/threads/utils";

type DeleteRequest = {
  thread: AgentThread;
  recentThreadId?: string | undefined;
};
type DeleteTarget = {
  id: string;
  title: string;
  path: string;
  newPath: string;
  wasCurrentNew: boolean;
};
const ThreadDeleteContext = createContext<
  ((request: DeleteRequest) => void) | null
>(null);

export function useThreadDeleteDialog() {
  const request = useContext(ThreadDeleteContext);
  if (!request) throw new Error("ThreadDeleteDialogProvider is required");
  return request;
}

/** Outside the changing rows and collapsible list: failed cleanup retains its target for retry. */
export function ThreadDeleteDialogProvider({
  children,
}: {
  children: ReactNode;
}) {
  const { t } = useI18n();
  const router = useRouter();
  const pathname = usePathname();
  const { agent_name: agentName } = useParams<{ agent_name?: string }>();
  const route = useRef({ pathname, agentName });
  route.current = { pathname, agentName };
  const lifetime = useFileActionLifetime();
  const { mutateAsync: deleteThread, isPending } = useDeleteThread();
  const [target, setTarget] = useState<DeleteTarget | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inFlight = useRef(false);
  const mounted = useRef(true);
  const cancelButton = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  useEffect(() => {
    if (error && !isPending) cancelButton.current?.focus();
  }, [error, isPending]);

  const requestDelete = useCallback(
    ({ thread, recentThreadId }: DeleteRequest) => {
      if (inFlight.current || !lifetime.active) return;
      const currentPath =
        typeof window === "undefined"
          ? route.current.pathname
          : window.location.pathname;
      const newPath = pathOfThread("new", {
        agent_name: route.current.agentName,
      });
      setError(null);
      setTarget({
        id: thread.thread_id,
        title: titleOfThread(thread),
        path: pathOfThread(thread),
        newPath,
        wasCurrentNew:
          currentPath === newPath && recentThreadId === thread.thread_id,
      });
    },
    [lifetime],
  );

  const confirm = useCallback(async () => {
    if (!target || inFlight.current || !lifetime.active) return;
    inFlight.current = true;
    setError(null);
    try {
      await deleteThread({
        threadId: target.id,
        onRemoteDeleted: () => {
          if (!mounted.current || !lifetime.active) return;
          // Navigation can change while the server is deleting. Never reset a
          // different conversation based on the route captured on first click.
          const currentPath =
            typeof window === "undefined"
              ? route.current.pathname
              : window.location.pathname;
          const currentThreadPath = pathOfThread(target.id, {
            agent_name: route.current.agentName,
          });
          if (
            currentPath !== target.path &&
            currentPath !== currentThreadPath &&
            !(target.wasCurrentNew && currentPath === target.newPath)
          )
            return;
          const nextPath = pathOfThread("new", {
            agent_name: route.current.agentName,
          });
          resetThreadChatAfterDelete({
            deletedThreadId: target.id,
            nextPath,
          });
          void router.replace(nextPath);
        },
      });
      if (mounted.current && lifetime.active) setTarget(null);
    } catch (failure) {
      if (mounted.current && lifetime.active) {
        setError(
          failure instanceof Error && failure.message
            ? failure.message
            : t.chats.deleteFailed,
        );
      }
    } finally {
      inFlight.current = false;
    }
  }, [deleteThread, lifetime, router, t.chats.deleteFailed, target]);

  return (
    <ThreadDeleteContext.Provider value={requestDelete}>
      {children}
      <Dialog
        open={target !== null}
        onOpenChange={(open) => {
          if (!open && !inFlight.current) setTarget(null);
        }}
      >
        <DialogContent
          role="alertdialog"
          showCloseButton={false}
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            cancelButton.current?.focus();
          }}
          onEscapeKeyDown={(event) => {
            if (inFlight.current) event.preventDefault();
          }}
          onInteractOutside={(event) => event.preventDefault()}
        >
          <DialogHeader>
            <DialogTitle>{t.chats.deleteChat}</DialogTitle>
            <DialogDescription className="break-words">
              {target && t.chats.deleteConfirm(target.title)}
            </DialogDescription>
          </DialogHeader>
          {error && (
            <p role="alert" className="text-destructive text-sm break-words">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button
              ref={cancelButton}
              variant="outline"
              disabled={isPending}
              onClick={() => {
                if (!inFlight.current) setTarget(null);
              }}
            >
              {t.common.cancel}
            </Button>
            <Button
              variant="destructive"
              data-testid="thread-delete-confirm-button"
              disabled={isPending}
              onClick={() => void confirm()}
            >
              {isPending ? t.chats.deleting : t.common.delete}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </ThreadDeleteContext.Provider>
  );
}

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useCallback, useRef } from "react";
import { toast } from "sonner";

import { hasCeiling } from "../agent-instances/presentation";
import { useAuth } from "../auth/AuthProvider";
import { useStorage } from "../features";
import { parentFolder } from "../file-areas/browser";
import { useBrowserProtocol } from "../file-areas/browser-protocol";
import { useFileActionLifetime } from "../file-areas/file-action-lifetime";
import { useStorageCopy } from "../file-areas/storage-copy";
import {
  readWorkspaceFolder,
  retryWorkspaceFolder,
  type WorkspaceList,
} from "../file-areas/workspace-folder";
import { useI18n } from "../i18n/hooks";

import {
  listSharedFiles,
  publishToShared,
  removeSelectedSharedFile,
  type PublicationSelection,
  type SharedFileInfo,
  type PublishOutcome,
  type PublishToSharedRequest,
} from "./api";

export const SHARED_FILES_QUERY_KEY = ["files", "shared"] as const;

export function useSharedFiles(folder?: string) {
  const { user } = useAuth();
  return useQuery({
    queryKey: [
      ...SHARED_FILES_QUERY_KEY,
      user?.id,
      user?.system_role,
      user?.permissions,
      ...(folder === undefined ? [] : ["folder", folder]),
    ],
    queryFn: ({ signal }): Promise<WorkspaceList<SharedFileInfo>> =>
      folder === undefined
        ? listSharedFiles(signal)
        : readWorkspaceFolder<SharedFileInfo>("shared", folder, signal),
    ...(folder !== undefined && { retry: retryWorkspaceFolder }),
    gcTime: 0,
    staleTime: 0,
    refetchOnWindowFocus: true,
  });
}

export function useRemoveSharedFile() {
  const queryClient = useQueryClient();
  const { user } = useAuth(),
    protocol = useBrowserProtocol();
  const refusalKey = [
    "file-browser-availability",
    user?.id,
    user?.system_role,
    user?.permissions,
    "shared-remove",
  ];
  const refusal = useQuery({
    queryKey: refusalKey,
    queryFn: () => false,
    enabled: false,
    initialData: false,
    staleTime: Infinity,
  });
  const enabled =
    !refusal.data &&
    !!user &&
    hasCeiling(user.permissions, "threads:delete") &&
    protocol.supports("shared", "remove");
  const live = useRef(enabled);
  live.current = enabled;
  const lifetime = useFileActionLifetime();
  const refused = (error: unknown) => {
    if (
      error &&
      typeof error === "object" &&
      "status" in error &&
      error.status === 404 &&
      "routeMissing" in error &&
      error.routeMissing === true
    ) {
      queryClient.setQueryData(refusalKey, true);
    }
    void queryClient.invalidateQueries({ queryKey: SHARED_FILES_QUERY_KEY });
    void queryClient.invalidateQueries({ queryKey: ["file-browser"] });
    void protocol.refresh();
  };
  const mutation = useMutation({
    mutationFn: (action: {
      selection: PublicationSelection;
      operationId: string;
    }) => {
      if (
        !live.current ||
        queryClient.getQueryData(refusalKey) === true ||
        !lifetime.active
      )
        throw new Error("Publication removal is unavailable");
      return removeSelectedSharedFile(
        action.selection,
        action.operationId,
        lifetime.signal,
      );
    },
    onError: refused,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: SHARED_FILES_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ["file-browser"] });
    },
  });
  return { ...mutation, enabled, refused };
}

export function usePublishToShared() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: PublishToSharedRequest) => publishToShared(request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: SHARED_FILES_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ["file-browser"] });
    },
  });
}

/**
 * *Share with everyone*, for a card or a panel action: publishes each path,
 * then says what happened where the person is looking. A failure part-way
 * leaves what already landed — the Shared tab shows it — and names the
 * failure rather than reporting a success it did not have.
 *
 * Whether something is already shared is the server's to say, not this
 * hook's to remember: the publish route answers with the entry that already
 * holds the same bytes instead of copying them again, so a second click
 * from anywhere, in any tab or session, says *already shared* and publishes
 * nothing. Nothing here is disabled on that account, because a page's memory
 * of its own clicks is exactly what a tab switch loses.
 */
export function useShareWithEveryone(threadId?: string) {
  const { t } = useI18n();
  const router = useRouter();
  const publish = usePublishToShared();
  const remove = useRemoveSharedFile();
  const lifetime = useFileActionLifetime();
  const copy = useStorageCopy("shared");
  const storage = useStorage();
  const { user } = useAuth();
  const available =
    !!user &&
    hasCeiling(user.permissions, "threads:write") &&
    storage.state !== "unavailable";
  const openShared = useCallback(() => {
    if (lifetime.active) router.push("/workspace/files?tab=shared");
  }, [lifetime, router]);
  const share = useCallback(
    async (paths: readonly string[], folder?: string) => {
      if (!available) return [];
      const outcomes: { path: string; outcome: PublishOutcome }[] = [];
      let failed = 0;
      let lastFailure: unknown = null;
      for (const path of paths) {
        if (!lifetime.active) break;
        try {
          outcomes.push({
            path,
            outcome: await publish.mutateAsync({
              path,
              thread_id: threadId,
              folder,
            }),
          });
        } catch (error) {
          if (!lifetime.active) break;
          console.error("Share with everyone failed:", path, error);
          lastFailure = error;
          failed += 1;
        }
      }
      const published = outcomes.map(({ outcome }) => outcome.file);
      if (!lifetime.active) return published;
      if (failed > 0) {
        lifetime.toasts.add(
          copy.error(
            lastFailure,
            t.shared.shareFailed,
            published.length > 0
              ? t.shared.sharedSome(published.length, paths.length)
              : undefined,
          ),
        );
      }
      if (published.length === 0) {
        return published;
      }
      // Only what this click put there is this click's to take back; what
      // was already there stays whoever's it was.
      const fresh = outcomes
        .filter(({ outcome }) => !outcome.alreadyShared)
        .map(({ outcome }) => outcome.file);
      if (fresh.length === 0) {
        if (failed > 0) return published;
        // Shared holds bytes, so the entry that was there may carry another
        // name than the one the person clicked; the sentence says both, or
        // it reads as the wrong file.
        const clicked = outcomes[0]!.path.split("/").pop()!;
        const sharedAs = published[0]!.name;
        lifetime.toasts.add(
          toast.info(
            published.length > 1
              ? t.shared.alreadySharedMany(published.length)
              : clicked === sharedAs
                ? t.shared.alreadyShared(clicked)
                : t.shared.alreadySharedAs(clicked, sharedAs),
            { action: { label: t.shared.openShared, onClick: openShared } },
          ),
        );
        return published;
      }
      // The folder is derived from the file, so the person never chose it:
      // the toast is where they find out which one it was.
      const undo =
        remove.enabled &&
        fresh.every(
          (file) => file.can_remove && !!file.publication_id && !!file.revision,
        )
          ? fresh.map((file) => ({
              selection: {
                path: file.path,
                name: file.name,
                publication_id: file.publication_id!,
                revision: file.revision!,
                can_remove: true,
              },
              operationId: crypto.randomUUID().replaceAll("-", ""),
            }))
          : [];
      const destination = parentFolder(fresh[0]!.path);
      const commonFolder = fresh.every(
        (file) => parentFolder(file.path) === destination,
      )
        ? destination
        : "";
      if (failed && !undo.length) return published;
      const notify = failed ? toast.info : toast.success;
      lifetime.toasts.add(
        notify(
          fresh.length === 1
            ? commonFolder
              ? t.shared.sharedInFolder(fresh[0]!.name, commonFolder)
              : t.shared.shared(fresh[0]!.name)
            : commonFolder
              ? t.shared.sharedManyInFolder(fresh.length, commonFolder)
              : t.shared.sharedMany(fresh.length),
          {
            // Handing a file to the whole company is one click, so taking it
            // back is one click too, right where the person is looking.
            action: undo.length
              ? {
                  label: t.shared.undo,
                  onClick: () => {
                    void (async () => {
                      try {
                        for (const action of undo) {
                          if (!lifetime.active) return;
                          await remove.mutateAsync(action);
                        }
                        if (!lifetime.active) return;
                        lifetime.toasts.add(
                          toast.success(t.shared.undone(fresh[0]!.name)),
                        );
                      } catch (error) {
                        if (!lifetime.active) return;
                        console.error("Undo share failed:", error);
                        lifetime.toasts.add(
                          toast.error(
                            copy.message(error, t.shared.removeFailed),
                          ),
                        );
                      }
                    })();
                  },
                }
              : undefined,
          },
        ),
      );
      return published;
    },
    [
      available,
      copy,
      lifetime,
      openShared,
      publish,
      remove,
      t.shared,
      threadId,
    ],
  );
  return {
    share,
    openShared,
    isPending: publish.isPending,
    available,
    unavailableTooltip: t.chat.filesUnavailable,
  };
}

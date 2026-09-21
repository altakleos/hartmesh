import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useCallback } from "react";
import { toast } from "sonner";

import { messageForFileAreaError } from "../file-areas";
import { useI18n } from "../i18n/hooks";

import {
  listSharedFiles,
  publishToShared,
  removeSharedFile,
  type PublishOutcome,
  type PublishToSharedRequest,
} from "./api";

export const SHARED_FILES_QUERY_KEY = ["files", "shared"] as const;

export function useSharedFiles() {
  return useQuery({
    queryKey: SHARED_FILES_QUERY_KEY,
    queryFn: () => listSharedFiles(),
    staleTime: 0,
    refetchOnWindowFocus: true,
  });
}

export function useRemoveSharedFile() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (path: string) => removeSharedFile(path),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: SHARED_FILES_QUERY_KEY });
    },
  });
}

export function usePublishToShared() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: PublishToSharedRequest) => publishToShared(request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: SHARED_FILES_QUERY_KEY });
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
  const openShared = useCallback(
    () => router.push("/workspace/files?tab=shared"),
    [router],
  );
  const share = useCallback(
    async (paths: readonly string[], folder?: string) => {
      const outcomes: { path: string; outcome: PublishOutcome }[] = [];
      let failed = 0;
      let lastFailure: unknown = null;
      for (const path of paths) {
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
          console.error("Share with everyone failed:", path, error);
          lastFailure = error;
          failed += 1;
        }
      }
      const published = outcomes.map(({ outcome }) => outcome.file);
      if (failed > 0) {
        toast.error(
          published.length > 0
            ? t.shared.sharedSome(published.length, paths.length)
            : messageForFileAreaError(lastFailure, t.shared.shareFailed),
        );
        return published;
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
        // Shared holds bytes, so the entry that was there may carry another
        // name than the one the person clicked; the sentence says both, or
        // it reads as the wrong file.
        const clicked = outcomes[0]!.path.split("/").pop()!;
        const sharedAs = published[0]!.name;
        toast.info(
          published.length > 1
            ? t.shared.alreadySharedMany(published.length)
            : clicked === sharedAs
              ? t.shared.alreadyShared(clicked)
              : t.shared.alreadySharedAs(clicked, sharedAs),
          { action: { label: t.shared.openShared, onClick: openShared } },
        );
        return published;
      }
      toast.success(
        fresh.length === 1
          ? t.shared.shared(fresh[0]!.name)
          : t.shared.sharedMany(fresh.length),
        {
          // Handing a file to the whole company is one click, so taking it
          // back is one click too, right where the person is looking.
          action: {
            label: t.shared.undo,
            onClick: () => {
              void (async () => {
                try {
                  for (const file of fresh) {
                    await remove.mutateAsync(file.path);
                  }
                  toast.success(t.shared.undone(fresh[0]!.name));
                } catch (error) {
                  console.error("Undo share failed:", error);
                  toast.error(
                    messageForFileAreaError(error, t.shared.removeFailed),
                  );
                }
              })();
            },
          },
        },
      );
      return published;
    },
    [openShared, publish, remove, t.shared, threadId],
  );
  return {
    share,
    openShared,
    isPending: publish.isPending,
  };
}

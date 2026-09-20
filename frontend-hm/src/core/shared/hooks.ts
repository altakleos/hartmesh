import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useCallback, useState } from "react";
import { toast } from "sonner";

import { messageForFileAreaError } from "../file-areas";
import { useI18n } from "../i18n/hooks";

import {
  listSharedFiles,
  publishToShared,
  removeSharedFile,
  type PublishToSharedRequest,
  type SharedFileInfo,
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
 */
export function useShareWithEveryone(threadId?: string) {
  const { t } = useI18n();
  const router = useRouter();
  const publish = usePublishToShared();
  const remove = useRemoveSharedFile();
  // What this card or panel has already handed over, so the action can say so
  // instead of silently publishing a second copy under an `_N` name.
  const [sharedPaths, setSharedPaths] = useState<readonly string[]>([]);
  const share = useCallback(
    async (paths: readonly string[], folder?: string) => {
      const published: SharedFileInfo[] = [];
      let failed = 0;
      let lastFailure: unknown = null;
      for (const path of paths) {
        try {
          published.push(
            await publish.mutateAsync({ path, thread_id: threadId, folder }),
          );
        } catch (error) {
          console.error("Share with everyone failed:", path, error);
          lastFailure = error;
          failed += 1;
        }
      }
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
      setSharedPaths((already) => [...already, ...paths]);
      toast.success(
        published.length === 1
          ? t.shared.shared(published[0]!.name)
          : t.shared.sharedMany(published.length),
        {
          // Handing a file to the whole company is one click, so taking it
          // back is one click too, right where the person is looking.
          action: {
            label: t.shared.undo,
            onClick: () => {
              void (async () => {
                try {
                  for (const file of published) {
                    await remove.mutateAsync(file.path);
                  }
                  setSharedPaths((already) =>
                    already.filter((path) => !paths.includes(path)),
                  );
                  toast.success(t.shared.undone(published[0]!.name));
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
    [publish, remove, t.shared, threadId],
  );
  const openShared = useCallback(
    () => router.push("/workspace/files?tab=shared"),
    [router],
  );
  return {
    share,
    openShared,
    isPending: publish.isPending,
    /** Whether every one of *paths* has already been handed over from here. */
    hasShared: useCallback(
      (paths: readonly string[]) =>
        paths.length > 0 && paths.every((path) => sharedPaths.includes(path)),
      [sharedPaths],
    ),
  };
}

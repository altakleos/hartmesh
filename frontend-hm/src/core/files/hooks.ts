import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useCallback } from "react";
import { toast } from "sonner";

import { useI18n } from "../i18n/hooks";

import {
  deleteMyFile,
  keepInMyFiles,
  listMyFiles,
  type KeepInMyFilesRequest,
  type MyFileInfo,
} from "./api";

export const MY_FILES_QUERY_KEY = ["files", "mine"] as const;

export function useMyFiles() {
  return useQuery({
    queryKey: MY_FILES_QUERY_KEY,
    queryFn: () => listMyFiles(),
    staleTime: 0,
    refetchOnWindowFocus: true,
  });
}

export function useDeleteMyFile() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (path: string) => deleteMyFile(path),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: MY_FILES_QUERY_KEY });
    },
  });
}

export function useKeepInMyFiles(threadId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: KeepInMyFilesRequest) =>
      keepInMyFiles(threadId, request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: MY_FILES_QUERY_KEY });
    },
  });
}

/**
 * *Save to my files*, for a card or a panel action: keeps each path, then
 * says what happened where the person is looking. A failure part-way leaves
 * what was already kept in place — the Files page shows it — and names the
 * failure rather than reporting a success it did not have.
 */
export function useSaveToMyFiles(threadId: string) {
  const { t } = useI18n();
  const router = useRouter();
  const keep = useKeepInMyFiles(threadId);
  const save = useCallback(
    async (paths: readonly string[], folder?: string) => {
      const kept: MyFileInfo[] = [];
      let failed = 0;
      // Every path gets its try: a render a rebuild deleted must not stop
      // the ones that exist from being kept.
      for (const path of paths) {
        try {
          kept.push(await keep.mutateAsync({ path, folder }));
        } catch (error) {
          // The Gateway's reason names paths and rules in system words; the
          // person gets what happened and what to do, the console the rest.
          console.error("Save to My files failed:", path, error);
          failed += 1;
        }
      }
      if (failed > 0) {
        toast.error(
          kept.length > 0
            ? t.files.savedSome(kept.length, paths.length)
            : t.files.saveFailed,
        );
        return kept;
      }
      if (kept.length === 0) {
        return kept;
      }
      // Where it went is part of what happened: a report's downloads are
      // filed under their own folder, and a person who is not told goes
      // looking at the root.
      toast.success(
        kept.length === 1
          ? folder
            ? t.files.savedToFolder(kept[0]!.name, folder)
            : t.files.saved(kept[0]!.name)
          : folder
            ? t.files.savedManyToFolder(kept.length, folder)
            : t.files.savedMany(kept.length),
        {
          action: {
            label: t.files.openMyFiles,
            onClick: () => router.push("/workspace/files"),
          },
        },
      );
      return kept;
    },
    [keep, router, t.files],
  );
  return { save, isPending: keep.isPending };
}

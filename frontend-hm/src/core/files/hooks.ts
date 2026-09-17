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
      try {
        for (const path of paths) {
          kept.push(await keep.mutateAsync({ path, folder }));
        }
      } catch (error) {
        toast.error(
          error instanceof Error ? error.message : t.files.saveFailed,
        );
        return kept;
      }
      if (kept.length === 0) {
        return kept;
      }
      toast.success(
        kept.length === 1
          ? t.files.saved(kept[0]!.name)
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

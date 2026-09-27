import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  discardAccountExport,
  getAccountExport,
  startAccountExport,
  type AccountExportStatus,
} from "./api";

export const ACCOUNT_EXPORT_QUERY_KEY = ["account", "export"] as const;

/** How often the dialog asks while the export is prepared, and while its parts wait to be downloaded. */
export const BUILDING_POLL_MS = 1500;
export const READY_POLL_MS = 5000;

/** The person's export while the dialog is open: followed closely while it builds, loosely once ready. */
export function useAccountExport(enabled: boolean) {
  return useQuery({
    queryKey: ACCOUNT_EXPORT_QUERY_KEY,
    queryFn: () => getAccountExport(),
    enabled,
    staleTime: 0,
    // Nothing of one person's export outlives the dialog: the next to sign in on this tab never sees it.
    gcTime: 0,
    retry: false,
    refetchInterval: (query) => {
      const state = query.state.data?.state;
      if (state === "building") return BUILDING_POLL_MS;
      if (state === "ready" || state === "downloaded") return READY_POLL_MS;
      return false;
    },
  });
}

export function useStartAccountExport() {
  const queryClient = useQueryClient();
  return useMutation({
    // A poll already on its way must not overwrite what this answers.
    onMutate: () =>
      queryClient.cancelQueries({ queryKey: ACCOUNT_EXPORT_QUERY_KEY }),
    mutationFn: () => startAccountExport(),
    onSuccess: (status) => {
      queryClient.setQueryData<AccountExportStatus | null>(
        ACCOUNT_EXPORT_QUERY_KEY,
        status,
      );
    },
  });
}

export function useDiscardAccountExport() {
  const queryClient = useQueryClient();
  return useMutation({
    onMutate: () =>
      queryClient.cancelQueries({ queryKey: ACCOUNT_EXPORT_QUERY_KEY }),
    mutationFn: () => discardAccountExport(),
    onSuccess: () => {
      queryClient.setQueryData<AccountExportStatus | null>(
        ACCOUNT_EXPORT_QUERY_KEY,
        null,
      );
    },
  });
}

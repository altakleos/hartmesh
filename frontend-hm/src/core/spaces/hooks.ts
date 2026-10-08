import { useQuery } from "@tanstack/react-query";

import { useAuth } from "@/core/auth/AuthProvider";

import { getSpace, getSpaceRecovery, listSpaceFiles, listSpaces } from "./api";

export function useSpaces(enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["spaces", user?.id],
    queryFn: ({ signal }) => listSpaces(signal),
    enabled,
    staleTime: 0,
  });
}

export function useSpace(id: string | null, enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["space", user?.id, id],
    queryFn: ({ signal }) => getSpace(id!, signal),
    enabled: enabled && id !== null,
    staleTime: 0,
  });
}

export function useSpaceFiles(id: string, path: string, enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["space-files", user?.id, id, path],
    queryFn: ({ signal }) => listSpaceFiles(id, path, signal),
    enabled,
    staleTime: 0,
  });
}

export function useSpaceRecovery(id: string, enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["space-recovery", user?.id, id],
    queryFn: ({ signal }) => getSpaceRecovery(id, signal),
    enabled,
    staleTime: 0,
  });
}

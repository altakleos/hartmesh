import { useQuery } from "@tanstack/react-query";

import { useAuth } from "@/core/auth/AuthProvider";

import { getConversationInstance } from "./api";

/** The server binding owns identity; metadata and URL parameters are display data. */
export function useConversationInstance(threadId: string, enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["conversation-instance", user?.id, user?.system_role, threadId],
    queryFn: ({ signal }) => getConversationInstance(threadId, signal),
    enabled: enabled && !!user,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
}

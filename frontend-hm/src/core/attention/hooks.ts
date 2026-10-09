import { useQuery } from "@tanstack/react-query";

import { useAuth } from "@/core/auth/AuthProvider";

import { listInput, type InboxView } from "./api";

export function useAttention(
  view: InboxView = "pending",
  offset = 0,
  work?: string,
  enabled = true,
) {
  const { user } = useAuth();
  const read =
    !!user &&
    (!user.permissions ||
      user.permissions.some((p) =>
        ["*", "agents:*", "agents:read"].includes(p),
      ));
  return useQuery({
    queryKey: [
      "attention",
      user?.id,
      user?.system_role,
      user?.permissions,
      view,
      offset,
      work,
    ],
    queryFn: ({ signal }) => listInput(view, offset, work, signal),
    enabled: enabled && read,
    retry: false,
    staleTime: 0,
    gcTime: 0,
    refetchInterval: 30000,
    refetchIntervalInBackground: false,
  });
}

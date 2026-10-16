import { useQuery } from "@tanstack/react-query";

import { getInstanceGrants } from "@/core/agent-instances/api";
import { useAuth } from "@/core/auth/AuthProvider";

import type { InputRequest } from "./api";

export function eligibleInputRecipient(
  permissions: number,
  purpose: InputRequest["purpose"],
) {
  const required = purpose === "information" ? 3 : 6;
  return (permissions & required) === required;
}
export function useInputRecipients(
  instance: string,
  purpose: InputRequest["purpose"],
  enabled: boolean,
) {
  const { user } = useAuth();
  const query = useQuery({
    queryKey: [
      "input-recipients",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance,
      enabled,
    ],
    queryFn: ({ signal }) => getInstanceGrants(instance, signal),
    enabled: enabled && !!instance,
    retry: false,
    gcTime: 0,
    staleTime: 0,
    refetchOnWindowFocus: false,
  });
  const grants =
    enabled && !query.isError
      ? (query.data?.grants.filter((row) =>
          eligibleInputRecipient(row.permissions, purpose),
        ) ?? [])
      : [];
  return {
    grants,
    known: enabled && query.isSuccess && !query.isError,
    error: query.isError,
    refresh: query.refetch,
  };
}

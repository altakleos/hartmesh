import { useQuery } from "@tanstack/react-query";

import { loadToolPlaneGovernance, ToolPlaneRequestError } from "./api";
import type { ToolPlaneScopeKind } from "./types";

export function useToolPlaneGovernance(
  scopeKind: ToolPlaneScopeKind,
  enabled = true,
) {
  const query = useQuery({
    queryKey: ["toolPlaneGovernance", scopeKind],
    queryFn: () => loadToolPlaneGovernance(scopeKind),
    enabled,
    retry: (count, error) =>
      !(error instanceof ToolPlaneRequestError) && count < 3,
  });
  const serviceUnavailable =
    query.error instanceof ToolPlaneRequestError &&
    query.error.isServiceUnavailable;

  return {
    governance: query.data,
    error: query.error,
    isLoading: query.isLoading,
    serviceUnavailable,
    // Loading and unexpected failures fail closed. Only the Gateway's explicit
    // "feature not installed" response permits the legacy write surfaces.
    legacyMutationBlocked: !serviceUnavailable,
  };
}

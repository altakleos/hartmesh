import { useQuery, type UseQueryOptions } from "@tanstack/react-query";
import { useEffect } from "react";

import { useAuth } from "@/core/auth/AuthProvider";
import { useProductIdentity } from "@/core/product/context";

import {
  type FeaturesResponse,
  fetchFeatures,
  selectAgentInstances,
  selectBranding,
  selectCustomerAdministration,
  selectBrowserControlEnabled,
  selectMcpTasksEnabled,
  selectRunInputsEnabled,
  selectSubagentBatchesCapability,
  selectStorageSpacesEnabled,
  selectStorageState,
  selectSeats,
  selectTelegram,
  type TelegramFeature,
  selectWorkspacePresentation,
} from "./api";
import { useTenantPresentation } from "./tenant-presentation";

/** All feature observers share transport, retries and the identity's cache. */
export function useFeatures<T>(
  select: (features: FeaturesResponse) => T,
  options: Pick<
    UseQueryOptions<FeaturesResponse, Error, T>,
    "refetchInterval"
  > = {},
) {
  const { user } = useAuth();
  return useQuery({
    queryKey: ["features", user?.id ?? null, user?.system_role ?? null],
    queryFn: fetchFeatures,
    select,
    // Re-check when a consumer mounts or the window regains focus, preserving
    // live deployment changes and recovery after a temporary discovery failure.
    staleTime: 0,
    refetchOnMount: true,
    retry: 2,
    ...options,
  });
}

/**
 * Whose workspace this is. A named company replaces the product's name in the
 * sidebar header, the tab title and About, and takes the product's own links
 * out of the menu. Until the deployment has answered -- still loading, or the
 * fetch failed and will be retried on the next mount or focus -- `isLoading`
 * is true and neither name is shown: deployment-authored copy waits for the
 * answer (the rule `welcome.tsx` applies), because a tenant's header flashing
 * the product's name on every load is the wrong first thing to see.
 */
export function useBranding() {
  const { data } = useFeatures(selectBranding);
  const snapshot = useTenantPresentation();
  const identity = useProductIdentity();
  const provider = identity?.mode === "configured" ? identity.provider : null;
  if (snapshot && snapshot.mode !== "legacy")
    return {
      companyName: snapshot.tenant?.name ?? null,
      primary: null,
      secondary: null,
      hasLogo: snapshot.tenant !== null,
      logo: snapshot.tenant?.logo ?? null,
      snapshotId: snapshot.snapshot_id,
      strict: true,
      providerName: provider?.name ?? null,
      supportURL: provider?.support_url ?? null,
      isLoading: false,
    };
  return {
    companyName: data?.companyName ?? null,
    primary: data?.primary ?? null,
    secondary: data?.secondary ?? null,
    hasLogo: data?.hasLogo ?? false,
    providerName: provider?.name ?? data?.providerName ?? null,
    supportURL: provider?.support_url ?? data?.supportURL ?? null,
    isLoading: data === undefined,
    logo: null,
    snapshotId: null,
    strict: false,
  };
}

/**
 * The name the tab carries after the page's own: the company's, else the
 * product's, else nothing while the deployment has not answered.
 */
export function useDocumentTitle(page: string, productName: string) {
  const { companyName, isLoading, strict } = useBranding();
  const identity = useProductIdentity();
  const configured = identity?.mode === "configured";
  useEffect(() => {
    document.title =
      isLoading && !configured
        ? page
        : `${page} - ${configured ? identity.name : strict ? productName : (companyName ?? productName)}`;
  }, [companyName, isLoading, page, productName, configured, identity, strict]);
}

/**
 * Whether the Gateway accepts Queue/Steer inputs for an active run. A missing,
 * malformed or failed answer reads as off, so the composer keeps its ordinary
 * wait-while-streaming behaviour.
 */
export function useRunInputsEnabled(enabled = true) {
  const { data } = useFeatures(selectRunInputsEnabled);
  return enabled && data === true;
}

export function useBrowserControlEnabled() {
  const { data, isPending } = useFeatures(selectBrowserControlEnabled);
  return {
    enabled: data ?? false,
    isLoading: isPending,
  };
}

export function useMcpTasksEnabled() {
  const { data, isPending } = useFeatures(selectMcpTasksEnabled);
  return {
    enabled: data ?? false,
    isLoading: isPending,
  };
}

/**
 * Fails closed: until the deployment has answered, nothing about Telegram
 * renders. A later failed read keeps the answer, so a Gateway restart does not
 * close an open dialog; the server still refuses by itself.
 */
export function useTelegram(): TelegramFeature & { isLoading: boolean } {
  const { data, isPending } = useFeatures(selectTelegram);
  return { ...(data ?? selectTelegram(undefined)), isLoading: isPending };
}

export function useStorageSpacesEnabled() {
  const { data, isPending, isError } = useFeatures(selectStorageSpacesEnabled);
  return { enabled: !isError && data === true, isLoading: isPending };
}

/** Storage administration belongs outside the business workspace for every role. */
export function useSpacesPageVisible() {
  const { enabled } = useStorageSpacesEnabled();
  const { profile } = useWorkspacePresentation();
  return enabled && profile !== "business";
}

export function useStorage() {
  const query = useFeatures(selectStorageState, {
    refetchInterval: (query) =>
      !query.state.error &&
      ["not-assigned", "fault"].includes(
        selectStorageState(query.state.data).own,
      )
        ? 15_000
        : false,
  });
  return {
    ...(query.isError
      ? selectStorageState(undefined)
      : (query.data ?? selectStorageState(undefined))),
    isLoading: query.isPending,
    dataUpdatedAt: query.dataUpdatedAt,
    refetch: query.refetch,
  };
}

export function useAgentInstances() {
  const query = useFeatures(selectAgentInstances);
  return {
    ...(query.isError
      ? selectAgentInstances(undefined)
      : (query.data ?? selectAgentInstances(undefined))),
    isLoading: query.isPending,
    dataUpdatedAt: query.dataUpdatedAt,
    refetch: query.refetch,
  };
}

export function useSeats() {
  const query = useFeatures(selectSeats);
  return !query.isError && query.data
    ? { ...query.data, isLoading: query.isPending }
    : null;
}

export function useSubagentBatchesCapability() {
  const { data, isPending } = useFeatures(selectSubagentBatchesCapability);
  return {
    repositoryAvailable: data?.repositoryAvailable ?? false,
    workerRunning: data?.workerRunning ?? false,
    maxRunning: data?.maxRunning ?? 0,
    isLoading: isPending,
  };
}

export function useWorkspacePresentation() {
  const { data, isPending } = useFeatures(selectWorkspacePresentation);
  return {
    profile: data?.profile,
    starters: data?.starters,
    isLoading: isPending,
  };
}

/**
 * Whether to offer the screens that only make sense to someone building the
 * deployment: skills, tools, subagents, integrations, and the developer
 * scheduled-task recipes.
 *
 * Hiding them is presentation, not authorization — the routes behind them are
 * unchanged and `authorization` has no permission covering them; `system_role`
 * is what limits a person.
 *
 * A control someone might need stays offered while the deployment's answer is
 * unknown, which is what every deployment did before this existed.
 * Deployment-authored copy follows the opposite rule and waits for the answer.
 */
export function useDeveloperSurfacesVisible() {
  const { profile, isLoading } = useWorkspacePresentation();
  const { user } = useAuth();
  if (isLoading || profile === undefined) {
    return true;
  }
  return profile !== "business" || user?.system_role === "admin";
}

export function useCustomerAdministration() {
  const { data, isPending, isError } = useFeatures(
    selectCustomerAdministration,
  );
  const effective = isError ? undefined : data;
  return {
    pluginManagement: effective?.pluginManagement ?? false,
    localSkillManagement: effective?.localSkillManagement ?? false,
    localMcpManagement: effective?.localMcpManagement ?? false,
    providerOperations: effective?.providerOperations ?? false,
    isLoading: isPending,
  };
}

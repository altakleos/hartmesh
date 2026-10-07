import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";

import { useAuth } from "@/core/auth/AuthProvider";

import {
  type FeaturesResponse,
  fetchFeatures,
  selectBranding,
  selectCustomerAdministration,
  selectBrowserControlEnabled,
  selectMcpTasksEnabled,
  selectSubagentBatchesCapability,
  selectStorageSpacesEnabled,
  selectWorkspacePresentation,
} from "./api";

/** All feature observers share transport, retries and the identity's cache. */
export function useFeatures<T>(select: (features: FeaturesResponse) => T) {
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
  return {
    companyName: data?.companyName ?? null,
    primary: data?.primary ?? null,
    secondary: data?.secondary ?? null,
    hasLogo: data?.hasLogo ?? false,
    providerName: data?.providerName ?? null,
    supportURL: data?.supportURL ?? null,
    isLoading: data === undefined,
  };
}

/**
 * The name the tab carries after the page's own: the company's, else the
 * product's, else nothing while the deployment has not answered.
 */
export function useDocumentTitle(page: string, productName: string) {
  const { companyName, isLoading } = useBranding();
  useEffect(() => {
    document.title = isLoading
      ? page
      : `${page} - ${companyName ?? productName}`;
  }, [companyName, isLoading, page, productName]);
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

export function useStorageSpacesEnabled() {
  const { data, isPending, isError } = useFeatures(selectStorageSpacesEnabled);
  return { enabled: !isError && data === true, isLoading: isPending };
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

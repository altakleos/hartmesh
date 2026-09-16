import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

export interface UiStarter {
  id: string;
  title: string;
  prompt: string;
}

export interface WorkspacePresentation {
  /** `business` keeps the developer screens for administrators. */
  profile: "business" | "developer";
  starters: UiStarter[];
}

export interface FeaturesResponse {
  agents_api: { enabled: boolean };
  browser_control?: { enabled: boolean };
  mcp_tasks?: { enabled: boolean };
  subagent_batches?: {
    enabled?: boolean;
    repository_available?: boolean;
    worker_running?: boolean;
    max_running?: number;
  };
  ui?: {
    profile?: string;
    starters?: { id?: unknown; title?: unknown; prompt?: unknown }[];
  };
}

export interface SubagentBatchesCapability {
  repositoryAvailable: boolean;
  workerRunning: boolean;
  maxRunning: number;
}

export async function fetchFeatures(): Promise<FeaturesResponse> {
  const res = await fetch(`${getBackendBaseURL()}/api/features`);
  if (!res.ok) {
    throw new Error(`Failed to load features: ${res.statusText}`);
  }
  return (await res.json()) as FeaturesResponse;
}

export async function fetchAgentsApiEnabled(): Promise<boolean> {
  return (await fetchFeatures()).agents_api.enabled;
}

export async function fetchBrowserControlEnabled(): Promise<boolean> {
  return (await fetchFeatures()).browser_control?.enabled ?? false;
}

export async function fetchMcpTasksEnabled(): Promise<boolean> {
  return (await fetchFeatures()).mcp_tasks?.enabled ?? false;
}

export async function fetchSubagentBatchesCapability(): Promise<SubagentBatchesCapability> {
  const feature = (await fetchFeatures()).subagent_batches;
  const legacyEnabled = feature?.enabled ?? false;
  return {
    repositoryAvailable: feature?.repository_available ?? legacyEnabled,
    workerRunning: feature?.worker_running ?? legacyEnabled,
    maxRunning: feature?.max_running ?? 0,
  };
}

/** How many starters Home will draw, mirroring the backend's own cap. */
const MAX_STARTERS = 6;

/**
 * What this deployment says the workspace should show.
 *
 * A Gateway that does not report `ui` is one from before this existed, so the
 * fallback is the profile that offers every screen — an upgrade must not take
 * screens away from the people who had them.
 */
export async function fetchWorkspacePresentation(): Promise<WorkspacePresentation> {
  const ui = (await fetchFeatures()).ui;
  const starters = Array.isArray(ui?.starters) ? ui.starters : [];
  return {
    profile: ui?.profile === "business" ? "business" : "developer",
    starters: starters
      .filter(
        (starter): starter is UiStarter =>
          typeof starter?.id === "string" &&
          typeof starter.title === "string" &&
          typeof starter.prompt === "string" &&
          starter.title.trim().length > 0 &&
          starter.prompt.trim().length > 0,
      )
      // The config enforces this too; holding it here as well keeps the grid
      // a grid if the block ever arrives from somewhere that has not.
      .slice(0, MAX_STARTERS),
  };
}

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
  branding?: {
    company_name?: unknown;
    colors?: { primary?: unknown; secondary?: unknown };
    has_logo?: unknown;
  };
}

/**
 * Whose workspace this is, as the deployment's tenant bundle says. Every
 * field is optional; a deployment that names no company is the product's own.
 */
export interface Branding {
  companyName: string | null;
  primary: string | null;
  secondary: string | null;
  /** Whether `logoURL()` serves a picture. */
  hasLogo: boolean;
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

const HEX_COLOR = /^#[0-9a-fA-F]{6}$/;

function color(value: unknown): string | null {
  return typeof value === "string" && HEX_COLOR.test(value) ? value : null;
}

/**
 * The company the workspace shows. A Gateway from before this existed reports
 * no `branding`, which is the same as a bundle that names nothing.
 */
export async function fetchBranding(): Promise<Branding> {
  const branding = (await fetchFeatures()).branding;
  const name =
    typeof branding?.company_name === "string"
      ? branding.company_name.trim()
      : "";
  return {
    companyName: name.length > 0 ? name : null,
    primary: color(branding?.colors?.primary),
    secondary: color(branding?.colors?.secondary),
    hasLogo: branding?.has_logo === true,
  };
}

/** Where the tenant's logo is served from; only meaningful when `hasLogo`. */
export function logoURL(): string {
  return `${getBackendBaseURL()}/api/branding/logo`;
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

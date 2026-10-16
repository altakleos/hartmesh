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
  storage_spaces?: {
    enabled?: boolean;
    native_attachments?: boolean;
    state?: unknown;
    reason?: unknown;
    own?: unknown;
    user_spaces?: unknown;
  };
  accounts?: { seats?: { allowed?: unknown; in_use?: unknown } };
  agent_instances?: {
    enabled?: unknown;
    execution?: unknown;
    reason?: unknown;
    ai_assistants?: {
      per_user?: unknown;
      allowed?: unknown;
      in_use?: unknown;
      mine?: unknown;
    };
    ai_employees?: { allowed?: unknown; in_use?: unknown };
  };
  people_search?: unknown;
  customer_administration?: {
    plugin_management?: boolean;
    local_skill_management?: boolean;
    local_mcp_management?: boolean;
    provider_operations?: boolean;
  };
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
    provider?: { display_name?: unknown; support_url?: unknown };
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
  providerName: string | null;
  supportURL: string | null;
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
  return selectAgentsApiEnabled(await fetchFeatures());
}

export function selectAgentsApiEnabled(features: FeaturesResponse): boolean {
  return features.agents_api.enabled;
}

export async function fetchBrowserControlEnabled(): Promise<boolean> {
  return selectBrowserControlEnabled(await fetchFeatures());
}

export function selectBrowserControlEnabled(
  features: FeaturesResponse,
): boolean {
  return features.browser_control?.enabled ?? false;
}

export function selectStorageSpacesEnabled(
  features: FeaturesResponse,
): boolean {
  return features.storage_spaces?.enabled === true;
}

export const STORAGE_REASONS = [
  "disk-too-small",
  "storage-pool-missing",
  "inventory-unreadable",
  "host-unqualified",
  "admission-failed",
] as const;
export type StorageReason = (typeof STORAGE_REASONS)[number];
export type StorageState = "off" | "ready" | "incomplete" | "unavailable";
export type OwnFilesState = "ready" | "not-assigned" | "no-seat" | "unverified";
export interface StorageFeature {
  known: boolean;
  enabled: boolean;
  state: StorageState;
  reason: StorageReason | null;
  own: OwnFilesState;
  userSpaces: boolean;
}
export interface SeatsFeature {
  allowed: number;
  inUse: number;
}

export const EXECUTION_REASONS = [
  "storage-unavailable",
  "agents-none",
  "operator-hold",
  "host-replaced",
  "space-unverified",
  "space-not-assigned",
] as const;
export type ExecutionReason = (typeof EXECUTION_REASONS)[number];
export interface AgentInstancesFeature {
  known: boolean;
  enabled: boolean;
  execution: "ready" | "unavailable";
  reason: ExecutionReason | null;
  assistants: { perUser: number; allowed: number; inUse: number; mine: number };
  employees: { allowed: number; inUse: number };
}

function wholeCount(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : 0;
}

export function selectAgentInstances(
  features: FeaturesResponse | undefined,
): AgentInstancesFeature {
  const agents = features?.agent_instances;
  if (
    !agents ||
    typeof agents.enabled !== "boolean" ||
    (agents.execution !== "ready" && agents.execution !== "unavailable")
  ) {
    return {
      known: false,
      enabled: false,
      execution: "unavailable",
      reason: null,
      assistants: { perUser: 0, allowed: 0, inUse: 0, mine: 0 },
      employees: { allowed: 0, inUse: 0 },
    };
  }
  return {
    known: true,
    enabled: agents.enabled,
    execution: agents.execution,
    reason: EXECUTION_REASONS.includes(agents.reason as ExecutionReason)
      ? (agents.reason as ExecutionReason)
      : null,
    assistants: {
      perUser: wholeCount(agents.ai_assistants?.per_user),
      allowed: wholeCount(agents.ai_assistants?.allowed),
      inUse: wholeCount(agents.ai_assistants?.in_use),
      mine: wholeCount(agents.ai_assistants?.mine),
    },
    employees: {
      allowed: wholeCount(agents.ai_employees?.allowed),
      inUse: wholeCount(agents.ai_employees?.in_use),
    },
  };
}

export function selectPeopleSearch(
  features: FeaturesResponse | undefined,
): "prefix" | "exact" {
  return features?.people_search === "prefix" ? "prefix" : "exact";
}

/** Failed, absent or malformed discovery never grants file availability. */
export function selectStorageState(
  features: FeaturesResponse | undefined,
): StorageFeature {
  const storage = features?.storage_spaces;
  const states: readonly unknown[] = [
    "off",
    "ready",
    "incomplete",
    "unavailable",
  ];
  if (
    !storage ||
    typeof storage.enabled !== "boolean" ||
    !states.includes(storage.state)
  ) {
    return {
      known: false,
      enabled: false,
      state: "unavailable",
      reason: null,
      own: "ready",
      userSpaces: false,
    };
  }
  const own: readonly unknown[] = [
    "ready",
    "not-assigned",
    "no-seat",
    "unverified",
  ];
  return {
    known: true,
    enabled: storage.enabled,
    state: storage.state as StorageState,
    reason: STORAGE_REASONS.includes(storage.reason as StorageReason)
      ? (storage.reason as StorageReason)
      : null,
    own: own.includes(storage.own) ? (storage.own as OwnFilesState) : "ready",
    userSpaces: storage.user_spaces === true,
  };
}

export { selectStorageState as selectStorage };

export function selectSeats(
  features: FeaturesResponse | undefined,
): SeatsFeature | null {
  const storage = selectStorageState(features);
  // The legacy off envelope reports zero counts. Those are not managed seats.
  if (!storage.known || !storage.enabled || storage.state === "off")
    return null;
  const seats = features?.accounts?.seats;
  const whole = (value: unknown): value is number =>
    typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
  return whole(seats?.allowed) && whole(seats?.in_use)
    ? { allowed: seats.allowed, inUse: seats.in_use }
    : null;
}

export async function fetchMcpTasksEnabled(): Promise<boolean> {
  return selectMcpTasksEnabled(await fetchFeatures());
}

export function selectMcpTasksEnabled(features: FeaturesResponse): boolean {
  return features.mcp_tasks?.enabled ?? false;
}

export async function fetchSubagentBatchesCapability(): Promise<SubagentBatchesCapability> {
  return selectSubagentBatchesCapability(await fetchFeatures());
}

export function selectSubagentBatchesCapability(
  features: FeaturesResponse,
): SubagentBatchesCapability {
  const feature = features.subagent_batches;
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

export function validateProviderSupportURL(value: unknown): string | null {
  if (
    typeof value !== "string" ||
    value.length > 2048 ||
    /[\s\\\p{Cc}\p{Cf}]/u.test(value)
  ) {
    return null;
  }
  try {
    const authority = /^https:\/\/([^/?#]+)/i.exec(value)?.[1];
    if (!authority || /[@%]/.test(authority)) return null;
    const url = new URL(value);
    return url.protocol === "https:" &&
      url.hostname &&
      url.port !== "0" &&
      !url.username &&
      !url.password
      ? url.href
      : null;
  } catch {
    return null;
  }
}

/**
 * The company the workspace shows. A Gateway from before this existed reports
 * no `branding`, which is the same as a bundle that names nothing.
 */
export async function fetchBranding(): Promise<Branding> {
  return selectBranding(await fetchFeatures());
}

export function selectBranding(features: FeaturesResponse): Branding {
  const branding = features.branding;
  const name =
    typeof branding?.company_name === "string"
      ? branding.company_name.trim()
      : "";
  const provider = branding?.provider;
  const providerName =
    typeof provider?.display_name === "string"
      ? provider.display_name.trim()
      : "";
  return {
    companyName: name.length > 0 ? name : null,
    primary: color(branding?.colors?.primary),
    secondary: color(branding?.colors?.secondary),
    hasLogo: branding?.has_logo === true,
    providerName:
      providerName &&
      providerName.length <= 80 &&
      !/[\p{Cc}\p{Cf}]/u.test(providerName)
        ? providerName
        : null,
    supportURL: validateProviderSupportURL(provider?.support_url),
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
  return selectWorkspacePresentation(await fetchFeatures());
}

export function selectWorkspacePresentation(
  features: FeaturesResponse,
): WorkspacePresentation {
  const ui = features.ui;
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

export function selectCustomerAdministration(features: FeaturesResponse) {
  const effective = features.customer_administration;
  return {
    pluginManagement: effective?.plugin_management === true,
    localSkillManagement: effective?.local_skill_management === true,
    localMcpManagement: effective?.local_mcp_management === true,
    providerOperations: effective?.provider_operations === true,
  };
}

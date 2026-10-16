import type { CreateAgentRequest, WorkPolicy } from "@/core/agents/types";
import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
import type { ExecutionReason } from "@/core/features/api";
import type { UserMemory } from "@/core/memory/types";

export type Custody = "personal" | "company";

export interface AgentInstance {
  id: string;
  name: string;
  principal: { kind: "nonhuman"; subject_id: string };
  custody: Custody;
  owner_id: string | null;
  creator_id: string;
  supervisor: { kind: "human"; subject_id: string };
  definition_revision: string;
  home_id: string | null;
  status: "provisioning" | "active" | "suspended" | "archived" | "deleted";
  generation: number;
  permissions: number;
}

export interface AgentInstanceSummary {
  id: string;
  name: string;
  supervisor_name: string | null;
  status: AgentInstance["status"];
}

export interface AgentInstanceDetail extends AgentInstance {
  activity: {
    running_runs: number;
    running: {
      run_id: string;
      thread_id: string;
      requester_id: string;
      work_id: string | null;
      objective: string | null;
    }[];
    last_active_at: string | null;
    open_conversations: number;
  };
  home: {
    state: "available" | "busy" | "unverified";
    workspace: "running" | "starting" | "absent";
  } | null;
  execution: {
    execution: "ready" | "unavailable";
    reason: ExecutionReason | null;
  };
}

export interface InstanceRole {
  id: string;
  name: string;
  description: string;
  soul: string;
  skills: string[];
  work_policy: WorkPolicy | null;
}

export interface InstanceConversation {
  thread_id: string;
  requester_id: string;
  title: string | null;
  updated_at: string;
  running: boolean;
}

export type AgentListEntry = AgentInstance | AgentInstanceSummary;

export function isFullInstance(entry: AgentListEntry): entry is AgentInstance {
  return "permissions" in entry;
}

export interface UnfinishedCreation {
  id: string;
  name: string;
  custody: Custody;
  created_at: string;
}

export interface InstanceList {
  instances: AgentListEntry[];
  ai_assistants: {
    per_user: number;
    allowed: number;
    in_use: number;
    mine: number;
  };
  unfinished: UnfinishedCreation[];
}

export interface CreateInstance {
  creation_id: string;
  name: string;
  definition_name: string;
  custody: Custody;
  supervisor_id?: string;
}

export interface LifecycleChange {
  operation_id: string;
  generation: number;
  action:
    | "suspend"
    | "archive"
    | "delete"
    | "restore"
    | "adopt"
    | "supervise"
    | "grant";
  definition_name?: string;
  supervisor_id?: string;
  member_id?: string;
  permissions?: number;
  only_if_idle?: boolean;
}

export interface LifecycleOperation {
  operation_id: string;
  generation: number;
  action: LifecycleChange["action"];
  complete: boolean;
  abandoned?: boolean;
  retry?: LifecycleChange;
}

export class InstanceApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public reason?: string,
    public executionReason?: string,
    public extra: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "InstanceApiError";
  }

  static fromResponse(status: number, payload: unknown): InstanceApiError {
    const body =
      typeof payload === "object" && payload !== null && !Array.isArray(payload)
        ? (payload as Record<string, unknown>)
        : {};
    const { reason, execution_reason: executionReason, ...extra } = body;
    return new InstanceApiError(
      status,
      typeof body.detail === "string"
        ? body.detail
        : `Agent operation failed (${status})`,
      typeof reason === "string" ? reason : undefined,
      typeof executionReason === "string" ? executionReason : undefined,
      extra,
    );
  }
}

function base(id?: string) {
  if (id && !/^[0-9a-f]{32}$/.test(id))
    throw new Error("Invalid instance identity");
  return `${getBackendBaseURL()}/api/agent-instances${id ? `/${id}` : ""}`;
}

export function instanceBaseURL(id: string) {
  return base(id);
}

export async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const signal = AbortSignal.any([
    AbortSignal.timeout(20_000),
    ...(init?.signal ? [init.signal] : []),
  ]);
  const response = await awaitAbortable(
    fetch(url, { cache: "no-store", ...init, signal }),
    signal,
    (late) => {
      void late.body?.cancel().catch(() => undefined);
    },
  );
  if (response.status === 204) return undefined as T;
  const payload: unknown = await awaitAbortable(response.json(), signal);
  if (!response.ok)
    throw InstanceApiError.fromResponse(response.status, payload);
  return payload as T;
}

export function write<T>(
  url: string,
  method: string,
  body: unknown,
  signal?: AbortSignal,
) {
  return request<T>(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
}

export function operationID() {
  return crypto.randomUUID().replaceAll("-", "");
}

export function listInstances(
  custody?: Custody,
  offset = 0,
  signal?: AbortSignal,
) {
  const query = new URLSearchParams({ limit: "100", offset: String(offset) });
  if (custody) query.set("custody", custody);
  return request<InstanceList>(`${base()}?${query}`, { signal });
}

export function removeUnfinished(id: string, signal?: AbortSignal) {
  return request<void>(base(id), { method: "DELETE", signal });
}

export function getInstance(id: string, signal?: AbortSignal) {
  return request<AgentInstanceDetail>(`${base(id)}?include_deleted=true`, {
    signal,
  });
}

export function listInstanceRoles(signal?: AbortSignal) {
  return request<{ roles: InstanceRole[] }>(`${base()}/roles`, { signal });
}

export function listInstanceConversations(
  id: string,
  offset = 0,
  signal?: AbortSignal,
) {
  return request<{ conversations: InstanceConversation[] }>(
    `${base(id)}/conversations?limit=50&offset=${offset}`,
    { signal },
  );
}

export function createInstance(body: CreateInstance, signal?: AbortSignal) {
  return write<AgentInstance>(base(), "POST", body, signal);
}

export interface CompanyCopyRequest {
  creation_id: string;
  generation: number;
  name: string;
  supervisor_id?: string;
}

export function createCompanyCopy(
  id: string,
  body: CompanyCopyRequest,
  signal?: AbortSignal,
) {
  return write<AgentInstance>(`${base(id)}/company-copy`, "POST", body, signal);
}

export function renameInstance(
  id: string,
  generation: number,
  name: string,
  signal?: AbortSignal,
) {
  return write<AgentInstance>(base(id), "PATCH", { generation, name }, signal);
}

export function getInstanceDefinition(id: string, signal?: AbortSignal) {
  return request<{
    revision: string;
    config: CreateAgentRequest & { memory_enabled?: boolean };
    soul: string;
  }>(`${base(id)}/definition`, { signal });
}

export function getInstanceGrants(id: string, signal?: AbortSignal) {
  return request<{ grants: { user_id: string; permissions: number }[] }>(
    `${base(id)}/grants`,
    { signal },
  );
}

export function getInstanceLifecycle(id: string, signal?: AbortSignal) {
  return request<{ operations: LifecycleOperation[] }>(
    `${base(id)}/lifecycle`,
    { signal },
  );
}

export function changeInstanceLifecycle(
  id: string,
  body: LifecycleChange,
  signal?: AbortSignal,
) {
  return write<{
    instance: AgentInstance;
    complete: boolean;
    operation_id: string;
    space_released?: boolean;
  }>(`${base(id)}/lifecycle`, "POST", body, signal);
}

export function createInstanceConversation(
  id: string,
  creationId: string,
  signal?: AbortSignal,
) {
  return write<{ thread_id: string }>(
    `${base(id)}/conversations`,
    "POST",
    { creation_id: creationId },
    signal,
  );
}

export function getConversationInstance(
  threadId: string,
  signal?: AbortSignal,
) {
  return request<{ instance: AgentInstance | null }>(
    `${base()}/conversations/${encodeURIComponent(threadId)}/instance`,
    { signal },
  );
}

export function getInstanceMemory(id: string, signal?: AbortSignal) {
  return request<UserMemory>(`${base(id)}/memory`, { signal });
}

export function createInstanceFact(
  id: string,
  content: string,
  signal?: AbortSignal,
) {
  return write<unknown>(
    `${base(id)}/memory/facts`,
    "POST",
    { content },
    signal,
  );
}

export function deleteInstanceFact(
  id: string,
  factId: string,
  signal?: AbortSignal,
) {
  return request<unknown>(
    `${base(id)}/memory/facts/${encodeURIComponent(factId)}`,
    { method: "DELETE", signal },
  );
}

export function clearInstanceMemory(id: string, signal?: AbortSignal) {
  return request<unknown>(`${base(id)}/memory`, { method: "DELETE", signal });
}

export function importInstanceMemory(
  id: string,
  document: unknown,
  signal?: AbortSignal,
) {
  return write<unknown>(
    `${base(id)}/memory/import`,
    "POST",
    { document },
    signal,
  );
}

export function abandonInstanceLifecycle(
  id: string,
  body: { generation: number; operation_id: string },
  signal?: AbortSignal,
) {
  return write<{ instance: AgentInstance; complete: true; abandoned: true }>(
    `${base(id)}/lifecycle/abandon`,
    "POST",
    body,
    signal,
  );
}

export type WorkPriority = "low" | "normal" | "high" | "urgent";
export type WorkSource = { kind: "space_file"; space_id: string; path: string };
export type VisibleWorkSource = WorkSource | { kind: "unavailable" };
export interface WorkAssignment {
  objective: string;
  success_criteria: string;
  responsibility?: string | null;
  priority?: WorkPriority;
  due_at?: string | null;
  review_required?: boolean;
  sources?: WorkSource[];
}
export interface WorkRecord extends Omit<WorkAssignment, "sources"> {
  human_input_request_id?: string | null;
  id: string;
  instance_id: string;
  creator_id: string;
  definition_revision: string;
  assignment_revision: number;
  revision: number;
  status: "open" | "blocked" | "submitted" | "completed" | "cancelled";
  progress: string;
  next_action: string;
  sources: VisibleWorkSource[];
  review_state: "none" | "pending" | "accepted" | "reported";
  outcome: {
    id: string;
    statement: string;
    evidence_revision: number;
    sources: VisibleWorkSource[];
  } | null;
  review: {
    actor_id: string;
    basis: "outcome_statement";
    accepted_at: string;
  } | null;
  blocker: {
    id: string;
    revision: number;
    kind: "information" | "decision";
    question: string;
    resolution?: { actor_id: string; statement: string };
  } | null;
  suggestion?: { id: string; statement: string } | null;
  attempt: {
    id: string;
    status:
      | "starting"
      | "running"
      | "stopping"
      | "uncertain"
      | "succeeded"
      | "failed"
      | "cancelled";
    thread_id?: string;
    run_id?: string | null;
    activation_id?: string;
    activation_request?: WorkActivation;
    requester_id?: string;
    candidate?: WorkRecord["outcome"];
    settled_at?: string | null;
    instance_generation?: number;
    workspace?: "starting" | "ready" | null;
    contained?: { reason: string; at: string } | null;
  } | null;
  execution_available: boolean;
  execution_reason?: ExecutionReason | null;
  availability: "explicit_activation" | "records_only" | "unavailable";
  work_enabled: boolean;
  needs_mandate_reconciliation: boolean;
  current_contents: "not_checked";
  created_at: string;
  updated_at: string;
}
export interface WorkCommand extends Partial<WorkAssignment> {
  operation_id: string;
  expected_revision: number;
  expected_assignment_revision: number;
  action:
    | "edit"
    | "cancel"
    | "reopen"
    | "changes_requested"
    | "reconcile_mandate"
    | "reconcile_attempt"
    | "accept"
    | "input"
    | "decide";
  note?: string;
  outcome_id?: string;
  evidence_revision?: number;
  basis?: "outcome_statement";
  acknowledge_unchecked_sources?: boolean;
  blocker_id?: string;
  blocker_revision?: number;
  attempt_id?: string;
  containment_operation_id?: string;
  request_basis?: {
    id: string;
    revision: number;
    request_revision: number;
    response_ids: string[];
  };
}
export interface WorkEvent {
  id: string;
  actor_id: string;
  actor_kind: "human" | "nonhuman" | "host";
  action: string;
  revision: number;
  assignment_revision: number;
  created_at: string;
  note: string | null;
  report?: {
    statement?: string;
    sources?: VisibleWorkSource[];
    attempt_id?: string;
    run_id?: string;
    status?: string;
    reason?: string;
    attachment_id?: string;
  } | null;
  record: WorkRecord;
}
export interface WorkCapacity {
  max: number;
  in_use: number;
  running: {
    instance_id: string;
    instance_name: string;
    work_id: string | null;
    objective: string;
    started_at: string | null;
    requester_id: string | null;
  }[];
}
function workBase(instance: string, work?: string) {
  if (work && !/^[0-9a-f]{32}$/.test(work))
    throw new Error("Invalid Work identity");
  return `${base(instance)}/work${work ? `/${work}` : ""}`;
}
export function listWork(instance: string, offset = 0, signal?: AbortSignal) {
  return request<{
    work: WorkRecord[];
    execution_available?: boolean;
    execution_reason?: ExecutionReason | null;
    availability?: "explicit_activation" | "unavailable";
    work_capacity?: WorkCapacity;
  }>(`${workBase(instance)}?limit=50&offset=${offset}`, { signal });
}
export function getWork(instance: string, work: string, signal?: AbortSignal) {
  return request<WorkRecord>(workBase(instance, work), { signal });
}
export function delegateWork(
  instance: string,
  body: WorkAssignment & { operation_id: string },
  signal?: AbortSignal,
) {
  return write<WorkRecord>(workBase(instance), "POST", body, signal);
}
export function commandWork(
  instance: string,
  work: string,
  body: WorkCommand | WorkStopCommand,
  signal?: AbortSignal,
) {
  return write<WorkRecord>(
    `${workBase(instance, work)}/commands`,
    "POST",
    body,
    signal,
  );
}

export interface WorkStopCommand {
  operation_id: string;
  expected_revision: number;
  expected_assignment_revision: number;
  action: "stop";
  attempt_id: string;
  note?: string;
}
export interface WorkActivation {
  operation_id: string;
  expected_revision: number;
  expected_assignment_revision: number;
  thread_id: string;
}
export function activateWork(
  instance: string,
  work: string,
  body: WorkActivation,
  signal?: AbortSignal,
) {
  return write<WorkRecord>(
    `${workBase(instance, work)}/activate`,
    "POST",
    body,
    signal,
  );
}
export function workHistory(
  instance: string,
  work: string,
  offset = 0,
  signal?: AbortSignal,
) {
  return request<{ events: WorkEvent[] }>(
    `${workBase(instance, work)}/history?limit=50&offset=${offset}`,
    { signal },
  );
}

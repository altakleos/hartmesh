import type { WorkPolicy } from "@/core/agents/types";
import { awaitAbortable } from "@/core/api/abort";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
import type { UserMemory } from "@/core/memory/types";

export interface AgentInstance {
  id: string;
  name: string;
  principal: { kind: "nonhuman"; subject_id: string };
  custody: "personal" | "company";
  owner_id: string | null;
  creator_id: string;
  supervisor: { kind: "human"; subject_id: string };
  definition_revision: string;
  home_id: string | null;
  status: "provisioning" | "active" | "suspended" | "archived" | "deleted";
  generation: number;
  permissions: number;
}

export interface CreateInstance {
  creation_id: string;
  name: string;
  definition_name: string;
  custody: "personal" | "company";
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
  ) {
    super(message);
  }
}

function base(id?: string) {
  if (id && !/^[0-9a-f]{32}$/.test(id))
    throw new Error("Invalid instance identity");
  return `${getBackendBaseURL()}/api/agent-instances${id ? `/${id}` : ""}`;
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
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
  const payload: unknown = await awaitAbortable(response.json(), signal);
  if (!response.ok) {
    const detail =
      typeof payload === "object" && payload !== null && "detail" in payload
        ? payload.detail
        : undefined;
    throw new InstanceApiError(
      response.status,
      typeof detail === "string"
        ? detail
        : `Agent operation failed (${response.status})`,
    );
  }
  return payload as T;
}

function write<T>(
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

export function listInstances(offset = 0, signal?: AbortSignal) {
  return request<{ instances: AgentInstance[] }>(
    `${base()}?include_deleted=true&limit=100&offset=${offset}`,
    { signal },
  );
}

export function getInstance(id: string, signal?: AbortSignal) {
  return request<AgentInstance>(`${base(id)}?include_deleted=true`, { signal });
}

export function createInstance(body: CreateInstance, signal?: AbortSignal) {
  return write<AgentInstance>(base(), "POST", body, signal);
}

export function createCompanyCopy(
  id: string,
  body: {
    creation_id: string;
    generation: number;
    name: string;
    supervisor_id?: string;
  },
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
    config: {
      name: string;
      description?: string;
      model?: string;
      memory_enabled?: boolean;
      work_policy?: WorkPolicy | null;
    };
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
  attempt: { id: string; status: string } | null;
  execution_available: false;
  availability: "records_only";
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
}
export interface WorkEvent {
  id: string;
  actor_id: string;
  actor_kind: "human" | "nonhuman";
  action: string;
  revision: number;
  assignment_revision: number;
  created_at: string;
  note: string | null;
  record: WorkRecord;
}
function workBase(instance: string, work?: string) {
  if (work && !/^[0-9a-f]{32}$/.test(work))
    throw new Error("Invalid Work identity");
  return `${base(instance)}/work${work ? `/${work}` : ""}`;
}
export function listWork(instance: string, offset = 0, signal?: AbortSignal) {
  return request<{ work: WorkRecord[] }>(
    `${workBase(instance)}?limit=50&offset=${offset}`,
    { signal },
  );
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
  body: WorkCommand,
  signal?: AbortSignal,
) {
  return write<WorkRecord>(
    `${workBase(instance, work)}/commands`,
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

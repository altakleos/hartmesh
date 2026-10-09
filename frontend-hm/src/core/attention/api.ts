import { request, write, type WorkSource } from "@/core/agent-instances/api";
import { getBackendBaseURL } from "@/core/config";

export interface InputRequest {
  id: string;
  instance_id: string;
  work_id: string;
  instance_name: string;
  work_objective: string;
  assignment_revision: number;
  basis_id: string;
  basis_revision: number;
  revision: number;
  request_revision: number;
  purpose: "information" | "decision" | "review";
  question: string;
  reason: string;
  expected_response: string;
  choices: string[];
  sources: (WorkSource | { kind: "unavailable" })[];
  state: "pending" | "answered" | "closed";
  closed_reason: string | null;
  recipient_id: string | null;
  creator_id: string;
  needs_routing: boolean;
  read_revision: number;
  can_recover_response: boolean;
  can_recover_management: boolean;
  can_respond: boolean;
  can_manage: boolean;
  updated_at: string;
  receipt?: {
    operation_id: string;
    response_id: string | null;
    actor_id: string;
  };
}
export interface InputResponse {
  id: string;
  actor_id: string;
  request_revision: number;
  text?: string;
  choice?: string;
  disposition: "supplied" | "cannot_provide" | "wrong_recipient";
  sources: (WorkSource | { kind: "unavailable" })[];
  created_at: string;
}
export interface CreateInput {
  operation_id: string;
  expected_work_revision: number;
  expected_assignment_revision: number;
  purpose: InputRequest["purpose"];
  question: string;
  reason: string;
  expected_response: string;
  choices?: string[];
  recipient_id?: string | null;
}
export interface Reply {
  operation_id: string;
  expected_request_revision: number;
  expected_assignment_revision: number;
  disposition: InputResponse["disposition"];
  text?: string;
  choice?: string;
  sources?: { kind: "space_file"; space_id: string; path: string }[];
}
export interface RequestCommand {
  operation_id: string;
  expected_revision: number;
  expected_request_revision: number;
  action: "route" | "withdraw";
  recipient_id?: string | null;
  note?: string;
  response_ids?: string[];
}
export type InboxView = "pending" | "answered" | "routing" | "all";
export interface Inbox {
  requests: InputRequest[];
  counts: { pending: number; routing: number; answered: number };
  has_more: boolean;
}
const base = () => `${getBackendBaseURL()}/api/human-input`;
export function listInput(
  view: InboxView = "pending",
  offset = 0,
  work?: string,
  signal?: AbortSignal,
) {
  const query = new URLSearchParams({
    view,
    offset: String(offset),
    limit: "50",
  });
  if (work) query.set("work_id", work);
  return request<Inbox>(`${base()}?${query}`, { signal });
}
export function getInput(id: string, signal?: AbortSignal) {
  return request<InputRequest>(`${base()}/${encodeURIComponent(id)}`, {
    signal,
  });
}
export function getResponses(id: string, offset = 0, signal?: AbortSignal) {
  return request<{ responses: InputResponse[]; has_more: boolean }>(
    `${base()}/${encodeURIComponent(id)}/responses?limit=100&offset=${offset}`,
    { signal },
  );
}
export function getHistory(id: string, offset = 0, signal?: AbortSignal) {
  return request<{
    events: {
      id: string;
      actor_id: string;
      action: string;
      revision: number;
      created_at: string;
    }[];
    has_more: boolean;
  }>(`${base()}/${encodeURIComponent(id)}/history?limit=50&offset=${offset}`, {
    signal,
  });
}
export function respond(id: string, body: Reply, signal?: AbortSignal) {
  return write<InputRequest>(
    `${base()}/${encodeURIComponent(id)}/responses`,
    "POST",
    body,
    signal,
  );
}
export function command(
  id: string,
  body: RequestCommand,
  signal?: AbortSignal,
) {
  return write<InputRequest>(
    `${base()}/${encodeURIComponent(id)}/commands`,
    "POST",
    body,
    signal,
  );
}
export function markRead(id: string, revision: number, signal?: AbortSignal) {
  return write(
    `${base()}/${encodeURIComponent(id)}/read`,
    "POST",
    { revision },
    signal,
  );
}
export function createInput(
  instance: string,
  work: string,
  body: CreateInput,
  signal?: AbortSignal,
) {
  return write<InputRequest>(
    `${getBackendBaseURL()}/api/agent-instances/${encodeURIComponent(instance)}/work/${encodeURIComponent(work)}/requests`,
    "POST",
    body,
    signal,
  );
}

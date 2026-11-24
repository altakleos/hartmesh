import type { WorkPolicy } from "@/core/agents/types";

import { instanceBaseURL, request, write } from "./api";

export interface ConfigurationProposal {
  id: string;
  instance_id: string;
  employee_name: string;
  field: "work_policy.enabled";
  proposed_value: boolean;
  base_value: boolean;
  current_value: boolean;
  base_generation: number;
  current_generation: number;
  base_revision: string;
  current_revision: string;
  candidate_digest: string;
  created_at: string;
  decided_at: string | null;
  decided_by: string | null;
  resolved_by: string | null;
  operation_id: string | null;
  status: "open" | "declined" | "applying" | "adopted" | "abandoned";
  stale: boolean;
  adopt_blocked_reason:
    | "stale"
    | "different-definition-owner"
    | "definition-unavailable"
    | "home-admin-required"
    | "employee-inactive"
    | "lifecycle-unavailable"
    | null;
  effective_work_policy: Required<WorkPolicy>;
}
export interface ProposalPage {
  proposals: ConfigurationProposal[];
  next_cursor: string | null;
}
type ReviewedChange = Pick<
  ConfigurationProposal,
  "base_generation" | "base_revision" | "candidate_digest"
>;

export function listProposals(
  instance: string,
  cursor: string | null,
  signal: AbortSignal,
) {
  const query = new URLSearchParams({ limit: "20" });
  if (cursor) query.set("cursor", cursor);
  return request<ProposalPage>(
    instanceBaseURL(instance) + "/proposals?" + query.toString(),
    { signal },
  );
}

function proposalURL(instance: string, id: string) {
  return instanceBaseURL(instance) + "/proposals/" + encodeURIComponent(id);
}

export function getProposal(instance: string, id: string, signal: AbortSignal) {
  return request<ConfigurationProposal>(proposalURL(instance, id), { signal });
}

export function decideProposal(
  instance: string,
  id: string,
  action: "adopt" | "decline",
  body: ReviewedChange | Record<string, never>,
  signal: AbortSignal,
) {
  return write<ConfigurationProposal>(
    proposalURL(instance, id) + "/" + action,
    "POST",
    body,
    signal,
  );
}

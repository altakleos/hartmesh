import type { InputRequest } from "@/core/attention/api";

export function inputRequest(
  overrides: Partial<InputRequest> = {},
): InputRequest {
  return {
    id: "a".repeat(32),
    instance_id: "b".repeat(32),
    work_id: "c".repeat(32),
    instance_name: "Researcher",
    work_objective: "Review the guide",
    assignment_revision: 1,
    basis_id: "d".repeat(32),
    basis_revision: 1,
    revision: 1,
    request_revision: 1,
    purpose: "information",
    question: "Which guide?",
    reason: "A source is needed",
    expected_response: "A filename",
    choices: [],
    sources: [],
    state: "pending",
    closed_reason: null,
    recipient_id: "reader",
    creator_id: "reader",
    needs_routing: false,
    read_revision: 0,
    can_recover_response: true,
    can_recover_management: true,
    can_respond: true,
    can_manage: true,
    updated_at: "2026-10-16T08:00:00Z",
    ...overrides,
  };
}

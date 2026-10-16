import type {
  AgentInstanceDetail,
  WorkRecord,
} from "@/core/agent-instances/api";

export function workInstance(): AgentInstanceDetail {
  return {
    id: "a".repeat(32),
    name: "Researcher",
    principal: { kind: "nonhuman", subject_id: "ai" },
    custody: "company",
    owner_id: null,
    creator_id: "reader",
    supervisor: { kind: "human", subject_id: "supervisor" },
    definition_revision: "revision",
    generation: 1,
    home_id: "b".repeat(32),
    status: "active",
    permissions: 7,
    activity: {
      running_runs: 0,
      running: [],
      last_active_at: null,
      open_conversations: 1,
    },
    home: { state: "available", workspace: "absent" },
    execution: { execution: "ready", reason: null },
  };
}
export function workRecord(): WorkRecord {
  return {
    id: "c".repeat(32),
    instance_id: "a".repeat(32),
    creator_id: "reader",
    definition_revision: "revision",
    objective: "Review the guide",
    success_criteria: "List gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 2,
    revision: 5,
    status: "open",
    progress: "",
    next_action: "",
    sources: [],
    review_state: "none",
    outcome: null,
    review: null,
    blocker: null,
    attempt: {
      id: "d".repeat(32),
      status: "running",
      thread_id: "work-chat",
      run_id: "run",
      requester_id: "reader",
    },
    execution_available: true,
    availability: "explicit_activation",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: "2026-10-16T07:00:00Z",
    updated_at: "2026-10-16T07:00:00Z",
  };
}

export interface WorkPolicy {
  enabled: boolean;
  default_priority?: "low" | "normal" | "high" | "urgent";
  review_required?: boolean;
  allow_derived?: boolean;
  max_derived_per_activation?: number;
  responsibilities?: { key: string; label: string }[];
}

export interface AgentModelSettings {
  temperature?: number | null;
  max_tokens?: number | null;
}

export type ReasoningEffort = "low" | "medium" | "high";

export interface Agent {
  name: string;
  description: string;
  work_policy?: WorkPolicy | null;
  model: string | null;
  tool_groups: string[] | null;
  skills: string[] | null;
  allowed_subagents?: string[] | null;
  model_settings?: AgentModelSettings | null;
  thinking_enabled?: boolean | null;
  reasoning_effort?: ReasoningEffort | null;
  soul?: string | null;
}

export interface CreateAgentRequest {
  name: string;
  description?: string;
  work_policy?: WorkPolicy | null;
  model?: string | null;
  tool_groups?: string[] | null;
  skills?: string[] | null;
  allowed_subagents?: string[] | null;
  model_settings?: AgentModelSettings | null;
  thinking_enabled?: boolean | null;
  reasoning_effort?: ReasoningEffort | null;
  soul?: string;
}

export interface UpdateAgentRequest {
  description?: string | null;
  work_policy?: WorkPolicy | null;
  model?: string | null;
  tool_groups?: string[] | null;
  skills?: string[] | null;
  allowed_subagents?: string[] | null;
  model_settings?: AgentModelSettings | null;
  thinking_enabled?: boolean | null;
  reasoning_effort?: ReasoningEffort | null;
  soul?: string | null;
}

import {
  AgentDefinitionError,
  createAgent,
  getAgent,
  updateAgent,
} from "@/core/agents/api";
import type { CreateAgentRequest } from "@/core/agents/types";
import { awaitAbortable } from "@/core/api/abort";

/** Write only public definition fields, never runtime/projection metadata. */
export async function prepareBriefDefinition(
  config: CreateAgentRequest,
  soul: string,
  enabled: boolean,
  signal: AbortSignal,
) {
  const {
    name,
    description,
    model,
    tool_groups,
    skills,
    allowed_subagents,
    model_settings,
    thinking_enabled,
    reasoning_effort,
  } = config;
  const fields = {
    description,
    model,
    tool_groups,
    skills,
    allowed_subagents,
    model_settings,
    thinking_enabled,
    reasoning_effort,
    soul,
    work_policy: {
      ...(config.work_policy ?? {
        enabled: true,
        default_priority: "normal",
        review_required: true,
        allow_derived: false,
        max_derived_per_activation: 3,
        responsibilities: [],
      }),
      enabled,
    },
  };
  let exists = true;
  try {
    await awaitAbortable(getAgent(name, signal), signal);
  } catch (error) {
    signal.throwIfAborted();
    if (error instanceof AgentDefinitionError && error.status === 404)
      exists = false;
    else throw error;
  }
  signal.throwIfAborted();
  if (exists) await awaitAbortable(updateAgent(name, fields, signal), signal);
  else await awaitAbortable(createAgent({ name, ...fields }, signal), signal);
  signal.throwIfAborted();
  return name;
}

import type { AgentThreadContext } from "./types";

/**
 * The chat mode is one dial, and this module is the only place it is read.
 *
 * Three questions are answered here and nowhere else: which modes a model can
 * offer, which mode a person's choice resolves to on that model, and what a
 * mode turns on in the agent. Both pickers and both places that start a run
 * derive from these, so a mode cannot mean one thing in the menu, another on
 * the first message and a third on a follow-up.
 *
 * Plan mode is Ultra's alone. It binds the `write_todos` tool, and a model
 * given that tool spends turns on it whether the task has steps or not — a
 * one-command report skill measured on a tenant cost five model calls, two
 * of them todo bookkeeping, with the todo prompt already saying not to. Pro
 * is the resolved default for every thinking-capable model, so that tax fell
 * on every ordinary turn. Ultra is where a visible plan has something to
 * track: it is also the mode that divides work between subagents.
 *
 * Reasoning is offered only where it differs from Pro. The two are the same
 * request except for `reasoning_effort` (low against medium), and a model
 * that does not honour that setting receives byte-identical requests from
 * both — the tenant's model is one. Offering both rows there tells the person
 * Pro costs more time for more accuracy and delivers neither, so the row is
 * derived from the model's own capability and a stored "thinking" choice
 * resolves to Pro on such a model.
 */
export type ChatMode = "flash" | "thinking" | "pro" | "ultra";

export type ReasoningEffort = NonNullable<
  AgentThreadContext["reasoning_effort"]
>;

/** The two capability facts a model publishes that shape the dial. */
export type ModelModeSupport = {
  supports_thinking?: boolean | null;
  supports_reasoning_effort?: boolean | null;
};

export type RunModeFlags = Pick<
  AgentThreadContext,
  "thinking_enabled" | "is_plan_mode" | "subagent_enabled" | "reasoning_effort"
>;

/** Whether the Reasoning row is worth offering on this model. */
export function offersReasoningMode(
  model: ModelModeSupport | undefined,
): boolean {
  return Boolean(model?.supports_thinking && model?.supports_reasoning_effort);
}

/**
 * The mode a person's choice comes to on this model.
 *
 * No thinking means Flash, whatever was stored. A stored Reasoning on a model
 * that ignores effort is Pro, because that is the request it would make. No
 * choice at all is Pro where the model thinks and Flash where it does not.
 */
export function resolveChatMode(
  mode: ChatMode | undefined,
  model: ModelModeSupport | undefined,
): ChatMode {
  if (!model?.supports_thinking) {
    return "flash";
  }
  if (mode === "thinking" && !offersReasoningMode(model)) {
    return "pro";
  }
  return mode ?? "pro";
}

const EFFORT_FOR_MODE: Record<ChatMode, ReasoningEffort> = {
  flash: "minimal",
  thinking: "low",
  pro: "medium",
  ultra: "high",
};

/** The effort a mode stands for; what the picker writes when one is chosen. */
export function reasoningEffortForMode(mode: ChatMode): ReasoningEffort {
  return EFFORT_FOR_MODE[mode];
}

/**
 * What a run sends for a mode. An explicit effort in the context is the
 * person's own choice and wins; without one, the mode's level applies, and
 * no mode at all sends no level rather than a value invented here.
 */
export function runFlagsForMode(
  mode: ChatMode | undefined,
  reasoningEffort?: AgentThreadContext["reasoning_effort"],
): RunModeFlags {
  return {
    thinking_enabled: mode !== "flash",
    is_plan_mode: mode === "ultra",
    subagent_enabled: mode === "ultra",
    reasoning_effort:
      reasoningEffort ??
      (mode === undefined ? undefined : EFFORT_FOR_MODE[mode]),
  };
}

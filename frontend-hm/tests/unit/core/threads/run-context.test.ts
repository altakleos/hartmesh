import { describe, expect, it } from "@rstest/core";

import {
  offersReasoningMode,
  reasoningEffortForMode,
  resolveChatMode,
  runFlagsForMode,
} from "@/core/threads/run-context";

/**
 * The mode dial, pinned row by row.
 *
 * Plan mode belongs to Ultra alone: Pro is the resolved default on every
 * thinking-capable model, and binding `write_todos` there cost a tenant two
 * bookkeeping calls out of five on a one-command skill. A change that lets
 * Pro bind the todo tool again fails here before it reaches a tenant.
 */
describe("runFlagsForMode", () => {
  it("pro thinks at medium effort and binds no plan or subagents", () => {
    expect(runFlagsForMode("pro")).toEqual({
      thinking_enabled: true,
      is_plan_mode: false,
      subagent_enabled: false,
      reasoning_effort: "medium",
    });
  });

  it("ultra is the only mode with a plan, and it has subagents too", () => {
    expect(runFlagsForMode("ultra")).toEqual({
      thinking_enabled: true,
      is_plan_mode: true,
      subagent_enabled: true,
      reasoning_effort: "high",
    });
  });

  it("thinking is low effort without a plan", () => {
    expect(runFlagsForMode("thinking")).toEqual({
      thinking_enabled: true,
      is_plan_mode: false,
      subagent_enabled: false,
      reasoning_effort: "low",
    });
  });

  it("flash turns thinking off at minimal effort", () => {
    expect(runFlagsForMode("flash")).toEqual({
      thinking_enabled: false,
      is_plan_mode: false,
      subagent_enabled: false,
      reasoning_effort: "minimal",
    });
  });

  it("an explicit effort wins over the mode's level", () => {
    expect(runFlagsForMode("pro", "high").reasoning_effort).toBe("high");
    expect(runFlagsForMode("flash", "low").reasoning_effort).toBe("low");
  });

  it("no mode means thinking on, nothing else, and no invented effort", () => {
    expect(runFlagsForMode(undefined)).toEqual({
      thinking_enabled: true,
      is_plan_mode: false,
      subagent_enabled: false,
      reasoning_effort: undefined,
    });
  });
});

/**
 * The picker writes the same level the run would fall back to, so a mode
 * chosen from the menu and a mode resolved by default send the same request.
 */
describe("reasoningEffortForMode", () => {
  it("is the run's own table", () => {
    for (const mode of ["flash", "thinking", "pro", "ultra"] as const) {
      expect(reasoningEffortForMode(mode)).toBe(
        runFlagsForMode(mode).reasoning_effort,
      );
    }
  });
});

const EFFORT_MODEL = {
  supports_thinking: true,
  supports_reasoning_effort: true,
};
const THINKING_ONLY = {
  supports_thinking: true,
  supports_reasoning_effort: false,
};
const PLAIN = { supports_thinking: false, supports_reasoning_effort: false };

/**
 * Reasoning and Pro differ only by effort, so a model that ignores effort
 * would receive byte-identical requests from both rows. The tenant's model
 * is one; offering both there promised more accuracy for more time and
 * delivered neither.
 */
describe("offersReasoningMode", () => {
  it("offers Reasoning only where it differs from Pro", () => {
    expect(offersReasoningMode(EFFORT_MODEL)).toBe(true);
    expect(offersReasoningMode(THINKING_ONLY)).toBe(false);
    expect(offersReasoningMode(PLAIN)).toBe(false);
    expect(offersReasoningMode(undefined)).toBe(false);
  });

  it("treats a model with no capability answer as offering nothing", () => {
    expect(
      offersReasoningMode({
        supports_thinking: null,
        supports_reasoning_effort: null,
      }),
    ).toBe(false);
  });
});

describe("resolveChatMode", () => {
  it("defaults to pro on a thinking model and flash otherwise", () => {
    expect(resolveChatMode(undefined, EFFORT_MODEL)).toBe("pro");
    expect(resolveChatMode(undefined, THINKING_ONLY)).toBe("pro");
    expect(resolveChatMode(undefined, PLAIN)).toBe("flash");
    expect(resolveChatMode(undefined, undefined)).toBe("flash");
  });

  it("keeps a chosen mode where the model supports it", () => {
    for (const mode of ["flash", "thinking", "pro", "ultra"] as const) {
      expect(resolveChatMode(mode, EFFORT_MODEL)).toBe(mode);
    }
  });

  it("is flash on a model that cannot think, whatever was stored", () => {
    for (const mode of ["thinking", "pro", "ultra"] as const) {
      expect(resolveChatMode(mode, PLAIN)).toBe("flash");
    }
  });

  it("resolves a stored Reasoning to Pro where the two are the same request", () => {
    expect(resolveChatMode("thinking", THINKING_ONLY)).toBe("pro");
    expect(resolveChatMode("pro", THINKING_ONLY)).toBe("pro");
    expect(resolveChatMode("ultra", THINKING_ONLY)).toBe("ultra");
    expect(resolveChatMode("flash", THINKING_ONLY)).toBe("flash");
  });
});
